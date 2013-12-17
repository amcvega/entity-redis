module Entity.Redis.Query where

import Control.Monad.Trans (liftIO)

import Data.Convertible (convert)
import Data.Either (rights)
import Data.List (intercalate)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BS
import Data.Maybe (isJust)

import Database.Redis
import Entity

import Entity.Redis.Backend


data FilterType a = IntersectOnly [Filter a]
                  | IntersectAndNots [Filter a] [Filter a]
                  | GenericFilter [Filter a] [Filter a] [Filter a]
                  | NoFilters

data SortingType a = NoSorting
                   | AlphaSortBy [Sort a]
                   | RangeSortBy [Sort a]


data SortType a = RangeSort | AlphaSort | RangeAlphaSort

-- queryStore :: SimpleQuery a -> a
-- queryStore = undefined

analyzeSorting :: SimpleQuery a -> SortingType a
analyzeSorting qry =
    let sorts = qSort qry
        filters = qInt qry
    in
     if (null sorts)
     then NoSorting
     else case sortType filters sorts of
         AlphaSort -> AlphaSortBy sorts
         _ -> RangeSortBy sorts
    where
        sortType filters' sorts =
            if any (\(Sort _ field) -> isSortedField field) sorts
            then RangeSort
            else AlphaSort
        checkSorted fld filter' =
            isJust $ toSf filter' `lookup` sortedIndexVals (fieldStore fld)
        toSf (Filter x _) = fieldOf x
        isSortedField field =
            isJust $ fieldOf field `lookup` sortedIndexVals (fieldStore field)


analyzeFilters :: SimpleQuery a -> FilterType a
analyzeFilters qry
    | noArgs qry = NoFilters
    | onlyIntersect qry = IntersectOnly (qInt qry)
    | otherwise = GenericFilter (qInt qry) (qUnion qry) (qNot qry)
    where
        onlyIntersect :: SimpleQuery a -> Bool
        onlyIntersect (SimpleQuery _ _ [] [] _  _) = True
        onlyIntersect _ = False

        noArgs (SimpleQuery _ [] [] [] [] _ ) = True
        noArgs _ = False

-- countQuery :: Connection -> SimpleQuery a -> IO (Either String (QueryResult a))
-- countQuery db qry = noRangedFilterQuery db qry
sort2BS (Sort _ attr ) =
    BS.pack $ intercalate ":" [storeName (fieldStore attr)
                              , "sorted"
                              , fieldOf attr]


noSortingCollect :: (Storeable a)
                    => ByteString
                    -> [ByteString]
                    -> Redis (Either String (QueryResult a))
noSortingCollect tmp tmps= do
    idsE <- smembers tmp
    _ <- del tmps
    case idsE of
        Left e -> return $ Left $ show e
        Right ids -> do
            recs <- mapM fetchRecord ids
            return $ Right $ Entries $ rights recs

rangeSortCollect :: (Storeable a)
                    =>Limit
                    -> [Sort a]
                    -> ByteString
                    -> [ByteString]
                    -> Redis (Either String (QueryResult a))
rangeSortCollect lim sorts tmp tmps = do
    let sorts' = map sort2BS sorts
        tmpSort = BS.append tmp $ BS.pack ":sort"
    _ <- zinterstore tmpSort (tmp : sorts') Sum
    let (offset, count) = limitArgs lim
    idsE <- case head sorts of
        (Sort Entity.Asc _) -> zrange tmpSort offset count
        (Sort Entity.Desc _) -> zrevrange tmpSort offset count
    _ <- del (tmpSort:tmps)
    case idsE of
       Right ids -> do
           recs <- mapM fetchRecord ids
           return $ Right $ Entries $ rights recs
       Left e -> return $ Left $ show e


rangeCollect :: (Storeable a)
                => SimpleQuery a
                -> ByteString
                -> [ByteString]
                -> Redis (Either String (QueryResult a))
rangeCollect qry tmp tmps = do
    let inters = qInt qry
    idsE <- if noRange inters
            then zrange tmp 0 (-1)
            else let start = computeStart inters
                     end = computeEnd inters
                 in
                  zrangebyscore tmp start end
    _ <- del tmps
    case idsE of
       Right ids -> do
           recs <- mapM fetchRecord ids
           return $ Right $ Entries $ rights recs
       Left e -> return $ Left $ show e
    where
        noRange = all isFilterOnly
        isFilterOnly x = case x of
            Filter {} -> True
            RangeFilter {} -> False
        computeStart :: [Filter a] -> Double
        computeStart = foldr comp 0
            where
                comp (Filter _ _) acc  = acc
                comp (RangeFilter _ x _) acc = acc + (convert x :: Double)
        computeEnd :: [Filter a] -> Double
        computeEnd = foldr comp 0
            where
                comp (Filter _ _) acc  = acc
                comp (RangeFilter _ _ x) acc = acc + (convert x :: Double)



alphaSortCollect :: (Storeable a)
                    => SimpleQuery a
                    -> [Sort a]
                    -> ByteString
                    -> [ByteString]
                    -> Redis (Either String (QueryResult a))
alphaSortCollect qry sorts tmp tmps = do
    let tmpSort = BS.append tmp $ BS.pack ":sort"
    idsE <- sort tmp $
            SortOpts (
                Just $
                BS.pack $
                queryStore qry ++  ":*->" ++
                getFieldName (head sorts)
                )
            (limitArgs (qLimit qry))
            []
            (case head sorts of
                  (Sort Entity.Asc _) -> Database.Redis.Asc
                  (Sort Entity.Desc _)-> Database.Redis.Desc
            )
            True
    _ <- del tmps
    case idsE of
        Right ids -> do
            recs <- mapM fetchRecord ids
            return $ Right $ Entries $ rights recs
        Left e -> return $ Left $ show e


getFieldName (Sort _ attr) = fieldOf attr

filteredQuery :: (Storeable a)
                 => Connection
                 -> SimpleQuery a
                 -> IO (Either String (QueryResult a))
filteredQuery db qry =
    if anyRangedFilters qry
    then rangedFilterQuery db qry
    else noRangedFilterQuery db qry


anyRangedFilters :: SimpleQuery a -> Bool
anyRangedFilters (SimpleQuery _ is _ _ _ _) = any isRange is
    where isRange RangeFilter{} = True
          isRange _ = False

rangedFilterQuery :: (Storeable a)
                     => Connection
                     -> SimpleQuery a
                     -> IO (Either String (QueryResult a))
rangedFilterQuery db qry  = do
    seed <- genRedisSeed db $ BS.pack "System:querycount"
    let inters' = map indexToKey inters
        unions' = map indexToKey unions
        inters = qInt qry
        unions = qUnion qry
        nots' = map indexToKey (qNot qry)
        tmp = BS.pack $ "System:Meta:queryop:" ++ show seed
        tmpInter = BS.append tmp $ BS.pack ":inter"
        tmpUnion = BS.append tmp $ BS.pack ":union"
        tmpMerge = BS.append tmp $ BS.pack ":merge"
        tmpNot = BS.append tmp $ BS.pack ":not"
        sorter = analyzeSorting qry
    runRedis db $ do
        _ <- zinterstore tmpInter inters' Sum
        _ <- zunionstore tmpUnion unions' Sum
        _ <- zunionstore tmpNot nots' Sum
        zinterstore tmpMerge
            (if not . null $ unions' then [tmpInter, tmpUnion] else [tmpInter])
            Sum
        if not . null $ nots'
           then do _ <- sdiffstore tmp [tmpMerge, tmpNot]; return ()
            else do _ <- rename tmpMerge tmp; return ()
        let tmps = [tmp, tmpInter, tmpUnion, tmpNot, tmpMerge]
        case qResult qry of
            ResultCount -> do
                c' <- zcard tmp
                _ <- del tmps
                case c' of
                    Left e -> return $ Left $ show e
                    Right c -> return $ Right $ Count $ fromInteger c
            ResultAll -> rangeCollect qry tmp tmps


noRangedFilterQuery :: (Storeable a)
                     => Connection
                     -> SimpleQuery a
                     -> IO (Either String (QueryResult a))
noRangedFilterQuery db qry = do
    seed <- genRedisSeed db $ BS.pack "System:querycount"
    let inters' = map indexToKey inters
        unions' = map indexToKey unions
        inters = qInt qry
        unions = qUnion qry
        nots' = map indexToKey (qNot qry)
        tmp = BS.pack $ "System:Meta:queryop:" ++ show seed
        tmpInter = BS.append tmp $ BS.pack ":inter"
        tmpUnion = BS.append tmp $ BS.pack ":union"
        tmpMerge = BS.append tmp $ BS.pack ":merge"
        tmpNot = BS.append tmp $ BS.pack ":not"
        sorter = analyzeSorting qry
    runRedis db $ do
        _ <- sinterstore tmpInter inters'
        _ <- sunionstore tmpUnion unions'
        _ <- sunionstore tmpNot nots'
        sinterstore tmpMerge
            (if not . null $ unions' then [tmpInter, tmpUnion] else [tmpInter])
        if not . null $ nots'
           then do _ <- sdiffstore tmp [tmpMerge, tmpNot]; return ()
            else do _ <- rename tmpMerge tmp; return ()
        let tmps = [tmp, tmpInter, tmpUnion, tmpNot, tmpMerge]
        case qResult qry of
            ResultCount -> do
                c' <- scard tmp
                _ <- del tmps
                case c' of
                    Left e -> return $ Left $ show e
                    Right c -> return $ Right $ Count $ fromInteger c
            ResultAll -> do
                case sorter of
                    NoSorting -> noSortingCollect tmp tmps
                    RangeSortBy sorts ->
                        rangeSortCollect (qLimit qry) sorts tmp tmps
                    AlphaSortBy sorts ->
                        alphaSortCollect qry sorts tmp tmps



findAll :: (MetaStore a, Storeable a)
           => Connection -> SimpleQuery a -> IO (Either String [Entity a])
findAll db qry = do
    let key = BS.pack $  intercalate ":" [queryStore qry, "all"]
    runRedis db $ do
        idsE <- smembers key
        case idsE of
            Right ids -> do
                recs <- mapM fetchRecord ids
                return $ Right $ rights recs
            Left e -> return $ Left $ show e
