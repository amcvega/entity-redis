{-# LANGUAGE FlexibleContexts #-}
module Entity.Redis.Interface where

import Control.Monad (liftM)
import Control.Monad.Trans (liftIO, MonadIO)

import Data.Convertible (Convertible, convert)
import Data.Either (rights)
import Data.Maybe (isJust)


import Database.Redis
-- import Tracker.Model
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BS

import Entity
import Entity.Redis.Backend
import Entity.Redis.Instances
import Entity.Redis.Query

import Data.Typeable

import Data.List (intercalate)




-- | Refactored IO Methods
filterQuery :: (Storeable a)
               => Connection
               -> SimpleQuery a
               -> IO (Either String (QueryResult a))
filterQuery db qry = do
    let ftype = analyzeFilters qry
        stype = analyzeSorting qry
    case qResult qry of
        ResultAll -> getEntries ftype stype
        ResultCount -> noRangedFilterQuery db qry
    where
        getEntries ftype stype = case (ftype, stype) of
            (NoFilters, _ ) -> do
                r <- findAll db qry
                case r of
                    Left e -> return $ Left e
                    Right rs -> return $ Right $ Entries rs
            (IntersectOnly fs, RangeSortBy (s:_)) -> do
                r <- findByFilterSort db fs s (qLimit qry)
                case r of
                    Left e -> return $ Left e
                    Right rs -> return $ Right $ Entries rs
            (IntersectOnly fs, AlphaSortBy (s:_)) -> do
                r <- findByFilterSort db fs s (qLimit qry)
                case r of
                    Left e -> return $ Left e
                    Right rs -> return $ Right $ Entries rs

            _ -> filteredQuery db qry -- noRangedFilterQuery db qry


findRecordsByIndex :: (Storeable a, Convertible val StoreVal)
                     => Connection
                     -> StoreField a val -> val -> IO (Either String [Entity a])
findRecordsByIndex db attr val = do
    let field = fieldOf attr
        key = intercalate ":" [ storeName (fieldStore attr)
                              , "indices", field, fromStore (toStore val)
                              ]
    runRedis db $ do
        idsE <- smembers $ BS.pack key
        case idsE of
            Right ids -> do
                recs <- mapM fetchRecord ids
                return $ Right $ rights recs
            Left e -> return $ Left $ show e

findRecordsInterUnion :: (Storeable a)
                         => Connection
                         -> [Filter a]
                         -> [Filter a]
                         -> IO (Either String [Entity a])
findRecordsInterUnion db inters unions = do
    seed <- genRedisSeed db $ BS.pack "System:interunioncount"
    let inters' = map indexToKey inters
        unions' = map indexToKey unions
        tmp = BS.pack $ "System:Meta:interunion:" ++ show seed
        tmpInter = BS.append tmp $ BS.pack ":inter"
        tmpUnion = BS.append tmp $ BS.pack ":union"
    runRedis db $ do
        -- | Unhappy with this.
        --   TODO: Detect if no range queries are needed and use set operations
        --         instead.
        zinterstore tmpInter inters' Sum
        zunionstore tmpUnion unions' Sum
        if not . null $ unions
            then zinterstore tmp [tmpInter, tmpUnion] Sum
            else zinterstore tmp [tmpInter] Sum

        idsE <- if noRange inters
                then zrange tmp 0 (-1)
                else let start = computeStart inters
                         end = computeEnd inters
                     in
                      zrangebyscore tmp start end
        case idsE of
            Left e -> return $ Left $ show e
            Right ids -> do
                recs <- mapM fetchRecord ids
                _ <- del [tmp, tmpInter, tmpUnion]
                return $ Right $ rights recs
    where
        noRange xs = all isFilterOnly xs
        isFilterOnly x = case x of
            Filter _ _ -> True
            RangeFilter _ _ _ -> False
        computeStart :: [Filter a] -> Double
        computeStart xs = foldr comp 0 xs
            where
                comp (Filter _ _) acc  = acc
                comp (RangeFilter _ x _) acc = acc + (convert x :: Double)
        computeEnd :: [Filter a] -> Double
        computeEnd xs = foldr comp 0 xs
            where
                comp (Filter _ _) acc  = acc
                comp (RangeFilter _ _ x) acc = acc + (convert x :: Double)




findByFilterSort :: (Storeable a)
                    => Connection
                    -> [Filter a]
                    -> Sort a
                    -> Limit
                    -> IO (Either String [Entity a])
findByFilterSort db filters sorter limit =
    case sortType filters sorter of
        RangeSort -> findByFilterSort' db filters sorter limit
        x -> findByAlphaSort x db filters sorter limit

    where
        sortType filters' (Sort _ field) =
            if isSortedField field
            then RangeSort
            else if any (checkSorted field) filters'
                 then RangeAlphaSort
                 else AlphaSort
        checkSorted fld filter' =
            isJust $ (toSf filter') `lookup` sortedIndexVals (fieldStore fld)
        toSf (Filter x _) = fieldOf x


isSortedField :: Storeable a => StoreField a b -> Bool
isSortedField field =
    isJust $ fieldOf field `lookup` sortedIndexVals (fieldStore field)


findByAlphaSort :: (Storeable a)
                    => SortType a
                    -> Connection
                    -> [Filter a]
                    -> Sort a
                    -> Limit
                    -> IO (Either String [Entity a])
findByAlphaSort stype db filters (Sort dir attr) limit = do
    res <- runRedis db $ incr $ BS.pack "System:alphacount"
    seed <- case res of
        Left _ -> error "Couldn't increment database. Should be impossible"
        Right seed' -> return seed'
    let filters' = map pair2BS filters
        tmp = BS.pack $ "System:Meta:AlphaSort:" ++ show seed
        keys' = filters'
    runRedis db $ do
        _ <- case stype of
            AlphaSort -> sinterstore tmp keys'
            _ -> zinterstore tmp keys' Max

        idsE <- sort tmp $
                SortOpts (Just . BS.pack $
                          storeName (fieldStore attr) ++":*->"++ fieldOf attr)
                (limitArgs limit)
                []
                (case dir of
                      Entity.Asc -> Database.Redis.Asc
                      Entity.Desc -> Database.Redis.Desc
                )
                True
        _ <- del [tmp]
        case idsE of
            Right ids -> liftM (Right . rights) (mapM fetchRecord ids)
            Left e -> return $ Left $ show e
    where
        sort2BS (Sort _ attr ) =
            BS.pack $ intercalate ":" [storeName (fieldStore attr)
                                      , "sorted"
                                      , fieldOf attr]
        pair2BS (Filter attr val) =
            BS.pack $ intercalate ":" [storeName (fieldStore attr)
                            , "indices"
                            , fieldOf attr
                            , fromStore (toStore val)]


findByFilterSort' :: (Storeable a)
                    => Connection
                    -> [Filter a]
                    -> Sort a
                    -> Limit
                    -> IO (Either String [Entity a])

findByFilterSort' db filters sorter@(Sort dir _) limit = do
    res <- runRedis db $ incr $ BS.pack "System:filtercount"
    seed <- case res of
        Left _ -> error "Couldn't increment database. Should be impossible"
        Right seed' -> return seed'
    let filters' = map pair2BS filters
        sorters' = [sort2BS sorter]
        tmp = BS.pack $ "System:Meta:ZINTERSTORE:" ++ show seed
        keys' = filters' ++ sorters'
    runRedis db $ do
        _ <- zinterstore tmp keys' Max
        let (offset, count) = limitArgs limit
        idsE <- case dir of
            Entity.Asc -> zrange tmp offset count
            Entity.Desc -> zrevrange tmp offset count
        _ <- del [tmp]
        case idsE of
            Right ids -> liftM (Right . rights) (mapM fetchRecord ids)
            Left e -> return $ Left $ show e
    where
        sort2BS (Sort _ attr) =
            BS.pack $ intercalate ":" [storeName (fieldStore attr)
                                      , "sorted"
                                      , fieldOf attr]
        pair2BS (Filter attr val) =
            BS.pack $ intercalate ":" [storeName (fieldStore attr)
                            , "indices"
                            , fieldOf attr
                            , fromStore (toStore val)]

findRecordsMulti :: (Storeable a)
                    => Connection
                    -> [Filter a]
                    -> IO (Either String [Entity a])
findRecordsMulti db filters = do
    let keys' = map pair2BS filters
    runRedis db $ do
        idsE <- sinter keys'
        case idsE of
            Right ids -> liftM (Right . rights) (mapM fetchRecord ids)
            Left e -> return . Left . show $ e
    where
        pair2BS (Filter attr val) =
            BS.pack $ intercalate ":" [storeName (fieldStore attr)
                            , "indices"
                            , fieldOf attr
                            , fromStore (toStore val)]



findRecordsByIndices :: (Storeable a, Convertible val StoreVal)
                        => Connection
                        -> [(StoreField a val, val)]
                        -> IO (Either String [Entity a])
findRecordsByIndices db filters = do
    let keys' = filtersToBSs filters
    runRedis db $ do
        idsE <- sinter keys'
        case idsE of
            Right ids -> do
                recs <- mapM fetchRecord ids
                return $ Right $ rights recs
            Left e -> return $ Left $ show e


filtersToBSs :: (Storeable a, Convertible val StoreVal)
                => [(StoreField a val, val)] -> [ByteString]
filtersToBSs = map (BS.pack . tobs)
    where
        tobs (attr, val) = intercalate ":" [storeName (fieldStore attr)
                                            , "indices"
                                            , fieldOf attr
                                            , fromStore (toStore val)]
        {-# INLINE tobs #-}



createRecord :: (Storeable a) => Connection -> a -> IO (Either String (Key a))
createRecord db record = runRedis db $ do
    resE <- create record
    case resE of
        Right key -> return $ Right key
        Left e    -> return $ Left e


updateRecord :: (Storeable a)
                => Connection
                -> Entity a -> Entity a -> IO (Either String (Entity a))
updateRecord db orig entity = runRedis db $ do
    result <- update orig entity
    case result of
        Right ent -> return $ Right ent
        Left e -> return $ Left e


findRecord :: (Storeable a)
              => Connection -> Key a -> IO (Either String (Entity a))
findRecord db key = runRedis db $ do
    resE <- find key
    case resE of
        Right r -> return $ Right r
        Left e -> return $ Left e

deleteRecord :: (Storeable a) => Connection -> Entity a -> IO (Either String ())
deleteRecord db entity@(Entity eid entry)  = runRedis db $ do
    result <- multiExec $ do
        res <- del [redisKey eid]
        mapM_ (unsetIndex entity) $ fieldList $ indexVals entry
        mapM_ (unsetUnique entity) $ fieldList $ uniqueVals entry
        mapM_ (unsetSortedIndex' entity) $ sorted entry
        _ <- srem (BS.pack $ intercalate ":" [storeName entry, "all"])
                  [BS.pack $ show eid]
        return res
    case result of
        TxSuccess _ -> return $ Right ()
        TxError e -> return $ Left e
        _ -> return $ Left "Redis Aborted"




findRecordByUnique :: (Storeable a, Convertible val StoreVal)
                     => Connection
                     -> StoreField a val -> val -> IO (Either String (Entity a))
findRecordByUnique db attr val = do
    let field = fieldOf attr
        key = intercalate ":" [storeName (fieldStore attr), "uniques", field]
    runRedis db $ do
        idsE <- hget (BS.pack key) (fromStore $ toStore val)
        case idsE of
            Right Nothing -> return $ Left "Not Found"
            Right (Just x) -> do
                entE <- find $ Key $ read $ BS.unpack x
                case entE of
                    Right r -> return $ Right r
                    Left e -> return $ Left e
            Left e -> return $ Left $ show e
