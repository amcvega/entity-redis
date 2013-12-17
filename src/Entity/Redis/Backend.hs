{-# LANGUAGE FlexibleContexts #-}
module Entity.Redis.Backend where


import Control.Monad.Trans (liftIO, MonadIO)

import Data.Convertible (Convertible, convert)
import Data.Either (rights)
import Data.Maybe (isJust)


import Database.Redis
-- import Tracker.Model
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BS

import Entity
import Entity.Redis.Instances

import Data.Typeable
import Data.List (intercalate)


-- * Backend

genRedisSeed :: Connection -> BS.ByteString -> IO Integer
genRedisSeed db lbl = do
    res <- runRedis db $ incr lbl
    case res of
        Left _ -> error "Couldn't Increment. WTf? Should be impossible."
        Right s -> return s


indexToKey :: Filter a -> ByteString
indexToKey (RangeFilter attr _ _) =
    BS.pack $ intercalate ":" [ storeName (fieldStore attr)
                              , "sorted"
                              , fieldOf attr
                              ]
indexToKey (Filter attr val) =
    BS.pack $ intercalate ":" [ storeName (fieldStore attr)
                              , "indices"
                              , fieldOf attr
                              , fromStore (toStore val)
                              ]


redisKey :: (Typeable a) => Key a -> BS.ByteString
redisKey key = BS.pack keyid
    where keytype = show $ typeOf key
          rectype = unwords $ tail $ words keytype
          keyid = rectype ++ ":" ++ show (unKey key)

-- redisVal :: (Redisable a) => a -> [(ByteString, ByteString)]
-- redisVal record = toRedis record



update :: (Storeable a)
          => Entity a -> Entity a -> Redis (Either String (Entity a))
update orig@(Entity _ oldstore) entity@(Entity key store) = do
    result <- multiExec $ do
        res <- hmset (redisKey key) $ toRedis entity
        mapM_ (unsetIndex orig) $ fieldList $ indexVals oldstore
        mapM_ (unsetUnique orig) $ fieldList $ uniqueVals oldstore
        mapM_ (unsetSortedIndex' orig) $ sorted oldstore
        mapM_ (saveIndex entity) $ fieldList $ indexVals store
        mapM_ (saveUnique entity) $ fieldList $  uniqueVals store
        mapM_ (saveSortedIndex' entity) $ sorted store
        return res
    case result of
        TxSuccess _ -> return $ Right entity
        TxError e -> return $ Left e
        _ -> return $ Left "Redis Aborted"


create :: (Storeable a) => a -> Redis (Either String (Key a))
create obj = do
    newId <- incr (BS.pack $ relName ++ ":id")
    case newId of
        Left _ -> return $ Left "Couldn't Incr. WTF?"
        Right x -> do
            let entity = toEntity (fromInteger x) obj
            res <- multiExec $ do
                res1 <- hmset (genIdKey entity) $ toRedis entity
                _ <- sadd (BS.pack $ relName ++ ":all") [BS.pack (show x)]
                mapM_ (saveIndex entity) $ fieldList $ indexVals obj
                mapM_ (saveUnique entity) $ fieldList $ uniqueVals obj
                mapM_ (saveSortedIndex' entity) $ sorted obj
                return res1
            case res of
                TxSuccess _ -> return $ Right (Key $ fromInteger x)
                TxError e -> return $ Left e
                _ -> return $ Left "Redis Aborted"
    where
        relName = storeName obj
        genIdKey ent = BS.pack $ relName ++ ":" ++ (show $ eKey ent)


find :: (Storeable a, RedisCtx m (Either t), MonadIO m)
        => Key a -> m (Either String (Entity a))
find kid = do
    res <- hgetall (BS.pack $ intercalate ":" [relName, show $ unKey kid])
    case res of
        Right rdata -> do
            let entityE = fromRedis $ (BS.pack "id", BS.pack $ show kid) : rdata --  kid rdata
            case entityE of
              Right entity -> return $ Right entity
              Left e -> return $ Left $ "Find Error: " ++ show e
        _ -> return $ Left "Error in getting data."
    where
        relName = storeName $ Entity kid undefined
        {-# INLINE relName#-}



unsetIndex :: (Storeable a) => Entity a -> (Field a, StoreVal) -> RedisTx ()
unsetIndex entity (Field field, sval) = do
    _ <- srem (BS.pack $ intercalate ":" [ relName, "indices"
                                         , fieldOf field
                                         , fromStore sval
                                         ])
         [BS.pack $ show key]
    return ()
    where
        key = eKey entity
        {-# INLINE key #-}
        relName = storeName entity
        {-# INLINE relName #-}


unsetUnique :: (Storeable a) => Entity a -> (Field a, StoreVal) -> RedisTx ()
unsetUnique entity (Field field, sval) = do
    _ <- hdel (BS.pack $ intercalate ":" [ relName, "uniques"
                                         , fieldOf field
                                         ])
         [BS.pack $ fromStore sval]
    return ()
    where
        relName = storeName entity




unsetSortedIndex :: (MetaStore a)
                    => Entity a -> (String, StoreVal) -> RedisTx ()
unsetSortedIndex entity (field, _) = do
    _ <- zrem (BS.pack $ intercalate ":" [ relName, "sorted"
                                         , field
                                         ])
         [BS.pack $ show key]
    return ()
    where
        key = eKey entity
        {-# INLINE key #-}
        relName = storeName entity
        {-# INLINE relName #-}


unsetSortedIndex' :: (MetaStore a, Storeable a)
                     => Entity a -> Sorted a -> RedisTx ()
unsetSortedIndex' entity (Sorted field) = do
    _ <- zrem (BS.pack $ intercalate ":" [ relName, "sorted"
                                         , fieldOf field
                                         ])
         [BS.pack $ show key]
    return ()
    where
        key = eKey entity
        {-# INLINE key #-}
        relName = storeName entity
        {-# INLINE relName #-}





saveSortedIndex :: (MetaStore a)
                   => Entity a -> (String, StoreVal) -> RedisTx ()
saveSortedIndex entity (field, sval) = do
    _ <- zadd (BS.pack $ intercalate ":" [ relName, "sorted"
                                         , field
                                         -- , fromStore sval
                                         ])
         [(fromStore sval, BS.pack $ show key)]
    return ()
    where
        key = eKey entity
        relName = storeName entity


saveSortedIndex' :: (MetaStore a, Storeable a)
                    => Entity a -> Sorted a -> RedisTx ()
saveSortedIndex' (Entity eid obj) (Sorted field) = do
    _ <- zadd (BS.pack $ intercalate ":" [ relName, "sorted"
                                         , fieldOf field
                                         ])
         [(convert (fieldAttr field obj) , BS.pack $ show key)]
    return ()
    where
        key = eid
        relName = storeName obj


saveIndex :: (Storeable a) => Entity a -> (Field a, StoreVal) -> RedisTx ()
saveIndex entity (Field field, sval) = do
    _ <- sadd (BS.pack $ intercalate ":" [ relName, "indices"
                                         , fieldOf field
                                         , fromStore sval ])
         [BS.pack $ show key]
    return ()
    where
        key = eKey entity
        relName = storeName entity

saveUnique :: (Storeable a, MetaStore a)
              => Entity a -> (Field a, StoreVal) -> RedisTx ()
saveUnique entity (Field field, sval) = do
    _ <- hset (BS.pack $ intercalate ":" [ relName, "uniques"
                                         , fieldOf field
                                         ])
         (BS.pack $ fromStore sval)
         (BS.pack $ show key)
    return ()
    where
        key = eKey entity
        relName = storeName entity

fetchRecord :: (Storeable a)
               => BS.ByteString -> Redis (Either String (Entity a))
fetchRecord x = do
    entE <- find $ Key $ read $ BS.unpack x
    case entE of
        Right r -> return $ Right r
        Left e -> return $ Left e



limitArgs :: Limit -> (Integer, Integer)
limitArgs (Limit offset count) = (offset, offset + count - 1)
limitArgs NoLimit = (0,-1)
