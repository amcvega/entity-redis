{-# LANGUAGE OverloadedStrings #-}
module Entity.Redis.Instances where

import Control.Applicative

import Data.Monoid (mappend)
import qualified Data.Text as T
import Data.Convertible
import Data.Time (timeZoneMinutes)
import Data.Char (toLower)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B
import Entity as E

class ToRedis a where
    toRedis :: a -> [(ByteString, ByteString)]


instance (Storeable a) => ToRedis (Entity a) where
    toRedis x = getVals
        where
            getVals = map bsPairs $ fieldList $ toList $ eVal x
            bsPairs (Field k,v) = (B.pack $ fieldOf k, fromStore v)


class  FromRedis a where
    fromRedis :: [(ByteString, ByteString)] -> Either String a


instance (Storeable a) => FromRedis (Entity a) where
    fromRedis (("id", kid):vals)  = Entity (read $ B.unpack kid) `fmap`
                                        (listToStoreable $ map unbsPairs vals)
        where unbsPairs (k,v) = (B.unpack k, toStore v)
    fromRedis _ = Left "Could not Parse at all"
