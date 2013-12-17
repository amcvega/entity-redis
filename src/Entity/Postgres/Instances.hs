module Entity.Postgres.Instances where

import Control.Applicative

import Data.Monoid (mappend)
import qualified Data.Text as T
import Data.Convertible
import Data.Time (timeZoneMinutes)
import Data.Char (toLower)
import Entity as E


import Database.PostgreSQL.Simple
import Database.PostgreSQL.Simple.FromField
import Database.PostgreSQL.Simple.FromRow
import Database.PostgreSQL.Simple.ToField
import Database.PostgreSQL.Simple.ToRow
import Database.PostgreSQL.Simple.TypeInfo.Static


instance FromField StoreVal where
    fromField = storeParser

storeParser :: FieldParser StoreVal
storeParser _ Nothing  = pure StoreNil
storeParser f mbs
    | t `elem` map typoid [text, varchar] = StoreText <$> fromField f mbs
    | t == typoid date = StoreDay <$> fromField f mbs
    | t `elem` map typoid [int8, int4, int2] =
        StoreInt <$> fromField f mbs
    | t `elem` map typoid [timestamptz] =
        StoreUTCTime <$> fromField f mbs
    | t == typoid timestamp = StoreLocalTime <$> fromField f mbs
    | t `elem` map typoid [float8, float4] =
        StoreDouble <$> fromField f mbs
    | t == typoid numeric = StoreDouble <$> fromRational <$> fromField f mbs
    | otherwise =  returnError ConversionFailed f $  "invalid oid: " ++ (show t)
    where
        t = typeOid f



instance ToField StoreVal where
    toField x = case x of
        StoreInt i -> toField i
        StoreText t -> toField t
        StoreDay d -> toField d
        StoreDouble d -> toField d
        StoreUTCTime t -> toField t


instance ToField (Filter a) where
    toField (Filter _ val) = toField . toStore $ val


instance (Storeable a) => ToRow (Entity a) where
    toRow = map (\(_,v) -> toField v) . fieldList . toList . eVal


instance (Storeable a) => FromRow (Entity a) where
    fromRow = sqlToEntity `fmap` valsToList `fmap` fromRow
        where
            valsToList :: (Storeable a) => [StoreVal] -> FieldList a
            valsToList (x:xs) =
                FieldList $ (KeyField, x) : zip (storeFields undefined) xs

            sqlToEntity :: (Storeable a)
                           => FieldList a -> (Entity a)
            sqlToEntity (FieldList []) = error "Empty Result."
            sqlToEntity (FieldList (x:xs)) =
                let res = Entity (fromStore $ snd x) `fmap`
                          (listToStoreable $
                           map (\(Field k,v) -> (fieldOf k, v)) xs)
                in
                 case res of
                     Left _ ->
                         error "Something went wrong. Could not assemble Entity"
                     Right r -> r
