{-# LANGUAGE OverloadedStrings, ScopedTypeVariables #-}
module Main where


import Test.Framework --  (defaultMain, testGroup)
import Test.Framework.Providers.HUnit (testCase)
import Test.Framework.Providers.QuickCheck2 (testProperty)

import Test.QuickCheck.Instances
import Test.HUnit hiding (Test)

import Data.List
import Data.Text (Text)
import Data.Time (Day, UTCTime)
import Database.PostgreSQL.Simple
import Database.PostgreSQL.Simple.ToField

import Entity
import Entity.Postgres



main :: IO ()
main = do
    conn <- connect defaultConnectInfo
    defaultMain $ tests conn


-- tests :: [Test]
tests conn = [
    testGroup "To Field Tests" [
         testProperty "Integer" $ \x ->
            show (toField x) == show (toField $ StoreInt x)
         , testProperty "Text" $ \x ->
             show ( toField x) == show ( toField $ StoreText x)
         , testProperty "Day" $ \x ->
             show (toField x) == show (toField $ StoreDay x)
         , testProperty "Double" $ \x ->
             show ( toField x ) == show ( toField $ StoreDouble x)
         , testProperty "UTCTime" $ \x ->
             show ( toField x) == show ( toField $ StoreUTCTime x)
         ]
    , testGroup "From Field Tests" [
           buildTest $ do
              res <- query_ conn "select 'hello' :: text"
              return $ testCase "test text" $
                  res @?= [Only (StoreText "hello") ]
         , buildTest $ do
                res <- query_ conn "select '2013-01-01' :: date"
                return $ testCase "test date" $
                    res @?= [Only (StoreDay $ read "2013-01-01")]
         , buildTest $ do
                res <- query_ conn "select 5"
                return $ testCase "test int" $
                    res @?= [Only (StoreInt 5)]
         , buildTest $ do
                res <- query_ conn
                       "select timestamp with time zone '2001-09-28 01:00:00' "
                return $ testCase "test time" $
                    res @?= [Only (StoreUTCTime $ read "2001-09-27 17:00:00")]
         , buildTest $ do
                res <- query_ conn
                       "select 500.12 "
                return $ testCase "test numeric" $
                    res @?= [Only (StoreDouble 500.12)]
         , buildTest $ do
                res <- query_ conn
                       "select 500.12 :: float8 "
                return $ testCase "test float8" $
                    res @?= [Only (StoreDouble 500.12)]
         , buildTest $ do
                res <- query_ conn
                       "select 500.12 :: float4 "
                return $ testCase "test float4" $
                    res @?= [Only (StoreDouble 500.12)]
         , buildTest $ do
                res <- query_ conn
                       "select 'hello' :: varchar "
                return $ testCase "test varchar" $
                    res @?= [Only (StoreText "hello")]
         ]
    ]
