{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE TemplateHaskell       #-}

module Options
    ( Settings (..)

    , setCasGalley
    , setCasBrig
    , setStart
    , setUpdate

    , cHosts
    , cPort
    , cKeyspace

    , settingsParser

    )
where

import Control.Lens
import qualified Data.Attoparsec.ByteString.Char8 as A
import Data.ByteString.Char8 (pack)
import Data.ByteString.Conversion hiding (parser)
import Data.Id
import Data.Monoid
import Data.Text.Strict.Lens
import Data.Word
import Data.Maybe
import Galley.Options
import Options.Applicative
import Options.Applicative.Types

import qualified Cassandra as C

data MigratorSettings = MigratorSettings
    { _setCasGalley :: !CassandraSettings
    , _setCasBrig   :: !CassandraSettings
    , _setStart     :: !(Maybe (UserId, TeamId))
    , _setUpdate    :: !Bool
    } deriving Show

data CassandraSettings = CassandraSettings
    { _cHosts    :: !String
    , _cPort     :: !Word16
    , _cKeyspace :: !C.Keyspace
    } deriving Show

makeLenses ''MigratorSettings
makeLenses ''CassandraSettings

settingsParser :: Parser MigratorSettings
settingsParser = MigratorSettings
    <$> cassandraSettingsParser "galley"
    <*> cassandraSettingsParser "brig"
    <*> optional (option startOption
                 (long "start-index" <>
                  help "start at (user,team) pair"))
    <*> switch (long "perform-updates" <>
                help "Perform actual updates (not a test run)")

cassandraSettingsParser :: String -> Parser CassandraSettings
cassandraSettingsParser ks = CassandraSettings
    <$> strOption
        ( long    ("cassandra-host-" ++ ks)
       <> metavar "HOST"
       <> help    ("Cassandra Host for: " ++ ks)
       <> value   "localhost"
       <> showDefault
        )

    <*> option auto
        ( long    ("cassandra-port-" ++ ks)
       <> metavar "PORT"
       <> help    ("Cassandra Port for: " ++ ks)
       <> value   9042
       <> showDefault
        )
    <*> ( C.Keyspace . view packed <$>
          strOption
          ( long    ("cassandra-keyspace-" ++ ks)
         <> metavar "STRING"
         <> help    ("Cassandra Keyspace for: " ++ ks)
         <> value   (ks ++ "_test")
         <> showDefault
          )
        )

startOption :: ReadM (UserId, TeamId)
startOption = readerAsk >>= \s -> do
    case A.parseOnly parser (pack s) of
      Left err -> error (show err)
      Right ok -> return ok
  where
    parser :: A.Parser (UserId, TeamId)
    parser = do
        u' <- A.takeWhile (/= ',')
        _  <- A.char ','
        t' <- A.takeByteString
        let u = fromMaybe (error "Failed to conv. into user id") (fromByteString u')
        let t = fromMaybe (error "Failed to conv. into team id") (fromByteString t')
        return (u, t)
