{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Cassandra as C
import Cassandra.Settings as C
import Control.Lens hiding ((.=))
import Data.Monoid
import Journal
import Network.HTTP.Client.OpenSSL
import Options as O
import Options.Applicative

import qualified System.Logger as Log


main :: IO ()
main = withOpenSSL $ do
    s <- execParser (info (helper <*> settingsParser) desc)
    lgr <- initLogger
    -- Setup for galley cassandra
    gc <- initCas (s^.setCasGalley) lgr
    -- Setup for brig cassandra
    bc <- initCas (s^.setCasBrig) lgr
    runCommand lgr gc bc (s^.setUpdate) (s^.setStart)
  where
    desc = header   "user-migrator"
        <> progDesc "User script"
        <> fullDesc

    initLogger
        = Log.new
        . Log.setOutput Log.StdOut
        . Log.setFormat Nothing
        . Log.setBufSize 0
        $ Log.defSettings

    initCas cas l
        = C.init l
        . C.setContacts        (cas^.cHosts) []
        . C.setPortNumber      (fromIntegral $ cas^.cPort)
        . C.setKeyspace        (cas^.cKeyspace)
        . C.setProtocolVersion C.V3
        $ C.defSettings
