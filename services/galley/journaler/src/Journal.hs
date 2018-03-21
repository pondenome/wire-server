{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE ScopedTypeVariables        #-}

module Journal where

import Cassandra as C hiding (pageSize)
import Control.Concurrent.Async.Lifted.Safe (mapConcurrently_)
import Control.Lens
import Control.Monad.Except
import Data.Id
import Data.ByteString.Conversion
import Data.Monoid ((<>))
import Data.Text (Text)
import Galley.Data ()
import Galley.Types.Teams
import Galley.Types.Teams.Intra
import System.Logger (Logger)

import qualified System.Logger        as Log

runCommand :: Logger -> ClientState -> ClientState -> Bool -> Maybe (UserId, TeamId) -> IO ()
runCommand l galley brig performUpdates start = void $ C.runClient galley $ do
    page <- case start of
        Just (u,_t) -> retry x5 $ paginate teamUserSelectFrom (paramsP Quorum (Identity u) pageSize)
        Nothing     -> retry x5 $ paginate teamUserSelect (paramsP Quorum () pageSize)
    scan 0 page
  where
    pageSize = 16

    scan :: Int -> Page TeamUser -> C.Client ()
    scan acc page = do
        let res   = result page
        let count = acc + Prelude.length res
        -- Just log a pointer for ease of repetition in case of failures
        case res of
            []        -> return ()
            ((u,t):_) -> Log.info l . Log.msg $ Log.val ("Current pointer at user: " <> toByteString' u <> " team: " <> toByteString' t)
        -- Process `pageSize` users in parallel
        mapConcurrently_ processUser res
        Log.info l . Log.msg $ Log.val ("Processed " <> toByteString' count <> " team members so far")
        when (hasMore page) $
            retry x5 (liftClient (nextPage page)) >>= scan count

    processUser :: TeamUser -> C.Client ()
    processUser (u, t) = do
        team <- retry x5 $ query1 teamSelect $ params Quorum (Identity t)
        -- Is this user part of a binding team?
        case team of
            Just (_tid, binding, deleted, name, _status)
                | binding == Just Binding &&   -- We look only at binding teams
                  deleted == False &&          -- Don't update deleted users
                  name `notElem` teamsToIgnore -- We can avoid updating most users by checking the team name
                                               -> handleBindingTeamUser (u, t)
                | otherwise                    -> Log.info l . Log.msg $ Log.val ("Ignoring user: " <> toByteString' u)
            Nothing -> Log.warn l . Log.msg $ Log.val ("No such team: " <> toByteString' t)

    handleBindingTeamUser :: TeamUser -> C.Client ()
    handleBindingTeamUser (u, t) = void $ C.runClient brig $ do
        -- Check the existing user
        user <- retry x5 $ query1 userSelect $ params Quorum (Identity u)
        -- Unless the look actually returns a user (it always should!), don't
        -- update as that would cause a "broken" insert
        case user of
            Just (_, Just _,  _) -> Log.info l . Log.msg $ Log.val ("User: " <> toByteString' u <> " already has team: " <> toByteString' t)
            -- ^ These users already have a team, don't update again
            Just (_, Nothing, _) | performUpdates -> do
                                     retry x5 $ write userUpdate $ params Quorum (t, u)
                                     Log.info l . Log.msg $ Log.val ("Updating user: " <> toByteString' u <> " with team: " <> toByteString' t)
                                 | otherwise      ->
                                     Log.info l . Log.msg $ Log.val ("Should update user: " <> toByteString' u <> " with team: " <> toByteString' t)
            -- ^ There is a user who is part of a binding team, but no team is set in the user profile
            Nothing  -> Log.warn l . Log.msg $ Log.val ("User: " <> toByteString' u <> " does not exist??")

    -- These teams were/are spam related, we should permanently deleted them
    teamsToIgnore :: [Text]
    teamsToIgnore = [ "dsfewwfww" 
                    , "ВАМ 57$, ЖМИТЕ НА ССЫЛКУ » http://dengibest.ru#-ВЫВЕСТИ-БОНУС"
                    ]

-- CQL queries
teamUserSelect :: PrepQuery R () TeamUser
teamUserSelect = "SELECT user, team FROM user_team"

teamUserSelectFrom :: PrepQuery R (Identity UserId) TeamUser
teamUserSelectFrom = "SELECT user, team FROM user_team WHERE token(user) > token(?)"

teamSelect :: PrepQuery R (Identity TeamId) TeamDB
teamSelect = "SELECT team, binding, deleted, name, status FROM team WHERE team = ?"

userSelect :: PrepQuery R (Identity UserId) UserDB
userSelect = "SELECT id, team, activated FROM user WHERE id = ?"

userUpdate :: PrepQuery W (TeamId, UserId) ()
userUpdate = "UPDATE user SET team = ? WHERE id = ?"

-- Utils

type UserDB   = (UserId, Maybe TeamId, Bool)
type TeamDB   = (TeamId, Maybe TeamBinding, Bool, Text, Maybe TeamStatus)
type TeamUser = (UserId, TeamId)
type UserRow  = (UserId, Bool)
