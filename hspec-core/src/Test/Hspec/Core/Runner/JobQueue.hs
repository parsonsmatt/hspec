{-# LANGUAGE CPP #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE ConstraintKinds #-}

module Test.Hspec.Core.Runner.JobQueue (
  MonadIO
, Job
, Concurrency(..)
, JobQueue
, withJobQueue
, enqueueJob
) where

import           Prelude ()
import           Test.Hspec.Core.Compat hiding (Monad)
import qualified Test.Hspec.Core.Compat as M

import           Control.Concurrent
import           Control.Concurrent.Async (Async, async, asyncThreadId, waitCatch, cancelMany)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map

import           Control.Monad.IO.Class (liftIO)
import qualified Control.Monad.IO.Class as M

-- for compatibility with GHC < 7.10.1
type Monad m = (Functor m, Applicative m, M.Monad m)
type MonadIO m = (Monad m, M.MonadIO m)

type Job m progress a = (progress -> m ()) -> m a

data Concurrency = Sequential | Concurrent

data JobQueue = JobQueue {
  _semaphore :: Semaphore
, _cancelQueue :: CancelQueue
}

data Semaphore = Semaphore {
  _wait :: IO ()
, _signal :: IO ()
}

type CancelQueue = IORef (Map ThreadId (Async ()))

withJobQueue :: Int -> (JobQueue -> IO a) -> IO a
withJobQueue concurrency = bracket new cancelAll
  where
    new :: IO JobQueue
    new = JobQueue <$> newSemaphore concurrency <*> newIORef Map.empty

    cancelAll :: JobQueue -> IO ()
    cancelAll (JobQueue _ cancelQueue) = readIORef cancelQueue >>= cancelMany . Map.elems

newSemaphore :: Int -> IO Semaphore
newSemaphore capacity = do
  sem <- newQSem capacity
  return $ Semaphore (waitQSem sem) (signalQSem sem)

enqueueJob :: MonadIO m => JobQueue -> Concurrency -> Job IO progress a -> IO (Job m progress (Either SomeException a))
enqueueJob (JobQueue sem cancelQueue) concurrency = case concurrency of
  Sequential -> runSequentially cancelQueue
  Concurrent -> runConcurrently sem cancelQueue

runSequentially :: forall m progress a. MonadIO m => CancelQueue -> Job IO progress a -> IO (Job m progress (Either SomeException a))
runSequentially cancelQueue action = do
  barrier <- newEmptyMVar
  let
    wait :: IO ()
    wait = takeMVar barrier

    signal :: m ()
    signal = liftIO $ putMVar barrier ()

  job <- runConcurrently (Semaphore wait pass) cancelQueue action
  return $ \ notifyPartial -> signal >> job notifyPartial

data Partial progress a = Partial progress | Done

runConcurrently :: forall m progress a. MonadIO m => Semaphore -> CancelQueue -> Job IO progress a -> IO (Job m progress (Either SomeException a))
runConcurrently (Semaphore wait signal) cancelQueue action = do
  result :: MVar (Partial progress a) <- newEmptyMVar
  ready  :: MVar ()                   <- newEmptyMVar
  let
    partialResult :: progress -> IO ()
    partialResult = replaceMVar result . Partial

    done :: IO ()
    done = replaceMVar result Done

    deregister :: ThreadId -> IO ()
    deregister tid =
      atomicModifyIORef' cancelQueue $ \ m -> (Map.delete tid m, ())

    worker :: IO a
    worker = (do
        readMVar ready
        bracket_ wait signal $
          interruptible (action partialResult) `finally` done
      ) `finally` (myThreadId >>= deregister)

  job <- mask_ $ do
    a <- async worker
    atomicModifyIORef' cancelQueue $ \ m ->
      (Map.insert (asyncThreadId a) (void a) m, ())
    putMVar ready ()
    return a

  let
    waitForResult :: (progress -> m ()) -> m (Either SomeException a)
    waitForResult notifyPartial = do
      r <- liftIO (takeMVar result)
      case r of
        Partial progress -> notifyPartial progress >> waitForResult notifyPartial
        Done -> liftIO $ waitCatch job

  return waitForResult

replaceMVar :: MVar a -> a -> IO ()
replaceMVar mvar p = tryTakeMVar mvar >> putMVar mvar p
