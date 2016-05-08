-- module Pijul( withRepository ) where

import Foreign.C.Types
import Foreign.C.String
import Foreign.Ptr
import Control.Exception
import Foreign.Marshal.Alloc
import Foreign.Storable
import Data.Typeable
import qualified Data.ByteString as B
import Data.IORef
import Foreign.ForeignPtr

data CRepository

foreign import ccall pijul_open_repository :: CString -> Ptr (Ptr CRepository) -> IO CInt
foreign import ccall pijul_close_repository :: Ptr CRepository -> IO ()

foreign import ccall "&pijul_close_repository" p_pijul_close_repository:: FunPtr (Ptr CRepository -> IO ())



type Repository = ForeignPtr CRepository

data Exn = Load | MutTxn | Commit | DoubleCommit | Apply deriving (Show, Typeable)

instance Exception Exn


openRepository::String -> IO Repository
openRepository path =
  withCString path $ \cpath ->
  alloca $ \p->do {
  op<-pijul_open_repository cpath p;
  if op ==0 then do {
    pp<-peek p;
    newForeignPtr p_pijul_close_repository pp
    } else throw Load
  }



data CTransaction

foreign import ccall pijul_mut_txn_begin :: Ptr CRepository -> Ptr (Ptr CTransaction) -> IO CInt
foreign import ccall pijul_mut_txn_commit :: Ptr CTransaction -> IO CInt
foreign import ccall "&pijul_mut_txn_destroy" p_pijul_mut_txn_destroy :: FunPtr(Ptr CTransaction -> IO ())

data Transaction = Transaction { repository :: Repository, transaction :: ForeignPtr CTransaction }

mutTxnBegin::Repository -> IO Transaction
mutTxnBegin repo = do
  withForeignPtr repo $ \crepo -> do {
    alloca $ \p_txn -> do {
        e <- pijul_mut_txn_begin crepo p_txn;
        if e == 0 then do {
          c_txn<-peek p_txn;
          txn <- newForeignPtr p_pijul_mut_txn_destroy c_txn;
          return $ Transaction { repository = repo, transaction = txn }
          }
        else throw MutTxn
        }
    }

commit::Transaction -> IO ()
commit txn =
  withForeignPtr (transaction txn) $ \p_txn -> do {
      r<-pijul_mut_txn_commit(p_txn);
      if r == 0 then return () else throw Commit
      }

data CPatch
type Patch = ForeignPtr CPatch
foreign import ccall pijul_empty_patch :: IO (Ptr CPatch)
foreign import ccall "&pijul_destroy_patch" p_pijul_destroy_patch :: FunPtr(Ptr CPatch -> IO ())

foreign import ccall pijul_apply_local_patch :: Ptr CTransaction -> CString -> CString -> Ptr CPatch -> Ptr CHashSet -> IO CInt

emptyPatch::IO Patch
emptyPatch = do
  c_patch <- pijul_empty_patch
  newForeignPtr p_pijul_destroy_patch c_patch

data CHashSet
type HashSet = ForeignPtr CHashSet

applyLocalPatch::Transaction -> String -> String -> Patch -> IO ()
applyLocalPatch txn branch path patch=
  withForeignPtr (transaction txn) $ \txn ->
  withCString branch $ \branch ->
  withCString path $ \path ->
  withForeignPtr patch $ \patch -> do
  result <- pijul_apply_local_patch txn branch path patch nullPtr
  if result == 0 then return () else throw Apply


{-
foreign import ccall pijul_add_file :: Ptr CRepository -> CString->CInt-> IO ()

addFile::Repository->String->Bool->IO ()
addFile rep path isDir=
    withCString path $ \cpath->pijul_add_file rep cpath (if isDir then 1 else 0)
-}

main = do
  repo<-openRepository "/tmp/test"
  patch <- emptyPatch
  txn <- mutTxnBegin repo
  applyLocalPatch txn "main" "/tmp/test" patch
  commit txn



{--data HashSet
data Iter
foreign import ccall pijul_load_patches :: CString -> CString-> Ptr (Ptr HashSet) ->Ptr (Ptr Iter) -> IO ()
foreign import ccall pijul_unload_patches :: Ptr HashSet -> Ptr Iter -> IO ()
foreign import ccall pijul_next_patch :: Ptr Iter -> Ptr CString -> Ptr CInt-> IO CInt
-}


{-

-- path is the path of the changes file. Changes files are in .pijul/changes.hex(branch).
-- for instance, for the default branch "main", .pijul/changes.6d61696e
loadPatchList::String->String->IO [B.ByteString]
loadPatchList path branch=
    withCString path $ \path->
        withCString branch $ \branch ->
            bracket
            (alloca $ \h->alloca $ \i->do {pijul_load_patches path branch h i;
                  hh<-peek h; ii<-peek i; return (hh,ii) })
            (\(h,i)->do {pijul_unload_patches h i})
            (\(_,i)->
                 let get patches=alloca $ \str -> alloca $ \len -> do {
                                   r<-pijul_next_patch i str len;
                                   if r==0 then do {
                                                  str_<-peek str;
                                                  len_<-peek len;
                                                  bs<-B.packCStringLen (str_,fromIntegral len_);
                                                  get (bs:patches)
                                                } else return patches
                                 } in get [])




foreign import ccall pijul_create_repository :: CString -> IO ()

createRepository :: String -> IO ()
createRepository path =
  withCString path $ \cpath -> pijul_create_repository cpath
-}
