import Foreign.C.Types
import Foreign.C.String
import Foreign.Ptr
import Control.Exception
import Foreign.Marshal.Alloc
import Foreign.Storable
import Data.Typeable
import qualified Data.ByteString as B

data CRepository

foreign import ccall pijul_open_repository :: CString -> Ptr (Ptr CRepository) -> IO CInt
foreign import ccall pijul_close_repository :: Ptr CRepository -> IO ()

type Repository = Ptr CRepository

data Exn = Load deriving (Show, Typeable)

instance Exception Exn


withRepository::String -> (Repository -> IO a) -> IO a
withRepository path f =
    withCString path $ \cpath->
        bracket
        (alloca $ \p->do { op<-pijul_open_repository cpath p;
                           if op ==0 then peek p
                           else throw Load })
        (\p -> do { pijul_close_repository p })
        f

foreign import ccall pijul_add_file :: Ptr CRepository -> CString->CInt-> IO ()

addFile::Repository->String->Bool->IO ()
addFile rep path isDir=
    withCString path $ \cpath->pijul_add_file rep cpath (if isDir then 1 else 0)

data HashSet
data Iter
foreign import ccall pijul_load_patches :: CString -> CString-> Ptr (Ptr HashSet) ->Ptr (Ptr Iter) -> IO ()
foreign import ccall pijul_unload_patches :: Ptr HashSet -> Ptr Iter -> IO ()
foreign import ccall pijul_next_patch :: Ptr Iter -> Ptr CString -> Ptr CInt-> IO CInt

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


main=do
  loadPatchList "/tmp/a" "main" >>= print
  withRepository "/tmp/a" $ \repo->do
      addFile repo "file" False
