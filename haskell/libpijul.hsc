import Foreign.C.Types
import Foreign.C.String
import Foreign.Ptr
import Control.Exception
import Foreign.Marshal.Alloc
import Foreign.Storable

data CRepository

foreign import ccall pijul_open_repository :: CString -> Ptr (Ptr CRepository) -> IO CInt
foreign import ccall pijul_close_repository :: Ptr CRepository -> IO ()

type Repository = Ptr CRepository

withRepository::String -> (Repository -> IO a) -> IO a
withRepository path f =
    withCString path $ \cpath->
        bracket
        (alloca $ \p->do {pijul_open_repository cpath p; peek p })
        (\p -> do { pijul_close_repository p })
        f

foreign import ccall pijul_add_file :: Ptr CRepository -> CString->CInt-> IO ()

addFile::Repository->String->Bool->IO ()
addFile rep path isDir=
    withCString path $ \cpath->pijul_add_file rep cpath (if isDir then 1 else 0)



main=
  withRepository "/tmp/a" $ \repo->
      addFile repo "file" False
