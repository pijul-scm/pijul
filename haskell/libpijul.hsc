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




main=
  withRepository "/tmp/a" $ \repo->print "bla"
