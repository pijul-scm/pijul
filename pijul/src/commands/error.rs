extern crate libpijul;
use std::io;
use std::error;
use std::fmt;
use std::path::PathBuf;

#[derive(Debug)]
pub enum Error{
    NotInARepository,
    InARepository,
    PathNotFound(PathBuf),
    IoError(io::Error),
    Repository(libpijul::error::Error)
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::NotInARepository => write!(f, "Not in a repository"),
            Error::InARepository => write!(f, "In a repository"),
            Error::PathNotFound(ref p) => write!(f, "Path not found: {}", p.to_string_lossy()),
            Error::IoError(ref err) => write!(f, "IO error: {}", err),
            Error::Repository(ref err) => write!(f, "Repository error: {}", err)
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::NotInARepository => "not in a repository",
            Error::InARepository => "In a repository",
            Error::PathNotFound(_) => "path not found",
            Error::IoError(ref err) => error::Error::description(err),
            Error::Repository(ref err) => libpijul::error::Error::description(err)
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::IoError(ref err) => Some(err),
            Error::Repository(ref err) => Some(err),
            Error::PathNotFound(_) => None,
            Error::NotInARepository => None,
            Error::InARepository => None
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IoError(err)
    }
}
impl From<libpijul::error::Error> for Error {
    fn from(err: libpijul::error::Error) -> Error {
        Error::Repository(err)
    }
}
