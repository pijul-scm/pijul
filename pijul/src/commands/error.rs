extern crate libpijul;
use std::io;
use std::error;
use std::fmt;
use std::string;
#[derive(Debug)]
pub enum Error{
    NotInARepository,
    InARepository,
    IoError(io::Error),
    Repository(libpijul::error::Error),
    NotEnoughArguments,
    MoveTargetNotDirectory,
    UTF8(string::FromUtf8Error)
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::NotInARepository => write!(f, "Not in a repository"),
            Error::InARepository => write!(f, "In a repository"),
            Error::IoError(ref err) => write!(f, "IO error: {}", err),
            Error::Repository(ref err) => write!(f, "Repository error: {}", err),
            Error::NotEnoughArguments => write!(f, "Not enough arguments"),
            Error::MoveTargetNotDirectory => write!(f, "Target of mv is not a directory"),
            Error::UTF8(ref err) => write!(f, "UTF8Error: {}",err)
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::NotInARepository => "not in a repository",
            Error::InARepository => "In a repository",
            Error::IoError(ref err) => error::Error::description(err),
            Error::Repository(ref err) => libpijul::error::Error::description(err),
            Error::NotEnoughArguments => "Not enough arguments",
            Error::MoveTargetNotDirectory => "Target of mv is not a directory",
            Error::UTF8(ref err) => err.description()
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::IoError(ref err) => Some(err),
            Error::Repository(ref err) => Some(err),
            Error::NotInARepository => None,
            Error::InARepository => None,
            Error::NotEnoughArguments => None,
            Error::MoveTargetNotDirectory => None,
            Error::UTF8(ref err) => Some(err)
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
impl From<string::FromUtf8Error> for Error {
    fn from(err: string::FromUtf8Error) -> Error {
        Error::UTF8(err)
    }
}
