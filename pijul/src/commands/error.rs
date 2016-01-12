extern crate libpijul;
use std::io;
use std::error;
use std::fmt;
use std::string;
extern crate ssh;
extern crate rustc_serialize;
extern crate hyper;
#[derive(Debug)]
pub enum Error{
    NotInARepository,
    InARepository,
    IoError(io::Error),
    Repository(libpijul::error::Error),
    NotEnoughArguments,
    MoveTargetNotDirectory,
    UTF8(string::FromUtf8Error),
    Hex(rustc_serialize::hex::FromHexError),
    SSH(ssh::Error),
    Hyper(hyper::error::Error)
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
            Error::SSH(ref err) => write!(f, "SSH: {}",err),
            Error::Hex(ref err) => write!(f, "Hex: {}",err),
            Error::Hyper(ref err) => write!(f, "Hyper: {}",err),
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
            Error::SSH(ref err) => err.description(),
            Error::Hex(ref err) => err.description(),
            Error::Hyper(ref err) => err.description(),
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
            Error::SSH(ref err) => Some(err),
            Error::Hex(ref err) => Some(err),
            Error::Hyper(ref err) => Some(err),
            Error::UTF8(ref err) => Some(err)
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IoError(err)
    }
}
impl From<ssh::Error> for Error {
    fn from(err: ssh::Error) -> Error {
        Error::SSH(err)
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
impl From<rustc_serialize::hex::FromHexError> for Error {
    fn from(err:rustc_serialize::hex::FromHexError) -> Error {
        Error::Hex(err)
    }
}
impl From<hyper::error::Error> for Error {
    fn from(err:hyper::error::Error) -> Error {
        Error::Hyper(err)
    }
}
