extern crate libpijul;
use std::io;
use std::error;
use std::fmt;
use std::path::Path;

#[derive(Debug)]
pub enum Error <'a>{
    NotInARepository,
    PathNotFound(&'a Path),
    IoError(io::Error),
    Repository(libpijul::Error)
}

impl <'a> fmt::Display for Error<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::NotInARepository => write!(f, "Not in a repository"),
            Error::PathNotFound(p) => write!(f, "Path not found: {}", p.to_string_lossy()),
            Error::IoError(ref err) => write!(f, "IO error: {}", err),
            Error::Repository(ref err) => write!(f, "Repository error: {}", err),
        }
    }
}

impl <'a> error::Error for Error<'a> {
    fn description(&self) -> &str {
        match *self {
            Error::NotInARepository => "not in a repository",
            Error::PathNotFound(_) => "path not found",
            Error::IoError(ref err) => error::Error::description(err),
            Error::Repository(ref err) => libpijul::Error::description(err),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::IoError(ref err) => Some(err),
            Error::Repository(ref err) => Some(err),
            Error::PathNotFound(_) => None,
            Error::NotInARepository => None
        }
    }
}

impl <'a> From<io::Error> for Error<'a> {
    fn from(err: io::Error) -> Error<'a> {
        Error::IoError(err)
    }
}
