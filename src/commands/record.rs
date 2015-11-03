extern crate clap;
use clap::{SubCommand};

use commands::StaticSubcommand;
use repository::{Repository,record};
use repository::fs_representation::{repo_dir, find_repo_root};

use std;
use std::io;
use std::fmt;
use std::error;

pub fn invocation() -> StaticSubcommand {
    return
        SubCommand::with_name("record")
        .about("record changes in the repository")
}

#[derive(Debug)]
pub enum Error {
    NotInARepository,
    IoError(io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::NotInARepository => write!(f, "Not in a repository"),
            Error::IoError(ref err) => write!(f, "IO error: {}", err),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::NotInARepository => "not in a repository",
            Error::IoError(ref err) => error::Error::description(err),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::IoError(ref err) => Some(err),
            Error::NotInARepository => None
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IoError(err)
    }
}

pub fn run() -> Result<Option<()>, Error> {
    let pwd = try!(std::env::current_dir());
    match find_repo_root(&pwd){
        None => return Err(Error::NotInARepository),
        Some(r) =>
        {
            let mut repo = try!(Repository::new(&repo_dir(r)));
            let recs = try!(record(&mut repo, &pwd));
            if recs.is_empty() {Ok(None)} else {Ok(Some(()))}
        }
    }
}
