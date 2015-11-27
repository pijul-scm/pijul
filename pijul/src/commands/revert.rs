/*
  Copyright Florent Becker and Pierre-Etienne Meunier 2015.

  This file is part of Pijul.

  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU Affero General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU Affero General Public License for more details.

  You should have received a copy of the GNU Affero General Public License
  along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
extern crate clap;
use clap::{SubCommand, ArgMatches,Arg};

use commands::StaticSubcommand;
extern crate libpijul;
use self::libpijul::{Repository,Patch,DEFAULT_BRANCH,HASH_SIZE};
use self::libpijul::fs_representation::{repo_dir, pristine_dir, find_repo_root, patches_dir, branch_changes_file,to_hex,read_changes};
use std::io;
use std::fmt;
use std::error;
use std::path::{Path,PathBuf};
use std::io::{BufWriter,BufReader};
use std::fs::File;
use std::collections::hash_set::{HashSet};
use std::collections::hash_map::{HashMap};
use std::fs::{hard_link,metadata};

/*
extern crate ssh2;
use std::net::TcpStream;
use self::ssh2::Session;
*/
extern crate regex;
use self::regex::Regex;

pub fn invocation() -> StaticSubcommand {
    return
        SubCommand::with_name("revert")
        .about("Rewrite the working copy from the pristine")
        .arg(Arg::with_name("repository")
             .help("Local repository.")
             )
}

pub struct Params<'a> {
    pub repository : &'a Path,
}

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    let repository = Path::new(args.value_of("repository").unwrap_or("."));
    Params { repository : repository }
}

#[derive(Debug)]
pub enum Error{
    NotInARepository,
    IoError(io::Error),
    //Serde(serde_cbor::error::Error),
    Repository(libpijul::Error)
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::NotInARepository => write!(f, "Not in a repository"),
            Error::IoError(ref err) => write!(f, "IO error: {}", err),
            //Error::Serde(ref err) => write!(f, "Serialization error: {}", err),
            Error::Repository(ref err) => write!(f, "Repository error: {}", err)
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::NotInARepository => "not in a repository",
            Error::IoError(ref err) => error::Error::description(err),
            //Error::Serde(ref err) => serde_cbor::error::Error::description(err),
            Error::Repository(ref err) => libpijul::Error::description(err)
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::IoError(ref err) => Some(err),
            Error::NotInARepository => None,
            //Error::Serde(ref err) => Some(err),
            Error::Repository(ref err) => Some(err)
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IoError(err)
    }
}

pub fn run<'a>(args : &Params<'a>) -> Result<(), Error> {
    let pwd = args.repository;
    match find_repo_root(&pwd){
        None => return Err(Error::NotInARepository),
        Some(r) =>
        {
            let repo_dir=pristine_dir(r);
            let mut repo = try!(Repository::new(&repo_dir).map_err(Error::Repository));
            repo.output_repository(&r,&Patch::empty());
            Ok(())
        }
    }
}
