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
use clap::{SubCommand, ArgMatches};

use commands::StaticSubcommand;
use repository::{Repository,record,sync_files};
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

pub fn parse_args(_: &ArgMatches) -> () {}

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

pub fn run(_ : &()) -> Result<Option<()>, Error> {
    let pwd = try!(std::env::current_dir());
    match find_repo_root(&pwd){
        None => return Err(Error::NotInARepository),
        Some(r) =>
        {
            let repo_dir=repo_dir(r);
            let (recs,syncs)= {
                let mut repo = try!(Repository::new(&repo_dir));
                try!(record(&mut repo, &pwd))
            };
            let mut repo = try!(Repository::new(&repo_dir));
            sync_files(&mut repo,&recs[..],&syncs);

            if recs.is_empty() {Ok(None)} else {Ok(Some(()))}
        }
    }
}
