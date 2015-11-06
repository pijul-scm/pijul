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
use repository::{Repository,record,apply,sync_files,HASH_SIZE};
use repository::patch::{Patch};
use repository::fs_representation::{repo_dir, pristine_dir, patches_dir, find_repo_root};

use std;
use std::io;
use std::fmt;
use std::error;

extern crate crypto;
use crypto::digest::Digest;
use crypto::sha2::Sha512;

extern crate serde_cbor;

use std::io::BufWriter;
use std::fs::File;
extern crate rand;
use std::path::{Path};


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
    Serde(serde_cbor::error::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::NotInARepository => write!(f, "Not in a repository"),
            Error::IoError(ref err) => write!(f, "IO error: {}", err),
            Error::Serde(ref err) => write!(f, "Serialization error: {}", err),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::NotInARepository => "not in a repository",
            Error::IoError(ref err) => error::Error::description(err),
            Error::Serde(ref err) => serde_cbor::error::Error::description(err),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::IoError(ref err) => Some(err),
            Error::Serde(ref err) => Some(err),
            Error::NotInARepository => None
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IoError(err)
    }
}

fn write_patch<'a>(patch:&Patch,dir:&Path)->Result<String,Error>{
    let mut name:[u8;20]=[0;20];
    for i in 0..name.len() { let r:u8=rand::random(); name[i] = 97 + (r%26) }
    let tmp=dir.join(std::str::from_utf8(&name[..]).unwrap());
    let mut buffer = BufWriter::new(try!(File::create(&tmp))); // change to uuid
    try!(serde_cbor::ser::to_writer(&mut buffer,&patch).map_err(Error::Serde));
    // hash
    let mut hasher = Sha512::new();
    hasher.input_str("hello world");
    let hash = hasher.result_str();
    try!(std::fs::rename(tmp,dir.join(&hash).with_extension("cbor")).map_err(Error::IoError));
    Ok(hash)
}

pub fn run(_ : &()) -> Result<Option<()>, Error> {
    let pwd = try!(std::env::current_dir());
    match find_repo_root(&pwd){
        None => return Err(Error::NotInARepository),
        Some(r) =>
        {
            let repo_dir=pristine_dir(r);
            let (changes,syncs)= {
                let mut repo = try!(Repository::new(&repo_dir));
                try!(record(&mut repo, &r))
            };
            if changes.is_empty() {
                println!("Nothing to record");
                Ok(None)
            } else {
                let patch = Patch { changes:changes };
                // save patch
                let patches_dir=patches_dir(r);
                let hash=write_patch(&patch,&patches_dir).unwrap();


                let mut repo = try!(Repository::new(&repo_dir));

                let mut intid=[0;HASH_SIZE];
                apply(&mut repo, &patch.changes[..], hash[..].as_bytes(), &mut intid[..]);
                sync_files(&mut repo,&patch.changes[..],&syncs, &[0;HASH_SIZE][..]);
                Ok(Some(()))
            }
        }
    }
}
