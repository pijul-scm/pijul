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
use clap::{SubCommand, ArgMatches, Arg};

use commands::StaticSubcommand;
use repository::{Repository,record,apply,sync_file_additions,HASH_SIZE,new_internal,register_hash,dependencies};
use repository;
use repository::patch::{Patch};
use repository::fs_representation::{repo_dir, pristine_dir, patches_dir, find_repo_root};
use std::sync::Arc;

use std;
use std::io;
use std::fmt;
use std::error;
use std::thread;
extern crate crypto;
use crypto::digest::Digest;
use crypto::sha2::Sha512;

extern crate serde_cbor;

use std::io::{BufWriter,BufReader,BufRead};
use std::fs::File;
extern crate rand;
use std::path::{Path};

extern crate libc;
use self::libc::funcs::posix88::unistd::{getpid};

pub fn invocation() -> StaticSubcommand {
    return
        SubCommand::with_name("record")
        .about("record changes in the repository")
        .arg(Arg::with_name("repository")
             .long("repository")
             .help("The repository where to record, defaults to the current directory.")
             .required(false));
}

pub struct Params<'a> {
    pub repository : &'a Path
}

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a>
{
    Params { repository : Path::new(args.value_of("repository").unwrap_or("."))}
}

#[derive(Debug)]
pub enum Error {
    NotInARepository,
    IoError(io::Error),
    Serde(serde_cbor::error::Error),
    SavingPatch,
    Repository(repository::Error)
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::NotInARepository => write!(f, "Not in a repository"),
            Error::IoError(ref err) => write!(f, "IO error: {}", err),
            Error::Serde(ref err) => write!(f, "Serialization error: {}", err),
            Error::Repository(ref err) => write!(f, "Repository: {}", err),
            Error::SavingPatch => write!(f, "Patch saving error"),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::NotInARepository => "not in a repository",
            Error::IoError(ref err) => error::Error::description(err),
            Error::Serde(ref err) => serde_cbor::error::Error::description(err),
            Error::Repository(ref err) => repository::Error::description(err),
            Error::SavingPatch => "saving patch"
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::IoError(ref err) => Some(err),
            Error::Serde(ref err) => Some(err),
            Error::Repository(ref err) => Some(err),
            Error::NotInARepository => None,
            Error::SavingPatch => None
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
    fn make_name(dir:&Path,name:&mut [u8])->std::path::PathBuf{
        for i in 0..name.len() { let r:u8=rand::random(); name[i] = 97 + (r%26) }
        let tmp=dir.join(std::str::from_utf8(&name[..]).unwrap());
        if std::fs::metadata(&tmp).is_err() { tmp } else { make_name(dir,name) }
    }
    let tmp=make_name(&dir,&mut name);

    let mut buffer = BufWriter::new(try!(File::create(&tmp))); // change to uuid
    try!(serde_cbor::ser::to_writer(&mut buffer,&patch).map_err(Error::Serde));

    // hash
    let mut buffer = BufReader::new(try!(File::open(&tmp).map_err(Error::IoError))); // change to uuid
    let mut hasher = Sha512::new();
    loop {
        let len=match buffer.fill_buf() {
            Ok(buf)=> if buf.len()==0 { break } else { hasher.input_str(unsafe {std::str::from_utf8_unchecked(buf)});buf.len() },
            Err(e)=>return Err(Error::IoError(e))
        };
        buffer.consume(len)
    }
    let hash = hasher.result_str();
    try!(std::fs::rename(tmp,dir.join(&hash).with_extension("cbor")).map_err(Error::IoError));
    Ok(hash)
}

pub fn run(params : &Params) -> Result<Option<()>, Error> {
    match find_repo_root(&params.repository){
        None => return Err(Error::NotInARepository),
        Some(r) =>
        {
            let repo_dir=pristine_dir(r);
            let (changes,syncs)= {
                let mut repo = try!(Repository::new(&repo_dir).map_err(Error::Repository));
                try!(record(&mut repo, &r).map_err(Error::Repository))
            };
            //println!("recorded");
            if changes.is_empty() {
                println!("Nothing to record");
                Ok(None)
            } else {
                //println!("patch: {:?}",changes);
                let deps=dependencies(&changes[..]);
                let patch = Patch { changes:changes,
                                    dependencies:deps };
                // save patch

                let patch_arc=Arc::new(patch);
                let child_patch=patch_arc.clone();
                let patches_dir=patches_dir(r);
                let hash_child=thread::spawn(move || {
                    write_patch(&child_patch,&patches_dir)
                });
                let mut internal=[0;HASH_SIZE];
                let mut repo = try!(Repository::new(&repo_dir).map_err(Error::Repository));
                new_internal(&mut repo,&mut internal);
                apply(&mut repo, &patch_arc, &internal[..]);
                sync_file_additions(&mut repo,&patch_arc.changes[..],&syncs, &internal);

                match hash_child.join() {
                    Ok(Ok(hash))=> {
                        register_hash(&mut repo,&internal[..],hash.as_bytes());
                        Ok(Some(()))
                    },
                    Ok(Err(x)) => {
                        Err(x)
                    },
                    Err(_)=>{
                        Err(Error::SavingPatch)
                    }
                }
                /*
                println!("Debugging");
                let mut repo = try!(Repository::new(&repo_dir));
                let mut buffer = BufWriter::new(File::create("debug").unwrap()); // change to uuid
                debug(&mut repo,&mut buffer);
                 */
            }
        }
    }
}
