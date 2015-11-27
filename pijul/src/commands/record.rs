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

extern crate libpijul;
use commands::StaticSubcommand;
use self::libpijul::{Repository,Patch,HASH_SIZE};
use self::libpijul::fs_representation::{repo_dir, pristine_dir, patches_dir, find_repo_root, branch_changes_file};
use std::sync::Arc;

use std::io;
use std::fmt;
use std::error;
use std::thread;


use std::io::{BufWriter};
use std::fs::File;
extern crate rand;
use std::path::{Path};

extern crate time;

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
    //Serde(serde_cbor::error::Error),
    SavingPatch,
    Repository(libpijul::Error)
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::NotInARepository => write!(f, "Not in a repository"),
            Error::IoError(ref err) => write!(f, "IO error: {}", err),
            //Error::Serde(ref err) => write!(f, "Serialization error: {}", err),
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
            //Error::Serde(ref err) => serde_cbor::error::Error::description(err),
            Error::Repository(ref err) => libpijul::Error::description(err),
            Error::SavingPatch => "saving patch"
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::IoError(ref err) => Some(err),
            //Error::Serde(ref err) => Some(err),
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


pub fn run(params : &Params) -> Result<Option<()>, Error> {
    match find_repo_root(&params.repository){
        None => return Err(Error::NotInARepository),
        Some(r) =>
        {
            let repo_dir=pristine_dir(r);
            let t0=time::precise_time_s();
            let (changes,syncs)= {
                let mut repo = try!(Repository::new(&repo_dir).map_err(Error::Repository));
                try!(repo.record(&r).map_err(Error::Repository))
            };
            let t1=time::precise_time_s();
            info!("computed patch in {}s", t1-t0);

            //println!("recorded");
            if changes.is_empty() {
                println!("Nothing to record");
                Ok(None)
            } else {
                //println!("patch: {:?}",changes);
                let patch=Patch::new(changes);
                // save patch
                println!("patch ready");
                let patch_arc=Arc::new(patch);
                let child_patch=patch_arc.clone();
                let patches_dir=patches_dir(r);
                let hash_child=thread::spawn(move || {
                    let t0=time::precise_time_s();
                    let hash=child_patch.save(&patches_dir);
                    let t1=time::precise_time_s();
                    info!("saved patch in {}s", t1-t0);
                    hash
                });

                let t0=time::precise_time_s();

                let mut internal=[0;HASH_SIZE];
                let mut repo = try!(Repository::new(&repo_dir).map_err(Error::Repository));
                repo.new_internal(&mut internal);
                //println!("applying");
                repo.apply(&patch_arc, &internal).unwrap();
                //println!("sync");
                let t1=time::precise_time_s();
                //info!("applied patch in {}s", t1-t0);
                repo.sync_file_additions(&patch_arc.changes[..],&syncs, &internal);
                /*
                if cfg!(debug_assertions){
                    let mut buffer = BufWriter::new(File::create(r.join("debug")).unwrap());
                    repo.debug(&mut buffer);
                }*/
                let t2=time::precise_time_s();
                info!("applied patch in {}s", t2-t0);

                match hash_child.join() {
                    Ok(Ok(hash))=> {
                        repo.register_hash(&internal[..],&hash[..]);
                        //println!("writing changes {:?}",internal);
                        repo.write_changes_file(&branch_changes_file(r,repo.get_current_branch())).unwrap();
                        Ok(Some(()))
                    },
                    Ok(Err(x)) => {
                        Err(Error::Repository(x))
                    },
                    Err(_)=>{
                        Err(Error::SavingPatch)
                    }
                }
            }
        }
    }
}
