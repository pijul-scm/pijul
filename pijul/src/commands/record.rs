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
use self::libpijul::{Repository};
use self::libpijul::patch::{Patch,HASH_SIZE};
use self::libpijul::fs_representation::{repo_dir, pristine_dir, patches_dir, find_repo_root, branch_changes_file,to_hex};
use std::sync::Arc;

use std::thread;
extern crate time;
use commands::error::Error;
use std::collections::HashSet;

extern crate rand;
use std::path::{Path};

use std::io::{BufWriter};
use std::fs::File;

use super::super::meta::{Meta};
use super::ask;

pub fn invocation() -> StaticSubcommand {
    return
        SubCommand::with_name("record")
        .about("record changes in the repository")
        .arg(Arg::with_name("repository")
             .long("repository")
             .help("The repository where to record, defaults to the current directory.")
             .required(false))
        .arg(Arg::with_name("all")
             .short("a")
             .long("all")
             .help("Answer 'y' to all questions")
             .takes_value(false)
             )
        .arg(Arg::with_name("message")
             .short("m")
             .long("name")
             .help("Answer 'y' to all questions")
             .takes_value(true)
             )
        .arg(Arg::with_name("author")
             .short("A")
             .long("author")
             .help("Author of this patch (multiple occurrences allowed)")
             .multiple(true)
             .takes_value(true)
             )
}

pub struct Params<'a> {
    pub repository : &'a Path,
    pub patch_name : Option<&'a str>,
    pub authors : Option<Vec<&'a str>>,
    pub yes_to_all : bool
}

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a>
{
    Params { repository : Path::new(args.value_of("repository").unwrap_or(".")),
             yes_to_all : args.is_present("all"),
             authors : args.values_of("author"),
             patch_name : args.value_of("mesage")
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
                let (changes,syncs)=try!(repo.record(&r).map_err(Error::Repository));
                if !params.yes_to_all {
                    let c=try!(super::ask::ask_record(&repo,&changes));
                    let selected =
                        changes.into_iter()
                        .enumerate()
                        .filter(|&(i,_)| { *(c.get(&i).unwrap_or(&false)) })
                        .map(|(_,x)| x)
                        .collect();

                    (selected,syncs)
                } else {
                    (changes,syncs)
                }
            };
            let t1=time::precise_time_s();
            debug!("creating patch took {}s",t1-t0);
            //println!("recorded");
            if changes.is_empty() {
                println!("Nothing to record");
                Ok(None)
            } else {
                //println!("patch: {:?}",changes);

                let patch={
                    let mut meta=Meta::load(r).unwrap_or(Meta::new());
                    let mut save_meta=false;
                    let authors :Vec<String>=
                        if let Some(ref authors)=params.authors {
                            authors.iter().map(|x| x.to_string()).collect()
                        } else if meta.authors.len()>0 {
                            meta.authors.iter().map(|x| x.to_string()).collect()
                        } else {
                            save_meta=true;
                            try!(ask::ask_authors())
                        };
                    let patch_name=
                        if let Some(ref m)=params.patch_name {
                            m.to_string()
                        } else {
                            try!(ask::ask_patch_name())
                        };
                    if save_meta {
                        try!(meta.save(r))
                    }
                    Patch::new(authors,
                               patch_name,
                               None,
                               self::time::now().to_timespec().sec,
                               changes)
                };
                // save patch
                let mut repo = try!(Repository::new(&repo_dir).map_err(Error::Repository));
                let () = try!(repo.register_patch(r, patch, &syncs).map_err(Error::Repository));
                Ok(Some(()))
            }
        }
    }
}
