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
use self::libpijul::patch::{Patch,Value};
use self::libpijul::fs_representation::{repo_dir, pristine_dir, find_repo_root};

extern crate time;
use commands::error::Error;

extern crate rand;
use std::path::{Path};

use super::super::meta::{Meta};
use super::ask;
use super::get_wd;
use std::collections::BTreeMap;

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
    pub repository : Option<&'a Path>,
    pub patch_name : Option<&'a str>,
    pub authors : Option<Vec<&'a str>>,
    pub yes_to_all : bool
}

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a>
{
    Params { repository : args.value_of("repository").and_then(|x| { Some(Path::new(x)) }),
             yes_to_all : args.is_present("all"),
             authors : args.values_of("author"),
             patch_name : args.value_of("message")
    }
}

pub fn run(args : &Params) -> Result<Option<()>, Error> {
    let wd=try!(get_wd(args.repository));
    match find_repo_root(&wd){
        None => return Err(Error::NotInARepository),
        Some(ref r) =>
        {
            let repo_dir=pristine_dir(r);
            let t0=time::precise_time_s();
            let (changes,syncs)= {
                let mut repo = try!(Repository::new(&repo_dir).map_err(Error::Repository));
                let (changes,syncs)=try!(repo.record(&r).map_err(Error::Repository));
                if !args.yes_to_all {
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
                    debug!("loading meta");
                    let mut save_meta=false;
                    let mut meta = match Meta::load(r) { Ok(m)=>m, Err(_)=> { save_meta=true; Meta::new() } };
                    let authors :Vec<BTreeMap<String,Value>>=
                        if let Some(ref authors)=args.authors {
                            let authors=authors.iter().map(|x| {
                                let mut b=BTreeMap::new();
                                b.insert("name".to_string(),Value::String(x.to_string()));
                                b
                            }).collect();
                            {
                                if meta.default_authors().and_then(|x| { if x.len()>0{Some(x)}else{None} }).is_none() {
                                    meta.set_default_authors(&authors);
                                    save_meta=true
                                }
                            }
                            authors
                        } else {
                            if let Some(default)=meta.default_authors().and_then(|x| { if x.len()>0{Some(x)}else{None} }) {
                                default
                            } else {
                                save_meta=true;
                                let authors=try!(ask::ask_authors());
                                //meta.set_default_authors(&authors);
                                authors
                            }
                        };
                    let patch_name=
                        if let Some(ref m)=args.patch_name {
                            m.to_string()
                        } else {
                            try!(ask::ask_patch_name())
                        };
                    if save_meta {
                        try!(meta.save(r))
                    }
                    debug!("new");
                    Patch::new(authors,
                               patch_name,
                               None,
                               self::time::now().to_timespec().sec,
                               changes)
                };
                debug!("register_patch");
                // save patch
                let mut repo = try!(Repository::new(&repo_dir).map_err(Error::Repository));
                let () = try!(repo.apply_local_patch(r, patch, &syncs).map_err(Error::Repository));
                Ok(Some(()))
            }
        }
    }
}
