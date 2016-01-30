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

use super::StaticSubcommand;
use super::error::Error;
use super::remote;
use std::path::Path;
extern crate libpijul;
use self::libpijul::fs_representation::{find_repo_root};
use self::libpijul::patch::{Patch};

use super::get_wd;
use super::super::meta::{Meta,Repository};

pub fn invocation() -> StaticSubcommand {
    return
        SubCommand::with_name("push")
        .about("push to a remote repository")
        .arg(Arg::with_name("remote")
             .help("Repository to push to.")
             )
        .arg(Arg::with_name("repository")
             .help("Local repository.")
             )
        .arg(Arg::with_name("all")
             .short("a")
             .long("all")
             .help("Answer 'y' to all questions")
             .takes_value(false)
             )
        .arg(Arg::with_name("set-default")
             .long("set-default")
             )
        .arg(Arg::with_name("port")
             .short("p")
             .long("port")
             .help("Port of the remote ssh server.")
             .takes_value(true)
             .validator(|val| { let x:Result<u16,_>=val.parse();
                                match x { Ok(_)=>Ok(()),
                                          Err(_)=>Err(val) }
             })
             )
}

#[derive(Debug)]
pub struct Params<'a> {
    pub repository : Option<&'a Path>,
    pub remote_id : Option<&'a str>,
    pub yes_to_all : bool,
    pub set_default : bool,
    pub port : Option<u64>
}

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    let repository = args.value_of("repository").and_then(|x| { Some(Path::new(x)) });
    let remote_id = args.value_of("remote");
    Params { repository : repository,
             remote_id : remote_id,
             yes_to_all : args.is_present("all"),
             set_default : args.is_present("set-default"),
             port : args.value_of("port").and_then(|x| { Some(x.parse().unwrap()) }) }
}

pub fn run<'a>(args : &Params<'a>) -> Result<(), Error> {
    let wd=try!(get_wd(args.repository));
    match find_repo_root(&wd){
        None => return Err(Error::NotInARepository),
        Some(ref r) => {
            let meta = match Meta::load(r) { Ok(m)=>m, Err(_)=> { Meta::new() } };
            let mut savable=false;
            let remote={
                if let Some(remote_id)=args.remote_id {
                    savable=true;
                    remote::parse_remote(remote_id,args.port,None)
                } else {
                    match meta.pull {
                        Some(Repository::SSH{ref address,ref port}) => remote::parse_remote(address,Some(*port as u64),Some(r)),
                        Some(Repository::String(ref host)) => remote::parse_remote(host,None,Some(r)),
                        None=>{
                            return Err(Error::MissingRemoteRepository)
                        }
                    }
                }
            };
            let mut session=try!(remote.session());
            let mut pushable=try!(session.pushable_patches(r));
            if !args.yes_to_all {
                let selected={
                    let mut patches=Vec::new();
                    for i in pushable.iter() {
                        let patch=try!(Patch::from_repository(r,i));
                        patches.push((&i[..],patch));
                    }
                    try!(super::ask::ask_apply(super::ask::Command::Push,&patches))
                };
                pushable=selected;
            }

            try!(session.push(r,&pushable));
            if args.set_default && savable {
                let mut meta = match Meta::load(r) { Ok(m)=>m, Err(_)=> { Meta::new() } };
                if let Some(remote_id)=args.remote_id {
                    if let Some(p)=args.port {
                        meta.push=Some(Repository::SSH{address:remote_id.to_string(),port:p});
                    } else {
                        meta.push=Some(Repository::String(remote_id.to_string()))
                    }
                }
                try!(meta.save(r));
            }
            Ok(())
        }
    }
}
