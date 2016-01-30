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
use std::path::Path;

extern crate libpijul;
use self::libpijul::fs_representation::{find_repo_root};
use self::libpijul::patch::{Patch};

use super::remote;
use std::fs::File;
use super::ask::{ask_apply,Command};
use super::get_wd;

use super::super::meta::{Meta,Repository};

pub fn invocation() -> StaticSubcommand {
    return
        SubCommand::with_name("pull")
        .about("pull from a remote repository")
        .arg(Arg::with_name("remote")
             .help("Repository from which to pull.")
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
    let repository = args.value_of("repository").and_then(|x| {Some(Path::new(x))});
    let remote_id = args.value_of("remote");
    //let remote=remote::parse_remote(&remote_id,args);
    Params { repository : repository,
             remote_id : remote_id,
             yes_to_all : args.is_present("all"),
             set_default : args.is_present("set-default"),
             port : args.value_of("port").and_then(|x| { Some(x.parse().unwrap()) }) }
}

pub fn run<'a>(args : &Params<'a>) -> Result<(), Error> {
    debug!("pull args {:?}",args);
    let wd=try!(get_wd(args.repository));
    match find_repo_root(&wd){
        None => return Err(Error::NotInARepository),
        Some(ref r) => {
            /*let mut meta=Meta::load(r).unwrap_or(Meta::new());

            let pe="pmeunier".to_string();
            meta.authors=vec!(pe.clone());
            meta.save(r);
            */
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
            let mut pullable=try!(session.pullable_patches(r));
            // Loading a patch's dependencies
            if !args.yes_to_all {
                let selected={
                    let mut patches=Vec::new();
                    for i in pullable.iter() {
                        let patch={
                            let filename=try!(session.download_patch(r,i));
                            let mut file=try!(File::open(&filename));
                            try!(Patch::from_reader(&mut file,Some(&filename)))
                        };
                        patches.push((&i[..],patch));
                    }
                    try!(ask_apply(Command::Pull,&patches))
                };
                pullable.remote=selected;
            }
            // Pulling and applying
            try!(session.pull(r,&pullable));
            if args.set_default && savable {
                let mut meta = match Meta::load(r) { Ok(m)=>m, Err(_)=> { Meta::new() } };
                if let Some(remote_id)=args.remote_id {
                    if let Some(p)=args.port {
                        meta.pull=Some(Repository::SSH{address:remote_id.to_string(),port:p});
                    } else {
                        meta.pull=Some(Repository::String(remote_id.to_string()))
                    }
                }
                try!(meta.save(r));
            }

            Ok(())
        }
    }
}
