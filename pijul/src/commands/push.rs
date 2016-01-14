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
use self::libpijul::fs_representation::{find_repo_root,patches_dir};
use std::fs::File;
use self::libpijul::patch::{Patch};
extern crate rustc_serialize;
use self::rustc_serialize::hex::{ToHex};
use super::ask::{ask_apply,Command};
use super::super::languages::{get_language,Translate,Language,Territory};

#[derive(Clone,Copy)]
enum Push {
    About,
    Remote,
    Local,
    Port
}
impl Translate for Push {
    fn trans(&self,l:Language,v:Option<Territory>)-> &'static str {
        match *self {
            Push::About => {
                match (l,v) {
                    (Language::FR,_) => "Pousse des patchs locaux vers un dépôt distant",
                    (Language::EN,_) => "Pushes local patches to a remote repository"
                }
            },
            Push::Remote => {
                match (l,v) {
                    (Language::FR,_) => "Adresse du dépôt distant",
                    (Language::EN,_) => "Location of the remote repository"
                }
            },
            Push::Local => {
                match (l,v) {
                    (Language::FR,_) => "Chemin du dépôt local",
                    (Language::EN,_) => "Path to the local repository"
                }
            },
            Push::Port => {
                match (l,v) {
                    (Language::FR,_) => "Port du serveur SSH distant",
                    (Language::EN,_) => "Port of the remote SSH host"
                }
            }
        }
    }
}

pub fn invocation() -> StaticSubcommand {
    let (l,t)=get_language();
    return
        SubCommand::with_name("push")
        .about(Push::About.trans(l,t))
        .arg(Arg::with_name("remote")
             .help(Push::Remote.trans(l,t))
             )
        .arg(Arg::with_name("repository")
             .help(Push::Local.trans(l,t))
             )
        .arg(Arg::with_name("port")
             .short("p")
             .long("port")
             .help(Push::Port.trans(l,t))
             .takes_value(true)
             .validator(|val| { let x:Result<u16,_>=val.parse();
                                match x { Ok(_)=>Ok(()),
                                          Err(_)=>Err(val) }
             }))
}

#[derive(Debug)]
pub struct Params<'a> {
    pub repository : &'a Path,
    pub remote : remote::Remote<'a>,
    pub remote_id : &'a str,
    pub yes_to_all : bool
}

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    let repository = Path::new(args.value_of("repository").unwrap_or("."));
    let remote_id = args.value_of("remote").unwrap();
    let remote=remote::parse_remote(&remote_id,args);
    Params { repository : repository,
             remote : remote,
             remote_id : remote_id,
             yes_to_all : args.is_present("all") }
}

pub fn run<'a>(args : &Params<'a>) -> Result<(), Error> {
    let pwd = args.repository;
    let (l,t)=get_language();
    match find_repo_root(&pwd){
        None => return Err(Error::NotInARepository),
        Some(r) => {
            let mut session=try!(args.remote.session());
            let mut pushable=try!(remote::pushable_patches(r,&mut session));
            if !args.yes_to_all {
                let selected={
                    let mut patches=Vec::new();
                    for i in pushable.iter() {
                        let patch={
                            let filename=patches_dir(r).join(i.to_hex()).with_extension("cbor");
                            let mut file=try!(File::open(filename));
                            try!(Patch::from_reader(&mut file))
                        };
                        patches.push((&i[..],patch));
                    }
                    try!(ask_apply(Command::Push,l,t,&patches))
                };
                pushable=selected;
            }
            remote::push(r,&mut session,&pushable)
        }
    }
}
