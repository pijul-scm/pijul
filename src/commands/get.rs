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
use repository::{Repository,apply,DEFAULT_BRANCH,HASH_SIZE,new_internal,sync_file_additions,has_patch,get_current_branch};
use repository::patch::{Patch};
use repository::fs_representation::{repo_dir, pristine_dir, find_repo_root, patches_dir, branch_changes_file,to_hex};
use repository;
use std;
use std::io;
use std::fmt;
use std::error;
use std::path::{Path,PathBuf};
use std::fs::{metadata};
use std::io::{BufWriter,BufReader,BufRead};
use std::fs::File;

use std::collections::hash_set::{HashSet};
use std::collections::hash_map::{HashMap};
extern crate serde_cbor;

use super::pull;
use super::init;


pub fn invocation() -> StaticSubcommand {
    return
        SubCommand::with_name("get")
        .about("clone a remote repository")
        .arg(Arg::with_name("remote")
             .help("Remote repository to clone.")
             )
        .arg(Arg::with_name("repository")
             .help("Local path.")
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

pub struct Params<'a> {
    pub pull_params:pull::Params<'a>
}

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    let x=pull::parse_args(args);
    Params { pull_params:x }
}

#[derive(Debug)]
pub enum Error{
    Init(init::Error),
    Pull(pull::Error)
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Init(ref err) => write!(f, "Init error: {}",err),
            Error::Pull(ref err) => write!(f, "Pull error: {}", err)
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Init(ref err) => init::Error::description(err),
            Error::Pull(ref err) => pull::Error::description(err),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::Init(ref err) => Some(err),
            Error::Pull(ref err) => Some(err)
        }
    }
}

pub fn run<'a>(args : &Params<'a>) -> Result<(), Error> {
    try!(init::run(&init::Params { location:args.pull_params.repository, allow_nested:false }).map_err(Error::Init));
    try!(pull::run(&args.pull_params).map_err(Error::Pull));
    Ok(())
}
