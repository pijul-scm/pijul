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
use libpijul::{Repository};
use commands::StaticSubcommand;
use self::libpijul::{DEFAULT_BRANCH};
use self::libpijul::patch::{read_changes_from_file};
use self::libpijul::fs_representation::{pristine_dir, find_repo_root, branch_changes_file};

use commands::error::Error;
use std::collections::{HashSet};

use std::path::{Path};

extern crate time;

extern crate rustc_serialize;
use self::rustc_serialize::hex::{FromHex};

pub fn invocation() -> StaticSubcommand {
    return
        SubCommand::with_name("apply")
        .about("apply a patch")
        .arg(Arg::with_name("patch")
             .help("Hash of the patch to apply, in hexadecimal.")
             .multiple(true)
             .required(true))
        .arg(Arg::with_name("repository")
             .long("repository")
             .help("The repository where to record, defaults to the current directory.")
             .required(false))
}

pub struct Params<'a> {
    pub repository : &'a Path,
    pub hex_hash : Vec<&'a str>
}

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a>
{
    Params {
        repository : Path::new(args.value_of("repository").unwrap_or(".")),
        hex_hash : args.values_of("patch").unwrap()
    }
}

pub fn run(params : &Params) -> Result<Option<()>, Error> {
    match find_repo_root(&params.repository){
        None => return Err(Error::NotInARepository),
        Some(ref target) =>
        {
            debug!("applying");
            let remote:HashSet<Vec<u8>>={
                let mut h=HashSet::new();
                for i in params.hex_hash.iter() {
                    h.insert(i.from_hex().unwrap());
                }
                h
            };
            debug!("remote={:?}",remote);
            let local:HashSet<Vec<u8>>={
                let changes_file=branch_changes_file(target,DEFAULT_BRANCH.as_bytes());
                read_changes_from_file(&changes_file).unwrap_or(HashSet::new())
            };
            debug!("local={:?}",local);
            let repo_dir=pristine_dir(target);
            let repo = try!(Repository::open(&repo_dir));
            let mut txn = try!(repo.mut_txn_begin());
            try!(txn.apply_patches(DEFAULT_BRANCH, target,&remote,&local));
            try!(txn.commit());
            Ok(Some(()))
        }
    }
}
