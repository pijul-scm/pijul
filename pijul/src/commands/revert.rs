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
extern crate libpijul;
use self::libpijul::{Repository};
use self::libpijul::patch::{Patch};
use self::libpijul::fs_representation::{repo_dir, pristine_dir, find_repo_root};
use std::path::{Path};

use commands::error;

pub fn invocation() -> StaticSubcommand {
    return
        SubCommand::with_name("revert")
        .about("Rewrite the working copy from the pristine")
        .arg(Arg::with_name("repository")
             .help("Local repository.")
             )
}

pub struct Params<'a> {
    pub repository : &'a Path,
}

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    let repository = Path::new(args.value_of("repository").unwrap_or("."));
    Params { repository : repository }
}

pub fn run<'a>(args : &Params<'a>) -> Result<(), error::Error> {
    let pwd = args.repository;
    match find_repo_root(&pwd){
        None => return Err(error::Error::NotInARepository),
        Some(r) =>
        {
            let repo_dir=pristine_dir(r);
            let mut repo = try!(Repository::new(&repo_dir));
            try!(repo.output_repository(&r,&Patch::empty()));
            Ok(())
        }
    }
}
