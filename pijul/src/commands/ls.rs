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
use commands::StaticSubcommand;
use clap::{SubCommand, ArgMatches,Arg};
extern crate libpijul;
use commands::error;
use self::libpijul::{Repository};
use self::libpijul::fs_representation::{repo_dir, pristine_dir, find_repo_root};
use std::path::Path;

pub fn invocation() -> StaticSubcommand {
    return 
        SubCommand::with_name("ls")
        .about("list tracked files")
        .arg(Arg::with_name("dir")
             .multiple(true)
             .help("Prefix of the list")
             )
        .arg(Arg::with_name("repository")
             .long("repository")
             .help("Repository to list.")
             );
}

pub struct Params<'a> {
    pub repository : &'a Path
}

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    Params { repository:Path::new(args.value_of("repository").unwrap_or(".")) }
}

pub fn run<'a>(args : &Params<'a>) -> Result<(), error::Error> {
    let pwd = args.repository;
    match find_repo_root(&pwd){
        None => return Err(error::Error::NotInARepository),
        Some(r) =>
        {
            let repo_dir=pristine_dir(r);
            let repo = try!(Repository::new(&repo_dir).map_err(error::Error::Repository));
            let files=repo.list_files();
            for f in files {
                println!("{:?}",f)
            }
            Ok(())
        }
    }
}
