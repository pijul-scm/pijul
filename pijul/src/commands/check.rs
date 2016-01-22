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
use clap::{SubCommand, Arg, ArgMatches};
use std::path::Path;

use commands::StaticSubcommand;
extern crate libpijul;
use self::libpijul::Repository;
use self::libpijul::fs_representation::{pristine_dir,find_repo_root};
use super::get_wd;
use std;
use super::error::Error;

pub struct Params<'a> {
    pub repository : Option<&'a Path>
}

pub fn invocation() -> StaticSubcommand {
    return
        SubCommand::with_name("check")
        .about("Check the sanity of a repository")
        .arg(Arg::with_name("repository")
             .index(1)
             .help("The repository to check, defaults to the current directory.")
             .required(false)
             );
}

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    Params {repository : args.value_of("repository").and_then(|x| { Some(Path::new(x)) }) }
}

pub fn run(args: &Params) -> Result<(),Error> {
    let wd=try!(get_wd(args.repository));
    match find_repo_root(&wd) {
        Some(ref repo_base) => {
            let _repository = Repository::new(&pristine_dir(&repo_base)).unwrap(); //.expect("Repository error");
            println!("Your repo looks alright Ma'am/Sir");
            Ok(())
        },

        None => {
            Err(Error::NotInARepository)
        }
    }
}
