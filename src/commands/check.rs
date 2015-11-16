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
use std::io::{Error, ErrorKind};

use commands::StaticSubcommand;
use repository::Repository;
use repository::fs_representation::{find_repo_root,pristine_dir};

pub struct Params<'a> {
    pub repository : &'a Path
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

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a>
{
    Params {repository : Path::new(args.value_of("repository").unwrap_or("."))}
}

pub fn run(args: &Params) -> Result<(),Error> {
    match find_repo_root(args.repository)
    {
        Some(repo_base) => {
            let _repository = Repository::new(&pristine_dir(&repo_base)).expect("Repository error");
            println!("Your repo looks alright Ma'am/Sir");
            Ok(())
        },

        None => {
            Err(Error::new(ErrorKind::NotFound, "not in a repository"))
        }
    }
}
