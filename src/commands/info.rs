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

use std;
use std::path::Path;
use clap::{SubCommand, Arg, ArgMatches};

use commands;
use repository::fs_representation::find_repo_root;

pub struct Params<'a> {
    pub directory : &'a Path
}

pub fn invocation() -> commands::StaticSubcommand {
    return 
        SubCommand::with_name("info")
        .about("Get information about the current repository, if any")
        .arg(Arg::with_name("dir")
             .index(1)
             .help("Pijul info will be given about this directory.")
             .required(false)
             );
}

pub fn parse_args<'a>(args : &'a ArgMatches) -> Params<'a>
{
    Params{ directory : Path::new(args.value_of("dir").unwrap_or(".")) }
}

pub fn run(request: &Params) -> () {
    match find_repo_root(request.directory) {
        Some(r) => println!("Current repository location: '{}'", r.display()),
        None => {
            println!("not in a repository");
            std::process::exit(1)
        }
    }
}
