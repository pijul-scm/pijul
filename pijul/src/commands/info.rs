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

use std::path::Path;
use clap::{SubCommand, Arg, ArgMatches};

use commands;
extern crate libpijul;
use self::libpijul::fs_representation::find_repo_root;
use commands::error::Error;
use super::get_wd;
pub struct Params<'a> {
    pub repository : Option<&'a Path>
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
    Params{ repository : args.value_of("dir").and_then(|x| { Some(Path::new(x)) }) }
}

pub fn run(args: &Params) -> Result<(),Error> {
    let wd = try!(get_wd(args.repository));
    match find_repo_root(&wd) {
        Some(ref r) =>
        { println!("Current repository location: '{}'", r.display());
          Ok(())
        },
        None => Err(Error::NotInARepository)
    }
}

