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
use std::io;

use commands::StaticSubcommand;
use repository::fs_representation;

pub fn invocation() -> StaticSubcommand {
    return
        SubCommand::with_name("init")
        .about("Create a new repository")
        .arg(Arg::with_name("directory")
             .index(1)
             .help("Where to create the repository, defaults to the current repository.")
             .required(false)
             );
}

pub struct Params<'a> {
    location : &'a Path,
    allow_nested : bool
}

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a>
{
    Params {location : Path::new(args.value_of("directory").unwrap_or(".")),
            allow_nested : false
    }
}

pub fn run (p : &Params) -> io::Result<()> {
    let dir = p.location;
    match fs_representation::find_repo_root(&dir) {
        Some(d) =>
            {
                if p.allow_nested
                {
                    fs_representation::create(&dir)
                }
                else
                {
                    let err_string = format!("Found repository at {}, refusing to create a nested repository.", d.display());
                    Err(io::Error::new(io::ErrorKind::Other, err_string))
                }
            }
        None =>
        {
            fs_representation::create(&dir)
        }
    }
}
