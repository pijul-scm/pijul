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

use commands::fs_operation;
use commands::fs_operation::Operation;
use commands::StaticSubcommand;
use clap::{SubCommand, ArgMatches,Arg};
use commands::error;

pub fn invocation() -> StaticSubcommand {
    return 
        SubCommand::with_name("remove")
        .about("remove file from the repository")
        .arg(Arg::with_name("files")
             .multiple(true)
             .help("Files to remove from the repository.")
             .required(true)
             )
        .arg(Arg::with_name("repository")
             .long("repository")
             .help("Repository to remove files from.")
             );
}

pub type Params<'a> = fs_operation::Params<'a>;

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    return fs_operation::parse_args(args);
}


pub fn run<'a>(args : &Params<'a>) -> Result<Option<()>, error::Error> {
    fs_operation::run(args, Operation::Remove)
}
