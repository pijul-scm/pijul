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
use commands::error;
use clap::{SubCommand, ArgMatches,Arg};


pub fn invocation() -> StaticSubcommand {
    return 
        SubCommand::with_name("add")
        .about("add a file to the repository")
        .arg(Arg::with_name("files")
             .multiple(true)
             .help("Files to add to the repository.")
             .required(true)
             )
        .arg(Arg::with_name("repository")
             .long("repository")
             .help("Repository where to add files.")
             );
}

// pub struct Params<'a> {
//     pub added_files : Vec<&'a Path>,
//     pub repository : &'a Path
// }

pub type Params<'a> = fs_operation::Params<'a>;

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    return fs_operation::parse_args(args);
}

pub type Error<'a> = error::Error<'a>;


pub fn run<'a>(args : &Params<'a>) -> Result<Option<()>, Error<'a>> {
    fs_operation::run(args, Operation::Add)
}
