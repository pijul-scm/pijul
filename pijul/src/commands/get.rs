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

use super::pull;
use super::init;

use commands::error::Error;

pub fn invocation() -> StaticSubcommand {
    return
        SubCommand::with_name("get")
        .about("clone a remote repository")
        .arg(Arg::with_name("remote")
             .help("Remote repository to clone.")
             )
        .arg(Arg::with_name("repository")
             .help("Local path.")
             )
        .arg(Arg::with_name("port")
             .short("p")
             .long("port")
             .help("Port of the remote ssh server.")
             .takes_value(true)
             .validator(|val| { let x:Result<u16,_>=val.parse();
                                match x { Ok(_)=>Ok(()),
                                          Err(_)=>Err(val) }
             })
             )
}

pub struct Params<'a> {
    pub pull_params:pull::Params<'a>
}

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    let x=pull::parse_args(args);
    Params { pull_params:x }
}


pub fn run<'a>(args : &Params<'a>) -> Result<(), Error> {
    try!(init::run(&init::Params { location:args.pull_params.repository, allow_nested:false }));
    try!(pull::run(&args.pull_params));
    Ok(())
}
