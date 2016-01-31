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

use super::StaticSubcommand;

use super::init;

use super::error::Error;
use super::remote::{Remote,parse_remote};
extern crate regex;
use self::regex::Regex;

pub fn invocation() -> StaticSubcommand {
    return
        SubCommand::with_name("clone")
        .about("clone a remote repository")
        .arg(Arg::with_name("from")
             .help("Repository to clone.")
             .required(true)
             )
        .arg(Arg::with_name("to")
             .help("Target.")
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
#[derive(Debug)]
pub struct Params<'a> {
    pub from:Remote<'a>,
    pub to:Remote<'a>
}

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    // At least one must not use its "port" argument
    let from = parse_remote(args.value_of("from").unwrap(),args.value_of("port").and_then(|x| { Some(x.parse().unwrap()) }),
                            None);
    let to =
        if let Some(to)=args.value_of("to") {
            parse_remote(to,args.value_of("port").and_then(|x| { Some(x.parse().unwrap()) }), None)
        } else {
            let basename=Regex::new(r"([^/:]*)").unwrap();
            let from=args.value_of("from").unwrap();
            if let Some(to)=basename.captures_iter(from).last().and_then(|to| { to.at(1) }) {
                parse_remote(to,args.value_of("port").and_then(|x| { Some(x.parse().unwrap()) }), None)
            } else {
                panic!("Could not parse target")
            }
        };
    Params { from:from, to:to }
}



pub fn run<'a>(args : &Params<'a>) -> Result<(), Error> {
    debug!("{:?}",args);
    match args.from {
        Remote::Local{ref path}=>{
            let mut to_session=try!(args.to.session());
            debug!("remote init");
            try!(to_session.remote_init());
            debug!("pushable?");
            let pushable=try!(to_session.pushable_patches(path));
            debug!("pushable = {:?}",pushable);
            to_session.push(path,&pushable)
        },
        _=>match args.to {
            Remote::Local{ref path} =>{
                // This is "darcs get"
                try!(init::run(&init::Params { location:path, allow_nested:false }));
                let mut session=try!(args.from.session());
                let pullable=try!(session.pullable_patches(path));
                session.pull(path,&pullable)
            },
            _=>unimplemented!()
        }
    }
}
