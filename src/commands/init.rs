extern crate clap;
use clap::{SubCommand, Arg, ArgMatches};
use std::path::Path;
use std::env::current_dir;
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
    location : Option<&'a Path>,
    allow_nested : bool
}

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a>
{
    Params {location : args.value_of("directory").map(|x| Path::new(x)),
            allow_nested : false
    }
}

pub fn run (p : &Params) -> io::Result<()> {
    let current = try!(current_dir());
    let dir = p.location.unwrap_or(&current);
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
