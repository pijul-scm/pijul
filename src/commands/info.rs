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
