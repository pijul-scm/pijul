extern crate clap;

use std;
use std::path::Path;
use clap::{SubCommand, Arg, ArgMatches};

use commands;
use repository::fs_representation::find_repo_root;

pub struct InfoArgs<'a> {
    pub directory : &'a str
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

pub fn parse_args<'a>(args : &'a ArgMatches) -> InfoArgs<'a>
{
    match args.value_of("dir") {
        Some(dir) => InfoArgs {directory : dir},
        None => InfoArgs {directory : "."}
    }
}

pub fn run(request: &InfoArgs) -> () {
    match find_repo_root(Path::new(request.directory)) {
        Some(r) => println!("Current repository location: '{}'", r.display()),
        None => {
            println!("not in a repository");
            std::process::exit(1)
        }
    }
}
