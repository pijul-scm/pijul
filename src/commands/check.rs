extern crate clap;
use clap::{SubCommand, Arg, ArgMatches};
use std::path::Path;
use std::io::{Error, ErrorKind};

use commands::StaticSubcommand;
use repository::Repository;
use repository::fs_representation::{find_repo_root,repo_dir};

pub struct Params<'a> {
    pub repository : &'a Path
}

pub fn invocation() -> StaticSubcommand {
    return
        SubCommand::with_name("check")
        .about("Check the sanity of a repository")
        .arg(Arg::with_name("repository")
             .index(1)
             .help("The repository to check, defaults to the current directory.")
             .required(false)
             );
}

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a>
{
    Params {repository : Path::new(args.value_of("repository").unwrap_or("."))}
}

pub fn run(args: &Params) -> Result<(),Error> {
    match find_repo_root(args.repository)
    {
        Some(repo_base) => {
            let _repository = try!(Repository::new(&repo_dir(&repo_base)));
            println!("Your repo looks alright Ma'am/Sir");
            Ok(())
        },

        None => {
            Err(Error::new(ErrorKind::NotFound, "not in a repository"))
        }
    }
}
