extern crate clap;

use std::path::Path;

use clap::{SubCommand, Arg};
use clap::ArgMatches;

use repository::*;

pub type StaticSubcommand = clap::App<'static, 'static, 'static, 'static, 'static, 'static>;

pub struct InfoArgs<'a> {
    pub directory : &'a str
}

fn info_invocation() -> StaticSubcommand {
    return 
        SubCommand::with_name("info")
        .about("Get information about the current repository, if any")
        .arg(Arg::with_name("dir")
             .index(1)
             .help("Pijul info will be given about this directory.")
             .required(false)
             );
}

pub fn parse_info_args<'a>(args : &'a ArgMatches) -> InfoArgs<'a>
{
    match args.value_of("dir") {
        Some(dir) => InfoArgs {directory : dir},
        None => InfoArgs {directory : "."}
    }
}

pub fn get_info(request: &InfoArgs) -> () {
    println!("info about {}", request.directory);
}

pub struct CheckArgs<'a> {
    pub repository : &'a str
}

fn check_invocation() -> StaticSubcommand {
    return
        SubCommand::with_name("check")
        .about("Check the sanity of a repository")
        .arg(Arg::with_name("repository")
             .index(1)
             .help("The repository to check, defaults to the current directory.")
             .required(false)
             );
}

pub fn parse_check_args<'a>(args: &'a ArgMatches) -> CheckArgs<'a>
{
    match args.value_of("repository") {
        Some(x) => CheckArgs {repository : x},
        None => CheckArgs {repository : "."}
    }
}

pub fn check_repo(args: &CheckArgs) -> Result<(),i32> {
    let mut repo_base = String::from(args.repository);
    repo_base.push_str("\0");
    let repo_base = Path::new(&repo_base);
    let _repository = try!(Repository::new(&repo_base));
    println!("Your repo looks alright Ma'am/Sir");
    Ok(())
}

pub fn all_command_invocations() -> Vec<StaticSubcommand> {
    return vec![
        info_invocation(),
        check_invocation()
        ];
}

