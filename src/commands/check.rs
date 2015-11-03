extern crate clap;
use clap::{SubCommand, Arg, ArgMatches};
use std::path::Path;

use commands::StaticSubcommand;
use repository::Repository;

pub struct CheckArgs<'a> {
    pub repository : &'a str
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

pub fn parse_args<'a>(args: &'a ArgMatches) -> CheckArgs<'a>
{
    CheckArgs {repository : args.value_of("repository").unwrap_or(".")}
}

pub fn run(args: &CheckArgs) -> Result<(),i32> {
    let mut repo_base = String::from(args.repository);
    repo_base.push_str("\0");
    let repo_base = Path::new(&repo_base);
    let _repository = try!(Repository::new(&repo_base));
    println!("Your repo looks alright Ma'am/Sir");
    Ok(())
}
