extern crate libc;
#[macro_use]
extern crate clap;

extern crate pijul;

use std::path::Path;

macro_rules! pijul_subcommand_dispatch {
    ($p:expr => $($subcommand_name:expr => $subcommand:ident),*) => {{
        match $p {
            $(($subcommand_name, Some(args)) =>
             {
                 let params = pijul::commands::$subcommand::parse_args(args);
                 match pijul::commands::$subcommand::run(&params) {
                     Ok(_) => (),
                     Err(e) => {
                         println!("error: {}", e);
                         std::process::exit(1)
                     }
                 }
             }
              ),*
                ("", None) => {
                    let repository = pijul::commands::check::Params
                    {repository : Path::new("/tmp/test")};
                    pijul::commands::check::run(&repository).unwrap()
                },
            _ => panic!("Incorrect subcommand name")
        }
    }}
}

fn main() {
    let app = clap_app!(
        pijul =>
            (version: "0.1.0")
            (author: "Pierre-Étienne Meunier and Florent Becker")
            (about: "Version Control: performant, distributed, easy to use; pick any three")
            );
    let app = app.subcommands(pijul::commands::all_command_invocations());

    let args = app.get_matches();

    pijul_subcommand_dispatch!(args.subcommand() =>
                               "info" => info,
                               "check" => check,
                               "init" => init,
                               "record" => record);
}

