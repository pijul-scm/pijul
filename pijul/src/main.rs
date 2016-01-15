#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;

extern crate libpijul;
mod commands;
extern crate time;

extern crate env_logger;
extern crate rustc_serialize;

mod meta;

macro_rules! pijul_subcommand_dispatch {
    ($p:expr => $($subcommand_name:expr => $subcommand:ident),*) => {{
        match $p {
            $(($subcommand_name, Some(args)) =>
             {
                 let params = commands::$subcommand::parse_args(args);
                 match commands::$subcommand::run(&params) {
                     Ok(_) => (),
                     Err(e) => {
                         println!("error: {}", e);
                         std::process::exit(1)
                     }
                 }
             }
              ),*
                ("", None) => {},
            _ => panic!("Incorrect subcommand name")
        }
    }}
}

fn main() {
    env_logger::init().unwrap();
    let time0=time::precise_time_s();
    let app = clap_app!(
        pijul =>
            (version: "0.1.0")
            (author: "Pierre-Étienne Meunier and Florent Becker")
            (about: "Version Control: performant, distributed, easy to use; pick any three")
            );
    let app = app.subcommands(commands::all_command_invocations());

    let args = app.get_matches();
    pijul_subcommand_dispatch!(args.subcommand() =>
                               "info" => info,
                               "check" => check,
                               "init" => init,
                               "add" => add,
                               "record" => record,
                               "pull" => pull,
                               "push" => push,
                               "apply" => apply,
                               "clone" => clone,
                               "remove" => remove,
                               "mv" => mv,
                               "ls" => ls,
                               "revert" => revert
                               );
    let time1=time::precise_time_s();
    info!(target:"pijul","whole command took: {}", time1-time0);
}

