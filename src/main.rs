extern crate libc;
#[macro_use]
extern crate clap;

extern crate pijul;

fn main() {
    let app = clap_app!(
        pijul =>
            (version: "0.1.0")
            (author: "Pierre-Étienne Meunier and Florent Becker")
            (about: "Version Control: performant, distributed, easy to use; pick any three")
            );
    let app = app.subcommands(pijul::commands::all_command_invocations());

    let args = app.get_matches();

    match args.subcommand() {
        ("info", Some(info_args)) =>
        {
            let request = pijul::commands::parse_info_args(info_args);
            pijul::commands::get_info(&request)
        },
        ("check", Some(check_args)) =>
        {
            let repository = pijul::commands::parse_check_args(check_args);
            match pijul::commands::check_repo(&repository) {
                Ok(()) => (),
                Err(e) => {
                    println!("err: {}", e);
                    std::process::exit(1)
                }
            }
        },
        ("", None) =>
        {
            let repository = pijul::commands::CheckArgs
                                   {repository : "/tmp/test\0"};
            pijul::commands::check_repo(&repository).unwrap()
        }
        _ => panic!("Incorrect subcommand name"),
    }
}

