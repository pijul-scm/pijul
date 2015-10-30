extern crate libc;
#[macro_use]
extern crate pijul;
#[macro_use]
extern crate clap;

use clap::{Arg, App};
use pijul::repository::*;

fn main() {
    let args = clap_app!(
        pijul =>
            (version: "0.1.0")
            (author: "Pierre-Étienne Meunier and Florent Becker")
            (about: "Version Control: performant, distributed, easy to use; pick any three")
            (@arg REPOSITORY: "Location of the repository")
            )
        .get_matches();

    let repository = args.value_of("REPOSITORY").unwrap_or("/tmp/test\0");
    match Env::new(repository) {
        Ok (env)=>{
            match Txn::new(env,None,0) {
                Ok(txn)=>{
                    let rep=Repository::new(txn);
                    println!("ok");
                },
                Err(e)=>{
                    println!("err:{}",e)
                }
            }
        },
        Err(e)=>{
            println!("err:{}",e)
        }
    }
}
