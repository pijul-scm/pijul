extern crate libc;
#[macro_use] extern crate pijul;
use pijul::repository::*;

fn main() {
    let x=
        with_repository!("/tmp/test\0",env,txn,{
            let rep=open_repository(txn);
            rep
        });
    match x {
        Ok(_)=>println!("ok"),
        Err(e)=>println!("err:{}",e)
    }
}
