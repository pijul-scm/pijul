extern crate libc;
extern crate pijul;
use pijul::repository::*;

fn main() {
    let x=
        with_repository("/tmp/test\0",|_,txn| {
            let rep=open_repository(txn);
            rep
        });
    match x {
        Ok(_)=>println!("ok"),
        Err(e)=>println!("err:{}",e)
    }
}
