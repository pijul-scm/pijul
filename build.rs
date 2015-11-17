extern crate gcc;
use std::process::{Command};
use std::io::{Write};
use std::fs::{File,remove_file};
fn main() {
    gcc::compile_library("libretrieve.a",&["src/repository/retrieve.c"]);
    {
        let mut f = File::create("empty.c").unwrap();
        f.write(b"int main(){}").unwrap();
    }
    let has_lmdb=
        match Command::new("gcc").arg("-llmdb").arg("empty.c").status() {
            Ok(e)=>e.success(),
            _=>false
        };
    remove_file("empty.c").unwrap();
    if has_lmdb {
        println!("cargo:rustc-flags=-l dylib=lmdb")
    } else {
        gcc::compile_library("liblmdb.a",&["src/repository/midl.c","src/repository/mdb.c"])
    }
}
