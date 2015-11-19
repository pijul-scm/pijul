extern crate gcc;
use std::process::{Command};
use std::io::{Write};
use std::fs::{File,remove_file};
use std::env;
fn main() {
    {
        let mut f = File::create("empty.c").unwrap();
        f.write(b"#include<lmdb.h>\nint main(){}").unwrap();
    }
    let has_lmdb=
        match Command::new("gcc").arg("-llmdb").arg("empty.c").status() {
            Ok(e)=>e.success(),
            _=>false
        };
    remove_file("empty.c").unwrap();
    if has_lmdb {
        gcc::compile_library("libretrieve.a",&["src/retrieve.c"]);
        println!("cargo:rustc-flags=-l dylib=lmdb")
    } else {
        let target = env::var("TARGET").unwrap();
        let windows = target.contains("windows");
        if windows {
            gcc::Config::new()
                .file("src/repository/midl.c")
                .file("src/repository/mdb.c")
                .define("_WIN32",None)
                .include("src/repository")
                .compile("liblmdb.a");
            gcc::Config::new()
                .file("src/repository/retrieve.c")
                .include("src/repository")
                .compile("libretrieve.a");
        } else {
            gcc::Config::new()
                .file("src/repository/midl.c")
                .file("src/repository/mdb.c")
                .include("src/repository")
                .compile("liblmdb.a");
            gcc::Config::new()
                .file("src/repository/retrieve.c")
                .include("src/repository")
                .compile("libretrieve.a");
        }
    }
}
