extern crate gcc;
use std::process::{Command};
use std::io::{Write};
use std::fs::{File,remove_file};
use std::env;

extern crate syntex;
extern crate serde_codegen;

use std::path::Path;


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
        } else {
            gcc::Config::new()
                .file("src/repository/midl.c")
                .file("src/repository/mdb.c")
                .include("src/repository")
                .compile("liblmdb.a");
        }
    }


    let out_dir = env::var_os("OUT_DIR").unwrap();

    let src = Path::new("src/patch.rs.in");
    let dst = Path::new(&out_dir).join("patch.rs");

    let mut registry = syntex::Registry::new();

    serde_codegen::register(&mut registry);
    registry.expand("", &src, &dst).unwrap();
}
