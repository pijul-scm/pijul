extern crate clap;

pub type StaticSubcommand = clap::App<'static, 'static, 'static, 'static, 'static, 'static>;

mod fs_operation;
mod remote;
mod escape;
mod ask;

pub mod info;
pub mod check;
pub mod init;
pub mod record;
pub mod add;
pub mod pull;
pub mod push;
pub mod apply;
pub mod clone;
pub mod remove;
pub mod mv;
pub mod ls;
pub mod revert;
#[cfg(test)]
mod test;
pub mod error;
use std::fs::{canonicalize};
use std::path::{Path,PathBuf};
use std::env::{current_dir};

extern crate libpijul;
use self::error::Error;

pub fn all_command_invocations() -> Vec<StaticSubcommand> {
    return vec![
        check::invocation(),
        info::invocation(),
        init::invocation(),
        record::invocation(),
        add::invocation(),
        pull::invocation(),
        push::invocation(),
        apply::invocation(),
        clone::invocation(),
        remove::invocation(),
        mv::invocation(),
        ls::invocation(),
        revert::invocation()
        ];
}

pub fn get_wd(repository_path:Option<&Path>)->Result<PathBuf,Error> {
    match repository_path {
        None =>{
            let p=try!(canonicalize(try!(current_dir())));
            Ok(p)
        },
        Some(a) if a.is_relative() => {
            let mut p=try!(canonicalize(try!(current_dir())));
            p.push(a);
            Ok(p)
        },
        Some(a)=>{
            let p=try!(canonicalize(a));
            Ok(p)
        }
    }
}
