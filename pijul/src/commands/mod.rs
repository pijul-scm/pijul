/*
  Copyright Florent Becker and Pierre-Etienne Meunier 2015.

  This file is part of Pijul.

  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU Affero General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU Affero General Public License for more details.

  You should have received a copy of the GNU Affero General Public License
  along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
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
pub mod login;
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
        revert::invocation(),
        login::invocation()
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
