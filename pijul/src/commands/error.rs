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
extern crate libpijul;
use std::io;
use std::error;
use std::fmt;
use std::string;
extern crate ssh;
extern crate rustc_serialize;
extern crate hyper;
extern crate toml;
#[derive(Debug)]
pub enum Error{
    NotInARepository,
    InARepository,
    IO(io::Error),
    Repository(libpijul::error::Error),
    NotEnoughArguments,
    MoveTargetNotDirectory,
    UTF8(string::FromUtf8Error),
    Hex(rustc_serialize::hex::FromHexError),
    SSH(ssh::Error),
    SSHUnknownServer(ssh::ServerKnown),
    Hyper(hyper::error::Error),
    MetaDecoding,
    MissingRemoteRepository,
    PatchNotFound(String,String),
    RemoteApplyFailed(String,i32,String),
    RemoteInitFailed(String,i32,String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::NotInARepository => write!(f, "Not in a repository"),
            Error::InARepository => write!(f, "In a repository"),
            Error::IO(ref err) => write!(f, "IO error: {}", err),
            Error::Repository(ref err) => write!(f, "Repository error: {}", err),
            Error::NotEnoughArguments => write!(f, "Not enough arguments"),
            Error::MoveTargetNotDirectory => write!(f, "Target of mv is not a directory"),
            Error::SSH(ref err) => write!(f, "SSH: {}",err),
            Error::SSHUnknownServer(ref err) => write!(f, "SSH: unable to identify server, {:?}",err),
            Error::Hex(ref err) => write!(f, "Hex: {}",err),
            Error::Hyper(ref err) => write!(f, "Hyper: {}",err),
            Error::UTF8(ref err) => write!(f, "UTF8Error: {}",err),
            Error::MetaDecoding => write!(f, "MetaDecoding"),
            Error::MissingRemoteRepository => write!(f, "Missing remote repository"),
            Error::PatchNotFound(ref path,ref hash) => write!(f, "Patch {} not found in {}", hash, path),
            Error::RemoteApplyFailed(ref id,ref code,ref msg) => write!(f, "Remote apply to {} failed with code {} and message:\n{} ", id, code, msg),
            Error::RemoteInitFailed(ref id,ref code,ref msg) => write!(f, "Remote apply to {} failed with code {} and message:\n{} ", id, code, msg),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::NotInARepository => "Not in a repository",
            Error::InARepository => "In a repository",
            Error::IO(ref err) => error::Error::description(err),
            Error::Repository(ref err) => libpijul::error::Error::description(err),
            Error::NotEnoughArguments => "Not enough arguments",
            Error::MoveTargetNotDirectory => "Target of mv is not a directory",
            Error::SSH(ref err) => err.description(),
            Error::SSHUnknownServer(_) => "SSH: unable to identify server",
            Error::Hex(ref err) => err.description(),
            Error::Hyper(ref err) => err.description(),
            Error::UTF8(ref err) => err.description(),
            Error::MetaDecoding => "Error in the decoding of metadata",
            Error::MissingRemoteRepository => "Missing remote repository",
            Error::PatchNotFound(_,_) => "Patch not found",
            Error::RemoteApplyFailed(_,_,_) => "Remote apply failed",
            Error::RemoteInitFailed(_,_,_) => "Remote init failed",
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::IO(ref err) => Some(err),
            Error::Repository(ref err) => Some(err),
            Error::NotInARepository => None,
            Error::InARepository => None,
            Error::NotEnoughArguments => None,
            Error::MoveTargetNotDirectory => None,
            Error::SSH(ref err) => Some(err),
            Error::SSHUnknownServer(_) => None,
            Error::Hex(ref err) => Some(err),
            Error::Hyper(ref err) => Some(err),
            Error::UTF8(ref err) => Some(err),
            Error::MetaDecoding => None,
            Error::MissingRemoteRepository => None,
            Error::PatchNotFound(_,_) => None,
            Error::RemoteApplyFailed(_,_,_) => None,
            Error::RemoteInitFailed(_,_,_) => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IO(err)
    }
}
impl From<ssh::Error> for Error {
    fn from(err: ssh::Error) -> Error {
        Error::SSH(err)
    }
}
impl From<libpijul::error::Error> for Error {
    fn from(err: libpijul::error::Error) -> Error {
        Error::Repository(err)
    }
}
impl From<string::FromUtf8Error> for Error {
    fn from(err: string::FromUtf8Error) -> Error {
        Error::UTF8(err)
    }
}
impl From<rustc_serialize::hex::FromHexError> for Error {
    fn from(err:rustc_serialize::hex::FromHexError) -> Error {
        Error::Hex(err)
    }
}
impl From<hyper::error::Error> for Error {
    fn from(err:hyper::error::Error) -> Error {
        Error::Hyper(err)
    }
}
