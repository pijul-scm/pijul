// Copyright Florent Becker and Pierre-Etienne Meunier 2015.
//
// This file is part of Pijul.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.
//
use std::io::prelude::*;
use std::io;
use std;
use std::fmt;
use std::path::PathBuf;
extern crate cbor;
extern crate rustc_serialize;
use self::rustc_serialize::hex::ToHex;
use sanakirja;

#[derive(Debug)]
pub enum Error {
    IO(io::Error),
    Sanakirja(sanakirja::Error),
    AlreadyApplied,
    AlreadyAdded,
    FileNotInRepo(PathBuf),
    Cbor(cbor::CborError),
    NothingToDecode(Option<PathBuf>),
    InternalHashNotFound(Vec<u8>),
    PatchNotFound(PathBuf, String),
    GPG(i32, String),
    Utf8(std::str::Utf8Error),
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::IO(ref err) => write!(f, "IO error: {}", err),
            Error::Sanakirja(ref err) => write!(f, "Sanakirja error: {}", err),
            Error::AlreadyApplied => write!(f, "Patch already applied"),
            Error::AlreadyAdded => write!(f, "File already here"),
            Error::Cbor(ref err) => write!(f, "Cbor error {}", err),
            Error::NothingToDecode(ref path) => write!(f, "Nothing to decode {:?}", path),
            Error::FileNotInRepo(ref path) => write!(f, "File {} not tracked", path.display()),
            Error::InternalHashNotFound(ref hash) => {
                write!(f, "Internal hash {} not found", hash.to_hex())
            }
            Error::PatchNotFound(ref path, ref hash) => {
                write!(f, "Patch {} not found in {}", hash, path.display())
            }
            Error::GPG(ref code, ref s) => write!(f, "GPG returned code {:?}, {:?}", code, s),
            Error::Utf8(ref e) => write!(f, "Utf8 Error {:?}", e),
        }
    }
}

impl std::error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::IO(ref err) => err.description(),
            Error::Sanakirja(ref err) => err.description(),
            Error::AlreadyApplied => "Patch already applied",
            Error::AlreadyAdded => "File already here",
            Error::Cbor(ref err) => err.description(),
            Error::NothingToDecode(_) => "Nothing to decode",
            Error::FileNotInRepo(_) => "Operation on untracked file",
            Error::InternalHashNotFound(_) => "Internal hash not found",
            Error::PatchNotFound(_, _) => "Patch not found",
            Error::GPG(_, _) => "GPG was unsuccessful",
            Error::Utf8(ref e) => e.description(),
        }
    }

    fn cause(&self) -> Option<&std::error::Error> {
        match *self {
            Error::IO(ref err) => Some(err),
            Error::Sanakirja(ref err) => Some(err),
            Error::AlreadyApplied => None,
            Error::AlreadyAdded => None,
            Error::Cbor(ref err) => Some(err),
            Error::NothingToDecode(_) => None,
            Error::FileNotInRepo(_) => None,
            Error::InternalHashNotFound(_) => None,
            Error::PatchNotFound(_, _) => None,
            Error::GPG(_, _) => None,
            Error::Utf8(ref e) => Some(e),
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IO(err)
    }
}

impl From<sanakirja::Error> for Error {
    fn from(err: sanakirja::Error) -> Error {
        Error::Sanakirja(err)
    }
}

impl From<cbor::CborError> for Error {
    fn from(err: cbor::CborError) -> Error {
        Error::Cbor(err)
    }
}
impl From<std::str::Utf8Error> for Error {
    fn from(err: std::str::Utf8Error) -> Error {
        Error::Utf8(err)
    }
}
