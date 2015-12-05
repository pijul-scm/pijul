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
use std::io::prelude::*;
use std::io;
use std;
use std::fmt;
use std::path::{PathBuf};
//extern crate serde_cbor;
extern crate bincode;

#[derive(Debug)]
pub enum Error{
    IoError(io::Error),
    AlreadyApplied,
    AlreadyAdded,
    FileNotInRepo(PathBuf),
    //PatchDecoding(serde_cbor::error::Error),
    //PatchEncoding(serde_cbor::error::Error)
    PatchDecoding(bincode::serde::DeserializeError),
    PatchEncoding(bincode::serde::SerializeError)
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::IoError(ref err) => write!(f, "IO error: {}", err),
            Error::AlreadyApplied => write!(f, "Patch already applied"),
            Error::AlreadyAdded => write!(f, "File already here"),
            Error::PatchEncoding(ref err) => write!(f, "Patch encoding error {}",err),
            Error::PatchDecoding(ref err) => write!(f, "Patch decoding error {}",err),
            Error::FileNotInRepo(ref path) => write!(f, "File {} not tracked", path.display())
        }
    }
}

impl std::error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::IoError(ref err) => err.description(),
            Error::AlreadyApplied => "Patch already applied",
            Error::AlreadyAdded => "File already here",
            Error::PatchEncoding(ref err) => err.description(),
            Error::PatchDecoding(ref err) => err.description(),
            Error::FileNotInRepo(_) => "Operation on untracked file"
        }
    }

    fn cause(&self) -> Option<&std::error::Error> {
        match *self {
            Error::IoError(ref err) => Some(err),
            Error::AlreadyApplied => None,
            Error::AlreadyAdded => None,
            Error::PatchEncoding(ref err) => Some(err),
            Error::PatchDecoding(ref err) => Some(err),
            Error::FileNotInRepo(_) => None
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IoError(err)
    }
}

