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

use std::path::Path;
use std::path::PathBuf;
use std::fs::{metadata,create_dir_all};
use std::io;
extern crate rustc_serialize;
use self::rustc_serialize::json::{encode,decode};
use std::io::{BufWriter,BufReader,Read,Write};
use std::fs::File;
use std::str::{from_utf8};
use std::fmt;
use std::error;

pub fn pijul_dir_name() -> &'static Path {
    return Path::new(".pijul")
}

pub fn repo_dir(p : &Path) -> PathBuf {
    return p.join(pijul_dir_name())
}

pub fn pristine_dir(p : &Path) -> PathBuf {
    return p.join(pijul_dir_name()).join("pristine")
}
pub fn patches_dir(p : &Path) -> PathBuf {
    return p.join(pijul_dir_name()).join("patches")
}
pub fn branch_changes_file(p : &Path, b: &[u8]) -> PathBuf {
    let changes=String::from("changes.") + &to_hex(b)[..];
    return p.join(pijul_dir_name()).join(changes)
}

pub fn find_repo_root(dir : &Path) -> Option<&Path> {
    let pijul_dir = repo_dir(dir);
    match metadata(pijul_dir) {
        Ok (attr) =>
            if attr.is_dir() {Some(dir)} else {None},
        Err(_) =>
            dir.parent().and_then(find_repo_root)
    }
}

pub fn create(dir : &Path) -> io::Result<()> {
    let mut repo_dir = repo_dir(dir);
    try!(create_dir_all(&repo_dir));
    repo_dir.push("pristine");
    try!(create_dir_all(&repo_dir));
    repo_dir.pop();
    repo_dir.push("patches");
    try!(create_dir_all(&repo_dir));
    Ok(())
}



#[derive(Debug)]
pub enum Error{
    IO(io::Error),
    Encoder(rustc_serialize::json::EncoderError),
    Decoder(rustc_serialize::json::DecoderError)
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::IO(ref err) => write!(f, "IO error: {}", err),
            Error::Encoder(ref err) => write!(f, "Encoder error: {}", err),
            Error::Decoder(ref err) => write!(f, "Decoder error: {}", err)
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::IO(ref err) => err.description(),
            Error::Encoder(ref err) => err.description(),
            Error::Decoder(ref err) => err.description()
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::IO(ref err) => Some(err),
            Error::Encoder(ref err) => Some(err),
            Error::Decoder(ref err) => Some(err)
        }
    }
}


pub fn write_changes(patches:&Vec<Vec<u8>>,changes_file:&Path)->Result<(),Error>{
    let file=try!(File::create(changes_file).map_err(Error::IO));
    let mut buffer = BufWriter::new(file);
    //try!(serde_cbor::ser::to_writer(&mut buffer,&patches).map_err(Error::Serde));
    let encoded=try!(encode(&patches).map_err(Error::Encoder));
    try!(buffer.write(encoded.as_bytes()).map_err(Error::IO));
    Ok(())
}

pub fn read_changes(changes_file:&Path)->Result<Vec<Vec<u8>>,Error> {
    let file=try!(File::open(changes_file).map_err(Error::IO));
    let mut r = BufReader::new(file);
    let mut s=Vec::new();
    try!(r.read_to_end(&mut s).map_err(Error::IO));
    let ss=from_utf8(&s).unwrap();
    let dec:Vec<Vec<u8>>=try!(decode(ss).map_err(Error::Decoder));
    Ok(dec)
}

// The following is from the rust project, see http://rust-lang.org/COPYRIGHT.
pub fn to_hex(x:&[u8]) -> String {
    let mut v = Vec::with_capacity(x.len() * 2);
    for &byte in x.iter() {
        v.push(CHARS[(byte >> 4) as usize]);
        v.push(CHARS[(byte & 0xf) as usize]);
    }

    unsafe {
        String::from_utf8_unchecked(v)
    }
}
static CHARS: &'static[u8] = b"0123456789abcdef";
