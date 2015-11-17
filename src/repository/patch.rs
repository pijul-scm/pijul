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
extern crate rustc_serialize;
use self::rustc_serialize::json::{encode,decode};
use std::io::{Read,Write};
use std::io;
use std::fmt;
use std::error;
use std::str::{from_utf8};
pub type LocalKey=Vec<u8>;
pub type ExternalKey=Vec<u8>;
pub type ExternalHash=Vec<u8>;
pub type Flag=u8;

#[derive(Debug,RustcEncodable,RustcDecodable)]
pub struct Edge {
    pub from:ExternalKey,
    pub to:ExternalKey,
    pub flag:Flag,
    pub introduced_by:ExternalHash
}


#[derive(Debug,RustcEncodable,RustcDecodable)]
pub enum Change {
    NewNodes{
        up_context:Vec<ExternalKey>,
        down_context:Vec<ExternalKey>,
        flag:Flag,
        line_num:usize,
        nodes:Vec<Vec<u8>>
    },
    Edges{ edges:Vec<Edge> }
}


#[derive(Debug,RustcEncodable,RustcDecodable)]
pub struct Patch {
    pub changes:Vec<Change>,
    pub dependencies:Vec<ExternalHash>
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



pub fn from_reader<R>(r:&mut R)->Result<Patch,Error> where R:Read {
    let mut s=Vec::new();
    try!(r.read_to_end(&mut s).map_err(Error::IO));
    let ss=from_utf8(&s).unwrap();
    let dec:Patch=try!(decode(ss).map_err(Error::Decoder));
    Ok(dec)
}

pub fn to_writer<W>(w:&mut W,p:&Patch)->Result<(),Error> where W:Write {
    let encoded=try!(encode(&p).map_err(Error::Encoder));
    try!(w.write(encoded.as_bytes()).map_err(Error::IO));
    Ok(())
}
