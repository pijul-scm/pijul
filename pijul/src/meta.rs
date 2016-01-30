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
extern crate toml;
extern crate libpijul;
use self::libpijul::fs_representation::{meta_file};
use self::libpijul::patch::Value;
use std::path::Path;
use std::collections::BTreeMap;
use commands::error::Error;
use std::fs::File;
use std::io::{Read,Write};
extern crate rustc_serialize;
use self::rustc_serialize::Encodable;
#[derive(Debug,RustcEncodable,RustcDecodable)]
pub enum Repository {
    String(String),
    SSH { address:String, port:u64 }
}


#[derive(Debug,RustcEncodable,RustcDecodable)]
pub struct Meta {
    pub default_authors:Vec<BTreeMap<String,Value>>,
    pub pull:Option<Repository>,
    pub push:Option<Repository>
}

impl Meta {
    pub fn load(r:&Path) -> Result<Meta,Error> {
        let mut str=String::new();
        {
            let mut f=try!(File::open(meta_file(r)));
            try!(f.read_to_string(&mut str));
        }
        match toml::decode_str(&str) {
            Some(m)=>Ok(m),
            None=>Err(Error::MetaDecoding)
        }
    }
    pub fn new()->Meta {
        Meta { default_authors:Vec::new(),pull:None,push:None }
    }
    pub fn save(self,r:&Path)->Result<(),Error> {
        let mut f=try!(File::create(meta_file(r)));
        let s:String= toml::encode_str(&self);
        try!(f.write_all(s.as_bytes()));
        Ok(())
    }
}
