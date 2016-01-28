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

#[derive(Debug)]
pub struct Meta {
    pub meta:toml::Table
}
pub const DEFAULT_AUTHORS:&'static str="default_authors";
pub const PULL:&'static str="pull";
pub const PUSH:&'static str="push";
pub const ADDRESS:&'static str="address";
pub const PORT:&'static str="port";

impl Meta {
    pub fn load(r:&Path) -> Result<Meta,Error> {
        let mut str=String::new();
        {
            let mut f=try!(File::open(meta_file(r)));
            try!(f.read_to_string(&mut str));
        }
        println!("{:?}",str);
        let mut parser = toml::Parser::new(&str);
        match parser.parse() {
            Some(table) => Ok(Meta { meta:table }),
            None => Err(Error::MetaDecoding)
        }
    }
    pub fn new()->Meta {
        Meta { meta:BTreeMap::new() }
    }
    pub fn default_authors(&self)->Option<Vec<BTreeMap<String,Value>>> {
        self.meta.get(DEFAULT_AUTHORS).and_then(|x| { toml::decode(x.clone()) })
    }
    pub fn set_default_authors(&mut self,authors:&Vec<BTreeMap<String,Value>>) {
        let mut e=toml::Encoder::new();
        authors.encode(&mut e).unwrap();
        self.meta.insert(DEFAULT_AUTHORS.to_string(),toml::Value::Table(e.toml));
    }
    pub fn set_default_repository(&mut self,key:&str,repo:toml::Value) {
        self.meta.insert(key.to_string(),repo);
    }
    pub fn get_default_repository<'a>(&'a self,key:&str)->Option<(&'a str,Option<u64>)> {
        self.meta.get(key).and_then(|x| {
            match x {
                &toml::Value::Table(ref t)=>
                    match (t.get(ADDRESS),t.get(PORT)) {
                        (Some(&toml::Value::String(ref x)),None)=> Some((&x[..],None)),
                        (Some(&toml::Value::String(ref x)),Some(&toml::Value::Integer(i)))=> Some((&x[..],Some(i as u64))),
                        _=>None
                    },
                &toml::Value::String(ref s)=>Some((&s[..],None)),
                _=>None
            }
        })
    }
    pub fn save(self,r:&Path)->Result<(),Error> {
        let mut f=try!(File::create(meta_file(r)));
        let s:String= toml::Value::Table(self.meta).to_string();
        try!(f.write_all(s.as_bytes()));
        Ok(())
    }
}
