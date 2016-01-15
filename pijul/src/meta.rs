extern crate toml;
extern crate libpijul;
use self::libpijul::fs_representation::{meta_file};
use std::path::Path;
use std::collections::BTreeMap;
use commands::error::Error;
use std::fs::File;
use std::io::{Read,Write};

#[derive(Debug,RustcEncodable,RustcDecodable)]
pub struct Meta {
    pub authors:Vec<String>,
    pub default_pull:Option<String>,
    pub default_push:Option<String>,
}


impl Meta {
    pub fn load(r:&Path) -> Result<Meta,Error> {
        let mut str=String::new();
        {
            let mut f=try!(File::open(meta_file(r)));
            try!(f.read_to_string(&mut str));
        }
        println!("{:?}",str);
        match toml::decode_str(&str) {
            Some(table)=>Ok(table),
            None=>{
                Err(Error::MetaDecoding)
            }
        }
    }
    pub fn new()->Meta {
        Meta { authors:vec!(),default_pull:None,default_push:None  }
    }
    pub fn save(&self,r:&Path)->Result<(),Error> {
        let mut f=try!(File::create(meta_file(r)));
        let s:String=toml::encode_str(self).to_string();
        try!(f.write_all(s.as_bytes()));
        println!("{:?}",s);
        Ok(())
    }
}
