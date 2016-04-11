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

use std::path::{Path,PathBuf,MAIN_SEPARATOR};
use std::fs::{metadata};

use std::io::{BufWriter,BufReader,Read,Write,BufRead};
use std::fs::File;
use std::str::{from_utf8};

use std;
extern crate crypto;
use self::crypto::digest::Digest;
use self::crypto::sha2::Sha512;
use std::collections::HashSet;
extern crate rand;
extern crate libc;
use self::libc::{memcmp,c_void,size_t};
use file_operations::Inode;
use std::collections::HashMap;

pub type Flag=u8;

use error::Error;

extern crate rustc_serialize;
use self::rustc_serialize::{Encodable,Decodable};
use self::rustc_serialize::hex::ToHex;

extern crate time;

extern crate cbor;

extern crate flate2;

use std::collections::BTreeMap;
use super::fs_representation::{patch_path};
use std::process::{Command};

pub type FileIndex = HashMap<LocalKey, Inode>;

#[derive(Debug,Clone,PartialEq,RustcEncodable,RustcDecodable)]
pub enum Value {
    String(String)
}

#[derive(Debug,RustcEncodable,RustcDecodable)]
pub struct Edge {
    pub from:ExternalKey,
    pub to:ExternalKey,
    pub introduced_by:ExternalHash,
}


#[derive(Debug,RustcEncodable,RustcDecodable)]
pub enum Change {
    NewNodes{
        up_context:Vec<ExternalKey>,
        down_context:Vec<ExternalKey>,
        flag:Flag,
        line_num:u32,
        nodes:Vec<Vec<u8>>
    },
    Edges{ flag:Flag,
           edges:Vec<Edge> }
}

#[derive(Debug,RustcEncodable,RustcDecodable)]
pub struct Patch {
    pub authors:Vec<BTreeMap<String,Value>>,
    pub name:String,
    pub description:Option<String>,
    pub timestamp:String,
    pub dependencies:HashSet<ExternalHash>,
    pub changes:Vec<Change>
}

impl Patch {

    pub fn new(authors:Vec<BTreeMap<String,Value>>,name:String,description:Option<String>,timestamp:self::time::Tm,changes:Vec<Change>)->Patch {
        let deps=dependencies(&changes);
        Patch {
            authors:authors,
            name:name,
            description:description,
            timestamp:format!("{}",timestamp.rfc3339()),
            changes:changes,
            dependencies:deps
        }
    }
    pub fn empty()->Patch {
        Patch { authors:vec!(),name:"".to_string(),description:None,
                timestamp:format!("{}",self::time::now().rfc3339()),
                changes:vec!(), dependencies:HashSet::new() }
    }

    pub fn from_repository(p:&Path,i:&[u8])->Result<Patch,Error> {
        let p=p.join(&patch_path(i,MAIN_SEPARATOR));
        let mut file=try!(File::open(&p));
        Patch::from_reader(&mut file,Some(&p))
    }
    pub fn from_reader<R>(r:R,p:Option<&Path>)->Result<Patch,Error> where R:Read {
        let d=try!(flate2::read::GzDecoder::new(r));
        let mut d=cbor::Decoder::from_reader(d);
        if let Some(d)=d.decode().next() {
            Ok(try!(d))
        } else {
            Err(Error::NothingToDecode(p.and_then(|p| Some(p.to_path_buf()))))
        }
    }

    pub fn to_writer<W>(&self,w:&mut W)->Result<(),Error> where W:Write {
        let e = flate2::write::GzEncoder::new(w,flate2::Compression::Best);
        let mut e = cbor::Encoder::from_writer(e);
        try!(self.encode(&mut e));
        //try!(bincode::rustc_serialize::encode_into(self,w,SizeLimit::Infinite).map_err(Error::PatchEncoding));
        Ok(())
    }
    pub fn save(&self,dir:&Path)->Result<Vec<u8>,Error>{
        debug!("saving patch");
        let mut name:[u8;20]=[0;20]; // random name initially
        fn make_name(dir:&Path,name:&mut [u8])->std::path::PathBuf{
            for i in 0..name.len() { let r:u8=rand::random(); name[i] = 97 + (r%26) }
            let tmp=dir.join(std::str::from_utf8(&name[..]).unwrap());
            if std::fs::metadata(&tmp).is_err() { tmp } else { make_name(dir,name) }
        }
        let tmp=make_name(&dir,&mut name);
        {
            let mut buffer = BufWriter::new(try!(File::create(&tmp)));
            try!(self.to_writer(&mut buffer));
        }
        // Sign
        let tmp_gpg=tmp.with_extension("gpg");
        let gpg=Command::new("gpg")
            .arg("--batch")
            .arg("--output")
            .arg(&tmp_gpg)
            .arg("--detach-sig")
            .arg(&tmp)
            .spawn();

        // hash
        let mut hasher = Sha512::new();
        {
            let mut buffer = BufReader::new(try!(File::open(&tmp)));
            loop {
                let len= {
                    let buf=try!(buffer.fill_buf());
                    if buf.len()==0 { break } else {
                        hasher.input(buf);buf.len()
                    }
                };
                buffer.consume(len)
            }
        }
        let mut hash=vec![0;hasher.output_bytes()];
        hasher.result(&mut hash);
        let mut f=dir.join(hash.to_hex());
        if let Ok(true)=gpg.and_then(|mut gpg| {
            let stat=try!(gpg.wait());
            Ok(stat.success())
        }) {
            f.set_extension("cbor.sig");
            try!(std::fs::rename(&tmp_gpg,&f));
        }
        f.set_extension("");
        f.set_extension("cbor.gz");
        try!(std::fs::rename(&tmp,&f));
        Ok(hash)
    }
}


pub fn write_changes(patches:&HashSet<&[u8]>,changes_file:&Path)->Result<(),Error>{
    let file=try!(File::create(changes_file));
    let mut buffer = BufWriter::new(file);
    let mut e = cbor::Encoder::from_writer(&mut buffer);
    try!(patches.encode(&mut e));
    //try!(bincode::rustc_serialize::encode_into(patches,&mut buffer,SizeLimit::Infinite).map_err(Error::PatchEncoding));
    //let encoded=try!(encode(&patches).map_err(Error::Encoder));
    //try!(buffer.write(encoded.as_bytes()).map_err(Error::IO));
    Ok(())
}

pub fn read_changes<R:Read>(r:R,p:Option<&Path>)->Result<HashSet<Vec<u8>>,Error> {
    let mut d=cbor::Decoder::from_reader(r);
    if let Some(d)=d.decode().next() {
        Ok(try!(d))
    } else {
        Err(Error::NothingToDecode(p.and_then(|p| Some(p.to_path_buf()))))
    }
}
pub fn read_changes_from_file(changes_file:&Path)->Result<HashSet<Vec<u8>>,Error> {
    let file=try!(File::open(changes_file));
    let r = BufReader::new(file);
    read_changes(r,Some(changes_file))
}

pub fn dependencies(changes:&[Change])->HashSet<ExternalHash> {
    let mut deps=HashSet::new();
    fn push_dep(deps:&mut HashSet<ExternalHash>,dep:ExternalHash) {
        // don't include ROOT_KEY as a dependency
        debug!(target:"dependencies","dep={}",dep.to_hex());
        if !if dep.len()==HASH_SIZE {unsafe { memcmp(dep.as_ptr() as *const c_void,
                                                     ROOT_KEY.as_ptr() as *const c_void,
                                                     HASH_SIZE as size_t)==0 }} else {false} {
            deps.insert(dep);
        }
    }
    for ch in changes {
        match *ch {
            Change::NewNodes { ref up_context,ref down_context, line_num:_,flag:_,nodes:_ } => {
                for c in up_context.iter().chain(down_context.iter()) {
                    if c.len()>LINE_SIZE { push_dep(&mut deps,c[0..c.len()-LINE_SIZE].to_vec()) }
                }
            },
            Change::Edges{ref edges,..} =>{
                for e in edges {
                    if e.from.len()>LINE_SIZE { push_dep(&mut deps,e.from[0..e.from.len()-LINE_SIZE].to_vec()) }
                    if e.to.len()>LINE_SIZE { push_dep(&mut deps,e.to[0..e.to.len()-LINE_SIZE].to_vec()) }
                    if e.introduced_by.len()>0 { push_dep(&mut deps,e.introduced_by.clone()) }
                }
            }
        }
    }
    deps
}

pub const HASH_SIZE:usize=20; // pub temporaire
pub const LINE_SIZE:usize=4;
pub const KEY_SIZE:usize=HASH_SIZE+LINE_SIZE;
pub const ROOT_KEY:&'static[u8]=&[0;KEY_SIZE];
pub const EDGE_SIZE:usize=1+KEY_SIZE+HASH_SIZE;

pub type LocalKey=Vec<u8>;
pub type ExternalKey=Vec<u8>;
pub type ExternalHash=Vec<u8>;

pub struct InternalKey {
    pub contents : [u8;HASH_SIZE]
}

impl InternalKey {
    pub fn as_slice<'a>(&'a self) ->&'a[u8] {
        &self.contents[..]
    }
    pub fn zero() -> Self {
        InternalKey {contents : [0;HASH_SIZE]}
    }
    
    pub fn to_hex(&self) -> String {
        self.contents.to_hex()
    }

    pub fn from_array(b: [u8;HASH_SIZE]) -> Self {
        unsafe { std::mem::transmute(b) }
    }
    
    pub fn from_slice(b: &[u8]) -> &Self {
        if b.len () >= HASH_SIZE
        {
            unsafe {
                {
                    std::mem::transmute(b.as_ptr())
                }
            }
        }
        else
        {
            panic!("Invalid internal key pointer")
        }
    }
}





use super::backend::*;




/// Gets the external key corresponding to the given key, returning an
/// owned vector. If the key is just a patch id, it returns the
/// corresponding external hash.
pub fn external_key(ext:&Db,key:&[u8])->ExternalKey {
    let mut result= external_hash(ext, &key[0..HASH_SIZE]).to_vec();
    if key.len()==KEY_SIZE { result.extend(&key[HASH_SIZE..KEY_SIZE]) };
    result
}

pub fn external_hash<'a,'b>(ext:&'a Db<'a,'b>,key:&[u8])->&'a [u8] {
    //println!("internal key:{:?}",&key[0..HASH_SIZE]);
    if key.len()>=HASH_SIZE
        && unsafe {memcmp(key.as_ptr() as *const c_void,ROOT_KEY.as_ptr() as *const c_void,HASH_SIZE as size_t)}==0 {
            //println!("is root key");
            &ROOT_KEY[0..HASH_SIZE]
        } else {
            //let ext = repository.db_external();
            match ext.get(&key[0..HASH_SIZE]) {
                Some(pv)=> {
                    pv
                },
                None=>{
                    println!("internal key or hash:{:?}",key);
                    panic!("external hash not found !")
                },
            }
        }
}


pub fn internal_hash<'a>(internal:&'a Db,key:&[u8])->Result<&'a InternalKey,Error> {
    debug!("internal_hash: {}, {}",key.to_hex(), key.len());
    if key.len()==HASH_SIZE
        && unsafe { memcmp(key.as_ptr() as *const c_void,ROOT_KEY.as_ptr() as *const c_void,HASH_SIZE as size_t) }==0 {
            Ok(InternalKey::from_slice(&ROOT_KEY))
        } else {
            match internal.get(key) {
                Some(k)=>Ok(InternalKey::from_slice(&k)),
                None=>Err(Error::InternalHashNotFound(key.to_vec()))
            }
        }
}

/// Create a new internal patch id, register it in the "external" and
/// "internal" bases, and write the result in its second argument
/// ("result").
///
/// When compiled in debug mode, this function is deterministic
/// and returns the last registered patch number, plus one (in big
/// endian binary on HASH_SIZE bytes). Otherwise, it returns a
/// random patch number not yet registered.
pub fn new_internal<T>(repository:&Transaction<T>) -> InternalKey {
    let mut result = InternalKey::zero();
    /*
    if cfg!(debug_assertions){
        let curs=self.txn.cursor(self.dbi_external).unwrap();
        if let Ok((k,_))=curs.get(b"",None,lmdb::Op::MDB_LAST) {
            unsafe { copy_nonoverlapping(k.as_ptr() as *const c_void,
                                         (&mut result.contents).as_mut_ptr() as *mut c_void, HASH_SIZE) }
        } else {
            for i in 0..HASH_SIZE { result.contents[i]=0 }
        };
        let mut i=HASH_SIZE-1;
        while i>0 && result.contents[i]==0xff {
            result.contents[i]=0;
            i-=1
        }
        if result.contents[i] != 0xff {
            result.contents[i]+=1
        } else {
            panic!("the last patch in the universe has arrived")
        }
    } else {
     */
    for i in 0..result.contents.len() { result.contents[i]=rand::random() }
    let ext = repository.db_external();
    loop {
        match ext.get(&result.contents) {
            None=>break,
            _=>{for i in 0..result.contents.len() { result.contents[i]=rand::random() }},
        }
    }
    result
}

pub fn register_hash<T>(repository:&mut Transaction<T>,internal:&InternalKey,external:&[u8]) -> Result<(),Error>{
    debug!(target:"apply","registering patch\n  {}\n  as\n  {}",
           external.to_hex(),internal.to_hex());
    let mut db_external = repository.db_external();
    let mut db_internal = repository.db_internal();
    try!(db_external.put(&internal.contents,external));
    try!(db_internal.put(external,&internal.contents));
    Ok(())
}
