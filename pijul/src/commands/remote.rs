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
use clap::{ArgMatches};

extern crate libpijul;
use self::libpijul::{Repository,DEFAULT_BRANCH};
use self::libpijul::patch::{Patch,read_changes_from_file,read_changes,HASH_SIZE};
use self::libpijul::fs_representation::{repo_dir, pristine_dir, patches_dir, branch_changes_base_path,branch_changes_file,to_hex,PIJUL_DIR_NAME,PATCHES_DIR_NAME};
use std::path::{Path,PathBuf};
use std::io::{BufWriter,BufReader};
use std::collections::hash_set::{HashSet};
use std::collections::hash_map::{HashMap};
use std::fs::{File,hard_link,copy,metadata};

use super::error::Error;
use std::str::{from_utf8};
extern crate ssh;
use std::io::prelude::*;
extern crate regex;
use self::regex::Regex;

extern crate rustc_serialize;
use self::rustc_serialize::hex::{ToHex};

use super::escape::unix::escape;
use std::borrow::Cow;
use super::init;
use std::collections::hash_set::Iter;


extern crate hyper;

#[derive(Debug)]
pub enum Remote<'a> {
    Ssh { user:Option<&'a str>, host:&'a str, port:Option<u16>, path:&'a Path },
    Uri { uri:&'a str },
    Local { path:&'a Path }
}

pub enum Session<'a> {
    Ssh {
        path:&'a Path,
        session:ssh::Session
    },
    Uri { uri:&'a str,
          client:hyper::Client },
    Local{path:&'a Path},
}


fn ssh_recv_file<'b>(s:&'b mut ssh::Session,p:&Path)->Result<Option<ssh::Scp<'b>>,ssh::Error>{
    debug!("recv_file {:?}",p);
    let mut scp=s.scp_new(ssh::READ,p).unwrap();
    scp.init().unwrap();
    loop {
        let req=scp.pull_request();
        debug!("req={:?}",req);
        match req {
            Ok(ssh::Request::NEWFILE)=>{
                scp.accept_request().unwrap();
                scp.reader();
                return Ok(Some(scp))
            },
            Ok(ssh::Request::WARNING)=>{
                info!("Warning: {}",from_utf8(scp.request_get_warning().unwrap()).unwrap());
            },
            Ok(ssh::Request::EOF)=>{
                return Ok(None)
            }
            Ok(e)=>{println!("e={:?}",e);panic!("")},
            Err(e)=>{
                return Err(e)
            }
        }
    }
}

impl <'a> Drop for Session<'a> {
    fn drop(&mut self){
        match *self {
            Session::Ssh{ref mut session,..}=>{let _=session.disconnect();},
            _=>{}
        }
    }
}

// TODO: remplacer upload_patches par upload_apply_patches, qui combine les deux (pas besoin d'etre efficace pour l'instant).

impl<'a> Session<'a> {
    pub fn changes(&mut self,branch:&[u8]) -> Result<HashSet<Vec<u8>>,Error> {
        match *self {
            Session::Ssh{ref path,ref mut session,..}=>{
                let patches_path=branch_changes_file(path,branch);
                let remote_file = try!(ssh_recv_file(session,&patches_path));
                let changes=match remote_file {
                    Some(r)=>try!(read_changes(r)),
                    None=>HashSet::new()
                };
                Ok(changes)
            },
            Session::Local{path} =>{
                let changes_file=branch_changes_file(path,branch);
                Ok(read_changes_from_file(&changes_file).unwrap_or(HashSet::new()))
            },
            Session::Uri {uri,ref mut client} =>{
                let mut uri=uri.to_string();
                uri = uri + "/" + PIJUL_DIR_NAME + "/" + &branch_changes_base_path(DEFAULT_BRANCH.as_bytes());
                let mut res = try!(client.get(&uri)
                                   .header(hyper::header::Connection::close())
                                   .send());
                let changes=read_changes(&mut res).unwrap_or(HashSet::new());
                debug!("http: {:?}",changes);
                Ok(changes)
            },
        }
    }
    pub fn download_patch(&mut self, local_patches:&Path, patch_hash:&[u8])->Result<PathBuf,Error>{
        match *self {
            Session::Local{path}=>{
                let hash=patch_hash.to_hex();
                debug!("local downloading {:?}",hash);
                let remote_file=patches_dir(path).join(&hash).with_extension("cbor");
                let local_file=local_patches.join(&hash).with_extension("cbor");
                if metadata(&local_file).is_err() {
                    debug!("hard linking {:?} to {:?}",remote_file,local_file);
                    try!(hard_link(&remote_file,&local_file).or_else(|_|{
                        copy(&remote_file, &local_file).and_then(|_| Ok(()))
                    }));
                }
                Ok(local_file)
            },
            Session::Ssh{ref path,ref mut session,..}=>{
                let hash=to_hex(patch_hash);
                let local_file=local_patches.join(&hash).with_extension("cbor");
                if metadata(&local_file).is_err() { // If we don't have it yet
                    let remote_file=patches_dir(path).join(&hash).with_extension("cbor");
                    let mut remote_file = try!(ssh_recv_file(session,&remote_file)).unwrap();
                    let mut contents = Vec::new();
                    debug!(target:"pull","downloading file to {:?}",local_file);
                    try!(remote_file.read_to_end(&mut contents));
                    let mut w=BufWriter::new(try!(File::create(&local_file)));
                    try!(w.write_all(&contents))
                }
                Ok(local_file)
            },
            Session::Uri{ref mut client,uri}=>{
                let hash=patch_hash.to_hex();
                let local_file=local_patches.join(&hash).with_extension("cbor");
                if metadata(&local_file).is_err() {
                    let uri = uri.to_string()
                        + "/" + PIJUL_DIR_NAME
                        + "/" + PATCHES_DIR_NAME
                        + "/" + &patch_hash.to_hex() + ".cbor";
                    let mut res = try!(client.get(&uri)
                                       .header(hyper::header::Connection::close())
                                       .send());
                    let mut body=Vec::new();
                    try!(res.read_to_end(&mut body));
                    let mut f=try!(File::create(&local_file));
                    try!(f.write_all(&body));
                    debug!("patch downloaded through http: {:?}",body);
                }
                Ok(local_file)
            }
        }
    }
    // patch hash in binary
    pub fn upload_patches(&mut self, local_patches:&Path, patch_hashes:&HashSet<Vec<u8>>)->Result<(),Error> {
        match *self {
            Session::Ssh { ref mut session, ref path, .. }=> {
                let remote_path=patches_dir(path);
                let mut scp=try!(session.scp_new(ssh::WRITE,&remote_path));
                try!(scp.init());
                for hash in patch_hashes {
                    let remote_patch=remote_path.join(hash.to_hex()).with_extension("cbor");
                    let local_patch=local_patches.join(hash.to_hex()).with_extension("cbor");
                    let mut buf = Vec::new();
                    {
                        let mut f = try!(File::open(&local_patch));
                        try!(f.read_to_end(&mut buf));
                    }
                    try!(scp.push_file(&remote_patch,buf.len(),0o644));
                    try!(scp.write(&buf));
                }
                Ok(())
            },
            Session::Local{path} =>{
                for patch_hash in patch_hashes {
                    let hash=to_hex(patch_hash);
                    let remote_file=patches_dir(path).join(&hash).with_extension("cbor");
                    let local_file=local_patches.join(&hash).with_extension("cbor");
                    if metadata(&remote_file).is_err() {
                        try!(hard_link(&local_file,&remote_file).or_else(|_|{
                            copy(&local_file, &remote_file).and_then(|_| Ok(()))
                        }));
                    }
                }
                Ok(())
            },
            _=>{panic!("upload to URI impossible")}
        }
    }
    // patch hash in binary
    /// Apply patches that have been uploaded.
    pub fn remote_apply(&mut self, patch_hashes:&HashSet<Vec<u8>>)->Result<(),Error> {
        match *self {
            Session::Ssh { ref mut session, ref path, .. }=> {
                let mut s=try!(session.channel_new());
                try!(s.open_session());
                let esc_path=escape(Cow::Borrowed(path.to_str().unwrap()));
                let mut patches="".to_string();
                for i in patch_hashes {
                    patches=patches + " " + &(i.to_hex());
                }
                try!(s.request_exec(format!("cd \"{}\"; pijul apply{}",esc_path, &patches).as_bytes()));
                try!(s.send_eof());
                let mut buf=Vec::new();
                try!(s.stdout().read_to_end(&mut buf));
                if buf.len() > 0 {
                    println!("{}",from_utf8(&buf).unwrap());
                }
                Ok(())
            },
            Session::Local{path} =>{
                let applied_patches:HashSet<Vec<u8>>=try!(self.changes(DEFAULT_BRANCH.as_bytes()));
                apply_patches(path,&patch_hashes,&applied_patches)
            }
            _=>{panic!("remote apply not possible")}
        }
    }
    pub fn remote_init(&mut self)->Result<(),Error> {
        match *self {
            Session::Ssh { ref mut session, ref path, .. }=> {
                let mut s=try!(session.channel_new());
                try!(s.open_session());
                let esc_path=escape(Cow::Borrowed(path.to_str().unwrap()));
                try!(s.request_exec(format!("mkdir -p \"{}\"; cd \"{}\"; pijul init",esc_path,esc_path).as_bytes()));
                try!(s.send_eof());
                let mut buf=Vec::new();
                try!(s.stdout().read_to_end(&mut buf));
                if buf.len() > 0 {
                    println!("{}",from_utf8(&buf).unwrap());
                }
                Ok(())
            },
            Session::Local{path} =>{
                try!(init::run(&init::Params { location:path, allow_nested:false }));
                Ok(())
            }
            _=>{panic!("remote init not possible")}
        }
    }
}



impl <'a>Remote<'a> {
    pub fn session(&self)->Result<Session<'a>,Error> {
        //fn from_remote(remote:&Remote<'a>) -> Result<Session<'a>,Error> {
        match *self {
            Remote::Local{path} => Ok(Session::Local{path:path}),
            Remote::Uri{uri} => Ok(Session::Uri {
                uri:uri,
                client:hyper::Client::new()
            }),
            Remote::Ssh{ref user,ref host,ref port,ref path}=>{
                let mut session = ssh::Session::new().unwrap();
                session.set_host(host).unwrap();
                match *port { None=>{}, Some(ref p)=>try!(session.set_port(*p as usize)) };
                match *user { None=>{}, Some(ref u)=>try!(session.set_username(u)) };
                session.parse_config(None).unwrap();
                try!(session.connect());
                if session.userauth_publickey_auto(None).is_err() {
                    try!(session.userauth_kbdint(None))
                }
                Ok(Session::Ssh { session:session, path:path })
            }
        }
    }
}

pub fn parse_remote<'a>(remote_id:&'a str,args:&'a ArgMatches)->Remote<'a> {
    let ssh=Regex::new(r"^([^:]*):(.*)$").unwrap();
    let uri=Regex::new(r"^([:alpha:]*)://(.*)$").unwrap();
    if uri.is_match(remote_id) {
        let cap=uri.captures(remote_id).unwrap();
        if cap.at(1).unwrap()=="file" { Remote::Local { path:Path::new(cap.at(2).unwrap()) } }
        else { Remote::Uri { uri:remote_id } }
    } else if ssh.is_match(remote_id) {
        let cap=ssh.captures(remote_id).unwrap();
        let port=match args.value_of("port") { Some(x)=>Some(x.parse().unwrap()), None=>None };
        let user_host=cap.at(1).unwrap();

        let (user,host)={
            let ssh_user_host=Regex::new(r"^([^@]*)@(.*)$").unwrap();
            if ssh_user_host.is_match(user_host) {
                let cap=ssh_user_host.captures(user_host).unwrap();
                (Some(cap.at(1).unwrap()),cap.at(2).unwrap())
            } else {
                (None,user_host)
            }
        };
        Remote::Ssh { user:user,host:host, port:port, path:Path::new(cap.at(2).unwrap()) }
    } else {
        Remote::Local { path:Path::new(remote_id) }
    }
}

// Reimplementation of hash_set::Difference (because of unstable features used there)

pub struct Pullable {
    pub local:HashSet<Vec<u8>>,
    pub remote:HashSet<Vec<u8>>
}

pub struct PullableIter<'a> { iter:Iter<'a,Vec<u8>>, pullable:&'a Pullable }

impl Pullable {
    pub fn iter<'a>(&'a self)->PullableIter<'a> {
        PullableIter { iter:self.remote.iter(), pullable:self }
    }
}

impl <'a> Iterator for PullableIter<'a> {
    type Item = &'a Vec<u8>;
    fn next(&mut self)->Option<&'a Vec<u8>> {
        loop {
            match self.iter.next() {
                None=>return None,
                Some(p) if !self.pullable.local.contains(p) => return Some(p),
                _=>{}
            }
        }
    }
}


pub fn pullable_patches<'a>(target:&Path,session:&mut Session<'a>) -> Result<Pullable, Error> {
    let remote_patches:HashSet<Vec<u8>>=try!(session.changes(DEFAULT_BRANCH.as_bytes()));
    let local_patches:HashSet<Vec<u8>>={
        let changes_file=branch_changes_file(target,DEFAULT_BRANCH.as_bytes());
        read_changes_from_file(&changes_file).unwrap_or(HashSet::new())
    };
    Ok(Pullable { local:local_patches, remote: remote_patches })
}

pub fn pull<'a>(target:&Path,session:&mut Session<'a>,pullable:&Pullable) -> Result<(), Error> {
    for i in pullable.iter() {
        try!(session.download_patch(&patches_dir(target),i));
    }
    apply_patches(target,&pullable.remote,&pullable.local)
}

pub fn pushable_patches<'a>(source:&Path,to_session:&mut Session<'a>) -> Result<HashSet<Vec<u8>>,Error> {
    debug!("source: {:?}",source);
    let mut from_changes:HashSet<Vec<u8>>={
        let changes_file=branch_changes_file(source,DEFAULT_BRANCH.as_bytes());
        debug!("changes_file: {:?}",changes_file);
        read_changes_from_file(&changes_file).unwrap_or(HashSet::new()) // empty repositories don't have this file
    };
    debug!("pushing: {:?}",from_changes);
    let to_changes=try!(to_session.changes(DEFAULT_BRANCH.as_bytes()));
    for i in to_changes.iter() {
        from_changes.remove(i);
    }
    Ok(from_changes)
}

pub fn push<'a>(source:&Path,to_session:&mut Session<'a>,pushable:&HashSet<Vec<u8>>) -> Result<(), Error> {
    try!(to_session.upload_patches(&patches_dir(source),pushable));
    try!(to_session.remote_apply(pushable));
    Ok(())
}


/// Assumes all patches have been downloaded. Only pull, push locally, and apply need this.
pub fn apply_patches<'a>(r:&Path,
                         remote_patches:&HashSet<Vec<u8>>,
                         local_patches:&HashSet<Vec<u8>>) -> Result<(), Error> {
    debug!("local {}, remote {}",local_patches.len(),remote_patches.len());
    let pullable=remote_patches.difference(&local_patches);
    let only_local={
        let mut only_local:HashSet<&[u8]>=HashSet::new();
        for i in local_patches.difference(&remote_patches) { only_local.insert(&i[..]); };
        only_local
    };
    fn apply_patches<'a>(mut repo:Repository<'a>, branch:&[u8], local_patches:&Path, patch_hash:&[u8], patches_were_applied:&mut bool, only_local:&HashSet<&[u8]>)->Result<Repository<'a>,Error>{
        if !try!(repo.has_patch(branch,patch_hash)) {
            let local_patch=local_patches.join(patch_hash.to_hex()).with_extension("cbor");
            debug!("local_patch={:?}",local_patch);
            let mut buffer = BufReader::new(try!(File::open(local_patch)));
            let patch=try!(Patch::from_reader(&mut buffer));
            for dep in patch.dependencies.iter() {
                repo= try!(apply_patches(repo,branch,local_patches,&dep,patches_were_applied,
                                         only_local))
            }
            let mut internal=[0;HASH_SIZE];
            repo.new_internal(&mut internal);
            //println!("pulling and applying patch {}",to_hex(patch_hash));
            let mut repo=try!(repo.apply(&patch, &internal,only_local));
            *patches_were_applied=true;
            repo.sync_file_additions(&patch.changes[..],&HashMap::new(), &internal);
            repo.register_hash(&internal[..],patch_hash);
            Ok(repo)
        } else {
            Ok(repo)
        }
    }
    let repo_dir=pristine_dir(r);
    let mut repo = try!(Repository::new(&repo_dir));
    let local_patches=patches_dir(r);
    let current_branch=repo.get_current_branch().to_vec();
    let pending={
        let (changes,_)= {
            let mut repo = try!(Repository::new(&repo_dir));
            try!(repo.record(&r))
        };
        let mut p=Patch::empty();
        p.changes=changes;
        p
    };
    let mut patches_were_applied=false;
    for p in pullable {
        repo=try!(apply_patches(repo,&current_branch,
                                &local_patches,p,&mut patches_were_applied,&only_local))
    }
    debug!(target:"pull","patches applied? {}",patches_were_applied);
    let mut repo = if patches_were_applied {
        try!(repo.write_changes_file(&branch_changes_file(r,&current_branch)));
        debug!(target:"pull","output_repository");
        try!(repo.output_repository(&r,&pending))
    } else { repo };
    if cfg!(debug_assertions){
        let mut buffer = BufWriter::new(File::create(r.join("debug")).unwrap());
        repo.debug(&mut buffer);
    }
    Ok(())
}
