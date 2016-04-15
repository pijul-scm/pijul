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

extern crate libpijul;
use self::libpijul::{Repository,DEFAULT_BRANCH};
use self::libpijul::patch::{read_changes_from_file,read_changes};
use self::libpijul::fs_representation::{repo_dir, pristine_dir, branch_changes_base_path,branch_changes_file,PIJUL_DIR_NAME,PATCHES_DIR_NAME,patch_path};
use std::path::{Path,PathBuf,MAIN_SEPARATOR};
use std::io::{BufWriter};
use std::collections::hash_set::{HashSet};
use std::fs::{File,hard_link,copy,metadata};

use super::error::Error;
use std::str::{from_utf8,from_utf8_unchecked};
extern crate ssh;
use self::ssh::Channel;
use std::io::prelude::*;
extern crate regex;
use self::regex::Regex;

extern crate rustc_serialize;
use self::rustc_serialize::hex::{ToHex};

use super::escape::unix::escape;
use std::borrow::Cow;
use super::init;
use std::collections::hash_set::Iter;
use std::fmt::Debug;
extern crate hyper;
use super::ask;

const HTTP_MAX_ATTEMPTS:usize=3;

#[derive(Debug)]
pub enum Remote<'a> {
    Ssh { user:Option<&'a str>, host:&'a str, port:Option<u64>, path:&'a Path, id:&'a str },
    Uri { uri:&'a str },
    Local { path:PathBuf }
}

pub enum Session<'a> {
    Ssh {
        id:&'a str,
        path:&'a Path,
        session:ssh::Session
    },
    Uri { uri:&'a str,
          client:hyper::Client },
    Local{path:&'a Path},
}


fn ssh_recv_file<'b,P:AsRef<Path>>(s:&'b mut ssh::Session,p:P)->Result<Option<ssh::Scp<'b>>,ssh::Error> where P:Debug {
    debug!("recv_file {:?}",p);
    let mut scp=s.scp_new(ssh::READ,p.as_ref()).unwrap();
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
                debug!("ssh: receiving changes");
                let remote_file = try!(ssh_recv_file(session,&patches_path));
                let changes= match remote_file {
                    Some(r)=>try!(read_changes(r,None)),
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
                let changes=read_changes(&mut res,None).unwrap_or(HashSet::new());
                debug!("http: {:?}",changes);
                Ok(changes)
            },
        }
    }
    pub fn download_patch(&mut self, repo_root:&Path, patch_hash:&[u8])->Result<PathBuf,Error>{
        let s=patch_path(patch_hash,MAIN_SEPARATOR);
        let local_file=repo_root.join(&s);
        if metadata(&local_file).is_ok() {
            Ok(local_file)
        } else {
            match *self {
                Session::Local{path}=>{
                    debug!("local downloading {:?}",patch_hash.to_hex());
                    let remote_file=path.join(&s);
                    //let local_file=patches_dir(repo_root).join(remote_file.file_name().unwrap());
                    debug!("hard linking {:?} to {:?}",remote_file,local_file);
                    try!(hard_link(&remote_file,&local_file).or_else(|_|{
                        copy(&remote_file, &local_file).and_then(|_| Ok(()))
                    }));
                    Ok(local_file)
                },
                Session::Ssh{ref path,ref mut session,..}=>{
                    let s_=patch_path(patch_hash,'/');
                    let remote_file=path.join(&s_);
                    if let Ok(Some(mut rem))=ssh_recv_file(session,&remote_file) {
                        let mut contents = Vec::new();
                        debug!(target:"pull","downloading file to {:?}",local_file);
                        try!(rem.read_to_end(&mut contents));
                        let mut w=BufWriter::new(try!(File::create(&local_file)));
                        try!(w.write_all(&contents));
                        Ok(local_file)
                    } else {
                        Err(Error::PatchNotFound(path.to_path_buf().to_string_lossy().into_owned(),
                                                 patch_hash.to_hex()))
                    }
                },
                Session::Uri{ref mut client,uri}=>{
                    //for remote_file in patch_path_iter(patch_hash,'/') {
                    let uri=uri.to_string() + "/" + &patch_path(patch_hash,'/');
                    debug!("downloading uri {:?}",uri);
                    let mut attempts=0;
                    while attempts<HTTP_MAX_ATTEMPTS {
                        match client.get(&uri).header(hyper::header::Connection::close()).send() {
                            Ok(ref mut res) if res.status==hyper::status::StatusCode::Ok => {
                                debug!("response={:?}",res);
                                let mut body=Vec::new();
                                try!(res.read_to_end(&mut body));
                                let mut f=try!(File::create(&local_file));
                                try!(f.write_all(&body));
                                debug!("patch downloaded through http: {:?}",body);
                                return Ok(local_file)
                            },
                            Ok(_) => {
                                break
                            },
                            Err(e)=>{
                                debug!("error downloading : {:?}",e);
                                attempts+=1;
                            }
                        }
                    }
                    Err(Error::PatchNotFound(repo_root.to_str().unwrap().to_string(),
                                             patch_hash.to_hex()))
                }
            }
        }
    }
    // patch hash in binary
    pub fn upload_patches(&mut self, repo_root:&Path, patch_hashes:&HashSet<Vec<u8>>)->Result<(),Error> {
        match *self {
            Session::Ssh { ref mut session, ref path, .. }=> {
                let mut remote_path=path.to_str().unwrap().to_string()+"/"+PIJUL_DIR_NAME+"/"+PATCHES_DIR_NAME+"/";
                let remote_len=remote_path.len();

                let mut local_path=repo_root.to_str().unwrap().to_string();
                local_path.push(MAIN_SEPARATOR);
                local_path.push_str(PIJUL_DIR_NAME);
                local_path.push(MAIN_SEPARATOR);
                local_path.push_str(PATCHES_DIR_NAME);
                local_path.push(MAIN_SEPARATOR);
                let local_len=local_path.len();

                let mut scp=try!(session.scp_new(ssh::WRITE,&remote_path));
                try!(scp.init());
                for hash in patch_hashes {
                    debug!("repo_root: {:?},hash:{:?}",repo_root,hash.to_hex());
                    remote_path.truncate(remote_len);
                    remote_path.push_str(&hash.to_hex());
                    remote_path.push_str(".cbor.gz");
                    local_path.truncate(local_len);
                    local_path.push_str(&hash.to_hex());
                    local_path.push_str(".cbor.gz");
                    let mut buf = Vec::new();
                    {
                        let mut f = try!(File::open(&local_path));
                        try!(f.read_to_end(&mut buf));
                    }
                    try!(scp.push_file(&remote_path,buf.len(),0o644));
                    try!(scp.write(&buf));
                }
                Ok(())
            },
            Session::Local{path} =>{
                let mut remote_path=path.to_str().unwrap().to_string();
                remote_path.push(MAIN_SEPARATOR);
                remote_path.push_str(PIJUL_DIR_NAME);
                remote_path.push(MAIN_SEPARATOR);
                remote_path.push_str(PATCHES_DIR_NAME);
                remote_path.push(MAIN_SEPARATOR);
                let remote_len=remote_path.len();

                let mut local_path=repo_root.to_str().unwrap().to_string();
                local_path.push(MAIN_SEPARATOR);
                local_path.push_str(PIJUL_DIR_NAME);
                local_path.push(MAIN_SEPARATOR);
                local_path.push_str(PATCHES_DIR_NAME);
                local_path.push(MAIN_SEPARATOR);
                let local_len=local_path.len();

                for hash in patch_hashes {
                    remote_path.truncate(remote_len);
                    remote_path.push_str(&hash.to_hex());
                    remote_path.push_str(".cbor.gz");

                    local_path.truncate(local_len);
                    local_path.push_str(&hash.to_hex());
                    local_path.push_str(".cbor.gz");

                    if metadata(&remote_path).is_err() {
                        try!(hard_link(&local_path,&remote_path).or_else(|_|{
                            copy(&local_path, &remote_path).and_then(|_| Ok(()))
                        }))
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
            Session::Ssh { ref mut session, ref path, ref id, .. }=> {
                debug!("ssh: remote_apply");
                let mut s  =try!(session.channel_new());
                try!(s.open_session());
                let esc_path=escape(Cow::Borrowed(path.to_str().unwrap()));
                let mut patches="".to_string();
                for i in patch_hashes {
                    patches=patches + " " + &(i.to_hex());
                }
                if patches.len()>0 {
                    let cmd=format!("cd \"{}\"; pijul apply{}",esc_path, &patches);
                    debug!("command line:{:?}",cmd);
                    try!(s.request_exec(cmd.as_bytes()));
                }
                try!(s.send_eof());
                let exitcode=s.get_exit_status().unwrap();
                if exitcode != 0 {
                    let mut buf=Vec::new();
                    try!(s.stdout().read_to_end(&mut buf));
                    try!(s.stderr().read_to_end(&mut buf));
                    let buf= unsafe { from_utf8_unchecked(&buf) }.to_string();
                    return Err(Error::RemoteApplyFailed(id.to_string(),exitcode as i32,buf))
                } else {
                    let mut buf=Vec::new();
                    try!(s.stdout().read_to_end(&mut buf));
                    try!(s.stderr().read_to_end(&mut buf));
                    let buf= unsafe { from_utf8_unchecked(&buf) }.to_string();
                    if buf.len() > 0 {
                        println!("{}",buf)
                    }
                    Ok(())
                }
            },
            Session::Local{path} =>{
                let applied_patches:HashSet<Vec<u8>>=try!(self.changes(DEFAULT_BRANCH.as_bytes()));
                let repo_dir=pristine_dir(path);
                let repo = try!(Repository::open(&repo_dir).map_err(Error::Repository));
                let mut txn = try!(repo.mut_txn_begin());
                try!(txn.apply_patches(DEFAULT_BRANCH, path, &patch_hashes,&applied_patches));
                try!(txn.commit());
                Ok(())
            }
            _=>{panic!("remote apply not possible")}
        }
    }
    pub fn remote_init(&mut self)->Result<(),Error> {
        match *self {
            Session::Ssh { ref mut session, ref path, ref id, .. }=> {
                let mut s : Channel =try!(session.channel_new());
                try!(s.open_session());
                let esc_path=escape(Cow::Borrowed(path.to_str().unwrap()));
                try!(s.request_exec(format!("mkdir -p \"{}\"; cd \"{}\"; pijul init",esc_path,esc_path).as_bytes()));
                try!(s.send_eof());
                let exitcode=s.get_exit_status().unwrap();
                if exitcode != 0 {
                    let mut buf=String::new();
                    try!(s.stdout().read_to_string(&mut buf));
                    try!(s.stderr().read_to_string(&mut buf));
                    return Err(Error::RemoteInitFailed(id.to_string(),exitcode as i32,buf))
                } else {
                    let mut buf=String::new();
                    try!(s.stdout().read_to_string(&mut buf));
                    try!(s.stderr().read_to_string(&mut buf));
                    if buf.len() > 0 {
                        println!("{}",buf);
                    }
                    Ok(())
                }
            },
            Session::Local{path} =>{
                try!(init::run(&init::Params { location:path, allow_nested:false }));
                Ok(())
            }
            _=>{panic!("remote init not possible")}
        }
    }

    pub fn pullable_patches(&mut self,target:&Path) -> Result<Pullable, Error> {
        let remote_patches:HashSet<Vec<u8>>=try!(self.changes(DEFAULT_BRANCH.as_bytes()));
        let local_patches:HashSet<Vec<u8>>={
            let changes_file=branch_changes_file(target,DEFAULT_BRANCH.as_bytes());
            read_changes_from_file(&changes_file).unwrap_or(HashSet::new())
        };
        Ok(Pullable { local:local_patches, remote: remote_patches })
    }

    pub fn pull(&mut self,target:&Path,pullable:&Pullable) -> Result<(), Error> {
        for i in pullable.iter() {
            try!(self.download_patch(&target,i));
        }
        let repo_dir=pristine_dir(target);
        let repo = try!(Repository::open(&repo_dir).map_err(Error::Repository));
        let mut txn = try!(repo.mut_txn_begin());
        try!(txn.apply_patches(DEFAULT_BRANCH, target,&pullable.remote,&pullable.local));
        debug!("pull: committing");
        try!(txn.commit());
        debug!("pull: commited");
        Ok(())
    }

    pub fn pushable_patches(&mut self, source:&Path) -> Result<HashSet<Vec<u8>>,Error> {
        debug!("source: {:?}",source);
        let mut from_changes:HashSet<Vec<u8>>={
            let changes_file=branch_changes_file(source,DEFAULT_BRANCH.as_bytes());
            debug!("changes_file: {:?}",changes_file);
            read_changes_from_file(&changes_file).unwrap_or(HashSet::new()) // empty repositories don't have this file
        };
        debug!("pushing: {:?}",from_changes);
        let to_changes=try!(self.changes(DEFAULT_BRANCH.as_bytes()));
        for i in to_changes.iter() {
            from_changes.remove(i);
        }
        Ok(from_changes)
    }

    pub fn push(&mut self, source:&Path,pushable:&HashSet<Vec<u8>>) -> Result<(), Error> {
        try!(self.upload_patches(source,pushable));
        try!(self.remote_apply(pushable));
        Ok(())
    }
}



impl <'a>Remote<'a> {
    pub fn session(&'a self)->Result<Session<'a>,Error> {
        //fn from_remote(remote:&Remote<'a>) -> Result<Session<'a>,Error> {
        match *self {
            Remote::Local{ref path} => Ok(Session::Local{path:path.as_path()}),
            Remote::Uri{uri} => Ok(Session::Uri {
                uri:uri,
                client:hyper::Client::new()
            }),
            Remote::Ssh{ref user,ref host,ref port,ref path,ref id}=>{
                let mut session = ssh::Session::new().unwrap();
                session.set_host(host).unwrap();
                match *port { None=>{}, Some(ref p)=>try!(session.set_port(*p as usize)) };
                match *user { None=>{}, Some(ref u)=>try!(session.set_username(u)) };
                session.parse_config(None).unwrap();
                debug!("ssh: trying to connect");
                try!(session.connect());
                debug!("ssh: connected");
                match try!(session.is_server_known()) {
                    ssh::ServerKnown::Known=>{
                        if session.userauth_publickey_auto(None).is_err() { try!(session.userauth_kbdint(None)) }
                        Ok(Session::Ssh { session:session, path:path, id:id })
                    },
                    ssh::ServerKnown::NotKnown=>{
                        if let Ok(true)=ask::ask_learn_ssh(host,&try!(session.get_pubkey_hash()).to_hex()) {
                            try!(session.write_knownhost());
                            if session.userauth_publickey_auto(None).is_err() { try!(session.userauth_kbdint(None)) }
                            Ok(Session::Ssh { session:session, path:path, id:id })
                        } else {
                            Err(Error::SSHUnknownServer(ssh::ServerKnown::NotKnown))
                        }
                    },
                    other=>{Err(Error::SSHUnknownServer(other))}
                }
            }
        }
    }
}

pub fn parse_remote<'a>(remote_id:&'a str,port:Option<u64>,base_path:Option<&'a Path>)->Remote<'a> {
    let ssh=Regex::new(r"^([^:]*):(.*)$").unwrap();
    let uri=Regex::new(r"^([:alpha:]*)://(.*)$").unwrap();
    if uri.is_match(remote_id) {
        let cap=uri.captures(remote_id).unwrap();
        if cap.at(1).unwrap()=="file" {
            if let Some(a)=base_path {
                let path=a.join(cap.at(2).unwrap());
                Remote::Local { path:path }
            } else {
                let path=Path::new(cap.at(2).unwrap()).to_path_buf();
                Remote::Local { path:path }
            }
        }
        else { Remote::Uri { uri:remote_id } }
    } else if ssh.is_match(remote_id) {
        let cap=ssh.captures(remote_id).unwrap();
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
        Remote::Ssh { user:user,host:host, port:port, path:Path::new(cap.at(2).unwrap()),id:remote_id }
    } else {
        if let Some(a)=base_path {
            let path=a.join(remote_id);
            Remote::Local { path:path }
        } else {
            let path=Path::new(remote_id).to_path_buf();
            Remote::Local { path:path }
        }
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
