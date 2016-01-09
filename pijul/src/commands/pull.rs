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
use clap::{SubCommand, ArgMatches,Arg};

use commands::StaticSubcommand;
extern crate libpijul;
use self::libpijul::{Repository,DEFAULT_BRANCH};
use self::libpijul::patch::{Patch,read_changes_from_file,read_changes,HASH_SIZE};
use self::libpijul::fs_representation::{repo_dir, pristine_dir, find_repo_root, patches_dir, branch_changes_base_path, branch_changes_file,to_hex};
use std::path::{Path,PathBuf};
use std::io::{BufWriter,BufReader};
use std::fs::File;
use std::collections::hash_set::{HashSet};
use std::collections::hash_map::{HashMap};
use std::fs::{hard_link,metadata};

use commands::error::Error;


extern crate ssh2;
use std::io::prelude::*;
use std::net::TcpStream;
use std::io;
extern crate rpassword;
use self::rpassword::read_password;
extern crate regex;
use self::regex::Regex;

#[cfg(windows)]
extern crate winapi;
#[cfg(windows)]
use self::winapi::{DWORD};

#[cfg(not(windows))]
extern crate libc;
#[cfg(not(windows))]
use self::libc::{getlogin};
#[cfg(not(windows))]
use std::slice;


pub fn invocation() -> StaticSubcommand {
    return
        SubCommand::with_name("pull")
        .about("pull from a remote repository")
        .arg(Arg::with_name("remote")
             .help("Repository from which to pull.")
             )
        .arg(Arg::with_name("repository")
             .help("Local repository.")
             )
        .arg(Arg::with_name("port")
             .short("p")
             .long("port")
             .help("Port of the remote ssh server.")
             .takes_value(true)
             .validator(|val| { let x:Result<u16,_>=val.parse();
                                match x { Ok(_)=>Ok(()),
                                          Err(_)=>Err(val) }
             })
             )
}
#[derive(Debug)]
pub enum Remote<'a> {
    Ssh { user:String, host:&'a str, port:u16, path:&'a Path },
    Uri { uri:&'a str },
    Local { path:&'a Path }
}


#[cfg(windows)]
fn get_user_name()->Result<String,Error> {
    // Untested
    let mut user_name:Vec<u16>=Vec::new();
    let mut len:[DWORD;1]=[255];
    loop {
        v.reserve(len[0] as usize);
        unsafe {
            if advapi32::GetUserNameW(user_name.as_mut_ptr(),len.as_mut_ptr()) != 0 {
                break
            } else {
                len[0] <<= 1
            };
        }
    }
    Ok(user_name.from_utf16())
}
#[cfg(not(windows))]
fn get_user_name()->Result<String,Error> {
    unsafe {
        let login=self::libc::getlogin();
        if login.is_null() {
            panic!("Cannot find user name")
        } else {
            String::from_utf8(slice::from_raw_parts(login as *const u8,self::libc::strlen(login) as usize).to_vec()).map_err(Error::UTF8)
        }
    }
}




enum Session<'a> {
    Ssh { addr:String,
          tcp:TcpStream,
          path:&'a Path,
          session:ssh2::Session
    },
    Uri,
    Local{path:&'a Path},
}

impl<'a> Session<'a> {
    fn from_remote(remote:&Remote<'a>) -> Result<Session<'a>,Error> {
        match *remote {
            Remote::Local{path} => Ok(Session::Local{path:path}),
            Remote::Uri{..} => Ok(Session::Uri),
            Remote::Ssh{ref user,ref host,ref port,ref path}=>{
                // Connect to the local SSH server
                // TODO: Parse ~/.ssh/config
                let addr=format!("{}:{}",host,port);
                debug!(target:"pull","user={:?}, addr={:?}",get_user_name(),addr);
                let tcp = TcpStream::connect(&addr[..]).unwrap();
                let mut session = ssh2::Session::new().unwrap();
                session.handshake(&tcp).unwrap();
                print!("{}@{} password: ",user,host);
                io::stdout().flush().ok().expect("Could not flush stdout");
                let password=read_password().unwrap();
                session.userauth_password(&user, &password).unwrap();
                Ok(Session::Ssh { addr:addr, session:session,tcp:tcp, path:path })
            }
        }
    }
    fn changes(&self,branch:&[u8]) -> HashSet<Vec<u8>> {
        match *self {
            Session::Ssh{ref path,ref session,..}=>{
                let patches_path=branch_changes_file(path,branch);
                let (remote_file, _) = session.scp_recv(&patches_path).unwrap();
                let changes=read_changes(remote_file).unwrap();
                changes
            },
            Session::Local{path} =>{
                let changes_file=branch_changes_file(path,branch);
                read_changes_from_file(&changes_file).unwrap_or(HashSet::new())
            },
            Session::Uri =>{unimplemented!()},
        }
    }
    fn download_patch(&self, local_patches:&Path, patch_hash:&[u8])->Result<PathBuf,Error>{
        match *self {
            Session::Local{path}=>{
                let hash=to_hex(patch_hash);
                let remote_file=patches_dir(path).join(&hash).with_extension("cbor");
                let local_file=local_patches.join(&hash).with_extension("cbor");
                if metadata(&local_file).is_err() {
                    try!(hard_link(&remote_file,&local_file));
                }
                Ok(local_file)
            },
            Session::Ssh{ref path,ref session,..}=>{
                let hash=to_hex(patch_hash);
                let local_file=local_patches.join(&hash).with_extension("cbor");
                if metadata(&local_file).is_err() { // If we don't have it yet

                    let remote_file=patches_dir(path).join(&hash).with_extension("cbor");
                    let (mut remote_file, _) = session.scp_recv(&remote_file).unwrap();
                    let mut contents = Vec::new();
                    debug!(target:"pull","downloading file to {:?}",local_file);
                    try!(remote_file.read_to_end(&mut contents));
                    let mut w=BufWriter::new(try!(File::create(&local_file)));
                    try!(w.write_all(&contents))
                }
                Ok(local_file)
            },
            Session::Uri=>{
                unimplemented!()
            }
        }
    }
}


#[derive(Debug)]
pub struct Params<'a> {
    pub repository : &'a Path,
    pub remote : Remote<'a>,
    pub remote_id : &'a str
}



pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    let repository = Path::new(args.value_of("repository").unwrap_or("."));
    let remote_id = args.value_of("remote").unwrap();
    let remote={
        let ssh=Regex::new(r"^([^:]*):(.*)$").unwrap();
        let uri=Regex::new(r"^([:alpha:]*)://(.*)$").unwrap();
        if ssh.is_match(remote_id) {
            let cap=ssh.captures(remote_id).unwrap();
            let port=match args.value_of("port") { Some(x)=>x.parse().unwrap(), None=>22 };
            let user_host=cap.at(1).unwrap();

            let (user,host)={
                let ssh_user_host=Regex::new(r"^([^@]*)@(.*)$").unwrap();
                if ssh_user_host.is_match(user_host) {
                    let cap=ssh_user_host.captures(user_host).unwrap();
                    (cap.at(1).unwrap().to_string(),cap.at(2).unwrap())
                } else {
                    (get_user_name().unwrap(),user_host)
                }
            };
            Remote::Ssh { user:user,host:host, port:port, path:Path::new(cap.at(2).unwrap()) }
        } else if uri.is_match(remote_id) {
            let cap=uri.captures(remote_id).unwrap();
            if cap.at(1).unwrap()=="file" { Remote::Local { path:Path::new(cap.at(2).unwrap()) } }
            else { Remote::Uri { uri:remote_id } }
        } else {
            Remote::Local { path:Path::new(remote_id) }
        }
    };
    Params { repository : repository,
             remote : remote,
             remote_id : remote_id }
}

pub fn run<'a>(args : &Params<'a>) -> Result<(), Error> {
    let pwd = args.repository;
    match find_repo_root(&pwd){
        None => return Err(Error::NotInARepository),
        Some(r) =>
        {
            println!("args={:?}",args);
            // get remote changes file
            let session=try!(Session::from_remote(&args.remote));
            let remote_patches:HashSet<Vec<u8>>=session.changes(DEFAULT_BRANCH.as_bytes());
            let local_patches:HashSet<Vec<u8>>={
                let changes_file=branch_changes_file(r,DEFAULT_BRANCH.as_bytes());
                read_changes_from_file(&changes_file).unwrap_or(HashSet::new())
            };

            debug!(target:"pull","local {}, remote {}",local_patches.len(),remote_patches.len());
            let pullable=remote_patches.difference(&local_patches);
            let only_local:HashSet<&[u8]>={
                let mut only_local:HashSet<&[u8]>=HashSet::new();
                for i in local_patches.difference(&remote_patches) {
                    debug!(target:"pull","only_local += {}",to_hex(&i));
                    only_local.insert(&i);
                }
                only_local
            };

            // Then filter the patches in some way.

            // Then download the patches, and apply.
            fn apply_patches<'a>(mut repo:Repository<'a>, session:&Session, branch:&[u8], remote:&Remote, local_patches:&Path, patch_hash:&[u8], patches_were_applied:&mut bool, only_local:&HashSet<&[u8]>)->Result<Repository<'a>,Error>{
                // download this patch
                //println!("has patch : {:?}",patch_hash);
                if !try!(repo.has_patch(branch,patch_hash)) {
                    let local_patch=try!(session.download_patch(local_patches,patch_hash));
                    let mut buffer = BufReader::new(try!(File::open(local_patch)));
                    let patch=try!(Patch::from_reader(&mut buffer));
                    for dep in patch.dependencies.iter() {
                        repo= try!(apply_patches(repo,session, branch,remote,local_patches,&dep,patches_were_applied,
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
                Patch { changes:changes,
                        dependencies:HashSet::new() }
            };
            let mut patches_were_applied=false;
            for p in pullable {
                repo=try!(apply_patches(repo,&session,&current_branch,&args.remote,&local_patches,p,&mut patches_were_applied,&only_local))
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
    }
}
