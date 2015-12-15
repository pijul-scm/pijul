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
use self::libpijul::patch::{Patch,read_changes,HASH_SIZE};
use self::libpijul::fs_representation::{repo_dir, pristine_dir, find_repo_root, patches_dir, branch_changes_file,to_hex};
use std::path::{Path,PathBuf};
use std::io::{BufWriter,BufReader};
use std::fs::File;
use std::collections::hash_set::{HashSet};
use std::collections::hash_map::{HashMap};
use std::fs::{hard_link,metadata};

use commands::error::Error;

/*
extern crate ssh2;
use std::net::TcpStream;
use self::ssh2::Session;
*/
extern crate regex;
use self::regex::Regex;

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

pub enum Remote<'a> {
    Ssh { host:&'a str, port:u16, path:&'a str },
    Uri { uri:&'a str },
    Local { path:&'a Path }
}

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
            Remote::Ssh { host:cap.at(1).unwrap(), port:port, path:cap.at(2).unwrap() }
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
            // get remote changes file
            let remote_patches:HashSet<Vec<u8>>=
                match args.remote {
                    Remote::Local{path}=>{
                        let changes_file=branch_changes_file(path,DEFAULT_BRANCH.as_bytes());
                        read_changes(&changes_file).unwrap_or(HashSet::new())
                    },
                    Remote::Ssh{..}=>{
                        /*
                        // Connect to the local SSH server
                        let tcp = TcpStream::connect(remote).unwrap();
                        let mut sess = Session::new().unwrap();
                        sess.handshake(&tcp).unwrap();
                        sess.userauth_agent("username").unwrap();

                        let (mut remote_file, stat) = sess.scp_recv(Path::new("remote")).unwrap();
                        println!("remote file size: {}", stat.size());
                        let mut contents = Vec::new();
                        remote_file.read_to_end(&mut contents).unwrap();
                         */
                        unimplemented!()
                    }
                    _=>{
                        unimplemented!()
                    }
                };
            let local_patches:HashSet<Vec<u8>>={
                let changes_file=branch_changes_file(r,DEFAULT_BRANCH.as_bytes());
                read_changes(&changes_file).unwrap_or(HashSet::new())
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
            fn download_patch(remote:&Remote, local_patches:&Path, patch_hash:&[u8])->Result<PathBuf,Error>{
                match *remote {
                    Remote::Local{path}=>{
                        let hash=to_hex(patch_hash);
                        let remote_file=patches_dir(path).join(&hash).with_extension("cbor");
                        let local_file=local_patches.join(&hash).with_extension("cbor");
                        if metadata(&local_file).is_err() {
                            try!(hard_link(&remote_file,&local_file));
                        }
                        Ok(local_file)
                    },
                    _=>{
                        unimplemented!()
                    }
                }
            }
            fn apply_patches<'a>(mut repo:Repository<'a>, branch:&[u8], remote:&Remote, local_patches:&Path, patch_hash:&[u8], patches_were_applied:&mut bool, only_local:&HashSet<&[u8]>)->Result<Repository<'a>,Error>{
                // download this patch
                //println!("has patch : {:?}",patch_hash);
                if !try!(repo.has_patch(branch,patch_hash)) {
                    let local_patch=try!(download_patch(remote,local_patches,patch_hash));
                    let mut buffer = BufReader::new(try!(File::open(local_patch)));
                    let patch=try!(Patch::from_reader(&mut buffer));
                    for dep in patch.dependencies.iter() {
                        repo= try!(apply_patches(repo,branch,remote,local_patches,&dep,patches_were_applied,
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
                        dependencies:vec!() }
            };
            let mut patches_were_applied=false;
            for p in pullable {
                repo=try!(apply_patches(repo,&current_branch,&args.remote,&local_patches,p,&mut patches_were_applied,&only_local))
            }
            let mut repo = if patches_were_applied {
                try!(repo.write_changes_file(&branch_changes_file(r,&current_branch)));
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
