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
//! This module defines operations related to files as stored on the file system.

use super::backend::*;
use super::error::*;
use super::patch::KEY_SIZE;
use rand;
use std;
use std::path::{Path,PathBuf};
use rustc_serialize::hex::ToHex;
use std::iter::{Iterator};

/// An Inode is a handle to a file; it is attached to a Line.
#[derive(Copy, Clone)]
pub struct Inode { contents: [u8; INODE_SIZE] }

impl ToHex for Inode {
    fn to_hex(&self) -> String {
        self.contents.to_hex()
    }
}

pub const INODE_SIZE:usize = 16;
pub const ROOT_INODE:Inode = Inode { contents:[0;INODE_SIZE] };
impl AsRef<[u8]> for Inode {
    fn as_ref(&self) -> &[u8] {
        self.contents.as_ref()
    }
}

impl Inode {
    pub fn from_slice(v:&[u8]) -> Self {
        let mut i = Inode { contents:[0;INODE_SIZE] };
        unsafe { std::ptr::copy_nonoverlapping(v.as_ptr(), i.contents.as_mut_ptr(), INODE_SIZE) };
        i
    }
    pub fn as_ptr(&self) -> *const u8 {
        self.contents.as_ptr()
    }
}

fn get_file_content<'a>(db_tree:&'a Db, inode: Inode) -> Option<&'a[u8]> {
    db_tree.get(inode.as_ref())
}


pub fn create_new_inode(ws:&mut Workspace, db_revtree:&mut Db,buf: &mut [u8]) {
    for i in 0..INODE_SIZE { buf[i]=rand::random() }
    let mut buf_ = [0;INODE_SIZE];
    unsafe { std::ptr::copy_nonoverlapping(buf.as_ptr(), buf_.as_mut_ptr(), INODE_SIZE) }
    let mut already_taken = true;
    while already_taken {
        already_taken = false;
        for (_,x) in db_revtree.iter(ws, &buf_, None) {
            if &buf[0..INODE_SIZE] == &x[0..INODE_SIZE] {
                already_taken = true;
                for i in 0..INODE_SIZE { buf[i]=rand::random() }
            }
            break
        }
    }
}

pub fn add_inode(ws:&mut Workspace, db_tree:&mut Db, db_revtree:&mut Db, inode:Option<&[u8]>, path:&std::path::Path, is_dir:bool)->Result<(),Error> {
    let mut buf = vec![0;INODE_SIZE];
    let mut components=path.components();
    let mut cs=components.next();
    while let Some(s)=cs { // need to peek at the next element, so no for.
        cs=components.next();
        let ss=s.as_os_str().to_str().unwrap();
        buf.extend(ss.as_bytes());
        let mut broken=false;
        {
            //debug!(target:"mv","mdb_get: dbi_tree, {}",buf.to_hex());
            match get_file_content(db_tree, Inode::from_slice(&buf)) {
                Some(v)=> {
                    //debug!(target:"mv","got Some({})",v.to_hex());
                    if cs.is_none() {
                        return Err(Error::AlreadyAdded)
                    } else {
                        // replace buf with existing inode
                        buf.clear();
                        buf.extend(v);
                    }
                },
                _ =>{
                    broken=true
                }
            }
        }
        if broken {
            let mut inode_:[u8;INODE_SIZE]=[0;INODE_SIZE];
            let inode = if cs.is_none() && inode.is_some() {
                inode.unwrap()
            } else {
                create_new_inode(ws, db_revtree, &mut inode_);
                &inode_[..]
            };
            //debug!(target:"mv","put: dbi_tree, {} {}",buf.to_hex(),inode.to_hex());
            try!(db_tree.put(&buf,&inode));
            try!(db_revtree.put(&inode,&buf));
            if cs.is_some() || is_dir {
                db_tree.put(&inode,&[]).unwrap();
            }
            //repository.set_db_tree(db_tree);
            //repository.set_db_revtree(db_revtree);
            // push next inode onto buf.
            buf.clear();
            buf.extend(inode)
        }
    }
    Ok(())
}


pub fn move_file<T>(ws:&mut Workspace, repository:&mut Transaction<T>, path:&std::path::Path, path_:&std::path::Path,is_dir:bool) -> Result<(), Error>{
    debug!(target:"mv","move_file: {:?},{:?}",path,path_);
    let inode= &mut (Vec::new());
    let parent= &mut (Vec::new());

    inode.extend_from_slice(ROOT_INODE.as_ref());
    let mut db_tree = repository.db_tree();
    let mut db_revtree = repository.db_tree();
    for c in path.components() {
        inode.truncate(INODE_SIZE);
        inode.extend(c.as_os_str().to_str().unwrap().as_bytes());
        //debug!(target:"mv","first get: {}",inode.to_hex());
        match db_tree.get(&inode) {
            Some(x)=> {
                //debug!(target:"mv","got some: {}",x.to_hex());
                std::mem::swap(inode,parent);
                (*inode).clear();
                (*inode).extend(x);
            },
            _=>{
                debug!(target:"mv","got none");
                return Err(Error::FileNotInRepo(path.to_path_buf()))
            }
        }
    }
    // Now the last inode is in "*inode"
    //debug!(target:"mv","txn.del parent={:?}",parent.to_hex());
    try!(db_tree.del(parent, None));
    let basename=path.file_name().unwrap();
    (*parent).truncate(INODE_SIZE);
    (*parent).extend(basename.to_str().unwrap().as_bytes());

    //debug!(target:"mv","inode={} path_={:?}",inode.to_hex(),path_);
    try!(add_inode(ws, &mut db_tree,&mut db_revtree, Some(&inode), path_,is_dir));
    let mut db_inodes = repository.db_inodes();
    let vv=
        match db_inodes.get(inode) {
            Some(v)=> {
                let mut vv=v.to_vec();
                vv[0]=1;
                Some(vv)
            },
            _=>None
        };
    if let Some(vv)=vv {
        try!(db_inodes.put(inode,&vv));
        //repository.set_db_inodes(db_inodes);
    };
    Ok(())
}


// This function returns a boolean indicating whether the directory we are trying to delete is non-empty, and deletes it if so.
fn rec_delete(ws:&mut Workspace, db_tree:&mut Db, db_revtree:&mut Db, db_inodes:&mut Db, key:&[u8])->Result<bool,Error> {
    //println!("rec_delete {}",to_hex(key));
    let mut children=Vec::new();
    // First, kill the inode itself, if it exists (or mark it deleted)
    //let mut db_tree = repository.db_tree();
    for (k,v) in db_tree.iter(ws, &key, None) {
        if key == k {
            if v.len()>0 {
                children.push((k.to_vec(),v.to_vec()));
            }
        } else {
            break
        }
    }
    //let mut db_revtree = repository.db_revtree();
    {
        for (a,b) in children {
            if try!(rec_delete(ws, db_tree, db_revtree, db_inodes, &b)) {
                //println!("deleting {} {}",to_hex(&a),to_hex(&b));
                try!(db_tree.del(&a,Some(&b)));
                try!(db_revtree.del(&b,Some(&a)));
            }
        }
    }
    let mut node_=[0;3+KEY_SIZE];
    // If the directory is empty, then mark the corresponding node as deleted (flag '2').
    //let mut db_inodes = repository.db_inodes();
    let b=
        match db_inodes.get(key) {
            Some(node) => {
                //debug!(target:"remove_file","node={}",node.to_hex());
                debug_assert!(node.len()==3+KEY_SIZE);
                unsafe {
                    std::ptr::copy_nonoverlapping(node.as_ptr(),
                                                  node_.as_mut_ptr(),
                                                  3+KEY_SIZE);
                }
                node_[0]=2;
                false
            },
            None=>true,
        };
    if !b {
        try!(db_inodes.put(key,&node_[..]));
        //repository.set_db_inodes(db_inodes);
    }
    Ok(b)
}

pub fn remove_file<T>(ws:&mut Workspace, repository:&mut Transaction<T>, path:&std::path::Path) -> Result<(), Error>{
    let mut inode=Vec::new();
    inode.extend_from_slice(ROOT_INODE.as_ref());
    let mut comp=path.components();
    let mut c=comp.next();
    let db_tree = repository.db_tree();
    loop {
        match c {
            Some(sc)=>{
                //println!("inode {} + {:?}",to_hex(&inode),sc);
                inode.extend(sc.as_os_str().to_str().unwrap().as_bytes());
                match db_tree.get(&inode) {
                    Some(x)=> { c=comp.next();
                                    if c.is_some() {inode.clear(); inode.extend(x) }
                    },
                    _ => return Err(Error::FileNotInRepo(path.to_path_buf()))
                }
            },
            _=>break
        }
    }
    let mut db_tree = repository.db_tree();
    let mut db_revtree = repository.db_revtree();
    let mut db_inodes = repository.db_inodes();
    try!(rec_delete(ws, &mut db_tree,&mut db_revtree,&mut db_inodes, &inode));
    Ok(())
}

pub fn list_files<T>(repository:&Transaction<T>)->Vec<PathBuf>{
    fn collect<T>(repo:&Transaction<T>,key:&[u8],pb:&Path, basename:&[u8],files:&mut Vec<PathBuf>) {
        //println!("collecting {:?},{:?}",to_hex(key),std::str::from_utf8_unchecked(basename));
        let db_inodes = repo.db_inodes();
        let add= match db_inodes.get(key) {
            Some(node) => node[0]<2,
            None=> true,
        };
        if add {
            let next_pb=pb.join(std::str::from_utf8(basename).unwrap());
            let next_pb_=next_pb.clone();
            if basename.len()>0 { files.push(next_pb) }
            let db_tree = repo.db_tree();
            let mut ws = Workspace::new();
            for (k,v) in db_tree.iter(&mut ws, key, None) {
                if v.len()>0 && k == key {
                    collect(repo,v,next_pb_.as_path(),&k[INODE_SIZE..],files);
                } else {
                    break
                }
            }
        }
    }
    let mut files=Vec::new();
    let mut pathbuf=PathBuf::new();
    collect(repository,ROOT_INODE.as_ref(), &mut pathbuf, &[], &mut files);
    files
}
