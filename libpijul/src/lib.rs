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
//! This crate implements operations on Pijul repositories.

use std::path::{Path,PathBuf};
use std::collections::{HashSet,HashMap};
#[macro_use]
extern crate log;
extern crate time;
#[macro_use]
extern crate bitflags;

// mod lmdb;
extern crate sanakirja;

pub mod error;
use self::error::*;

pub trait RepositoryEnv<'env, R>:Sized {
    fn open<P:AsRef<Path>>(&self, path:P) -> Result<Self, Error>;
    fn mut_txn_begin(&'env self) -> Result<R,Error>;
}

extern crate rustc_serialize;
use rustc_serialize::hex::ToHex;
//use self::contents::{Inode, OwnedInode, Line, Graph, LineBuffer};
//use self::contents::{LINE_ONSTACK, LINE_VISITED, DIRECTORY_FLAG, INODE_SIZE, ROOT_INODE};

extern crate rand;

pub mod fs_representation;
pub mod patch;

mod sanakirja_backend;
pub use sanakirja_backend::backend;

mod file_operations;
pub use file_operations::Inode;

pub mod graph;

mod optimal_diff;
pub use optimal_diff::diff;

impl <'a,'env:'a,T:'a,W> graph::LineBuffer<'a,'env,T> for W where W:std::io::Write {
    fn output_line(&mut self,_:&[u8],c:backend::Contents<'a,'env,T>) -> Result<(),Error> {
        for i in c {
            try!(self.write(i)); // .expect("output_line: could not write");
        }
        Ok(())
    }
}


mod record;
mod output;
mod apply;

pub type Transaction<'env> = backend::Transaction<'env,()>;

pub use backend::{Repository,DEFAULT_BRANCH};

pub use patch::internal_hash;


impl<'env,T> backend::Transaction<'env,T> {
    pub fn add_file<P:AsRef<Path>>(&mut self, path:P, is_dir:bool)->Result<(),Error>{
        let mut db_tree = self.db_tree();
        let mut db_revtree = self.db_revtree();
        file_operations::add_inode(&mut db_tree, &mut db_revtree, None, path.as_ref(), is_dir)
    }
    pub fn list_files(&self) -> Result<Vec<PathBuf>, Error> {
        file_operations::list_files(self)
    }

    pub fn remove_file<P:AsRef<Path>>(&mut self, path:P) -> Result<(),Error> {
        file_operations::remove_file(self, path.as_ref())
    }
    pub fn move_file<P:AsRef<Path>, Q:AsRef<Path>>(&mut self, path:P, path_:Q,is_dir:bool) -> Result<(), Error>{
        file_operations::move_file(self, path.as_ref(), path_.as_ref(), is_dir)
    }
    pub fn retrieve_and_output<'a,'name,W:std::io::Write>(&self,branch:&backend::Branch<'name,'a,'env,T>,key:&[u8],l:&mut W) -> Result<(),Error> {
        let db_contents = self.db_contents();
        let mut redundant_edges = Vec::new();
        let graph = graph::retrieve(branch,key);
        graph::output_file(branch, &db_contents, l, graph,&mut redundant_edges)
    }

    pub fn branch_patches<'a>(&'a self,db_external:&'a backend::Db<'a,'env,T>, branch_name:&str)->Result<HashSet<&'a[u8]>,Error> {
        let mut patches = HashSet::new();
        let db_patches = self.db_branches();
        for (br_name,patch_hash) in db_patches.iter(branch_name.as_bytes(), None) {
            debug!("branch_patches: {:?}, {:?}",
                   String::from_utf8_lossy(br_name),
                   patch_hash.to_hex());
            if br_name == branch_name.as_bytes() {
                patches.insert(patch::external_hash(&db_external, patch_hash));
            } else {
                debug!("not the right branch name");
                break
            }
        }
        Ok(patches)
    }
    fn write_changes_file<P:AsRef<Path>>(&self, branch_name:&str, path:P)->Result<(),Error> {
        let db_external = self.db_external();
        let patches = try!(self.branch_patches(&db_external, branch_name));
        debug!("write_changes_file, patches = {:?}", patches);
        let changes_file = fs_representation::branch_changes_file(path.as_ref(), branch_name.as_bytes());
        try!(patch::write_changes(&patches,&changes_file));
        Ok(())
    }
    pub fn apply_patches<P:AsRef<Path>>(&mut self, branch_name:&str, r:P, remote_patches:&HashSet<Vec<u8>>, local_patches:&HashSet<Vec<u8>>) -> Result<(),Error> {

        debug!("apply_patches");
        let result = try!(apply::apply_patches(self, branch_name, r.as_ref(), remote_patches, local_patches));
        debug!("/apply_patches");
        try!(self.write_changes_file(branch_name, r));
        Ok(result)
    }
    pub fn apply_local_patch<P:AsRef<Path>>(&mut self, branch_name:&str, location:P, patch: patch::Patch, inode_updates:&HashMap<patch::LocalKey,file_operations::Inode>) -> Result<(), Error>{

        debug!("apply_local_patch");
        let result = try!(apply::apply_local_patch(self,branch_name,location.as_ref(),patch,inode_updates));
        debug!("/apply_local_patch");
        try!(self.write_changes_file(branch_name, location));
        Ok(result)
    }
    pub fn record<P:AsRef<Path>>(&mut self,branch_name:&str, working_copy:P)->Result<(Vec<patch::Change>,HashMap<patch::LocalKey,file_operations::Inode>),Error>{
        record::record(self,branch_name,working_copy.as_ref())
    }
    pub fn output_repository<P:AsRef<Path>>(&mut self, branch_name:&str, working_copy:P, pending:&patch::Patch) -> Result<(),Error>{
        debug!("outputting repository");
        let result = output::output_repository(self,branch_name,working_copy.as_ref(),pending);
        debug!("/outputting repository");
        result
    }
    pub fn debug<W>(&self,branch_name:&str, w:&mut W) where W:std::io::Write {
        debug!("debugging branch {:?}", branch_name);
        let mut styles=Vec::with_capacity(16);
        for i in 0..16 {
            styles.push(("color=").to_string()
                        +["red","blue","green","black"][(i >> 1)&3]
                        +if (i as u8)&graph::DELETED_EDGE!=0 { ", style=dashed"} else {""}
                        +if (i as u8)&graph::PSEUDO_EDGE!=0 { ", style=dotted"} else {""})
        }
        w.write(b"digraph{\n").unwrap();
        let db_nodes = self.db_nodes(branch_name).unwrap();
        let db_contents = self.db_contents();
        let mut cur=&[][..];
        for (k,v) in db_nodes.iter(b"", None) {
            if k!=cur {
                let f=db_contents.contents(k);
                let cont:&[u8]=
                    match f.and_then(|mut x| x.next()) {
                        Some(ww)=>ww,
                        _=>b""
                    };
                write!(w,"n_{}[label=\"{}: {}\"];\n", k.to_hex(), k.to_hex(),
                       match std::str::from_utf8(&cont) { Ok(x)=>x.to_string(), Err(_)=> cont.to_hex() }
                ).unwrap();
                cur=k;
            }
            debug!("debug: {:?}", v.to_hex());
            let flag=v[0];
            write!(w,"n_{}->n_{}[{},label=\"{}\"];\n", k.to_hex(), &v[1..(1+patch::KEY_SIZE)].to_hex(), styles[(flag&0xff) as usize], flag).unwrap();
        }
        w.write(b"}\n").unwrap();
    }

}


pub trait Len {
    fn len(&self)->usize;
}

impl<'a> Len for &'a[u8] {
    fn len(&self)->usize {
        (self as &[u8]).len()
    }
}


pub fn eq<'a,'b,C:Iterator<Item=&'a[u8]>+Len, D:Iterator<Item=&'b[u8]>+Len>(c:&mut C, d:&mut D) -> bool {

    fn eq_rec<'a,'b,I:Iterator<Item=&'a [u8]>, J:Iterator<Item=&'b [u8]>>(sc:&[u8], c:&mut I, sd:&[u8], d:&mut J) -> bool {
        //debug!("eq_rec: {:?} {:?}", String::from_utf8_lossy(sc), String::from_utf8_lossy(sd));
        if sc.len() == 0 {
            if let Some(cc) = c.next() {
                eq_rec(cc, c, sd, d)
            } else {
                sd.len() == 0 && d.next().is_none()
            }
        } else if sd.len() == 0 {
            if let Some(dd) = d.next() {
                eq_rec(sc, c, dd, d)
            } else {
                sc.len() == 0 && c.next().is_none()
            }
        } else {
            let m = std::cmp::min(sc.len(), sd.len());
            if &sc[0..m] == &sd[0..m] {
                eq_rec(&sc[m..], c, &sd[m..], d)
            } else {
                false
            }
        }
    }
    c.len() == d.len() && eq_rec(&[], c, &[], d)
}
