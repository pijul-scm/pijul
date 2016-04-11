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
#[macro_use]
extern crate log;
extern crate time;

mod lmdb;
pub mod error;
use self::error::*;

pub trait RepositoryEnv<'env, R>:Sized {
    fn open<P:AsRef<Path>>(&self, path:P) -> Result<Self, Error>;
    fn mut_txn_begin(&'env self) -> Result<R,Error>;
}

extern crate rustc_serialize;

//use self::contents::{Inode, OwnedInode, Line, Graph, LineBuffer};
//use self::contents::{LINE_ONSTACK, LINE_VISITED, DIRECTORY_FLAG, INODE_SIZE, ROOT_INODE};

extern crate rand;

pub mod fs_representation;
pub mod patch;

mod lmdb_backend;
pub use lmdb_backend::backend;

mod file_operations;

mod graph;

mod optimal_diff;
pub use optimal_diff::diff;

impl <'a,W> graph::LineBuffer<'a> for W where W:std::io::Write {
    fn output_line(&mut self,_:&[u8],c:backend::Contents) {
        for i in c {
            self.write(i).unwrap(); // .expect("output_line: could not write");
        }
    }
}


mod record;
mod output;
mod apply;

pub const DEFAULT_BRANCH:&'static str = "main";
pub type Transaction<'env> = backend::Transaction<'env,()>;

pub use backend::Repository;

impl<'env,T> backend::Transaction<'env,T> {
    fn add_file<P:AsRef<Path>>(&mut self, path:P, is_dir:bool)->Result<(),Error>{
        let mut db_tree = self.db_tree();
        let mut db_revtree = self.db_revtree();
        file_operations::add_inode(&mut db_tree, &mut db_revtree, None, path.as_ref(), is_dir)
    }
    fn list_files(&self) -> Vec<PathBuf> {
        file_operations::list_files(self)
    }

    fn remove_file<P:AsRef<Path>>(&mut self, path:P) -> Result<(),Error> {
        file_operations::remove_file(self, path.as_ref())
    }
    fn move_file<P:AsRef<Path>, Q:AsRef<Path>>(&mut self, path:P, path_:Q,is_dir:bool) -> Result<(), Error>{
        file_operations::move_file(self, path.as_ref(), path_.as_ref(), is_dir)
    }
    fn retrieve_and_output<W:std::io::Write>(&self,branch:&backend::Db,key:&[u8],l:&mut W) {
        let db_contents = self.db_contents();
        let mut redundant_edges = Vec::new();
        let graph = graph::retrieve(branch,key).unwrap();
        graph::output_file(branch, &db_contents, l, graph,&mut redundant_edges);
    }

    fn write_changes_file<P:AsRef<Path>>(&self, branch_name:&[u8], path:P)->Result<(),Error> {
        unimplemented!()
    }

    pub fn debug<W>(&self,branch_name:&[u8], w:&mut W) where W:std::io::Write {
        unimplemented!() /*
            let mut styles=Vec::with_capacity(16);
            for i in 0..16 {
            styles.push(("color=").to_string()
            +["red","blue","green","black"][(i >> 1)&3]
            +if (i as u8)&DELETED_EDGE!=0 { ", style=dashed"} else {""}
            +if (i as u8)&PSEUDO_EDGE!=0 { ", style=dotted"} else {""})
    }
            w.write(b"digraph{\n").unwrap();
            let curs=self.txn.cursor(self.dbi_nodes).unwrap();
            let mut op=lmdb::Op::MDB_FIRST;
            let mut cur=&[][..];
            while let Ok((k,v))=curs.get(cur,None,op) {
            op=lmdb::Op::MDB_NEXT;
            if k!=cur {
            let f=self.txn.get(self.dbi_contents, k);
            let cont:&[u8]=
            match f {
            Ok(Some(ww))=>ww,
            _=>&[]
    };
            write!(w,"n_{}[label=\"{}: {}\"];\n", k.to_hex(), k.to_hex(),
            match str::from_utf8(&cont) { Ok(x)=>x.to_string(), Err(_)=> cont.to_hex() }
    ).unwrap();
            cur=k;
    }
            let flag=v[0];
            if true || flag & PARENT_EDGE == 0 {
            write!(w,"n_{}->n_{}[{},label=\"{}\"];\n", k.to_hex(), &v[1..(1+KEY_SIZE)].to_hex(), styles[(flag&0xff) as usize], flag).unwrap();
    }
    }
            w.write(b"}\n").unwrap();*/
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
                eq_rec(&sc[m..], c, &sd[0..m], d)
            } else {
                false
            }
        }
    }
    c.len() == d.len() && eq_rec(&[], c, &[], d)
}
