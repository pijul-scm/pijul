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

use super::error::*;
use std;

pub mod backend {
    use super::super::lmdb;
    use std::path::Path;
    use super::super::error::Error;
    use std::mem::{replace,transmute};

    use super::super::Len;
    use std::ptr::null_mut;

    pub struct Transaction<'env,Parent> {
        txn: lmdb::Txn<'env>,
        dbi_tree: lmdb::Dbi,
        dbi_revtree: lmdb::Dbi,
        dbi_inodes: lmdb::Dbi,
        dbi_revinodes: lmdb::Dbi,
        dbi_nodes: lmdb::Dbi,
        dbi_contents: lmdb::Dbi,
        dbi_internal: lmdb::Dbi,
        dbi_external: lmdb::Dbi,
        dbi_branches: lmdb::Dbi,
        dbi_revdep: lmdb::Dbi,
        parent:Parent
    }
    pub const DEFAULT_BRANCH:&'static str = "main";



    pub struct Contents<'a> {
        pub contents:Option<&'a [u8]>
    }

    impl<'a> Iterator for Contents<'a> {
        type Item = &'a[u8];
        fn next(&mut self) -> Option<&'a[u8]> {
            replace(&mut self.contents, None)
        }
    }

    impl<'a> Len for Contents<'a> {
        fn len(&self) -> usize {
            self.contents.map(|x| x.len()).unwrap_or(0)
        }
    }

    impl<'a> Contents<'a> {
        pub fn from_slice<'b>(x:&'b [u8])->Contents<'b> {
            Contents { contents: Some(x) }
        }
        pub fn clone(&self) -> Contents<'a> {
            Contents { contents: self.contents }
        }
    }
    pub type Repository = lmdb::Env;
    impl Repository {
        pub fn open<P:AsRef<Path>>(path:P) -> Result<Self,Error> {
            let env=try!(lmdb::Env_::new());
            let _=try!(env.reader_check());
            try!(env.set_maxdbs(10));
            try!(env.set_mapsize( (1 << 30) ));
            Ok(try!(env.open(path.as_ref(),0,0o755)))
        }
        pub fn mut_txn_begin<'env>(&'env self) -> Result<Transaction<'env,()>,Error> {
            unsafe {
                let txn=try!(self.txn(0));
                let dbi_nodes=try!(txn.unsafe_dbi_open(b"nodes\0",lmdb::MDB_CREATE|lmdb::MDB_DUPSORT|lmdb::MDB_DUPFIXED));
                let dbi_tree=try!(txn.unsafe_dbi_open(b"tree\0",lmdb::MDB_CREATE));
                let dbi_revtree=try!(txn.unsafe_dbi_open(b"revtree\0",lmdb::MDB_CREATE));
                let dbi_inodes=try!(txn.unsafe_dbi_open(b"inodes\0",lmdb::MDB_CREATE));
                let dbi_revinodes=try!(txn.unsafe_dbi_open(b"revinodes\0",lmdb::MDB_CREATE));
                let dbi_contents=try!(txn.unsafe_dbi_open(b"contents\0",lmdb::MDB_CREATE));
                let dbi_internal=try!(txn.unsafe_dbi_open(b"internal\0",lmdb::MDB_CREATE));
                let dbi_external=try!(txn.unsafe_dbi_open(b"external\0",lmdb::MDB_CREATE));
                let dbi_branches=try!(txn.unsafe_dbi_open(b"branches\0",lmdb::MDB_CREATE));
                let dbi_revdep=try!(txn.unsafe_dbi_open(b"revdep\0",lmdb::MDB_CREATE|lmdb::MDB_DUPSORT));


                let repo = Transaction {
                    txn: txn,
                    dbi_tree: dbi_tree,
                    dbi_revtree: dbi_revtree,
                    dbi_inodes: dbi_inodes,
                    dbi_revinodes: dbi_revinodes,
                    dbi_nodes: dbi_nodes,
                    dbi_contents: dbi_contents,
                    dbi_internal: dbi_internal,
                    dbi_external: dbi_external,
                    dbi_branches: dbi_branches,
                    dbi_revdep: dbi_revdep,
                    parent:()
                };
                Ok(repo)
            }
        }
    }

    pub struct Db<'txn,'env:'txn> { dbi: lmdb::Dbi, txn:&'txn lmdb::Txn<'env> }

    impl<'env,T> Transaction<'env,T>{
        
        pub fn db_tree<'txn>(&'txn self) -> Db<'txn,'env> { Db { dbi:self.dbi_tree,txn:&self.txn } }
        pub fn db_revtree<'txn>(&'txn self) -> Db<'txn,'env> { Db { dbi:self.dbi_revtree, txn:&self.txn } }

        pub fn db_inodes<'txn>(&'txn self) -> Db<'txn,'env> { Db { dbi: self.dbi_inodes, txn:&self.txn } }
        pub fn db_revinodes<'txn>(&'txn self) -> Db<'txn,'env> { Db { dbi:self.dbi_revinodes, txn:&self.txn } }

        pub fn db_contents<'txn>(&'txn self) -> Db<'txn,'env> { Db { dbi: self.dbi_contents, txn:&self.txn } }

        pub fn db_revdep<'txn>(&'txn self) -> Db<'txn,'env> { Db { dbi: self.dbi_revdep, txn:&self.txn } }

        pub fn db_nodes<'txn>(&'txn self, branch:&str) -> Db<'txn,'env> {
            if branch == DEFAULT_BRANCH {
                Db { dbi:self.dbi_nodes, txn:&self.txn }
            } else {
                panic!("The LMDB backend does not handle multi-head repositories")
            }
        }

        pub fn db_branches<'txn>(&'txn self) -> Db<'txn,'env> {
            Db { dbi:self.dbi_branches, txn:&self.txn }
        }

        pub fn db_internal(&self) -> Db { Db { dbi: self.dbi_internal, txn: &self.txn } }
        pub fn db_external(&self) -> Db { Db { dbi: self.dbi_external, txn: &self.txn } }

        pub fn abort(self) {
            self.txn.abort();
        }
        pub fn child(&mut self) -> Transaction<'env, &mut Self> {
            unsafe {
                let parent_txn = self.txn.txn;
                let txn = null_mut();
                let e = lmdb::mdb_txn_begin(self.txn.env.env,self.txn.txn,0,transmute(&txn));
                assert!(e==0);
                let repo = Transaction {
                    txn: lmdb::Txn { txn:txn, env:self.txn.env },
                    dbi_tree: self.dbi_tree,
                    dbi_revtree: self.dbi_revtree,
                    dbi_inodes: self.dbi_inodes,
                    dbi_revinodes: self.dbi_revinodes,
                    dbi_nodes: self.dbi_nodes,
                    dbi_contents: self.dbi_contents,
                    dbi_internal: self.dbi_internal,
                    dbi_external: self.dbi_external,
                    dbi_branches: self.dbi_branches,
                    dbi_revdep: self.dbi_revdep,
                    parent: self
                };
                repo
            }
        }
    }

    impl <'env,T> Transaction<'env,T> {
        pub fn commit(self) -> Result<(),Error> {
            try!(self.txn.commit());
            Ok(())
        }
    }

    
    impl<'txn,'env> Db<'txn,'env> {
        
        pub fn put(&mut self, key:&[u8], value:&[u8]) -> Result<(),Error> {
            try!(self.txn.put(self.dbi, key, value, 0));
            Ok(())
        }
        pub fn del(&mut self, key:&[u8], value:Option<&[u8]>) -> Result<(),Error> {
            try!(self.txn.del(self.dbi, key, value));
            Ok(())
        }
        pub fn get<'a>(&'a self, key:&[u8]) -> Option<&'a[u8]> {
            self.txn.get(self.dbi, key).unwrap_or(None)
        }
        pub fn iter<'a>(&'a self, starting_key:&[u8], starting_value:Option<&[u8]>) -> Iter<'a> {
            unsafe {
                let curs = self.txn.cursor(self.dbi).unwrap();
                let current = lmdb::cursor_get(curs.cursor, starting_key, starting_value, lmdb::Op::MDB_SET_RANGE).ok();
                Iter {
                    current: current,
                    cursor: curs
                }
            }
        }

        pub fn contents<'a>(&'a self, key:&[u8]) -> Option<Contents<'a>> {
            self.get(key).and_then(|contents| Some(Contents { contents:Some(contents) }))
        }

    }
    pub struct Iter<'a> {
        current:Option<(&'a[u8],&'a[u8])>,
        cursor: lmdb::Cursor<'a>,
    }

    impl<'a> Iterator for Iter<'a> {
        type Item = (&'a[u8],&'a[u8]);
        fn next(&mut self) -> Option<Self::Item> {
            debug!("{:?}", self.current);
            unsafe {
                if let Some((key,value)) = self.current {
                    self.current = lmdb::cursor_get(self.cursor.cursor, key, Some(value), lmdb::Op::MDB_NEXT).ok();
                    Some((key,value))
                } else {
                    None
                }
            }
        }
    }
}
