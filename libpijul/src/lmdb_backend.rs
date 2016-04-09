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
    use std::mem::replace;

    use super::super::Len;


    pub struct Repository<'env> {
        txn: lmdb::Txn<'env>,
        dbi_tree: lmdb::Dbi,
        dbi_revtree: lmdb::Dbi,
        dbi_inodes: lmdb::Dbi,
        dbi_revinodes: lmdb::Dbi,
        dbi_nodes: lmdb::Dbi,
        dbi_contents: lmdb::Dbi,
        dbi_internal: lmdb::Dbi,
        dbi_external: lmdb::Dbi
    }
    const MAIN_BRANCH:&'static [u8] = b"main";



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

    impl lmdb::Env {
        fn open<P:AsRef<Path>>(&self, path:P) -> Result<Self,Error> {
            let env=try!(lmdb::Env_::new());
            let _=try!(env.reader_check());
            try!(env.set_maxdbs(10));
            try!(env.set_mapsize( (1 << 30) ));
            Ok(try!(env.open(path.as_ref(),0,0o755)))
        }
        fn mut_txn_begin<'env>(&'env self) -> Result<Repository<'env>,Error> {
            unsafe {
                let txn=try!(self.unsafe_txn(0));
                let dbi_nodes=try!(txn.unsafe_dbi_open(b"nodes\0",lmdb::MDB_CREATE|lmdb::MDB_DUPSORT|lmdb::MDB_DUPFIXED));
                let dbi_tree=try!(txn.unsafe_dbi_open(b"tree\0",lmdb::MDB_CREATE));
                let dbi_revtree=try!(txn.unsafe_dbi_open(b"revtree\0",lmdb::MDB_CREATE));
                let dbi_inodes=try!(txn.unsafe_dbi_open(b"inodes\0",lmdb::MDB_CREATE));
                let dbi_revinodes=try!(txn.unsafe_dbi_open(b"revinodes\0",lmdb::MDB_CREATE));
                let dbi_contents=try!(txn.unsafe_dbi_open(b"contents\0",lmdb::MDB_CREATE));
                let dbi_internal=try!(txn.unsafe_dbi_open(b"internal\0",lmdb::MDB_CREATE));
                let dbi_external=try!(txn.unsafe_dbi_open(b"external\0",lmdb::MDB_CREATE));


                let repo = Repository{
                    txn: txn,
                    dbi_tree: dbi_tree,
                    dbi_revtree: dbi_revtree,
                    dbi_inodes: dbi_inodes,
                    dbi_revinodes: dbi_revinodes,
                    dbi_nodes: dbi_nodes,
                    dbi_contents: dbi_contents,
                    dbi_internal: dbi_internal,
                    dbi_external: dbi_external
                };
                Ok(repo)
            }
        }
    }

    pub type Db = lmdb::Dbi;
    impl<'env> Repository<'env>{
        
        pub fn db_tree(&self) -> Db { self.dbi_tree }
        pub fn set_db_tree(&mut self, db:Db) { self.dbi_tree = db }
        pub fn db_revtree(&self) -> Db { self.dbi_revtree }
        pub fn set_db_revtree(&mut self, db:Db) { self.dbi_revtree = db }
        pub fn db_inodes(&self) -> Db { self.dbi_inodes }
        pub fn set_db_inodes(&mut self, db:Db) { self.dbi_inodes = db }
        pub fn db_revinodes(&self) -> Db { self.dbi_revinodes }
        pub fn set_db_revinodes(&mut self, db:Db) { self.dbi_revinodes = db }
        pub fn db_nodes(&self, branch:&[u8]) -> Db {
            if branch == MAIN_BRANCH {
                self.dbi_nodes
            } else {
                panic!("The LMDB backend does not handle multi-head repositories")
            }
        }
        pub fn set_db_nodes(&mut self, branch:&[u8], db: Db) {
            if branch == MAIN_BRANCH {
                self.dbi_nodes = db
            } else {
                panic!("The LMDB backend does not handle multi-head repositories")
            }
        }

        pub fn db_internal(&self) -> Db { self.dbi_internal }
        pub fn set_db_internal(&mut self, db:Db) { self.dbi_internal = db }
        pub fn db_external(&self) -> Db { self.dbi_external }
        pub fn set_db_external(&mut self, db:Db) { self.dbi_external = db }


        
        pub fn put(&mut self, db:&mut Db, key:&[u8], value:&[u8]) -> Result<(),Error> {
            try!(self.txn.put(*db, key, value, 0));
            Ok(())
        }
        pub fn del(&mut self, db:&mut Db, key:&[u8], value:Option<&[u8]>) -> Result<(),Error> {
            try!(self.txn.del(*db, key, value));
            Ok(())
        }
        pub fn get<'a>(&'a self, db:&Db, key:&[u8]) -> Option<&'a[u8]> {
            self.txn.get(*db, key).unwrap_or(None)
        }

        pub fn contents<'a>(&'a self, key:&[u8]) -> Option<Contents<'a>> {
            let db = self.dbi_contents;
            self.get(&db, key).and_then(|contents| Some(Contents { contents:Some(contents) }))
        }

        pub fn iter<'a>(&'a self, db:&Db, starting_key:&[u8], starting_value:Option<&[u8]>) -> Iter<'a,'env> {
            unimplemented!()
        }
        pub fn commit(self) -> Result<(),Error> {
            try!(self.txn.commit());
            Ok(())
        }
    }
    pub struct Iter<'a,'env:'a> {
        current:Option<(&'a[u8],&'a[u8])>,
        repo:&'a Repository<'env>
    }
    impl<'a,'b> Iterator for Iter<'a,'b> {
        type Item = (&'a[u8],&'a[u8]);
        fn next(&mut self) -> Option<Self::Item> {
            unimplemented!()
        }
    }
}
