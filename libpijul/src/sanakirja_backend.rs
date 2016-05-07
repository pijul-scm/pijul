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


pub mod backend {
    //use super::super::lmdb;
    use super::super::error::Error;
    use super::super::Len;
    use sanakirja;
    use sanakirja::Transaction as Tra;
    use std::path::Path;
    use std::marker::PhantomData;
    use rand;
    use std::cell::UnsafeCell;
    use std;
    
    pub struct Transaction<'env,T> {
        txn: UnsafeCell<sanakirja::MutTxn<'env,T>>,
        db_tree: UnsafeCell<sanakirja::Db>,
        db_revtree: UnsafeCell<sanakirja::Db>,
        db_inodes: UnsafeCell<sanakirja::Db>,
        db_revinodes: UnsafeCell<sanakirja::Db>,
        db_contents: UnsafeCell<sanakirja::Db>,
        db_internal: UnsafeCell<sanakirja::Db>,
        db_external: UnsafeCell<sanakirja::Db>,
        db_branches: UnsafeCell<sanakirja::Db>,
        db_revdep: UnsafeCell<sanakirja::Db>,
        db_nodes: UnsafeCell<sanakirja::Db>,
    }

    pub const DEFAULT_BRANCH:&'static str = "main";

    pub struct Contents<'a,'env:'a,T:'a> {value:sanakirja::Value<'a,sanakirja::MutTxn<'env,T>>}

    impl<'a,'env,T> Iterator for Contents<'a,'env,T> {
        type Item = &'a[u8];
        fn next(&mut self) -> Option<Self::Item> {
            self.value.next()
        }
    }
    impl<'a,'env,T> Len for Contents<'a,'env,T> {
        fn len(&self) -> usize {
            self.value.len() as usize
        }
    }

    impl<'a,'env,T> Contents<'a,'env,T> {
        pub fn from_slice(x:&'a [u8])->Contents<'a,'env,T> {
            Contents { value:sanakirja::Value::from_slice(x) }
        }
        pub fn clone(&self) -> Contents<'a,'env,T> {
            Contents { value:self.value.clone() }
        }
    }
    pub struct Repository { env:sanakirja::Env }
    #[derive(Debug,PartialEq)]
    enum Root {
        TREE,
        REVTREE,
        INODES,
        REVINODES,
        CONTENTS,
        INTERNAL,
        EXTERNAL,
        BRANCHES,
        REVDEP,
        NODES
    }
    fn open_db<T>(txn:&mut sanakirja::MutTxn<T>, num:Root) -> Result<sanakirja::Db, sanakirja::Error> {
        if let Some(db) = txn.root(num as isize) {
            Ok(db)
        } else {
            txn.create_db()
        }
    }
    impl Repository {
        pub fn open<P:AsRef<Path>>(path:P) -> Result<Self,Error> {
            Ok(Repository { env: try!(sanakirja::Env::new(path, 1<<30)) })
        }
        pub fn mut_txn_begin<'env>(&'env self) -> Result<Transaction<'env,()>,Error> {
            let mut txn = self.env.mut_txn_begin();
            let db_tree = try!(open_db(&mut txn, Root::TREE));
            let db_revtree = try!(open_db(&mut txn, Root::REVTREE));
            let db_inodes = try!(open_db(&mut txn, Root::INODES));
            let db_revinodes = try!(open_db(&mut txn, Root::REVINODES));
            let db_contents = try!(open_db(&mut txn, Root::CONTENTS));
            let db_internal = try!(open_db(&mut txn, Root::INTERNAL));
            let db_external = try!(open_db(&mut txn, Root::EXTERNAL));
            let db_branches = try!(open_db(&mut txn, Root::BRANCHES));
            let db_revdep = try!(open_db(&mut txn, Root::REVDEP));
            let db_nodes = try!(open_db(&mut txn, Root::NODES));
            
            let repo = Transaction {
                txn: UnsafeCell::new(txn),
                db_tree: UnsafeCell::new(db_tree),
                db_revtree: UnsafeCell::new(db_revtree),
                db_inodes: UnsafeCell::new(db_inodes),
                db_revinodes: UnsafeCell::new(db_revinodes),
                db_contents: UnsafeCell::new(db_contents),
                db_internal: UnsafeCell::new(db_internal),
                db_external: UnsafeCell::new(db_external),
                db_branches: UnsafeCell::new(db_branches),
                db_revdep: UnsafeCell::new(db_revdep),
                db_nodes: UnsafeCell::new(db_nodes)
            };
            Ok(repo)
        }
    }

    pub struct Db<'txn,'env,T> {
        db: *mut sanakirja::Db,
        txn:*mut sanakirja::MutTxn<'env,T>,
        marker:PhantomData<&'txn()>,
    }
    pub struct Branch<'name,'txn,'env,T> {
        db: sanakirja::Db,
        name: &'name str,
        parent: *mut sanakirja::Db,
        txn:*mut sanakirja::MutTxn<'env,T>,
        marker:PhantomData<&'txn()>,
    }

    impl<'env,T> Transaction<'env,T>{
        
        pub fn db_tree<'txn>(&'txn self) -> Db<'txn,'env,T> {
            unsafe {
                Db { db:self.db_tree.get(),
                     txn:self.txn.get(),
                     marker:PhantomData,
                }
            }
        }
        pub fn db_revtree<'txn>(&'txn self) -> Db<'txn,'env,T> {
            unsafe {
                Db { db:self.db_revtree.get(),
                     txn:self.txn.get(),
                     marker:PhantomData,
                                    }
            }
        }
        pub fn db_inodes<'txn>(&'txn self) -> Db<'txn,'env,T> {
            unsafe {
                Db { db: self.db_inodes.get(),
                     txn: self.txn.get(),
                     marker:PhantomData,
                }
            }
        }
        pub fn db_revinodes<'txn>(&'txn self) -> Db<'txn,'env,T> {
            unsafe {
                Db { db:self.db_revinodes.get(),
                     txn: self.txn.get(),
                     marker:PhantomData,
                }
            }
        }
        pub fn db_contents<'txn>(&'txn self) -> Db<'txn,'env,T> {
            unsafe {
                Db { db: self.db_contents.get(),
                     txn: self.txn.get(),
                     marker:PhantomData,
                }
            }
        }
        pub fn db_revdep<'txn>(&'txn self) -> Db<'txn,'env,T> {
            unsafe {
                Db { db: self.db_revdep.get(),
                     txn: self.txn.get(),
                     marker:PhantomData,
                }
            }
        }
        pub fn db_nodes<'name,'txn,'a>(&'txn self, name:&'name str) -> Result<Branch<'name,'txn,'env,T>, Error> {
            unsafe {
                let txn = &mut *self.txn.get();
                let db_nodes = &mut *self.db_nodes.get();
                let branch =
                    if let Some(branch) = txn.open_db(&db_nodes, name.as_bytes()) {
                        branch
                    } else {
                        try!(txn.create_db())
                    };
                Ok(Branch { db: branch,
                            name: name,
                            txn: txn,
                            marker: PhantomData,
                            parent: db_nodes
                })
            }
        }
        pub fn db_branches<'txn>(&'txn self) -> Db<'txn,'env,T> {
            unsafe {
                Db { db: self.db_branches.get(),
                     txn: self.txn.get(),
                     marker:PhantomData,
                }
            }
        }

        pub fn db_internal<'txn>(&'txn self) -> Db<'txn,'env,T> {
            unsafe {
                Db { db: self.db_internal.get(),
                     txn: self.txn.get(),
                     marker:PhantomData,
                }
            }
        }
        pub fn db_external<'txn>(&'txn self) -> Db<'txn,'env,T> {
            unsafe {
                Db { db: self.db_external.get(),
                     txn: self.txn.get(),
                     marker:PhantomData,
                }
            }
        }
        pub fn dump<W:std::io::Write>(&self, mut w:W) {
            let databases = [(Root::TREE, &self.db_tree),
                             (Root::REVTREE, &self.db_revtree),
                             (Root::INODES, &self.db_inodes),
                             (Root::REVINODES, &self.db_revinodes),
                             (Root::CONTENTS, &self.db_contents),
                             (Root::INTERNAL, &self.db_internal),
                             (Root::EXTERNAL, &self.db_external),
                             (Root::BRANCHES, &self.db_branches),
                             (Root::REVDEP, &self.db_revdep),
                             (Root::NODES, &self.db_nodes)];
            let txn = unsafe {&mut *self.txn.get() };
            let mut ws=Vec::new();
            let mut ws0=Vec::new();
            let mut v_=Vec::new();
            for &(ref name,ref i) in databases.iter() {
                write!(w,"\n--------\ndatabase {:?}\n\n", name);
                if *name == Root::NODES {
                    for (k,mut v) in txn.iter(unsafe {&*i.get()}, b"", None, &mut ws) {
                        write!(w, "{:?} {:?}\n", k.to_hex(), v_.to_hex());
                        let db = unsafe { sanakirja::Db::from_value(v.next().unwrap()) };

                        for (k,v) in txn.iter(&db, b"", None, &mut ws0) {
                            v_.clear();
                            for vv in v {
                                v_.extend(vv)
                            }
                            write!(w, " > {:?}\n   {:?}\n", k.to_hex(), v_.to_hex());
                        }
                    }
                } else {
                    for (k,v) in txn.iter(unsafe {&*i.get()}, b"", None, &mut ws) {
                        v_.clear();
                        for vv in v {
                            v_.extend(vv)
                        }
                        write!(w, "{:?} {:?}\n", k.to_hex(), v_.to_hex());
                    }
                }
            }
        }
        pub fn abort(self) {
            // self.txn.abort();
        }
        pub fn child(&mut self) -> Transaction<'env, &mut sanakirja::transaction::MutTxn<'env,T>> {
            unsafe {
                // The clones here are fine, since we cannot perform
                // any operation on the parent while the child is in
                // scope.
                let txn = (&mut *self.txn.get()).mut_txn_begin();
                let repo = Transaction {
                    txn: UnsafeCell::new(txn),
                    db_tree: UnsafeCell::new((&*self.db_tree.get()).clone()),
                    db_revtree: UnsafeCell::new((&*self.db_revtree.get()).clone()),
                    db_inodes: UnsafeCell::new((&*self.db_inodes.get()).clone()),
                    db_revinodes: UnsafeCell::new((&*self.db_revinodes.get()).clone()),
                    db_contents: UnsafeCell::new((&*self.db_contents.get()).clone()),
                    db_internal: UnsafeCell::new((&*self.db_internal.get()).clone()),
                    db_external: UnsafeCell::new((&*self.db_external.get()).clone()),
                    db_branches: UnsafeCell::new((&*self.db_branches.get()).clone()),
                    db_revdep: UnsafeCell::new((&*self.db_revdep.get()).clone()),
                    db_nodes: UnsafeCell::new((&* self.db_nodes.get()).clone())
                };
                repo
            }
        }
    }

    impl <'env> Transaction<'env,()> {
        pub fn commit(self) -> Result<(),Error> {
            unsafe {
                let mut txn = self.txn.into_inner();

                txn.set_root(Root::TREE as isize, self.db_tree.into_inner());
                txn.set_root(Root::REVTREE as isize, self.db_revtree.into_inner());
                txn.set_root(Root::INODES as isize, self.db_inodes.into_inner());
                txn.set_root(Root::REVINODES as isize, self.db_revinodes.into_inner());
                txn.set_root(Root::CONTENTS as isize, self.db_contents.into_inner());
                txn.set_root(Root::INTERNAL as isize, self.db_internal.into_inner());
                txn.set_root(Root::EXTERNAL as isize, self.db_external.into_inner());
                txn.set_root(Root::BRANCHES as isize, self.db_branches.into_inner());
                txn.set_root(Root::REVDEP as isize, self.db_revdep.into_inner());
                txn.set_root(Root::NODES as isize, self.db_nodes.into_inner());

                try!(txn.commit());
                Ok(())
            }
        }
    }
    pub struct Workspace { ws:Vec<u64> }
    impl Workspace {
        pub fn new() -> Workspace {
            Workspace{ ws:Vec::new() }
        }
    }
    use rustc_serialize::hex::ToHex;



    impl<'txn,'env,T> Db<'txn,'env,T> {
        
        pub fn put(&mut self, key:&[u8], value:&[u8]) -> Result<(),Error> {
            debug!("put {:?} {:?}", key.to_hex(), value.to_hex());
            let mut rng = rand::thread_rng();
            unsafe {
                let mut txn = &mut *self.txn;
                try!(txn.put(&mut rng, &mut *self.db, key, value));
            }
            Ok(())
        }
        pub fn del(&mut self, key:&[u8], value:Option<&[u8]>) -> Result<(),Error> {
            debug!("del {:?} {:?}", key.to_hex(), value);
            let mut rng = rand::thread_rng();
            unsafe {
                let mut txn = &mut *self.txn;
                try!(txn.del(&mut rng, &mut * self.db, key, value));
            }
            Ok(())
        }
        pub fn get<'a>(&'a self, key:&[u8]) -> Option<&'a[u8]> {
            unsafe {
                let txn = &*self.txn;
                txn.get(&*self.db, key, None).and_then(|mut x| x.next())
            }
        }
        pub fn iter<'a,'b>(&'a self, ws:&'b mut Workspace, starting_key:&[u8], starting_value:Option<&[u8]>) -> Iter<'a,'b,sanakirja::MutTxn<'env,T>> {
            unsafe {
                let txn = &*self.txn;
                Iter { iter: txn.iter(&*self.db, starting_key, starting_value, &mut ws.ws) }
            }
        }
        pub fn contents<'a>(&'a self, key:&[u8]) -> Option<Contents<'a,'env,T>> {
            unsafe {
                let txn = &*self.txn;
                txn.get(&*self.db, key, None).and_then(|x| Some(Contents { value:x }))
            }
        }
    }



    impl<'name,'txn,'env,T> Branch<'name,'txn,'env,T> {
        
        pub fn put(&mut self, key:&[u8], value:&[u8]) -> Result<(),Error> {
            debug!("put {:?} {:?}", key.to_hex(), value.to_hex());
            let mut rng = rand::thread_rng();
            unsafe {
                let mut txn = &mut *self.txn;
                try!(txn.put(&mut rng, &mut self.db, key, value));
            }
            Ok(())
        }
        pub fn del(&mut self, key:&[u8], value:Option<&[u8]>) -> Result<(),Error> {
            debug!("del {:?} {:?}", key.to_hex(), value);
            let mut rng = rand::thread_rng();
            unsafe {
                let mut txn = &mut *self.txn;
                try!(txn.del(&mut rng, &mut self.db, key, value));
            }
            Ok(())
        }
        pub fn get<'a>(&'a self, key:&[u8]) -> Option<&'a[u8]> {
            unsafe {
                let txn = &*self.txn;
                txn.get(&self.db, key, None).and_then(|mut x| x.next())
            }
        }
        pub fn iter<'a,'b>(&'a self, ws:&'b mut Workspace, starting_key:&[u8], starting_value:Option<&[u8]>) -> Iter<'a,'b,sanakirja::MutTxn<'env,T>> {
            unsafe {
                let txn = &*self.txn;
                Iter { iter: txn.iter(&self.db, starting_key, starting_value, &mut ws.ws) }
            }
        }
        pub fn contents<'a>(&'a self, key:&[u8]) -> Option<Contents<'a,'env,T>> {
            unsafe {
                let txn = &*self.txn;
                txn.get(&self.db, key, None).and_then(|x| Some(Contents { value:x }))
            }
        }
        pub fn commit_branch(self, name:&str) -> Result<(),Error> {
            unsafe {
                let mut rng = rand::thread_rng();
                let txn = &mut *self.txn;
                try!(txn.put_db(&mut rng, &mut *self.parent, name.as_bytes(), self.db));
                Ok(())
            }
        }
    }



    pub struct Iter<'a,'b,T:'a> {iter:sanakirja::Iter<'a,'b,T>}
    impl<'a,'b,'env,T> Iterator for Iter<'a,'b,sanakirja::MutTxn<'env,T>> {
        type Item=(&'a[u8],&'a[u8]);
        fn next(&mut self)->Option<Self::Item> {
            if let Some((a,mut b)) = self.iter.next() {
                let b0 = b.next();
                assert!(b.next().is_none());
                Some((a,b0.unwrap_or(b"")))
            } else {
                None
            }
        }
    }
}
