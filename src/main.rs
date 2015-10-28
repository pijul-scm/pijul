extern crate libc;

use libc::{c_int, c_uint,c_char,size_t};
use libc::types::os::arch::posix88::mode_t;
use std::ptr;

use std::slice;
use std::fmt;
use std::str;
extern crate pijul;
use pijul::mdb::{MDB_env,MDB_txn,mdb_env_create,mdb_reader_check,mdb_env_set_maxdbs,mdb_env_set_mapsize,mdb_env_open,mdb_txn_begin,mdb_txn_commit,mdb_txn_abort,mdb_env_close,mdb_dbi_open,MDB_CREATE,MDB_DUPSORT,MDB_val,MDB_NOTFOUND};
use pijul::mdb;

fn with_repository<T,F>(path:&str,f: F)->Result<T,c_int>
    where F:Fn(*mut MDB_env,*mut MDB_txn)->Result<T,c_int> {
        unsafe {
            let env: *mut MDB_env = ptr::null_mut();
            mdb_env_create(std::mem::transmute(&env));
            let mut dead:c_int=0;
            let e=mdb_reader_check(env,&mut dead);
            if e != 0 { println!("mdb_reader_check");return Err(e) };
            let e=mdb_env_set_maxdbs(env,10);
            if e != 0 { println!("mdb_env_set_maxdbs");return Err(e) };
            let e=mdb_env_set_mapsize(env,std::ops::Shl::shl(1,30) as size_t);
            if e !=0 { println!("mdb_env_set_mapsize");return Err(e) };
            let e=mdb_env_open(env,path.as_ptr() as *const c_char,0,0o755);
            if e !=0 { println!("mdb_env_open");return Err(e) };
            let txn: *mut MDB_txn = ptr::null_mut();
            let e=mdb_txn_begin(env,ptr::null_mut(),0,std::mem::transmute(&txn));
            if e!=0 { println!("mdb_txn_begin");mdb_txn_abort(txn);return Err(e) };
            let x=
                match f(env,txn) {
                    Ok(x)=>{
                        let e=mdb_txn_commit(txn);
                        if e==0 {Ok(x)} else {Err(e)}
                    },
                    Err(e)=>{
                        let _=mdb_txn_abort(txn);
                        Err(e)
                    }
                };
            mdb_env_close(env);
            x
        }
    }

struct Repository {
    t:*mut MDB_txn,
    nodes: Result<c_uint,c_int>,
    contents: Result<c_uint,c_int>,
    revdep: Result<c_uint,c_int>,
    internalhashes: Result<c_uint,c_int>,
    externalhashes: Result<c_uint,c_int>,
    branches: Result<c_uint,c_int>,
    tree: Result<c_uint,c_int>,
    revtree: Result<c_uint,c_int>,
    inodes: Result<c_uint,c_int>,
    revinodes: Result<c_uint,c_int>,
    current_branch: Vec<u8>
}

fn open_base(t:*mut MDB_txn,base:&mut Result<c_uint,c_int>, name:&str, flags:c_uint)->Result<c_uint,c_int>{
    match *base {
        Ok(n)=>Ok(n),
        Err(_)=>{
            let mut x=0;
            let result=unsafe {mdb_dbi_open(t,name.as_ptr() as *const c_char,flags,&mut x)};
            if result==0 {
                *base=Ok(x);
                *base
            } else {
                Err(result)
            }
        }
    }
}

fn mdb_put(t:*mut MDB_txn,base:c_uint,key:&mut MDB_val,value:&mut MDB_val,flag:c_uint)->Result<(),c_int>{
    unsafe {
        let ret=mdb::mdb_put(t,base,key as *mut MDB_val, value as *mut MDB_val,flag);
        if ret==0 {Ok(())} else
            {Err(ret)}
    }
}
fn mdb_get(t:*mut MDB_txn,base:c_uint,key:&mut MDB_val,value:&mut MDB_val)->Result<bool,c_int>{
    unsafe {
        let ret=mdb::mdb_get(t,base,key as *mut MDB_val, value as *mut MDB_val);
        if ret==0 {Ok(true)} else
            if ret==MDB_NOTFOUND {Ok(false)} else {Err(ret)}
    }
}

impl Repository {
    fn dbi_nodes(&mut self)->Result<c_uint,c_int> { open_base(self.t,&mut self.nodes,"nodes",MDB_CREATE|MDB_DUPSORT) }
    fn dbi_contents(&mut self)->Result<c_uint,c_int> { open_base(self.t,&mut self.contents,"contents",MDB_CREATE) }
    fn dbi_revdep(&mut self)->Result<c_uint,c_int> { open_base(self.t,&mut self.contents,"revdep",MDB_CREATE|MDB_DUPSORT) }
    fn dbi_internalhashes(&mut self)->Result<c_uint,c_int> { open_base(self.t,&mut self.internalhashes,"internal",MDB_CREATE) }
    fn dbi_externalhashes(&mut self)->Result<c_uint,c_int> { open_base(self.t,&mut self.externalhashes,"external",MDB_CREATE) }
    fn dbi_branches(&mut self)->Result<c_uint,c_int> { open_base(self.t,&mut self.branches,"branches",MDB_CREATE|MDB_DUPSORT) }
    fn dbi_tree(&mut self)->Result<c_uint,c_int> { open_base(self.t,&mut self.tree,"tree",MDB_CREATE|MDB_DUPSORT) }
    fn dbi_revtree(&mut self)->Result<c_uint,c_int> { open_base(self.t,&mut self.revtree,"revtree",MDB_CREATE) }
    fn dbi_inodes(&mut self)->Result<c_uint,c_int> { open_base(self.t,&mut self.inodes,"inodes",MDB_CREATE) }
    fn dbi_revinodes(&mut self)->Result<c_uint,c_int> { open_base(self.t,&mut self.revinodes,"revinodes",MDB_CREATE) }
}

fn from_val(v:MDB_val)->Vec<u8>{unsafe {slice::from_raw_parts(v.mv_data,v.mv_size as usize).to_vec()}}


fn main() {
    let x=
        with_repository("/tmp/test\0",|_,txn| {
            let mut rep=
                Repository {
                    t:txn,
                    nodes: Err(0),
                    contents: Err(0),
                    revdep: Err(0),
                    internalhashes: Err(0),
                    externalhashes: Err(0),
                    branches: Err(0),
                    tree: Err(0),
                    revtree: Err(0),
                    inodes: Err(0),
                    revinodes: Err(0),
                    current_branch: Vec::from("main")
                };
            match rep.dbi_branches() {
                Ok(dbi)=>{
                    let mut k=MDB_val { mv_size:1 as size_t, mv_data:"\0".as_ptr() };
                    let mut v=MDB_val { mv_size:0 as size_t, mv_data:ptr::null_mut() };
                    match mdb_get(txn,dbi,&mut k,&mut v) {
                        Ok(e) => {
                            if e {rep.current_branch=from_val(v)};
                            Ok(0)
                        },
                        Err(e)=> { Err(e) }
                    }
                },
                Err(e)=>{Err(e)}
            }
        });
    match x {
        Ok(_)=>println!("ok"),
        Err(e)=>println!("err:{}",e)
    }
}
