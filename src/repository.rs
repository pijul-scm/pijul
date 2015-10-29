extern crate libc;

use self::libc::{c_int, c_uint,c_char,size_t};
use self::libc::types::os::arch::posix88::mode_t;
use std::ptr;

use std::slice;
use std::fmt;
use std::str;
use std;
use mdb::{Val,mdb_env_create,mdb_reader_check,mdb_env_set_maxdbs,mdb_env_set_mapsize,mdb_env_open,mdb_txn_begin,mdb_txn_commit,mdb_txn_abort,mdb_env_close,mdb_dbi_open,mdb_cursor_get,mdb_cursor_put,mdb_cursor_del,MDB_CREATE,MDB_DUPSORT,MDB_NOTFOUND};
use mdb;

/*
fn with_cursor<T,F>(txn:*mut Txn,dbi:c_uint,f:F)->Result<T,c_int>
    where F:Fn(*mut Cursor)->Result<T,c_int> {
        unsafe {
            let mut cursor:*mut Cursor=ptr::null_mut();
            let ok=mdb_cursor_open(txn,dbi,std::mem::transmute(&txn));
            if ok!=0 { return Err(ok) };
            let x=f(cursor);
            mdb_cursor_close(cursor);
            x
        }
    }
 */

fn mdb_cursor_open(txn:*mut Txn,dbi:c_uint)->Result<*mut Cursor,c_int> {
    unsafe {
        let mut cursor:*mut Cursor=ptr::null_mut();
        let ok=mdb::mdb_cursor_open(txn,dbi,std::mem::transmute(&txn));
        if ok!=0 { Err(ok) } else { Ok(cursor) }
    }
}

fn mdb_cursor_close(curs:*mut Cursor) {
    unsafe {
        mdb::mdb_cursor_close(curs);
    }
}


pub struct Repository {
    pub t:*mut mdb::Txn,
    pub nodes: Result<c_uint,c_int>,
    pub contents: Result<c_uint,c_int>,
    pub revdep: Result<c_uint,c_int>,
    pub internalhashes: Result<c_uint,c_int>,
    pub externalhashes: Result<c_uint,c_int>,
    pub branches: Result<c_uint,c_int>,
    pub tree: Result<c_uint,c_int>,
    pub revtree: Result<c_uint,c_int>,
    pub inodes: Result<c_uint,c_int>,
    pub revinodes: Result<c_uint,c_int>,
    pub current_branch: Vec<u8>
}


pub fn open_base(t:*mut Txn,base:&mut Result<c_uint,c_int>, name:&str, flags:c_uint)->Result<c_uint,c_int>{
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

fn mdb_put(t:*mut Txn,base:c_uint,key:&mut Val,value:&mut Val,flag:c_uint)->Result<(),c_int>{
    unsafe {
        let ret=mdb::mdb_put(t,base,key as *mut Val, value as *mut Val,flag);
        if ret==0 {Ok(())} else
            {Err(ret)}
    }
}
fn mdb_get(t:*mut Txn,base:c_uint,key:&mut Val,value:&mut Val)->Result<bool,c_int>{
    unsafe {
        let ret=mdb::mdb_get(t,base,key as *mut Val, value as *mut Val);
        if ret==0 {Ok(true)} else
            if ret==MDB_NOTFOUND {Ok(false)} else {Err(ret)}
    }
}
pub fn from_val(v:Val)->Vec<u8>{unsafe {slice::from_raw_parts(v.mv_data,v.mv_size as usize).to_vec()}}

impl Repository {
    pub fn dbi_nodes(&mut self)->Result<c_uint,c_int> { open_base(self.t,&mut self.nodes,"nodes",MDB_CREATE|MDB_DUPSORT) }
    pub fn dbi_contents(&mut self)->Result<c_uint,c_int> { open_base(self.t,&mut self.contents,"contents",MDB_CREATE) }
    pub fn dbi_revdep(&mut self)->Result<c_uint,c_int> { open_base(self.t,&mut self.contents,"revdep",MDB_CREATE|MDB_DUPSORT) }
    pub fn dbi_internalhashes(&mut self)->Result<c_uint,c_int> { open_base(self.t,&mut self.internalhashes,"internal",MDB_CREATE) }
    pub fn dbi_externalhashes(&mut self)->Result<c_uint,c_int> { open_base(self.t,&mut self.externalhashes,"external",MDB_CREATE) }
    pub fn dbi_branches(&mut self)->Result<c_uint,c_int> { open_base(self.t,&mut self.branches,"branches",MDB_CREATE|MDB_DUPSORT) }
    pub fn dbi_tree(&mut self)->Result<c_uint,c_int> { open_base(self.t,&mut self.tree,"tree",MDB_CREATE|MDB_DUPSORT) }
    pub fn dbi_revtree(&mut self)->Result<c_uint,c_int> { open_base(self.t,&mut self.revtree,"revtree",MDB_CREATE) }
    pub fn dbi_inodes(&mut self)->Result<c_uint,c_int> { open_base(self.t,&mut self.inodes,"inodes",MDB_CREATE) }
    pub fn dbi_revinodes(&mut self)->Result<c_uint,c_int> { open_base(self.t,&mut self.revinodes,"revinodes",MDB_CREATE) }
}

impl Repository {
    pub fn new(txn:*mut mdb::Txn)->Repository{
        let mut rep=Repository {
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
                let mut k=Val { mv_size:1 as size_t, mv_data:"\0".as_ptr() };
                let mut v=Val { mv_size:0 as size_t, mv_data:ptr::null_mut() };
                match mdb_get(txn,dbi,&mut k,&mut v) {
                    Ok(e) => { if e {rep.current_branch=from_val(v)} }
                    Err(e)=> { }
                }
            },
            Err(e)=>{ }
        };
        rep
    }
}

const INODE_SIZE:usize = 10;

macro_rules! with_cursor {
    ( $txn:expr,$dbi:expr,$x:ident,$e:expr )=> ({
        {
            let $x=try!(mdb_cursor_open($txn,$dbi));
            let x=$e;
            mdb_cursor_close($x);
            x
        }
    })
}


pub fn initialize_repository(env:*mut *mut Env,txn:*mut *mut Txn,path:&str)->Result<(),c_int> {
    unsafe {
        mdb_env_create(env);
        let mut dead:c_int=0;
        let e=mdb_reader_check(*env,&mut dead);
        if e != 0 { println!("mdb_reader_check");return Err(e) };
        let e=mdb_env_set_maxdbs(*env,10);
        if e != 0 { println!("mdb_env_set_maxdbs");return Err(e) };
        let e=mdb_env_set_mapsize(*env,std::ops::Shl::shl(1,30) as size_t);
        if e !=0 { println!("mdb_env_set_mapsize");return Err(e) };
        let e=mdb_env_open(*env,path.as_ptr() as *const c_char,0,0o755);
        if e !=0 { println!("mdb_env_open");return Err(e) };

        let e=mdb_txn_begin(*env,ptr::null_mut(),0,txn);
        if e!=0 { println!("mdb_txn_begin");mdb_txn_abort(*txn);mdb_env_close(*env);return Err(e) };
        Ok(())
    }
}

pub fn env_close(env:*mut Env){
    unsafe {
        mdb_env_close(env);
    }
}
pub fn txn_commit(txn:*mut Txn)->c_int{
    unsafe { mdb_txn_commit(txn) }
}

pub fn txn_abort(txn:*mut Txn)->c_int{
    unsafe { mdb_txn_abort(txn) }
}
pub type Env=mdb::Env;
pub type Txn=mdb::Txn;
pub type Cursor=mdb::Cursor;

impl Drop for Cursor {
    fn drop(&mut self){
        println!("cursor dropped!");
        unsafe {mdb_cursor_close(self)}
    }
}

#[macro_export]
macro_rules! with_repository {
    ( $path:expr, $env:ident, $txn:ident, $f:expr )=> ({
        let $env: *mut $crate::repository::Env = std::ptr::null_mut();
        let $txn: *mut $crate::repository::Txn = std::ptr::null_mut();
        match unsafe { $crate::repository::initialize_repository(std::mem::transmute(&$env),
                                                                 std::mem::transmute(&$txn),
                                                                 $path) } {
            Ok(())=> {
                let x=
                    match { $f } {
                        Ok(x)=>{
                            let e=$crate::repository::txn_commit($txn);
                            if e==0 {Ok(x)} else {Err(e)}
                        },
                        Err(e)=>{
                            let _=$crate::repository::txn_abort($txn);
                            Err(e)
                        }
                    };
                $crate::repository::env_close($env);
                x
            },
            Err(e)=>{Err(e)}
        }
    })
}



pub fn add_inode(txn:*mut mdb::Txn,repo:&mut Repository,inode0:Vec<u8>,path0:Vec<&str>)->Result<(),c_int>{
    let dbi_nodes=try!(repo.dbi_nodes());
    let mut inode:Vec<u8>=Vec::with_capacity(INODE_SIZE);

    let curs=mdb_cursor_open(txn,dbi_nodes);

    Ok(())
}
