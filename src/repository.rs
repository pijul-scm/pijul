extern crate libc;
use self::libc::{c_int, c_uint,c_char,size_t};
use self::libc::types::os::arch::posix88::mode_t;
use self::libc::funcs::c95::string::strncpy;
use std::ptr;

use std::slice;
use std::fmt;
use std::str;
use std;
use std::collections::HashMap;
extern crate rand;

const INODE_SIZE:usize=16;

#[allow(missing_copy_implementations)]
pub enum MDB_env {}
pub enum MDB_txn {}
pub enum MDB_cursor {}

pub struct Repository{
    mdb_env:*mut MDB_env
}
extern "C" {
    pub fn mdb_env_create(env: *mut *mut MDB_env) -> c_int;
    pub fn mdb_env_open(env: *mut MDB_env, path: *const c_char, flags: c_uint, mode: mode_t) -> c_int;
    pub fn mdb_env_close(env: *mut MDB_env);
    pub fn mdb_env_set_maxdbs(env: *mut MDB_env,maxdbs:c_uint)->c_int;
    pub fn mdb_env_set_mapsize(env: *mut MDB_env,mapsize:size_t)->c_int;
    pub fn mdb_reader_check(env:*mut MDB_env,dead:*mut c_int)->c_int;
}
impl Repository {
    pub fn new(path:&std::path::Path)->Result<Repository,c_int>{
        unsafe {
            let env=ptr::null_mut();
            let e=mdb_env_create(std::mem::transmute(&env));
            if e != 0 { println!("mdb_env_create");return Err(e) };
            let mut dead:c_int=0;
            let e=mdb_reader_check(env,&mut dead);
            if e != 0 { println!("mdb_reader_check");return Err(e) };
            let e=mdb_env_set_maxdbs(env,10);
            if e != 0 { println!("mdb_env_set_maxdbs");return Err(e) };
            let e=mdb_env_set_mapsize(env,std::ops::Shl::shl(1,30) as size_t);
            if e !=0 { println!("mdb_env_set_mapsize");return Err(e) };
            let p=path.as_os_str().to_str();
            match p {
                Some(pp) => {
                    let e=mdb_env_open(env,pp.as_ptr() as *const i8,0,0o755);
                    if e !=0 { println!("mdb_env_open");return Err(e) };

                    let repo=Repository{
                        mdb_env:env
                    };
                    Ok(repo)
                },
                None => {
                    println!("invalid path");
                    Err(0)
                }
            }
        }
    }
}

fn add_inode(repo:&mut Repository, inode:&Option<[u8;INODE_SIZE]>, path:&std::path::Component){
    ()
}
