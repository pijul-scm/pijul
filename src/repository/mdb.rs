#![allow(dead_code)]
extern crate libc;
use self::libc::{c_int, c_uint,c_char,c_void,size_t};
use self::libc::types::os::arch::posix88::mode_t;
use std::ptr;

use std;
use std::io::Error;
use std::marker::PhantomData;

#[allow(missing_copy_implementations)]
pub enum MdbEnv {}
pub enum MdbTxn {}
pub enum MdbCursor {}


pub type MdbDbi=c_uint;
#[repr(C)]
pub struct MDB_val {
    pub mv_size:size_t,
    pub mv_data: *const c_void
}

#[repr(C)]
pub enum MDB_cursor_op {
    MDB_FIRST,
    MDB_FIRST_DUP,
    MDB_GET_BOTH,
    MDB_GET_BOTH_RANGE,
    MDB_GET_CURRENT,
    MDB_GET_MULTIPLE,
    MDB_LAST,
    MDB_LAST_DUP,
    MDB_NEXT,
    MDB_NEXT_DUP,
    MDB_NEXT_MULTIPLE,
    MDB_NEXT_NODUP,
    MDB_PREV,
    MDB_PREV_DUP,
    MDB_PREV_NODUP,
    MDB_SET,
    MDB_SET_KEY,
    MDB_SET_RANGE
}

extern "C" {
    pub fn mdb_env_create(env: *mut *mut MdbEnv) -> c_int;
    pub fn mdb_env_open(env: *mut MdbEnv, path: *const c_char, flags: c_uint, mode: mode_t) -> c_int;
    pub fn mdb_env_close(env: *mut MdbEnv);
    pub fn mdb_env_set_maxdbs(env: *mut MdbEnv,maxdbs:c_uint)->c_int;
    pub fn mdb_env_set_mapsize(env: *mut MdbEnv,mapsize:size_t)->c_int;
    pub fn mdb_reader_check(env:*mut MdbEnv,dead:*mut c_int)->c_int;
    pub fn mdb_txn_begin(env: *mut MdbEnv,parent: *mut MdbTxn, flags:c_uint, txn: *mut *mut MdbTxn)->c_int;
    pub fn mdb_txn_commit(txn: *mut MdbTxn)->c_int;
    pub fn mdb_txn_abort(txn: *mut MdbTxn);
    pub fn mdb_dbi_open(txn: *mut MdbTxn, name: *const c_char, flags:c_uint, dbi:*mut MdbDbi)->c_int;
    pub fn mdb_get(txn: *mut MdbTxn, dbi:MdbDbi, key: *mut MDB_val, val:*mut MDB_val)->c_int;
    pub fn mdb_put(txn: *mut MdbTxn, dbi:MdbDbi, key: *mut MDB_val, val:*mut MDB_val,flags:c_uint)->c_int;
    pub fn mdb_del(txn: *mut MdbTxn, dbi:MdbDbi, key: *mut MDB_val, val:*mut MDB_val)->c_int;
    pub fn mdb_cursor_get(cursor: *mut MdbCursor, key: *mut MDB_val, val:*mut MDB_val,flags:c_uint)->c_int;
    //pub fn mdb_cursor_put(cursor: *mut MdbCursor, key: *mut MDB_val, val:*mut MDB_val,flags:c_uint)->c_int;
    pub fn mdb_cursor_del(cursor: *mut MdbCursor, flags:c_uint)->c_int;
    pub fn mdb_cursor_open(txn: *mut MdbTxn, dbi:MdbDbi, cursor:*mut *mut MdbCursor)->c_int;
    pub fn mdb_cursor_close(cursor: *mut MdbCursor);
    pub fn mdb_drop(txn:*mut MdbTxn,dbi:MdbDbi,del:c_int)->c_int;
}

pub unsafe fn get<'a>(txn:*mut MdbTxn,dbi:MdbDbi,key:&[u8])->Result<&'a[u8],c_int> {
    let mut k=MDB_val { mv_data:key.as_ptr() as *const c_void, mv_size:key.len() as size_t };
    let mut v:MDB_val=std::mem::zeroed();
    let e=mdb_get(txn,dbi,&mut k,&mut v);
    if e==0 { Ok(std::slice::from_raw_parts(v.mv_data as *const u8, v.mv_size as usize)) } else {Err(e)}
}

pub unsafe fn put(txn:*mut MdbTxn,dbi:MdbDbi,key:&[u8],val:&[u8],flag:c_uint)->Result<(),c_int> {
    let mut k=MDB_val { mv_data:key.as_ptr() as *const c_void, mv_size:key.len() as size_t };
    let mut v=MDB_val { mv_data:val.as_ptr() as *const c_void, mv_size:val.len() as size_t };
    let e=mdb_put(txn,dbi,&mut k,&mut v,flag);
    if e==0 { Ok(()) } else {Err(e)}
}

pub unsafe fn del(txn:*mut MdbTxn,dbi:MdbDbi,key:&[u8],val:Option<&[u8]>)->Result<(),c_int> {
    let mut k=MDB_val { mv_data:key.as_ptr() as *const c_void, mv_size:key.len() as size_t };
    let e= match val {
        Some(val)=> {
            let mut v=MDB_val { mv_data:val.as_ptr() as *const c_void, mv_size:val.len() as size_t };
            mdb_del(txn,dbi,&mut k,&mut v)
        },
        None => mdb_del(txn,dbi,&mut k,std::ptr::null_mut())
    };
    if e==0 { Ok(()) } else {Err(e)}
}


//const MDB_REVERSEKEY: c_uint = 0x02;
pub const MDB_DUPSORT: c_uint = 0x04;
//const MDB_INTEGERKEY: c_uint = 0x08;
//const MDB_DUPFIXED: c_uint = 0x10;
//const MDB_INTEGERDUP: c_uint = 0x20;
//const MDB_REVERSEDUP: c_uint =  0x40;
pub const MDB_CREATE: c_uint = 0x40000;
pub const MDB_NOTFOUND: c_int = -30798;

pub const MDB_NODUPDATA:c_uint = 0x20;

pub struct Cursor<'a> {
    pub cursor:*mut MdbCursor,
    marker:PhantomData<&'a()>
}

impl <'a> Cursor<'a> {
    pub fn new(txn:*mut MdbTxn,dbi:MdbDbi)->Result<Cursor<'a>,Error>{
        unsafe {
            let curs=ptr::null_mut();
            let e=mdb_cursor_open(txn,dbi,std::mem::transmute(&curs));
            if e!=0 { Err(Error::from_raw_os_error(e)) } else { Ok(Cursor { cursor:curs,marker:PhantomData }) }
        }
    }
}
impl <'a> Drop for Cursor<'a> {
    fn drop(&mut self){
        unsafe {mdb_cursor_close(self.cursor);}
    }
}
