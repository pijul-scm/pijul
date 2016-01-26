// This file contains general bindings to lmdb.
// These are unsafe, but allow efficient operation in most circumstances.

#![allow(dead_code)]

extern crate libc;

#[cfg(not(windows))]
use self::libc::{c_int, c_uint,c_char,c_void,size_t,mode_t};
#[cfg(windows)]
use self::libc::{c_int, c_uint,c_char,c_void,size_t};
use std::ptr;
use std::ffi::CString;
use std::io::{Error};

use std::marker::PhantomData;
use std::path::Path;

use std::mem;
use std::slice;
use std::io;


#[allow(missing_copy_implementations)]
pub enum MdbEnv {}
pub enum MdbTxn {}
pub enum MdbCursor {}



#[cfg(windows)]
type mode_t=c_int;

pub type Dbi=c_uint;
#[repr(C)]
pub struct MDB_val {
    pub mv_size:size_t,
    pub mv_data: *const c_void
}

#[repr(C)]
pub enum Op {
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
    pub fn mdb_dbi_open(txn: *mut MdbTxn, name: *const c_char, flags:c_uint, dbi:*mut Dbi)->c_int;
    pub fn mdb_get(txn: *mut MdbTxn, dbi:Dbi, key: *mut MDB_val, val:*mut MDB_val)->c_int;
    pub fn mdb_put(txn: *mut MdbTxn, dbi:Dbi, key: *mut MDB_val, val:*mut MDB_val,flags:c_uint)->c_int;
    pub fn mdb_del(txn: *mut MdbTxn, dbi:Dbi, key: *mut MDB_val, val:*mut MDB_val)->c_int;
    pub fn mdb_cursor_get(cursor: *const MdbCursor, key: *mut MDB_val, val:*mut MDB_val,flags:c_uint)->c_int;
    pub fn mdb_cursor_put(cursor: *mut MdbCursor, key: *mut MDB_val, val:*mut MDB_val,flags:c_uint)->c_int;
    pub fn mdb_cursor_del(cursor: *mut MdbCursor, flags:c_uint)->c_int;
    pub fn mdb_cursor_open(txn: *mut MdbTxn, dbi:Dbi, cursor:*mut *mut MdbCursor)->c_int;
    pub fn mdb_cursor_close(cursor: *mut MdbCursor);
    pub fn mdb_drop(txn:*mut MdbTxn,dbi:Dbi,del:c_int)->c_int;
}


pub struct Env { pub env:*mut MdbEnv }

pub struct Txn<'a> { pub txn:*mut MdbTxn,env:PhantomData<&'a Env> }

unsafe fn txn<'a,'b>(env:&'a Env,parent:*mut MdbTxn,flags:usize)->Result<Txn<'b>,Error> {
    let txn=ptr::null_mut();
    let e= mdb_txn_begin(env.env,parent,flags as c_uint,mem::transmute(&txn));
    if e==0 {
        Ok(Txn { txn:txn,env:PhantomData })
    } else {
        Err(Error::from_raw_os_error(e))
    }
}
pub struct Env_ { env:*mut MdbEnv }
impl Env_ {
    pub fn new()->Result<Env_,io::Error> {
        let env=ptr::null_mut();
        let e= unsafe {mdb_env_create(mem::transmute(&env)) };
        if e==0 {
            Ok(Env_ { env:env })
        } else {
            Err(Error::from_raw_os_error(e))
        }
    }
    pub fn open(self,path:&Path,flags:c_uint,mode:mode_t)->Result<Env,io::Error> {
        unsafe {
            let cstr=CString::new(path.to_str().unwrap()).unwrap();
            let e=mdb_env_open(self.env,cstr.as_ptr() as *const c_char,
                               flags,
                               mode);
            if e==0 {
                Ok(Env { env:self.env })
            } else {
                Err(io::Error::from_raw_os_error(e))
            }
        }
    }
    pub fn reader_check(&self)->Result<usize,io::Error> {
        unsafe {
            let mut dead:c_int=0;
            let e=mdb_reader_check(self.env,&mut dead);
            if e != 0 { Err(io::Error::from_raw_os_error(e)) }
            else { Ok(dead as usize) }

        }
    }
    pub fn set_maxdbs(&self,dbs:usize)->Result<(),io::Error> {
        unsafe {
            let e=mdb_env_set_maxdbs(self.env,dbs as c_uint);
            if e != 0 { Err(io::Error::from_raw_os_error(e)) }
            else { Ok(()) }
        }
    }
    pub fn set_mapsize(&self,size:usize)->Result<(),io::Error> {
        unsafe {
            let e=mdb_env_set_mapsize(self.env,size as size_t);
            if e != 0 { Err(io::Error::from_raw_os_error(e)) }
            else { Ok(()) }
        }
    }
}


impl Env {
    pub fn txn<'a>(&'a self,flags:usize)->Result<Txn<'a>,Error> {
        unsafe { txn(&self,ptr::null_mut(),flags) }
    }
    pub unsafe fn unsafe_txn<'a,'b>(&'b self,flags:usize)->Result<Txn<'a>,Error> {
        txn(self,ptr::null_mut(),flags)
    }
}

//pub struct Dbi { dbi:MdbDbi }

impl <'a>Txn<'a> {
    pub unsafe fn unsafe_commit(&mut self) -> Result<(),Error> {
        let e=mdb_txn_commit(self.txn);
        self.txn=ptr::null_mut();
        if e==0 { Ok(()) } else { Err(Error::from_raw_os_error(e)) }
    }
    pub fn commit(self)->Result<(),Error> {
        let mut txn=self;
        let e=unsafe {mdb_txn_commit(txn.txn) };
        txn.txn=ptr::null_mut();
        if e==0 { Ok(()) } else { Err(Error::from_raw_os_error(e)) }
    }
    pub unsafe fn unsafe_abort(&mut self) {
        mdb_txn_abort(self.txn);
        self.txn=ptr::null_mut();
    }
    pub fn abort(self) {
        let mut txn=self;
        unsafe {mdb_txn_abort(txn.txn)};
        txn.txn=ptr::null_mut()
    }

    pub fn dbi_open(&self,name:&[u8],flag:c_uint)->Result<Dbi,Error> {
        let mut d=0;
        let name=try!(CString::new(name));
        let e=unsafe { mdb_dbi_open(self.txn,name.as_ptr() as *const c_char,flag,&mut d) };
        if e==0 { Ok(d) } else { Err(Error::from_raw_os_error(e)) }
    }
    pub unsafe fn unsafe_dbi_open(&self,name:&[u8],flag:c_uint)->Result<Dbi,Error> {
        let mut d=0;
        let e=mdb_dbi_open(self.txn,name.as_ptr() as *const c_char,flag,&mut d);
        if e==0 { Ok(d) } else { Err(Error::from_raw_os_error(e)) }
    }
    pub fn get<'b>(&'b self,dbi:Dbi,key:&[u8])->Result<Option<&'b[u8]>,Error> {
        unsafe {
            let mut k=MDB_val { mv_data:key.as_ptr() as *const c_void, mv_size:key.len() as size_t };
            let mut v=MDB_val { mv_data:key.as_ptr() as *const c_void, mv_size:key.len() as size_t };

            let e=mdb_get(self.txn,dbi,&mut k,&mut v);
            if e==0 { Ok(Some(slice::from_raw_parts(v.mv_data as *const u8, v.mv_size as usize))) }
            else if e==MDB_NOTFOUND {
                Ok(None)
            } else {Err(Error::from_raw_os_error(e))}
        }
    }
    pub fn put<'b>(&'b mut self,dbi:Dbi,key:&[u8],value:&[u8],flags:c_uint)->Result<bool,Error> {
        unsafe {
            let mut k=MDB_val { mv_data:key.as_ptr() as *const c_void, mv_size:key.len() as size_t };
            let mut v=MDB_val { mv_data:value.as_ptr() as *const c_void, mv_size:value.len() as size_t };
            let e=mdb_put(self.txn,dbi,&mut k,&mut v,flags);
            if e==0 {
                Ok(false)
            } else {
                if e==MDB_KEYEXIST {
                    Ok(true)
                } else {
                    Err(Error::from_raw_os_error(e))
                }
            }
        }
    }

    pub fn del<'b>(&'b mut self,dbi:Dbi,key:&[u8],val:Option<&[u8]>)->Result<bool,io::Error> {
        unsafe {
            let mut k=MDB_val { mv_data:key.as_ptr() as *const c_void, mv_size:key.len() as size_t };
            let e= match val {
                Some(val)=> {
                    let mut v=MDB_val { mv_data:val.as_ptr() as *const c_void, mv_size:val.len() as size_t };
                    mdb_del(self.txn,dbi,&mut k,&mut v)
                },
                None => mdb_del(self.txn,dbi,&mut k,ptr::null_mut())
            };
            if e==0 { Ok(true) } else if e==MDB_NOTFOUND { Ok(false) } else { Err(Error::from_raw_os_error(e)) }
        }
    }

    pub fn drop<'b>(&'b mut self,dbi:Dbi,delete_dbi:bool)->Result<(),io::Error> {
        unsafe {
            let e=mdb_drop(self.txn,dbi,if delete_dbi { 1 } else { 0 });
            if e==0 { Ok(()) }
            else { Err(io::Error::from_raw_os_error(e)) }
        }
    }


    pub fn txn<'b>(&'b self,env:&'a Env,flags:usize)->Result<Txn<'b>,Error> {
        unsafe { txn(env,self.txn,flags) }
    }

    pub unsafe fn unsafe_cursor<'b>(&'b self,dbi:Dbi)->Result<*mut MdbCursor,io::Error> {
        let curs=ptr::null_mut();
        let e=mdb_cursor_open(self.txn,dbi,mem::transmute(&curs));
        if e!=0 { Err(io::Error::from_raw_os_error(e)) }
        else { Ok(curs) }
    }

    pub fn cursor<'b>(&'b self,dbi:Dbi)->Result<Cursor<'b>,io::Error> {
        unsafe {
            Ok(Cursor { cursor:try!(self.unsafe_cursor(dbi)),txn:PhantomData })
        }
    }
}

pub struct Cursor<'a> {
    pub cursor:*mut MdbCursor,
    txn:PhantomData<&'a Txn<'a>>
}
pub struct MutCursor<'a> {
    pub cursor:*mut MdbCursor,
    txn:PhantomData<&'a Txn<'a>>
}

impl <'a> Drop for Cursor<'a> {
    fn drop(&mut self){
        unsafe {mdb_cursor_close(self.cursor);}
    }
}
impl <'a> Drop for MutCursor<'a> {
    fn drop(&mut self){
        unsafe {mdb_cursor_close(self.cursor);}
    }
}

impl <'a> Drop for Txn<'a> {
    fn drop(&mut self){
        if !self.txn.is_null() {
            unsafe { mdb_txn_abort(self.txn) }
        }
    }
}

impl Drop for Env {
    fn drop(&mut self){
        unsafe {mdb_env_close(self.env) }
    }
}

pub unsafe fn cursor_get<'a>(curs:*const MdbCursor,key:&[u8],val:Option<&[u8]>,op:Op)->Result<(&'a[u8],&'a[u8]),c_int> {
    let mut k= MDB_val { mv_data:key.as_ptr() as *const c_void, mv_size:key.len() as size_t };
    match val {
        Some(val)=>{
            let mut v=MDB_val { mv_data:val.as_ptr() as *const c_void,mv_size:val.len() as size_t };
            let e=mdb_cursor_get(curs,&mut k,&mut v,op as c_uint);
            if e==0 { Ok((slice::from_raw_parts(k.mv_data as *const u8, k.mv_size as usize),
                          slice::from_raw_parts(v.mv_data as *const u8, v.mv_size as usize))) } else {Err(e)}
        },
        None =>{
            let mut v:MDB_val = mem::zeroed();
            let e=mdb_cursor_get(curs,&mut k,&mut v,op as c_uint);
            if e==0 { Ok((slice::from_raw_parts(k.mv_data as *const u8, k.mv_size as usize),
                          slice::from_raw_parts(v.mv_data as *const u8, v.mv_size as usize))) } else {Err(e)}
        }
    }
}

pub unsafe fn cursor_del<'a>(curs:*mut MdbCursor,flag:c_uint)->Result<(),Error> {
    let e=mdb_cursor_del(curs,flag as c_uint);
    if e==0 { Ok(()) } else {
        Err(Error::from_raw_os_error(e))
    }
}

impl <'a>Cursor<'a> {

    pub fn as_ptr(&self)->*mut MdbCursor {
        self.cursor
    }

    pub fn get<'b>(&'b self,key:&[u8],val:Option<&[u8]>,op:Op)->Result<(&'a[u8],&'a[u8]),c_int> {
        unsafe {
            cursor_get(self.as_ptr(),key,val,op)
        }
    }

    pub fn put(&mut self,key:&[u8],val:&[u8],flags:c_uint)->Result<(),c_int> {
        unsafe {
            let mut k= MDB_val { mv_data:key.as_ptr() as *const c_void, mv_size:key.len() as size_t };
            let mut v=MDB_val { mv_data:val.as_ptr() as *const c_void,mv_size:val.len() as size_t };
            let e=mdb_cursor_put(self.cursor,&mut k,&mut v,flags);
            if e==0 { Ok(()) } else {Err(e)}
        }
    }
    pub fn del(&mut self,flags:c_uint)->Result<(),c_int> {
        unsafe {
            let e=mdb_cursor_del(self.cursor,flags);
            if e==0 { Ok(()) } else {Err(e)}
        }
    }
}

pub const MDB_REVERSEKEY:c_uint=0x02;
pub const MDB_DUPSORT:c_uint=0x04;
pub const MDB_INTEGERKEY:c_uint=0x08;
pub const MDB_DUPFIXED:c_uint=0x10;
pub const MDB_INTEGERDUP:c_uint=0x20;
pub const MDB_REVERSEDUP:c_uint=0x40;
pub const MDB_CREATE:c_uint=0x40000;

pub const MDB_NOTFOUND: c_int = -30798;
pub const MDB_KEYEXIST: c_int = -30799;

pub const MDB_NODUPDATA:c_uint=0x20;
