extern crate libc;
#[macro_use]
extern crate bitflags;


#[cfg(not(windows))]
use self::libc::{c_int, c_uint,c_char,c_void,size_t,mode_t};
#[cfg(windows)]
use self::libc::{c_int, c_uint,c_char,c_void,size_t};
use std::ptr;

use std::io::{Error};

use std::marker::PhantomData;
use std::path::Path;
#[allow(missing_copy_implementations)]
pub enum MdbEnv {}
pub enum MdbTxn {}
pub enum MdbCursor {}

#[cfg(windows)]
type mode_t=c_int;


pub type MdbDbi=c_uint;
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
    fn mdb_env_create(env: *mut *mut MdbEnv) -> c_int;
    fn mdb_env_open(env: *mut MdbEnv, path: *const c_char, flags: c_uint, mode: mode_t) -> c_int;
    fn mdb_env_close(env: *mut MdbEnv);
    fn mdb_env_set_maxdbs(env: *mut MdbEnv,maxdbs:c_uint)->c_int;
    fn mdb_env_set_mapsize(env: *mut MdbEnv,mapsize:size_t)->c_int;
    fn mdb_reader_check(env:*mut MdbEnv,dead:*mut c_int)->c_int;
    fn mdb_txn_begin(env: *mut MdbEnv,parent: *mut MdbTxn, flags:c_uint, txn: *mut *mut MdbTxn)->c_int;
    fn mdb_txn_commit(txn: *mut MdbTxn)->c_int;
    fn mdb_txn_abort(txn: *mut MdbTxn);
    fn mdb_dbi_open(txn: *mut MdbTxn, name: *const c_char, flags:c_uint, dbi:*mut MdbDbi)->c_int;
    fn mdb_get(txn: *mut MdbTxn, dbi:MdbDbi, key: *mut MDB_val, val:*mut MDB_val)->c_int;
    fn mdb_put(txn: *mut MdbTxn, dbi:MdbDbi, key: *mut MDB_val, val:*mut MDB_val,flags:c_uint)->c_int;
    fn mdb_del(txn: *mut MdbTxn, dbi:MdbDbi, key: *mut MDB_val, val:*mut MDB_val)->c_int;
    fn mdb_cursor_get(cursor: *mut MdbCursor, key: *mut MDB_val, val:*mut MDB_val,flags:c_uint)->c_int;
    fn mdb_cursor_put(cursor: *mut MdbCursor, key: *mut MDB_val, val:*mut MDB_val,flags:c_uint)->c_int;
    fn mdb_cursor_del(cursor: *mut MdbCursor, flags:c_uint)->c_int;
    fn mdb_cursor_open(txn: *mut MdbTxn, dbi:MdbDbi, cursor:*mut *mut MdbCursor)->c_int;
    fn mdb_cursor_close(cursor: *mut MdbCursor);
    fn mdb_drop(txn:*mut MdbTxn,dbi:MdbDbi,del:c_int)->c_int;
}


pub struct Env { env:*mut MdbEnv }

pub struct Txn<'a> { txn:*mut MdbTxn,env:PhantomData<&'a Env> }

fn txn<'a>(env:&'a Env,parent:*mut MdbTxn)->Result<Txn<'a>,Error> {
    let txn=ptr::null_mut();
    let e= unsafe {mdb_txn_begin(env.env,parent,0,std::mem::transmute(&txn)) };
    if e==0 {
        Ok(Txn { txn:txn,env:PhantomData })
    } else {
        Err(Error::from_raw_os_error(e))
    }
}
pub struct Env_ { env:*mut MdbEnv }
impl Env_ {
    pub fn new()->Result<Env_,c_int> {
        let env=ptr::null_mut();
        let e= unsafe {mdb_env_create(std::mem::transmute(&env)) };
        if e==0 {
            Ok(Env_ { env:env })
        } else {
            Err(e)
        }
    }
    pub fn open(self,path:&Path,flags:c_uint,mode:mode_t)->Result<Env,c_int> {
        unsafe {
            let e=mdb_env_open(self.env,path.to_str().unwrap().as_ptr() as *const c_char,
                               flags,
                               mode);
            if e==0 {
                Ok(Env { env:self.env })
            } else {
                Err(e)
            }
        }
    }
    pub fn reader_check(&self)->Result<usize,std::io::Error> {
        unsafe {
            let mut dead:c_int=0;
            let e=mdb_reader_check(self.env,&mut dead);
            if e != 0 { Err(std::io::Error::from_raw_os_error(e)) }
            else { Ok(dead as usize) }

        }
    }
    pub fn set_maxdbs(&self,dbs:usize)->Result<(),std::io::Error> {
        unsafe {
            let e=mdb_env_set_maxdbs(self.env,dbs as c_uint);
            if e != 0 { Err(std::io::Error::from_raw_os_error(e)) }
            else { Ok(()) }
        }
    }
    pub fn set_mapsize(&self,size:usize)->Result<(),std::io::Error> {
        unsafe {
            let e=mdb_env_set_mapsize(self.env,size as size_t);
            if e != 0 { Err(std::io::Error::from_raw_os_error(e)) }
            else { Ok(()) }
        }
    }
}


impl Env {
    pub fn txn<'a>(&'a self)->Result<Txn<'a>,Error> {
        txn(&self,std::ptr::null_mut())
    }
}

pub struct Dbi<'a> { dbi:MdbDbi, env:PhantomData<&'a Env> }

impl <'a>Txn<'a> {
    pub fn commit(self)->Result<(),Error> {
        let e=unsafe {mdb_txn_commit(self.txn)};
        if e==0 { Ok(()) } else { Err(Error::from_raw_os_error(e)) }
    }
    pub fn abort(self) {
        unsafe {mdb_txn_abort(self.txn)}
    }
    pub fn dbi_open(&self,name:&[u8],flag:DbiOpen)->Result<Dbi<'a>,Error> {
        let mut d=0;
        let e=unsafe { mdb_dbi_open(self.txn,name.as_ptr() as *const c_char,flag.bits(),&mut d) };
        if e==0 { Ok(Dbi { dbi:d, env:self.env }) } else { Err(Error::from_raw_os_error(e)) }
    }
    pub fn get<'b>(&'b self,dbi:&Dbi<'a>,key:&[u8])->Result<Option<&'b[u8]>,Error> {
        unsafe {
            let mut k=MDB_val { mv_data:key.as_ptr() as *const c_void, mv_size:key.len() as size_t };
            let mut v=MDB_val { mv_data:key.as_ptr() as *const c_void, mv_size:key.len() as size_t };

            let e=mdb_get(self.txn,dbi.dbi,&mut k,&mut v);
            if e==0 { Ok(Some(std::slice::from_raw_parts(v.mv_data as *const u8, v.mv_size as usize))) }
            else if e==MDB_NOTFOUND {
                Ok(None)
            } else {Err(Error::from_raw_os_error(e))}
        }
    }
    pub fn put<'b>(&'b mut self,dbi:&Dbi<'a>,key:&[u8],value:&[u8],flags:Put)->Result<(),Error> {
        unsafe {
            let mut k=MDB_val { mv_data:key.as_ptr() as *const c_void, mv_size:key.len() as size_t };
            let mut v=MDB_val { mv_data:value.as_ptr() as *const c_void, mv_size:value.len() as size_t };
            let e=mdb_put(self.txn,dbi.dbi,&mut k,&mut v,flags.bits());
            if e==0 { Ok(()) } else { Err(Error::from_raw_os_error(e)) }
        }
    }

    pub fn del<'b>(&'b mut self,dbi:&Dbi<'a>,key:&[u8],val:Option<&[u8]>)->Result<bool,std::io::Error> {
        unsafe {
            let mut k=MDB_val { mv_data:key.as_ptr() as *const c_void, mv_size:key.len() as size_t };
            let e= match val {
                Some(val)=> {
                    let mut v=MDB_val { mv_data:val.as_ptr() as *const c_void, mv_size:val.len() as size_t };
                    mdb_del(self.txn,dbi.dbi,&mut k,&mut v)
                },
                None => mdb_del(self.txn,dbi.dbi,&mut k,std::ptr::null_mut())
            };
            if e==0 { Ok(true) } else if e==MDB_NOTFOUND { Ok(false) } else { Err(Error::from_raw_os_error(e)) }
        }
    }

    pub fn drop<'b>(&'b mut self,dbi:&Dbi<'a>,delete_dbi:bool)->Result<(),std::io::Error> {
        unsafe {
            let e=mdb_drop(self.txn,dbi.dbi,if delete_dbi { 1 } else { 0 });
            if e==0 { Ok(()) }
            else { Err(std::io::Error::from_raw_os_error(e)) }
        }
    }


    pub fn txn<'b>(&'b self,env:&'a Env)->Result<Txn<'b>,Error> {
        txn(env,self.txn)
    }


    pub fn cursor<'b>(&'b mut self,dbi:&Dbi)->Result<Cursor<'b>,std::io::Error> {
        unsafe {
            let curs=ptr::null_mut();
            let e=mdb_cursor_open(self.txn,dbi.dbi,std::mem::transmute(&curs));
            if e!=0 { Err(std::io::Error::from_raw_os_error(e)) }
            else { Ok(Cursor { cursor:curs,txn:PhantomData }) }
        }
    }
}

pub struct Cursor<'a> {
    pub cursor:*mut MdbCursor,
    txn:PhantomData<&'a Txn<'a>>
}

impl <'a> Drop for Cursor<'a> {
    fn drop(&mut self){
        unsafe {mdb_cursor_close(self.cursor);}
    }
}

impl <'a> Drop for Txn<'a> {
    fn drop(&mut self){
        unsafe { mdb_txn_abort(self.txn) }
    }
}

impl Drop for Env {
    fn drop(&mut self){
        unsafe {mdb_env_close(self.env) }
    }
}

impl <'a>Cursor<'a> {
    pub fn get(&self,key:&[u8],val:Option<&[u8]>,op:Op)->Result<&'a[u8],c_int> {
        unsafe {
            let mut k= MDB_val { mv_data:key.as_ptr() as *const c_void, mv_size:key.len() as size_t };
            match val {
                Some(val)=>{
                    let mut v=MDB_val { mv_data:val.as_ptr() as *const c_void,mv_size:val.len() as size_t };
                    let e=mdb_cursor_get(self.cursor,&mut k,&mut v,op as c_uint);
                    if e==0 { Ok(std::slice::from_raw_parts(v.mv_data as *const u8, v.mv_size as usize)) } else {Err(e)}
                },
                None =>{
                    let mut v:MDB_val = std::mem::zeroed();
                    let e=mdb_cursor_get(self.cursor,&mut k,&mut v,op as c_uint);
                    if e==0 { Ok(std::slice::from_raw_parts(v.mv_data as *const u8, v.mv_size as usize)) } else {Err(e)}
                }
            }
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

bitflags! {
    flags DbiOpen:c_uint {
        const MDB_REVERSEKEY=0x02,
        const MDB_DUPSORT=0x04,
        const MDB_INTEGERKEY=0x08,
        const MDB_DUPFIXED=0x10,
        const MDB_INTEGERDUP=0x20,
        const MDB_REVERSEDUP=0x40,
        const MDB_CREATE=0x40000
    }
}

pub const MDB_NOTFOUND: c_int = -30798;
pub const MDB_KEYEXIST: c_int = -30799;

bitflags! {
    flags Put:c_uint {
        const MDB_NODUPDATA=0x20
    }
}
