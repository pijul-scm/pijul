extern crate libc;

use libc::{c_int, c_uint,c_char,size_t,c_void};
use libc::types::os::arch::posix88::mode_t;
use std::ptr;
use std::ffi::{CString};

use std::slice;
use std::fmt;
use std::str;
use std::ffi::CStr;


#[allow(non_camel_case_types)]
#[allow(missing_copy_implementations)]
pub enum MDB_env {}
pub enum MDB_txn {}

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


pub const MDB_REVERSEKEY: c_uint = 0x02;
pub const MDB_DUPSORT: c_uint = 0x04;
pub const MDB_INTEGERKEY: c_uint = 0x08;
pub const MDB_DUPFIXED: c_uint = 0x10;
pub const MDB_INTEGERDUP: c_uint = 0x20;
pub const MDB_REVERSEDUP: c_uint =  0x40;
pub const MDB_CREATE: c_uint = 0x40000;



#[repr(C)]
struct MDB_val {
    mv_size:size_t,
    mv_data: *const u8
}

impl fmt::Display for MDB_val {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // The `f` value implements the `Write` trait, which is what the
        // write! macro is expecting. Note that this formatting ignores the
        // various flags provided to format strings.
        unsafe {
            let dat = unsafe { slice::from_raw_parts(self.mv_data,self.mv_size as usize) };
            match str::from_utf8(dat){
                Ok(e)=>write!(f, "MDB_val {:?}", e),
                Err(e)=>write!(f, "MDB_val [error: could not decode utf8]")
            }
        }
    }
}



extern "C" {
    fn mdb_env_create(env: *mut *mut MDB_env) -> c_int;
    fn mdb_env_open(env: *mut MDB_env, path: *const c_char, flags: c_uint, mode: mode_t) -> c_int;
    fn mdb_env_close(env: *mut MDB_env);
    fn mdb_env_set_maxdbs(env: *mut MDB_env,maxdbs:c_uint)->c_int;
    fn mdb_env_set_mapsize(env: *mut MDB_env,mapsize:size_t)->c_int;
    fn mdb_txn_begin(env: *mut MDB_env,parent: *mut MDB_txn, flags:c_uint, txn: *mut *mut MDB_txn)->c_int;
    fn mdb_txn_commit(txn: *mut MDB_txn)->c_int;
    fn mdb_txn_abort(txn: *mut MDB_txn)->c_int;
    fn mdb_dbi_open(txn: *mut MDB_txn, name: *const c_char, flags:c_uint, dbi:*mut c_uint)->c_int;
    fn mdb_get(txn: *mut MDB_txn, dbi:c_uint, key: *mut MDB_val, val:*mut MDB_val)->c_int;
    fn mdb_put(txn: *mut MDB_txn, dbi:c_uint, key: *mut MDB_val, val:*mut MDB_val,flags:c_uint)->c_int;
}



fn withRepository<T,F>(path:&str,f: F)->T
    where F:Fn(*mut MDB_env,*mut MDB_txn)->T {
    unsafe {
        let env: *mut MDB_env = ptr::null_mut();
        mdb_env_create(std::mem::transmute(&env));
        mdb_env_set_maxdbs(env,10);
        mdb_env_set_mapsize(env,10485760 << 7);
        mdb_env_open(env,CString::new(path).unwrap().as_ptr(),0,0o755);
        let txn: *mut MDB_txn = ptr::null_mut();
        mdb_txn_begin(env,ptr::null_mut(),0,std::mem::transmute(&txn));
        let t=f(env,txn);
        mdb_txn_commit(txn);
        mdb_env_close(env);
        t
    }
v v v v v v v
=============
}

struct Repository {
    t:*mut MDB_txn,
    nodes:c_uint, nodesOpen: bool
}
impl Repository {
    fn dbiNodes(&mut self)->c_uint {
        if(self.nodesOpen) {
            self.nodes
        } else {
            unsafe {
                let st=CString::new("nodes").unwrap().as_ptr();
                mdb_dbi_open(self.t,st,MDB_CREATE|MDB_DUPSORT,std::mem::transmute(&self.nodes));
                self.nodesOpen=true;
                self.nodes
            }
        }
    }
}

fn main() {
    withRepository("/tmp/test",|env,txn| {
        println!("Hello, world!");
        let mut rep=Repository { t:txn, nodes:0, nodesOpen:false };
        let key="key";
        let value="value";
        let mut k=MDB_val { mv_size:key.len() as size_t, mv_data:key.as_ptr() };
        let mut v=MDB_val { mv_size:value.len() as size_t, mv_data:value.as_ptr() };

        unsafe { mdb_put(txn,rep.dbiNodes(),&mut k as *mut MDB_val,&mut v as *mut MDB_val,0); }

        let mut k=MDB_val { mv_size:key.len() as size_t, mv_data:key.as_ptr() };
        let mut w=MDB_val { mv_size:0 as size_t, mv_data:ptr::null_mut() };

        unsafe { mdb_get(txn,rep.dbiNodes(),&mut k as *mut MDB_val,&mut w as *mut MDB_val) };
        println!("got: {}",w);
        ()
    })
*************
}

struct Repository {
    t:*mut MDB_txn,
    nodes:c_uint, nodesOpen: bool
}
impl Repository {
    fn dbiNodes(&mut self)->c_uint {
        if(self.nodesOpen) {
            self.nodes
        } else {
            unsafe {
                let st=CString::new("nodes").unwrap().as_ptr();
                mdb_dbi_open(self.t,st,MDB_CREATE|MDB_DUPSORT,std::mem::transmute(&self.nodes));
                self.nodesOpen=true;
                self.nodes
            }
        }
    }
}

fn mdbPut(txn:*mut MDB_txn, dbi:c_uint, key:&str,value:&str,flags:c_uint) -> c_int{
    unsafe {
        let k=MDB_val { mv_size:key.len() as size_t, mv_data:key.as_ptr() };
        let v=MDB_val { mv_size:value.len() as size_t, mv_data:value.as_ptr() };
        mdb_put(txn,dbi,std::mem::transmute(&k),std::mem::transmute(&v),flags)
    }
}


fn main() {
    withRepository("/tmp/test",|env,txn| {
        println!("Hello, world!");
        let mut rep=Repository { t:txn, nodes:0, nodesOpen:false };
        let x=mdbPut(txn,rep.dbiNodes(),"a","b",0);
        println!("x={}",x);
    })
^ ^ ^ ^ ^ ^ ^
}

struct Repository {
    t:*mut MDB_txn,
    nodes:c_uint, nodesOpen: bool
}
impl Repository {
    fn dbiNodes(&mut self)->c_uint {
        if(self.nodesOpen) {
            self.nodes
        } else {
            unsafe {
                let st=CString::new("nodes").unwrap().as_ptr();
                mdb_dbi_open(self.t,st,MDB_CREATE|MDB_DUPSORT,std::mem::transmute(&self.nodes));
                self.nodesOpen=true;
                self.nodes
            }
        }
    }
}

fn main() {
    withRepository("/tmp/test",|env,txn| {
        println!("Hello, world!");
        let mut rep=Repository { t:txn, nodes:0, nodesOpen:false };
        let key="key";
        let value="value";
        let mut k=MDB_val { mv_size:key.len() as size_t, mv_data:key.as_ptr() };
        let mut v=MDB_val { mv_size:value.len() as size_t, mv_data:value.as_ptr() };

        unsafe { mdb_put(txn,rep.dbiNodes(),&mut k as *mut MDB_val,&mut v as *mut MDB_val,0); }

        let mut k=MDB_val { mv_size:key.len() as size_t, mv_data:key.as_ptr() };
        let mut w=MDB_val { mv_size:0 as size_t, mv_data:ptr::null_mut() };

        unsafe { mdb_get(txn,rep.dbiNodes(),&mut k as *mut MDB_val,&mut w as *mut MDB_val) };
        println!("got: {}",w);
        ()
    })
}
