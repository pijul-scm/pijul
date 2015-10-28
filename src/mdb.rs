extern crate libc;
use std::ptr;

use std::slice;
use std::str;
use self::libc::{c_int, c_uint,c_char,size_t};
use self::libc::types::os::arch::posix88::mode_t;
use std::fmt;

#[allow(missing_copy_implementations)]
enum MDB_env {}
enum MDB_txn {}
enum MDB_cursor {}

pub type Env=MDB_env;
pub type Txn=MDB_txn;
pub type Cursor=MDB_cursor;

#[repr(C)]
enum MDB_cursor_op {
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
pub const MDB_NOTFOUND: c_int = -30798;


#[repr(C)]
struct MDB_val {
    pub mv_size:size_t,
    pub mv_data: *const u8
}
pub type Val=MDB_val;

impl fmt::Display for MDB_val {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // The `f` value implements the `Write` trait, which is what the
        // write! macro is expecting. Note that this formatting ignores the
        // various flags provided to format strings.
        unsafe {
            let dat = slice::from_raw_parts(self.mv_data,self.mv_size as usize);
            match str::from_utf8(dat){
                Ok(e)=>write!(f, "MDB_val {:?}", e),
                Err(e)=>write!(f, "MDB_val [error: could not decode utf8]")
            }
        }
    }
}



extern "C" {
    pub fn mdb_env_create(env: *mut *mut MDB_env) -> c_int;
    pub fn mdb_env_open(env: *mut MDB_env, path: *const c_char, flags: c_uint, mode: mode_t) -> c_int;
    pub fn mdb_env_close(env: *mut MDB_env);
    pub fn mdb_env_set_maxdbs(env: *mut MDB_env,maxdbs:c_uint)->c_int;
    pub fn mdb_env_set_mapsize(env: *mut MDB_env,mapsize:size_t)->c_int;
    pub fn mdb_reader_check(env:*mut MDB_env,dead:*mut c_int)->c_int;
    pub fn mdb_txn_begin(env: *mut MDB_env,parent: *mut MDB_txn, flags:c_uint, txn: *mut *mut MDB_txn)->c_int;
    pub fn mdb_txn_commit(txn: *mut MDB_txn)->c_int;
    pub fn mdb_txn_abort(txn: *mut MDB_txn)->c_int;
    pub fn mdb_dbi_open(txn: *mut MDB_txn, name: *const c_char, flags:c_uint, dbi:*mut c_uint)->c_int;
    pub fn mdb_get(txn: *mut MDB_txn, dbi:c_uint, key: *mut MDB_val, val:*mut MDB_val)->c_int;
    pub fn mdb_put(txn: *mut MDB_txn, dbi:c_uint, key: *mut MDB_val, val:*mut MDB_val,flags:c_uint)->c_int;
    pub fn mdb_cursor_get(cursor: *mut MDB_cursor, key: *mut MDB_val, val:*mut MDB_val,flags:c_uint)->c_int;
    pub fn mdb_cursor_put(cursor: *mut MDB_cursor, key: *mut MDB_val, val:*mut MDB_val,flags:c_uint)->c_int;
    pub fn mdb_cursor_del(cursor: *mut MDB_cursor, flags:c_uint)->c_int;

    pub fn mdb_cursor_open(txn: *mut MDB_txn, dbi:c_uint, cursor:*mut *mut MDB_cursor)->c_int;
    pub fn mdb_cursor_close(cursor: *mut MDB_cursor);
}
