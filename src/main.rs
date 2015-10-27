extern crate libc;

use libc::{c_int, c_uint,c_char,size_t,c_void};//, c_void, c_char, size_t};
use libc::types::os::arch::posix88::mode_t;
use std::ptr;
use std::ffi::{CString};

#[allow(non_camel_case_types)]
#[allow(missing_copy_implementations)]
pub enum MDB_env {}
pub enum MDB_txn {}


#[repr(C)]
struct MDB_val {
    mv_size:size_t,
    mv_data: *mut c_void
}


extern "C" {
    fn mdb_env_create(env: *mut *mut MDB_env) -> c_int;
    fn mdb_env_open(env: *mut MDB_env, path: *const c_char, flags: c_uint, mode: mode_t) -> c_int;
    fn mdb_env_close(env: *mut MDB_env);
}

fn main() {
    println!("Hello, world!");
    unsafe {
        let env: *mut MDB_env = ptr::null_mut();
        //let p_env: *mut *mut MDB_env = std::mem::transmute(&env);
        mdb_env_create(std::mem::transmute(&env));
        let c = CString::new("/tmp/test").unwrap();
        println!("{}",mdb_env_open(env,c.as_ptr(),0,0o755));
        mdb_env_close(env);
    }
}
