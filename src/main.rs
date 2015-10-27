extern crate libc;

use libc::{c_int, c_uint,c_char,size_t};
use libc::types::os::arch::posix88::mode_t;
use std::ptr;

use std::slice;
use std::fmt;
use std::str;


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
            let dat = slice::from_raw_parts(self.mv_data,self.mv_size as usize);
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
    fn mdb_reader_check(env:*mut MDB_env,dead:*mut c_int)->c_int;
    fn mdb_txn_begin(env: *mut MDB_env,parent: *mut MDB_txn, flags:c_uint, txn: *mut *mut MDB_txn)->c_int;
    fn mdb_txn_commit(txn: *mut MDB_txn)->c_int;
    fn mdb_txn_abort(txn: *mut MDB_txn)->c_int;
    fn mdb_dbi_open(txn: *mut MDB_txn, name: *const c_char, flags:c_uint, dbi:*mut c_uint)->c_int;
    fn mdb_get(txn: *mut MDB_txn, dbi:c_uint, key: *mut MDB_val, val:*mut MDB_val)->c_int;
    fn mdb_put(txn: *mut MDB_txn, dbi:c_uint, key: *mut MDB_val, val:*mut MDB_val,flags:c_uint)->c_int;
}



fn with_repository<T,F>(path:&str,f: F)->T
    where F:Fn(*mut MDB_env,*mut MDB_txn)->T {
    unsafe {
        let env: *mut MDB_env = ptr::null_mut();
        mdb_env_create(std::mem::transmute(&env));
        let mut dead:c_int=0;
        mdb_reader_check(env,&mut dead);
        mdb_env_set_maxdbs(env,10);
        mdb_env_set_mapsize(env,10485760 << 7);
        mdb_env_open(env,path.as_ptr() as *const c_char,0,0o755);
        let txn: *mut MDB_txn = ptr::null_mut();
        mdb_txn_begin(env,ptr::null_mut(),0,std::mem::transmute(&txn));
        let t=f(env,txn);
        mdb_txn_commit(txn);
        mdb_env_close(env);
        t
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

fn main() {
    with_repository("/tmp/test",|_,txn| {
        println!("Hello, world!");
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
                unsafe {
                    let ok=mdb_get(txn,dbi,&mut k as *mut MDB_val,&mut v as *mut MDB_val);
                    if ok==0 {
                        rep.current_branch=slice::from_raw_parts(v.mv_data,v.mv_size as usize).to_vec()
                    }
                };
            },
            Err(_)=>()
        };





        ////////////
        let key="key";
        let value="value";
        match rep.dbi_nodes() {
            Ok(dbi)=>{
                let mut k=MDB_val { mv_size:key.len() as size_t, mv_data:key.as_ptr() };
                let mut v=MDB_val { mv_size:value.len() as size_t, mv_data:value.as_ptr() };

                unsafe { mdb_put(txn,dbi,&mut k as *mut MDB_val,&mut v as *mut MDB_val,0); }

                let mut k=MDB_val { mv_size:key.len() as size_t, mv_data:key.as_ptr() };
                let mut w=MDB_val { mv_size:0 as size_t, mv_data:ptr::null_mut() };

                unsafe { mdb_get(txn,dbi,&mut k as *mut MDB_val,&mut w as *mut MDB_val) };
                println!("got: {}",w)
            },
            Err(e)=>{
                println!("error {}",e)
            }
        }
        ()
    })
}
