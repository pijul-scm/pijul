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

#[allow(missing_copy_implementations)]
pub enum MDB_env {}
pub enum MDB_txn {}
pub enum MDB_cursor {}
type MDB_dbi=c_uint;
#[repr(C)]
pub struct MDB_val {
    pub mv_size:size_t,
    pub mv_data: *const c_char
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
    fn mdb_txn_abort(txn: *mut MDB_txn);
    fn mdb_dbi_open(txn: *mut MDB_txn, name: *const c_char, flags:c_uint, dbi:*mut MDB_dbi)->c_int;
    fn mdb_get(txn: *mut MDB_txn, dbi:MDB_dbi, key: *mut MDB_val, val:*mut MDB_val)->c_int;
    fn mdb_put(txn: *mut MDB_txn, dbi:MDB_dbi, key: *mut MDB_val, val:*mut MDB_val,flags:c_uint)->c_int;
    fn mdb_cursor_get(cursor: *mut MDB_cursor, key: *mut MDB_val, val:*mut MDB_val,flags:c_uint)->c_int;
    fn mdb_cursor_put(cursor: *mut MDB_cursor, key: *mut MDB_val, val:*mut MDB_val,flags:c_uint)->c_int;
    fn mdb_cursor_del(cursor: *mut MDB_cursor, flags:c_uint)->c_int;
    fn mdb_cursor_open(txn: *mut MDB_txn, dbi:MDB_dbi, cursor:*mut *mut MDB_cursor)->c_int;
    fn mdb_cursor_close(cursor: *mut MDB_cursor);
}


pub const MDB_REVERSEKEY: c_uint = 0x02;
pub const MDB_DUPSORT: c_uint = 0x04;
pub const MDB_INTEGERKEY: c_uint = 0x08;
pub const MDB_DUPFIXED: c_uint = 0x10;
pub const MDB_INTEGERDUP: c_uint = 0x20;
pub const MDB_REVERSEDUP: c_uint =  0x40;
pub const MDB_CREATE: c_uint = 0x40000;
pub const MDB_NOTFOUND: c_int = -30798;

const INODE_SIZE:usize=16;

const MAX_DBS:usize=10;
pub enum DBI {
    NODES,
    CONTENTS,
    REVDEP,
    INTERNAL_HASHES,
    EXTERNAL_HASHES,
    BRANCHES,
    TREE,
    REVTREE,
    INODES,
    REVINODES
}

const dbis:[(&'static str,c_uint);MAX_DBS]=[("nodes\0",MDB_CREATE|MDB_DUPSORT),
                                            ("contents\0",MDB_CREATE),
                                            ("revdep\0",MDB_CREATE|MDB_DUPSORT),
                                            ("internal\0",MDB_CREATE),
                                            ("external\0",MDB_CREATE),
                                            ("branches\0",MDB_CREATE|MDB_DUPSORT),
                                            ("tree\0",MDB_CREATE|MDB_DUPSORT),
                                            ("revtree\0",MDB_CREATE),
                                            ("inodes\0",MDB_CREATE),
                                            ("revinodes\0",MDB_CREATE)
                                            ];

pub struct Repository{
    mdb_env:*mut MDB_env,
    mdb_txn:*mut MDB_txn,
    dbi_:[Option<MDB_dbi>;MAX_DBS]
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
            let e=mdb_env_set_maxdbs(env,MAX_DBS as c_uint);
            if e != 0 { println!("mdb_env_set_maxdbs");return Err(e) };
            let e=mdb_env_set_mapsize(env,std::ops::Shl::shl(1,30) as size_t);
            if e !=0 { println!("mdb_env_set_mapsize");return Err(e) };
            let p=path.as_os_str().to_str();
            match p {
                Some(pp) => {
                    let e=mdb_env_open(env,pp.as_ptr() as *const i8,0,0o755);
                    if e !=0 { println!("mdb_env_open");return Err(e) };

                    let txn=ptr::null_mut();
                    let e=mdb_txn_begin(env,ptr::null_mut(),0,std::mem::transmute(&txn));
                    if e !=0 { println!("mdb_env_open");return Err(e) };

                    let repo=Repository{
                        mdb_env:env,
                        mdb_txn:txn,
                        dbi_:[None;MAX_DBS]
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
    fn dbi(&mut self,num:DBI)->MDB_dbi{
        let n=num as usize;
        match self.dbi_[n] {
            Some(dbi) => {dbi},
            None => {
                unsafe {
                    let d=0;
                    let (name,flag)=dbis[n];
                    unsafe {mdb_dbi_open(self.mdb_txn,name.as_ptr() as *const c_char,flag,std::mem::transmute(&d))};
                    self.dbi_[n]=Some(d);
                    d
                }
            }
        }
    }
}

impl Drop for Repository {
    fn drop(&mut self){
        unsafe {
            mdb_txn_abort(self.mdb_txn);
            mdb_env_close(self.mdb_env)
        }
    }
}

fn add_inode(repo:&mut Repository, inode:&Option<[c_char;INODE_SIZE]>, path:&std::path::Path)->Result<(),()>{
    let mut buf:Vec<c_char>=Vec::with_capacity(INODE_SIZE);
    // Init to 0
    for i in 0..INODE_SIZE-1 {
        buf.push(0)
    }
    let mut components=path.components();
    let mut cs=components.next();
    while cs.is_some(){
        let s=cs.unwrap();
        cs=components.next();
        match s.as_os_str().to_str(){
            Some(ss) => {
                buf.truncate(INODE_SIZE);
                for c in ss.as_bytes() { buf.push(*c as c_char) }
                let mut k=MDB_val { mv_data:buf.as_ptr(), mv_size:buf.len()as size_t };
                let mut v=MDB_val { mv_data:ptr::null_mut(), mv_size:0 };
                let ret= unsafe { mdb_get(repo.mdb_txn,repo.dbi(DBI::TREE),&mut k,&mut v) };
                if ret==0 {
                    // replace buf with existing inode
                    buf.clear();
                    let pv:*const c_char=v.mv_data;
                    unsafe { for c in 0..v.mv_size-1 { buf.push(*pv.offset(c as isize)) } }
                } else {
                    let inode = if cs.is_none() && inode.is_some() {
                        inode.unwrap()
                    } else {
                        let mut inode:[c_char;INODE_SIZE]=[0;INODE_SIZE];
                        for i in 0..INODE_SIZE-1 { inode[i]=rand::random() }
                        inode
                    };
                    v.mv_data=inode.as_ptr();
                    v.mv_size=INODE_SIZE as size_t;
                    unsafe { mdb_put(repo.mdb_txn,repo.dbi(DBI::TREE),&mut k,&mut v,0) };
                    unsafe { mdb_put(repo.mdb_txn,repo.dbi(DBI::REVTREE),&mut v,&mut k,0) };
                    if cs.is_some() {
                        k.mv_data="".as_ptr() as *const c_char;
                        k.mv_size=0;
                        unsafe { mdb_put(repo.mdb_txn,repo.dbi(DBI::TREE),&mut v,&mut k,0) };
                    }
                    // push next inode onto buf.
                    buf.clear();
                    for c in &inode { buf.push(*c) }
                }
            },
            None => {
                return Err(())
            }
        }
    }
    Ok(())
}

pub fn add_file(repo:&mut Repository, path:&std::path::Path)->Result<(),()>{
    add_inode(repo,&None,path)
}

struct newnodes<'a> {
    up_context:Vec<&'a [u8]>,
    down_context:Vec<&'a [u8]>,
    nodes:Vec<&'a [u8]>
}

enum Change<'a> {
    NewNodes(newnodes<'a>),
    Edges(Vec<(&'a [u8], u8, &'a[u8], &'a [u8])>)
}

struct Cursor {
    cursor:*mut MDB_cursor
}
impl Cursor {
    fn new(txn:*mut MDB_txn,dbi:MDB_dbi)->Result<Cursor,c_int>{
        unsafe {
            let curs=ptr::null_mut();
            let e=mdb_cursor_open(txn,dbi,std::mem::transmute(&curs));
            if e!=0 { Err(e) } else { Ok(Cursor { cursor:curs }) }
        }
    }
}
impl Drop for Cursor {
    fn drop(&mut self){
        unsafe {mdb_cursor_close(self.cursor);}
    }
}

pub fn record(repo:&mut Repository,working_copy:&std::path::Path){
    let actions:Vec<Change>=Vec::new();
    let line_num=1;
    let updatables:HashMap<&[u8;INODE_SIZE],&[u8]>=HashMap::new();
    let cursor=Cursor::new(repo.mdb_txn, repo.dbi(DBI::NODES));
}
