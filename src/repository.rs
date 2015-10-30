extern crate libc;
use self::libc::{c_int, c_uint,c_char,size_t};
use self::libc::types::os::arch::posix88::mode_t;
use self::libc::funcs::c95::string::strncpy;
use std::ptr;

use std::slice;
use std::fmt;
use std::str;
use std;
extern crate rand;
use std::sync::Arc;
use std::rc::Rc;
use std::ffi::CString;
use mdb::{Val,mdb_env_create,mdb_reader_check,mdb_env_set_maxdbs,mdb_env_set_mapsize,mdb_env_open,mdb_txn_begin,mdb_txn_commit,mdb_txn_abort,mdb_env_close,mdb_cursor_get,mdb_cursor_put,mdb_cursor_del,MDB_CREATE,MDB_DUPSORT,MDB_NOTFOUND};
use mdb;

pub struct Repository {
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
    pub current_branch: Rc<Vec<c_char>>
}


pub fn open_base(t:*mut mdb::MDB_txn,base:&mut Result<c_uint,c_int>, name:&str, flags:c_uint)->Result<c_uint,c_int>{
    match *base {
        Ok(n)=>Ok(n),
        Err(_)=>{
            let mut x:c_uint=0;
            let result= unsafe {mdb::mdb_dbi_open(t,name.as_ptr() as *const c_char,
                                                  flags,std::mem::transmute(&x))};
            if result==0 {
                *base=Ok(x);
                *base
            } else {
                Err(result)
            }
        }
    }
}
/*
fn mdb_put(t:&mut Txn,base:c_uint,key:&mut Val,value:&mut Val,flag:c_uint)->Result<(),c_int>{
    unsafe {
        let ret=mdb::mdb_put(&mut t.txn,base,key as *mut Val, value as *mut Val,flag);
        if ret==0 {Ok(())} else
            {Err(ret)}
    }
}
*/
fn mdb_get(t:&Rc<Txn>,base:c_uint,key:&mut Val,value:&mut Val)->Result<bool,c_int>{
    unsafe {
        let ret=mdb::mdb_get(t.txn,base,key as *mut Val, value as *mut Val);
        if ret==0 {Ok(true)} else
            if ret==MDB_NOTFOUND {Ok(false)} else {Err(ret)}
    }
}

fn mdb_put(t:&Rc<Txn>,base:c_uint,key:&Vec<c_char>,value:&Vec<c_char>,flags:c_uint)->Result<(),c_int>{
    unsafe {
        let mut k=Val { mv_size:key.len() as size_t,mv_data:key.as_ptr() };
        let mut v=Val { mv_size:value.len() as size_t,mv_data:value.as_ptr() };
        let ret=mdb::mdb_put(t.txn,base,&mut k,&mut v,flags);
        if ret==0 {Ok(())} else {Err(ret)}
    }
}



pub fn from_val(v:Val)->Vec<c_char>{unsafe {slice::from_raw_parts(v.mv_data,v.mv_size as usize).to_vec()}}
impl Repository {
    pub fn dbi_nodes(&mut self,t:&Rc<Txn>)->Result<c_uint,c_int> { open_base(t.txn,&mut self.nodes,"nodes\0",MDB_CREATE|MDB_DUPSORT) }

    pub fn dbi_contents(&mut self,t:&Rc<Txn>)->Result<c_uint,c_int> { open_base(t.txn,&mut self.contents,"contents\0",MDB_CREATE) }
    pub fn dbi_revdep(&mut self,t:&Rc<Txn>)->Result<c_uint,c_int> { open_base(t.txn,&mut self.contents,"revdep\0",MDB_CREATE|MDB_DUPSORT) }
    pub fn dbi_internalhashes(&mut self,t:&Rc<Txn>)->Result<c_uint,c_int> { open_base(t.txn,&mut self.internalhashes,"internal\0",MDB_CREATE) }
    pub fn dbi_externalhashes(&mut self,t:&Rc<Txn>)->Result<c_uint,c_int> { open_base(t.txn,&mut self.externalhashes,"external\0",MDB_CREATE) }
    pub fn dbi_branches(&mut self,t:&Rc<Txn>)->Result<c_uint,c_int> { open_base(t.txn,&mut self.branches,"branches\0",MDB_CREATE|MDB_DUPSORT) }
    pub fn dbi_tree(&mut self,t:&Rc<Txn>)->Result<c_uint,c_int> { open_base(t.txn,&mut self.tree,"tree\0",MDB_CREATE|MDB_DUPSORT) }
    pub fn dbi_revtree(&mut self,t:&Rc<Txn>)->Result<c_uint,c_int> { open_base(t.txn,&mut self.revtree,"revtree\0",MDB_CREATE) }
    pub fn dbi_inodes(&mut self,t:&Rc<Txn>)->Result<c_uint,c_int> { open_base(t.txn,&mut self.inodes,"inodes\0",MDB_CREATE) }
    pub fn dbi_revinodes(&mut self,t:&Rc<Txn>)->Result<c_uint,c_int> { open_base(t.txn,&mut self.revinodes,"revinodes\0",MDB_CREATE)}

    pub fn new(txn:Rc<Txn>)->Repository{
        let mut rep=Repository {
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
            current_branch: Rc::new(unsafe {Vec::from_raw_parts(DEFAULT_BRANCH.as_ptr() as *mut c_char, DEFAULT_BRANCH.len(),DEFAULT_BRANCH.len()) })
        };
        match rep.dbi_branches(&txn) {
            Ok(dbi)=>{
                let mut k=Val { mv_size:1 as size_t, mv_data:"\0".as_ptr() as *mut c_char };
                let mut v=Val { mv_size:0 as size_t, mv_data:ptr::null_mut() };
                match mdb_get(&txn,dbi,&mut k,&mut v) {
                    Ok(e) => { if e {rep.current_branch=Rc::new(from_val(v))} }
                    Err(e)=> { }
                }
            },
            Err(e)=>{ }
        };
        rep
    }
}

pub const DEFAULT_BRANCH:&'static str = "main";

pub struct Env {env:*mut mdb::MDB_env}
pub struct Txn { env:Arc<Env>,active:bool,txn:*mut mdb::MDB_txn }

pub struct Cursor {txn:Rc<Txn>,cursor:*mut mdb::MDB_cursor}

impl Clone for Env {
    fn clone(&self)->Env{
        Env {env:self.env}
    }
}

impl Cursor {
    pub fn new(t:&Rc<Txn>,dbi:c_uint)->Result<Cursor,c_int> {
        let mut cursor:*mut mdb::MDB_cursor=ptr::null_mut();
        let ok= unsafe {mdb::mdb_cursor_open(t.txn,dbi,std::mem::transmute(&cursor))};
        if ok!=0 { Err(ok) } else { Ok(Cursor { txn:Rc::clone(&t),cursor:cursor }) }
    }
}
impl Drop for Cursor {
    fn drop(&mut self){
        unsafe {mdb::mdb_cursor_close(self.cursor)}
    }
}

impl Drop for Txn {
    fn drop(&mut self){
        self.abort()
    }
}

impl Txn {
    pub fn new(env:Arc<Env>,parent:Option<Txn>,flags:c_uint)->Result<Rc<Txn>,c_int>{

        let mut t:*mut mdb::MDB_txn = std::ptr::null_mut();
        let e= unsafe { mdb_txn_begin(env.env,
                                      match parent { None=>{ptr::null_mut()},
                                                     Some(x)=>{x.txn} },
                                      flags,
                                      std::mem::transmute(&t)) };
        assert!(e==0);
        if e!=0 { println!("error: mdb_txn_begin");
                  unsafe {mdb_txn_abort(t)};
                  Err(e) }
        else {
            Ok(Rc::new(Txn { env:Arc::clone(&env),active:true,txn:t }))
        }
    }
    pub fn commit(&mut self)->Result<(),c_int>{
        if self.active {
            self.active=false;
            unsafe { let e=mdb_txn_commit(self.txn);if(e==0) { Ok(()) } else { Err(e) } }
        } else { Err(-1) }
    }
    pub fn abort(&mut self){
        if self.active {
            self.active=false;
            unsafe { mdb_txn_abort(self.txn) }
        }
    }
}


impl Env {
    pub fn new(path:&str)->Result<Arc<Env>,c_int>{
        let env:*mut mdb::MDB_env = std::ptr::null_mut();
        unsafe {
            mdb_env_create(std::mem::transmute(&env));
            let mut dead:c_int=0;
            let e=mdb_reader_check(env,&mut dead);
            if e != 0 { println!("mdb_reader_check");return Err(e) };
            let e=mdb_env_set_maxdbs(env,10);
            if e != 0 { println!("mdb_env_set_maxdbs");return Err(e) };
            let e=mdb_env_set_mapsize(env,std::ops::Shl::shl(1,30) as size_t);
            if e !=0 { println!("mdb_env_set_mapsize");return Err(e) };
            let e=mdb_env_open(env,path.as_ptr() as *const c_char,0,0o755);
            if e !=0 { println!("mdb_env_open");return Err(e) };
        };
        Ok (Arc::new(Env { env:env }))
    }
}
impl Drop for Env {
    fn drop(&mut self){
        unsafe {mdb_env_close(self.env)}
    }
}

////////////////////////////////


const INODE_SIZE:usize = 16;
const ROOT_INODE:[c_char;INODE_SIZE]=[0;INODE_SIZE];

pub fn add_inode(txn:&Rc<Txn>,repo:&mut Repository,inode0:&Vec<c_char>,path0:Vec<&str>)->Result<(),c_int>{
    let dbi_inodes=try!(repo.dbi_inodes(&txn));
    let curs=Cursor::new(txn,dbi_inodes);
    let mut k=Val { mv_size:0,mv_data:ptr::null() };
    let mut v=Val { mv_size:0,mv_data:ptr::null() };
    let mut it=path0.iter();
    let mut elem=it.next();
    let mut dir=ROOT_INODE.to_vec();
    let mut inode:Vec<c_char>=vec!(0;INODE_SIZE);

    while elem.is_some() {
        let mut h=elem.unwrap();
        for c in h.bytes() { dir.push(c as c_char) };
        k.mv_size=dir.len() as size_t;
        k.mv_data=dir.as_ptr() as *const i8;
        let exists=try!(mdb_get (txn,try!(repo.dbi_tree(&txn)), &mut k, &mut v));
        if exists {
            dir.clear();
            dir.reserve(v.mv_size as usize);
            unsafe { strncpy(dir.as_mut_ptr(),v.mv_data,v.mv_size) };
            elem=it.next();
        } else {
            elem=it.next();
            let inode:&Vec<c_char>=
                if elem.is_some() || inode0.len()==0 {
                    dir.clear();
                    for i in 0..INODE_SIZE-1 {dir.push(rand::random())}
                    &dir
                } else {
                    inode0
                };
            mdb_put(txn,try!(repo.dbi_tree(&txn)),&dir,&inode,0);
            mdb_put(txn,try!(repo.dbi_revtree(&txn)),&inode,&dir,0);
            if elem.is_some() {
                let emptyVal=vec!();
                mdb_put(txn,try!(repo.dbi_tree(&txn)),&inode,&emptyVal,0);
            }
        }
    }
    Ok(())
}

