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

use std::ffi::CString;
use mdb::{Val,mdb_env_create,mdb_reader_check,mdb_env_set_maxdbs,mdb_env_set_mapsize,mdb_env_open,mdb_txn_begin,mdb_txn_commit,mdb_txn_abort,mdb_env_close,mdb_cursor_get,mdb_cursor_put,mdb_cursor_del,MDB_CREATE,MDB_DUPSORT,MDB_NOTFOUND,MDB_cursor_op};
use mdb;
use std::marker::{PhantomData} ;

pub struct Env {env:*mut mdb::MDB_env}
pub struct Txn<'a> { _marker:PhantomData<&'a ()>,active:bool,txn:*mut mdb::MDB_txn }

impl<'a> Drop for Txn<'a> {
    fn drop(&mut self){
        self.abort()
    }
}

impl <'env> Txn<'env> {
    pub fn new(env:&'env Env,parent:Option<Txn<'env>>,flags:c_uint)->Result<Txn<'env>,c_int>{

        let t:*mut mdb::MDB_txn = std::ptr::null_mut();
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
            Ok(Txn { _marker:PhantomData,active:true,txn:t })
        }
    }
    pub fn commit(&mut self)->Result<(),c_int>{
        if self.active {
            self.active=false;
            unsafe { let e=mdb_txn_commit(self.txn);if e==0 { Ok(()) } else { Err(e) } }
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
    pub fn new(path:&str)->Result<Env,c_int>{
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
        Ok (Env { env:env })
    }
}

impl Drop for Env {
    fn drop(&mut self){
        unsafe {mdb_env_close(self.env)}
    }
}



/*
struct Cursor {txn:Rc<Txn>,cursor:*mut mdb::MDB_cursor}

impl Clone for Env {
    fn clone(&self)->Env{
        Env {env:self.env}
    }
}

impl Cursor {
    fn new(t:&Rc<Txn>,dbi:c_uint)->Result<Rc<Cursor>,c_int> {
        let mut cursor:*mut mdb::MDB_cursor=ptr::null_mut();
        let ok= unsafe {mdb::mdb_cursor_open(t.txn,dbi,std::mem::transmute(&cursor))};
        if ok!=0 { Err(ok) } else { Ok(Rc::new(Cursor { txn:Rc::clone(&t),cursor:cursor })) }
    }
}
impl Drop for Cursor {
    fn drop(&mut self){
        unsafe {mdb::mdb_cursor_close(self.cursor)}
    }
}
*/





////////////////////////////////



pub struct Repository<'a> {
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
    pub current_branch: &'a [i8],
    _marker:PhantomData<&'a ()>
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
fn mdb_get<'a>(t:&'a Txn<'a>,base:c_uint,key:&[u8])->Result<&'a [c_char],c_int>{
    let mut key = Val { mv_size: key.len() as size_t,
                        mv_data: key.as_ptr() as *mut c_char };
    let mut data = Val { mv_size: 0,
                         mv_data: ptr::null_mut() };
    let ret=unsafe {mdb::mdb_get(t.txn,base,&mut key, &mut data)};
    if ret==0 {
        unsafe {
            Ok(slice::from_raw_parts(data.mv_data as *const c_char,data.mv_size as usize))
        }
    } else {Err(ret)}
}

fn mdb_put<'a>(t:&'a mut Txn<'a>,base:c_uint,key:&[u8],value:&[u8],flags:c_uint)->Result<(),c_int>{
    let mut k=Val { mv_size:key.len() as size_t,mv_data:key.as_ptr() as *const c_char };
    let mut v=Val { mv_size:value.len() as size_t,mv_data:value.as_ptr() as *const c_char};
    let ret=unsafe {mdb::mdb_put(t.txn,base,&mut k,&mut v,flags)};
    if ret==0 {Ok(())} else {Err(ret)}
}



pub fn from_val(v:Val)->Vec<c_char>{unsafe {slice::from_raw_parts(v.mv_data,v.mv_size as usize).to_vec()}}
impl <'a> Repository<'a> {
    fn dbi_nodes(&mut self,t:&'a Txn<'a>)->Result<c_uint,c_int> { open_base(t.txn,&mut self.nodes,"nodes\0",MDB_CREATE|MDB_DUPSORT) }

    fn dbi_contents(&mut self,t:&'a Txn<'a>)->Result<c_uint,c_int> { open_base(t.txn,&mut self.contents,"contents\0",MDB_CREATE) }
    fn dbi_revdep(&mut self,t:&'a Txn<'a>)->Result<c_uint,c_int> { open_base(t.txn,&mut self.contents,"revdep\0",MDB_CREATE|MDB_DUPSORT) }
    fn dbi_internalhashes(&mut self,t:&'a Txn<'a>)->Result<c_uint,c_int> { open_base(t.txn,&mut self.internalhashes,"internal\0",MDB_CREATE) }
    fn dbi_externalhashes(&mut self,t:&'a Txn<'a>)->Result<c_uint,c_int> { open_base(t.txn,&mut self.externalhashes,"external\0",MDB_CREATE) }
    fn dbi_branches(&mut self,t:&'a Txn<'a>)->Result<c_uint,c_int> { open_base(t.txn,&mut self.branches,"branches\0",MDB_CREATE|MDB_DUPSORT) }
    fn dbi_tree(&mut self,t:&'a Txn<'a>)->Result<c_uint,c_int> { open_base(t.txn,&mut self.tree,"tree\0",MDB_CREATE|MDB_DUPSORT) }
    fn dbi_revtree(&mut self,t:&'a Txn<'a>)->Result<c_uint,c_int> { open_base(t.txn,&mut self.revtree,"revtree\0",MDB_CREATE) }
    fn dbi_inodes(&mut self,t:&'a Txn<'a>)->Result<c_uint,c_int> { open_base(t.txn,&mut self.inodes,"inodes\0",MDB_CREATE) }
    fn dbi_revinodes(&mut self,t:&'a Txn<'a>)->Result<c_uint,c_int> { open_base(t.txn,&mut self.revinodes,"revinodes\0",MDB_CREATE)}

    pub fn new(txn:&'a Txn<'a>)->Repository{
        let mut rep:Repository<'a>=Repository {
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
            current_branch: unsafe {slice::from_raw_parts(DEFAULT_BRANCH.as_ptr() as *const c_char,
                                                          DEFAULT_BRANCH.len()) },
            _marker:PhantomData
        };
        match rep.dbi_branches(&txn) {
            Ok(dbi)=>{
                match mdb_get(&txn,dbi,"\0".as_bytes()) {
                    Ok(e) => { rep.current_branch=e }
                    Err(e)=> { }
                }
            },
            Err(e)=>{ }
        };
        rep
    }
}

const DEFAULT_BRANCH:&'static str = "main";










/*
const INODE_SIZE:usize = 16;
const ROOT_INODE:[c_char;INODE_SIZE]=[0;INODE_SIZE];

fn add_inode(txn:&Rc<Txn>,repo:&mut Repository,inode0:&Vec<c_char>,path0:Vec<&str>)->Result<(),c_int>{
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

pub fn add_file(txn:&Rc<Txn>,repo:&mut Repository,path0:Vec<&str>)->Result<(),c_int>{
    add_inode(&txn,repo,&vec!(),path0)
}
*/
/*

struct Line { key:Val,half_deleted:bool,children:Vec<Rc<Line>>, index:isize, lowlink:usize, on_stack:bool, spit:bool }

impl Line {
    fn new(key:Val)->Rc<Line> {
        Rc::new(Line { key:key, half_deleted:false,children:vec!(),index:-1,lowlink:0,on_stack:false,spit:false })
    }
}

struct Neighbors { curs:Rc<Cursor>, cursor_flag:MDB_cursor_op, flag:c_char, key:Val, value:Val }
impl Neighbors {
    fn new(txn:&Rc<Txn>, dbi:mdb::Dbi, key:&Val,flag:c_char)->Result<Neighbors,c_int> {
        let c=try!(Cursor::new(txn,dbi));
        Ok(Neighbors {curs:c, cursor_flag:MDB_cursor_op::MDB_GET_BOTH_RANGE, flag:flag,
                      key:Val{mv_size:key.mv_size,mv_data:key.mv_data},
                      value:Val{mv_size:1,mv_data:vec!(flag).as_ptr()}})
    }
}
impl Iterator for Neighbors {
    type Item=Val;
    fn next(&mut self)->Option<Val>{
        let ok= unsafe {mdb::mdb_cursor_get(self.curs.cursor,&mut self.key,&mut self.value,self.cursor_flag as c_uint)};
        self.cursor_flag=mdb::MDB_cursor_op::MDB_NEXT_DUP;
        if ok==0 && self.value.mv_size>0 {
            if unsafe {*self.value.mv_data == self.flag} { Some(self.value) } else {None}
        } else {if ok==mdb::MDB_NOTFOUND {None} else {panic!("mdb_cursor_get")}}
    }
}


fn retrieve(txn:&Rc<Txn>,repo:&mut Repository, key0:Vec<c_char>)->Result<(),c_int>{
    let sink=Line::new(Val{mv_size:0,mv_data:ptr::null_mut()});
    let mut visited:HashMap<&Vec<c_char>>,Rc<Line>>=HashMap::new();
    let dbi_nodes=try!(repo.dbi_nodes(&txn));
    let retr=|pkey:Rc<Vec<c_char>>| {
        match visited.get(&*pkey) {
            Some(l)=>l,
            None =>{
                let l=Line::new(Val{mv_size:pkey.len() as size_t,mv_data:pkey.as_ptr()});
                visited.insert(pkey,l.clone());
                &l
                //real_pseudo_children_iter(&txn,dbi_nodes,vkey,|chi|{
                //()
                //})
            }
        }
    };
    retr(Rc::new(key0));
    Ok(())
}
*/
