/*
  Copyright Florent Becker and Pierre-Etienne Meunier 2015.

  This file is part of Pijul.

  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU Affero General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU Affero General Public License for more details.

  You should have received a copy of the GNU Affero General Public License
  along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
extern crate libc;
extern crate rustc_serialize;
use self::libc::{c_int, c_uint,c_char,c_uchar,c_void,size_t};
use self::libc::{memcmp};
use std::ptr::{copy_nonoverlapping};
use std::ptr;

use std::slice;
use std::str;

use std::collections::HashMap;
extern crate rand;
use std::path::{PathBuf,Path};

use std::io::prelude::*;
use std::io;
use std::error;

use std::fmt;
use std::collections::HashSet;
use std::fs::{metadata};
pub mod fs_representation;

use self::fs_representation::{to_hex};

#[cfg(not(windows))]
use std::os::unix::fs::PermissionsExt;

use std::fs;

mod mdb;
use self::mdb::*;

use self::rustc_serialize::json::{encode,decode};

pub type LocalKey=Vec<u8>;
pub type ExternalKey=Vec<u8>;
pub type ExternalHash=Vec<u8>;
pub type Flag=u8;

pub const HASH_SIZE:usize=20; // pub temporaire
pub const LINE_SIZE:usize=4;
pub const KEY_SIZE:usize=HASH_SIZE+LINE_SIZE;
pub const ROOT_KEY:&'static[u8]=&[0;KEY_SIZE];


#[derive(Debug,RustcEncodable,RustcDecodable)]
pub struct Edge {
    pub from:ExternalKey,
    pub to:ExternalKey,
    pub flag:Flag,
    pub introduced_by:ExternalHash
}


#[derive(Debug,RustcEncodable,RustcDecodable)]
pub enum Change {
    NewNodes{
        up_context:Vec<ExternalKey>,
        down_context:Vec<ExternalKey>,
        flag:Flag,
        line_num:usize,
        nodes:Vec<Vec<u8>>
    },
    Edges{ edges:Vec<Edge> }
}


#[derive(Debug,RustcEncodable,RustcDecodable)]
pub struct Patch {
    pub changes:Vec<Change>,
    pub dependencies:Vec<ExternalHash>
}

impl Patch {

    pub fn new(changes:Vec<Change>)->Patch {
        let deps=dependencies(&changes);
        Patch {
            changes:changes,
            dependencies:deps
        }
    }
    pub fn from_reader<R>(r:&mut R)->Result<Patch,Error> where R:Read {
        let mut s=Vec::new();
        try!(r.read_to_end(&mut s).map_err(Error::IoError));
        let ss=std::str::from_utf8(&s).unwrap();
        let dec:Patch=try!(decode(ss).map_err(Error::PatchDecoding));
        Ok(dec)
    }

    pub fn to_writer<W>(&self,w:&mut W)->Result<(),Error> where W:Write {
        let encoded=try!(encode(self).map_err(Error::PatchEncoding));
        try!(w.write(encoded.as_bytes()).map_err(Error::IoError));
        Ok(())
    }

}
pub fn dependencies(changes:&[Change])->Vec<ExternalHash> {
    let mut deps=Vec::new();
    fn push_dep(deps:&mut Vec<ExternalHash>,dep:ExternalHash) {
        if !if dep.len()==HASH_SIZE {unsafe { memcmp(dep.as_ptr() as *const c_void,
                                                     ROOT_KEY.as_ptr() as *const c_void,
                                                     HASH_SIZE as size_t)==0 }} else {false} {
            deps.push(dep)
        }
    }
    for ch in changes {
        match *ch {
            Change::NewNodes { ref up_context,ref down_context, line_num:_,flag:_,nodes:_ } => {
                for c in up_context.iter().chain(down_context.iter()) {
                    if c.len()>LINE_SIZE { push_dep(&mut deps,c[0..c.len()-LINE_SIZE].to_vec()) }
                }
            },
            Change::Edges{ref edges} =>{
                for e in edges {
                    if e.from.len()>LINE_SIZE { push_dep(&mut deps,e.from[0..e.from.len()-LINE_SIZE].to_vec()) }
                    if e.to.len()>LINE_SIZE { push_dep(&mut deps,e.to[0..e.to.len()-LINE_SIZE].to_vec()) }
                    if e.introduced_by.len()>0 { push_dep(&mut deps,e.introduced_by.clone()) }
                }
            }
        }
    }
    deps
}







/// The repository structure, on which most functions work.
pub struct Repository{
    mdb_env:*mut MdbEnv,
    mdb_txn:*mut MdbTxn,
    dbi_nodes:MdbDbi,
    dbi_revdep:MdbDbi,
    dbi_contents:MdbDbi,
    dbi_internal:MdbDbi,
    dbi_external:MdbDbi,
    dbi_branches:MdbDbi,
    dbi_tree:MdbDbi,
    dbi_revtree:MdbDbi,
    dbi_inodes:MdbDbi,
    dbi_revinodes:MdbDbi
}

#[derive(Debug)]
pub enum Error{
    IoError(io::Error),
    AlreadyApplied,
    FsRepresentation(fs_representation::Error),
    PatchDecoding(rustc_serialize::json::DecoderError),
    PatchEncoding(rustc_serialize::json::EncoderError)
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::IoError(ref err) => write!(f, "IO error: {}", err),
            Error::AlreadyApplied => write!(f, "Patch already applied"),
            Error::FsRepresentation(ref err) => write!(f, "FS Representation error: {}",err),
            Error::PatchEncoding(ref err) => write!(f, "Patch encoding error {}",err),
            Error::PatchDecoding(ref err) => write!(f, "Patch decoding error {}",err)
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::IoError(ref err) => err.description(),
            Error::AlreadyApplied => "Patch already applied",
            Error::FsRepresentation(ref err) => err.description(),
            Error::PatchEncoding(ref err) => err.description(),
            Error::PatchDecoding(ref err) => err.description()
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::IoError(ref err) => Some(err),
            Error::FsRepresentation(ref err) => Some(err),
            Error::AlreadyApplied => None,
            Error::PatchEncoding(ref err) => Some(err),
            Error::PatchDecoding(ref err) => Some(err)
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IoError(err)
    }
}


impl Drop for Repository {
    fn drop(&mut self){
        unsafe {
            println!("dropping repository");
            if std::thread::panicking() {
                mdb_txn_abort(self.mdb_txn);
            } else {
                mdb_txn_commit(self.mdb_txn);
            }
            mdb_env_close(self.mdb_env)
        }
    }
}
const INODE_SIZE:usize=16;
/// The size of internal patch id. Allocate a buffer of this size when calling e.g. apply.
const ROOT_INODE:&'static[u8]=&[0;INODE_SIZE];
/// The name of the default branch, "main".
pub const DEFAULT_BRANCH:&'static str="main";

const PSEUDO_EDGE:u8=1;
const FOLDER_EDGE:u8=2;
const PARENT_EDGE:u8=4;
const DELETED_EDGE:u8=8;
pub type Inode=Vec<u8>;
const DIRECTORY_FLAG:usize = 0x200;

const LINE_HALF_DELETED:c_uchar=16;
const LINE_VISITED:c_uchar=8;
const LINE_ONSTACK:c_uchar=4;
const LINE_SPIT:c_uchar=2;

#[repr(C)]
struct c_line {
    key:*const c_char,
    flags:c_uchar,
    children:*mut*mut c_line,
    children_capacity:size_t,
    children_off:size_t,
    index:c_uint,
    lowlink:c_uint,
    scc:c_uint
}

extern "C"{
    // retrieve uses hash tables growing monotonically. For time and
    // memory, we need it in C (or with fast hashtables and no copy of
    // anything) + free without RC.
    fn c_retrieve(txn:*mut MdbTxn,dbi_nodes:MdbDbi,
                  key:*const c_char) -> *mut c_line;
    fn c_free_line(c_line:*mut c_line);
}
fn tarjan(line:&mut Line)->usize{
    fn dfs(stack:&mut Vec<*mut c_line>,index:&mut c_uint, scc:&mut c_uint, l:*mut c_line){
        unsafe {
            (*l).index = *index;
            (*l).lowlink = *index;
            (*l).flags |= LINE_ONSTACK | LINE_VISITED;
            stack.push(l);
            *index = *index + 1;
            for i in 0..(*l).children_off {
                let child=*((*l).children.offset(i as isize));
                if (*child).flags & LINE_VISITED == 0 {
                    dfs(stack,index,scc,child);
                    (*l).lowlink=std::cmp::min((*l).lowlink, (*child).lowlink);
                } else {
                    if (*child).flags & LINE_ONSTACK != 0 {
                        (*l).lowlink=std::cmp::min((*l).lowlink, (*child).index)
                    }
                }
            }
            if (*l).index == (*l).lowlink {
                //println!("SCC: {:?}",slice::from_raw_parts((*l).key,KEY_SIZE));
                loop {
                    match stack.pop() {
                        None=>break,
                        Some(p)=>{
                            (*p).scc=*scc;
                            (*p).flags = (*p).flags ^ LINE_ONSTACK;
                            if p == l { break }
                        }
                    }
                }
                *scc+=1
            }
        }
    }
    let mut stack=vec!();
    let mut index=0;
    let mut scc=0;
    dfs(&mut stack, &mut index, &mut scc, line.c_line);
    (scc-1) as usize
}

struct Line { c_line:*mut c_line }
impl Drop for Line {
    fn drop(&mut self){
        unsafe {c_free_line(self.c_line)}
    }
}


trait LineBuffer<'a> {
    fn output_line(&mut self,&'a[u8],&'a[u8]) -> ();
}

struct Diff<'a> {
    lines_a:Vec<&'a[u8]>,
    contents_a:Vec<&'a[u8]>
}

impl <'a> LineBuffer<'a> for Diff<'a> {
    fn output_line(&mut self,k:&'a[u8],c:&'a[u8]) {
        //println!("outputting {:?} {}",k,unsafe {std::str::from_utf8_unchecked(c)});
        self.lines_a.push(k);
        self.contents_a.push(c);
    }
}

impl <'a,W> LineBuffer<'a> for W where W:std::io::Write {
    fn output_line(&mut self,_:&'a[u8],c:&'a[u8]) {
        self.write(c).unwrap(); // .expect("output_line: could not write");
    }
}



struct CursIter<'a> {
    cursor:&'a mut mdb::MdbCursor,
    op:c_uint,
    edge_flag:u8,
    include_pseudo:bool,
    key:MDB_val,
    val:MDB_val,
}

impl <'a>CursIter<'a> {
    fn new(curs:&mut Cursor<'a>,key:&'a [u8],flag:u8,include_pseudo:bool)->CursIter<'a>{
        CursIter { cursor:unsafe { &mut *curs.cursor },
                   key:MDB_val{mv_data:key.as_ptr() as *const c_void, mv_size:key.len() as size_t},
                   val:MDB_val{mv_data:[flag].as_ptr() as *const c_void, mv_size:1},
                   include_pseudo:include_pseudo,
                   edge_flag:flag,
                   op:Op::MDB_GET_BOTH_RANGE as c_uint }
    }
}

impl <'a>Iterator for CursIter<'a> {
    type Item=&'a [u8];
    fn next(&mut self)->Option<&'a[u8]>{
        unsafe {
            // Include_pseudo works because PSEUDO_EDGE==1, hence there is no "gap" in flags between regular edges and their pseudo version, for each kind of edge.
            let e=mdb_cursor_get(self.cursor,&mut self.key,&mut self.val,self.op as c_uint);
            self.op=Op::MDB_NEXT_DUP as c_uint;
            if e==0 && self.val.mv_size>0 && ((*(self.val.mv_data as *const u8) == self.edge_flag) || (self.include_pseudo && (*(self.val.mv_data as *const u8) == (self.edge_flag|PSEUDO_EDGE)))) {
                Some(slice::from_raw_parts(self.val.mv_data as *const u8,self.val.mv_size as usize))
            } else {
                None
            }
        }
    }
}


#[cfg(not(windows))]
fn permissions(attr:&std::fs::Metadata)->usize{
    attr.permissions().mode() as usize
}
#[cfg(windows)]
fn permissions(attr:&std::fs::Metadata)->usize{
    0
}



// A node is alive if it has a PARENT_EDGE or a PARENT_EDGE|FOLDER_EDGE to another node (or if it is the root node, but we will never call this function on the root node).
fn nonroot_is_alive(cursor:&mut Cursor,key:&[u8])->bool {
    let mut flag=[PARENT_EDGE];
    match unsafe { mdb::cursor_get(&cursor,&key,Some(&flag[..]),Op::MDB_GET_BOTH_RANGE) } {
        Ok(v)=>{
            if v.len()<1 { false } else {
                v[0]==PARENT_EDGE || {
                    flag[0]=PARENT_EDGE|FOLDER_EDGE;
                    match unsafe { mdb::cursor_get(&cursor,&key,Some(&flag[..]),Op::MDB_GET_BOTH_RANGE) } {
                        Ok(w)=>if w.len()<1 { false } else { w[0]==FOLDER_EDGE|PARENT_EDGE },
                        Err(_)=>false
                    }
                }
            }
        },
        Err(_) => {
            false
        }
    }
}



impl Repository {
    pub fn new(path:&std::path::Path)->Result<Repository,Error>{
        unsafe {
            let env=ptr::null_mut();
            let e=mdb_env_create(std::mem::transmute(&env));
            if e != 0 { println!("mdb_env_create");
                        return Err(Error::IoError(std::io::Error::from_raw_os_error(e))) };
            let mut dead:c_int=0;
            let e=mdb_reader_check(env,&mut dead);
            if e != 0 { println!("mdb_reader_check");
                        return Err(Error::IoError(std::io::Error::from_raw_os_error(e))) }
            let e=mdb_env_set_maxdbs(env,10);
            if e != 0 { println!("mdb_env_set_maxdbs");
                        return Err(Error::IoError(std::io::Error::from_raw_os_error(e))) }
            let e=mdb_env_set_mapsize(env,std::ops::Shl::shl(1,30) as size_t);
            if e !=0 { println!("mdb_env_set_mapsize");
                       return Err(Error::IoError(std::io::Error::from_raw_os_error(e))) }

            let pp=std::ffi::CString::new(path.to_str().unwrap()).unwrap();
            let e=mdb_env_open(env,pp.as_ptr() as *const i8,0,0o755);
            if e !=0 { println!("mdb_env_open {:?}", path);
                       return Err(Error::IoError(std::io::Error::from_raw_os_error(e))) }

            let txn=ptr::null_mut();
            let e=mdb_txn_begin(env,ptr::null_mut(),0,std::mem::transmute(&txn));
            if e !=0 { println!("mdb_txn_begin");
                       return Err(Error::IoError(std::io::Error::from_raw_os_error(e))) }
            fn open_dbi(txn:*mut MdbTxn,name:&str,flag:c_uint)->MdbDbi {
                let mut d=0;
                let e=unsafe { mdb_dbi_open(txn,name.as_ptr() as *const c_char,flag,&mut d) };
                if e==0 { d } else {
                    panic!("Database could not be opened")
                }
            }
            let repo=Repository{
                mdb_env:env,
                mdb_txn:txn,
                dbi_nodes:open_dbi(txn,"nodes\0",MDB_CREATE|MDB_DUPSORT),
                dbi_revdep:open_dbi(txn,"revdep\0",MDB_CREATE|MDB_DUPSORT),
                dbi_contents:open_dbi(txn,"contents\0",MDB_CREATE),
                dbi_internal:open_dbi(txn,"internal\0",MDB_CREATE),
                dbi_external:open_dbi(txn,"external\0",MDB_CREATE),
                dbi_branches:open_dbi(txn,"branches\0",MDB_CREATE|MDB_DUPSORT),
                dbi_tree:open_dbi(txn,"tree\0",MDB_CREATE),
                dbi_revtree:open_dbi(txn,"revtree\0",MDB_CREATE),
                dbi_inodes:open_dbi(txn,"inodes\0",MDB_CREATE),
                dbi_revinodes:open_dbi(txn,"revinodes\0",MDB_CREATE),
            };
            Ok(repo)
        }
    }



    fn create_new_inode(&mut self,buf:&mut [u8]){
        let curs_revtree=Cursor::new(self.mdb_txn,self.dbi_revtree).unwrap();
        loop {
            for i in 0..INODE_SIZE { buf[i]=rand::random() }
            let mut k = MDB_val{ mv_data:buf.as_ptr() as *const c_void, mv_size:buf.len()as size_t };
            let mut v = MDB_val{ mv_data:ptr::null_mut(), mv_size:0 };
            let e= unsafe { mdb_cursor_get(curs_revtree.cursor, &mut k,&mut v,Op::MDB_SET_RANGE as c_uint) };
            if e==MDB_NOTFOUND {
                break
            } else if e==0 && (k.mv_size as usize)>=INODE_SIZE && unsafe { memcmp(buf.as_ptr() as *const c_void, k.mv_data as *const c_void, INODE_SIZE as size_t) } != 0 {
                break
            } else {
                panic!("Wrong encoding in create_new_inode")
            }
        }
    }

    fn add_inode(&mut self, inode:&Option<&[u8]>, path:&std::path::Path, is_dir:bool)->Result<(),()>{
        let mut buf:Inode=vec![0;INODE_SIZE];
        let mut components=path.components();
        let mut cs=components.next();
        while let Some(s)=cs { // need to peek at the next element, so no for.
            cs=components.next();
            match s.as_os_str().to_str(){
                Some(ss) => {
                    buf.truncate(INODE_SIZE);
                    buf.extend(ss.as_bytes());
                    match unsafe { mdb::get(self.mdb_txn,self.dbi_tree,&buf) } {
                        Ok(v)=> {
                            // replace buf with existing inode
                            buf.clear();
                            let _=unsafe { copy_nonoverlapping(v.as_ptr(),buf.as_mut_ptr(),v.len()) };
                        },
                        Err(_) =>{
                            let mut inode_:[u8;INODE_SIZE]=[0;INODE_SIZE];
                            let inode = if cs.is_none() && inode.is_some() {
                                inode.unwrap()
                            } else {
                                self.create_new_inode(&mut inode_);
                                &inode_[..]
                            };
                            unsafe {
                                mdb::put(self.mdb_txn,self.dbi_tree,&buf,&inode,0).unwrap();
                                mdb::put(self.mdb_txn,self.dbi_revtree,&inode,&buf,0).unwrap();
                            }
                            if cs.is_some() || is_dir {
                                unsafe {
                                    mdb::put(self.mdb_txn,self.dbi_tree,&inode,&[],0).unwrap()
                                }
                            }
                            // push next inode onto buf.
                            buf.clear();
                            buf.extend(inode)
                        }
                    }
                },
                None => {
                    return Err(())
                }
            }
        }
        Ok(())
    }
    /// Adds a file in the repository. Additions need to be recorded in
    /// order to produce a patch.
    pub fn add_file(&mut self, path:&std::path::Path, is_dir:bool)->Result<(),()>{
        //println!("Adding {:?}",path);
        self.add_inode(&None,path,is_dir)
    }


    pub fn move_file(&mut self, path:&std::path::Path, path_:&std::path::Path,is_dir:bool) {

        let inode= &mut (Vec::new());
        let parent= &mut (Vec::new());

        (*inode).extend(ROOT_INODE);
        for c in path.components() {
            inode.extend(c.as_os_str().to_str().unwrap().as_bytes());
            match unsafe { mdb::get(self.mdb_txn,self.dbi_tree,&inode) } {
                Ok(x)=> {
                    std::mem::swap(inode,parent);
                    (*inode).clear();
                    (*inode).extend(x);
                },
                Err(_)=>{
                    panic!("this path doesn't exist")
                }
            }
        }
        // Now the last inode is in "*inode"
        let basename=path.file_name().unwrap();
        (*parent).extend(basename.to_str().unwrap().as_bytes());
        let mut par=MDB_val { mv_data:parent.as_ptr() as *const c_void, mv_size:parent.len() as size_t };
        unsafe { mdb_del(self.mdb_txn,self.dbi_tree,&mut par,std::ptr::null_mut()) };
        self.add_inode(&Some(inode),path_,is_dir).unwrap();

        match unsafe { mdb::get(self.mdb_txn,self.dbi_inodes,inode) } {
            Ok(v)=> {
                let mut vv=v.to_vec();
                vv[0]=1;
                unsafe { mdb::put(self.mdb_txn,self.dbi_inodes,inode,&vv,0).unwrap() }
            },
            Err(_)=>{
                // Was not in inodes, nothing to do.
            }
        }
    }

    pub fn remove_file(&mut self, path:&std::path::Path) {
        let mut inode=Vec::new();
        inode.extend(ROOT_INODE);
        for c in path.components() {
            inode.extend(c.as_os_str().to_str().unwrap().as_bytes());
            match unsafe { mdb::get(self.mdb_txn,self.dbi_tree,&inode) } {
                Ok(x)=> { inode.clear(); inode.extend(x) },
                Err(_)=>{ panic!("this path doesn't exist") }
            }
        }
        // Now the inode for "path" is in "inode"
        match unsafe { mdb::get(self.mdb_txn,self.dbi_inodes,&inode) } {
            Ok(node) => {
                let mut node_=node.to_vec();
                node_[0]=2;
                unsafe { mdb::put(self.mdb_txn,self.dbi_inodes,&inode,&node_,0).unwrap() }
            },
            Err(_)=>{
                panic!("unregistered inode")
            }
        }
    }


    pub fn get_current_branch<'a>(&self)->&'a[u8] {
        unsafe {
            match mdb::get(self.mdb_txn,self.dbi_branches,&[0]) {
                Ok(b)=>b,
                Err(_)=>DEFAULT_BRANCH.as_bytes()
            }
        }
    }

    fn retrieve(&self,key:&[u8])->Result<Line,()>{
        unsafe {
            let c_line=c_retrieve(self.mdb_txn,self.dbi_nodes,
                                  key.as_ptr() as *const c_char);
            if !c_line.is_null() {
                Ok (Line {c_line:c_line})
            } else {Err(())}
        }
    }



    fn output_file<'a,B>(&self,buf:&mut B,file:&'a mut Line) where B:LineBuffer<'a> {
        let max_level=tarjan(file);
        let mut counts=vec![0;max_level+1];
        let mut lines=vec![vec!();max_level+1];
        for i in 0..lines.len() { lines[i]=Vec::new() }
        fn fill_lines(counts:&mut Vec<usize>,
                      lines:&mut Vec<Vec<*mut c_line>>,
                      cl:*mut c_line){
            unsafe {
                //println!("fill lines : {}",(*cl).lowlink);
                if (*cl).flags & LINE_SPIT == 0 {
                    (*cl).flags |= LINE_SPIT;
                    (*counts.get_unchecked_mut((*cl).scc as usize)) += 1;
                    (*lines.get_unchecked_mut((*cl).scc as usize)).push (cl);
                    let children:&[*mut c_line]=slice::from_raw_parts((*cl).children, (*cl).children_off as usize);
                    for child in children {
                        fill_lines(counts,lines,*child)
                    }
                }
            }
        }
        fill_lines(&mut counts, &mut lines, file.c_line);
        // Then add undetected conflicts.
        unsafe  {
            for i in 0..counts.len() {
                if *counts.get_unchecked(i) > 1 {
                    for line in lines.get_unchecked(i) {
                        let children:&[*mut c_line]=slice::from_raw_parts((**line).children, (**line).children_off as usize);
                        for child in children {
                            for j in (**line).scc+1 .. (**child).scc {
                                (*counts.get_unchecked_mut(j as usize)) += 1
                            }}}}}
        }
        fn contents<'a>(repo:&Repository,key:&'a[u8]) -> &'a[u8] {
            match unsafe { mdb::get(repo.mdb_txn,repo.dbi_contents,key) } {
                Ok(v)=>v,
                Err(_) =>&[]
            }
        }

        // Finally, output everybody.
        let mut i:usize=max_level;
        let mut nodes=Vec::new();
        let mut visited=HashSet::new();
        loop {
            //assert!(counts[i]>=1);
            if counts[i]==0 { break }
            else if counts[i] == 1 {
                let key= unsafe { slice::from_raw_parts((*lines[i][0]).key as *const u8, KEY_SIZE as usize) };
                buf.output_line(&key,contents(self,key));
                if i==0 { break } else { i-=1 }
            } else {
                fn get_conflict<'a,B>(repo:&Repository, counts:&Vec<usize>, l:*const c_line, b:&mut B,
                                      nodes:&mut Vec<&'a[u8]>, visited:&mut HashSet<*const c_line>,
                                      is_first:&mut bool,
                                      next:&mut usize)
                    where B:LineBuffer<'a> {
                        unsafe {
                            if counts[(*l).scc as usize] <= 1 {
                                if ! *is_first {b.output_line(&[],b"================================");}else{*is_first=false}
                                for key in nodes {
                                    b.output_line(key,contents(repo,key))
                                }
                                *next=(*l).scc as usize
                            } else {
                                if !visited.contains(&l) {
                                    visited.insert(l);
                                    let mut min_order=None;
                                    for c in 0..(*l).children_off {
                                        let ll=(**((*l).children.offset(c as isize))).scc;
                                        min_order=Some(match min_order { None=>ll, Some(m)=>std::cmp::min(m,ll) })
                                    }
                                    match min_order {
                                        None=>(),
                                        Some(m)=>{
                                            if (*l).flags & LINE_HALF_DELETED != 0 {
                                                for c in 0..(*l).children_off {
                                                    let chi=*((*l).children.offset(c as isize));
                                                    if (*chi).scc==m {
                                                        get_conflict(repo,counts,chi,b,nodes,visited,is_first,next)
                                                    }
                                                }
                                            }
                                            nodes.push(slice::from_raw_parts((*l).key as *const u8, KEY_SIZE));
                                            for c in 0..(*l).children_off {
                                                let chi=*((*l).children.offset(c as isize));
                                                if (*chi).scc==m {
                                                    get_conflict(repo,counts,chi,b,nodes,visited,is_first,next)
                                                }
                                            }
                                            let _=nodes.pop();
                                        }
                                    }
                                    let _=visited.remove(&l);
                                }
                            }
                        }
                    }
                let mut next=0;
                buf.output_line(&[],b">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                let mut is_first=true;
                for j in 0..(lines[i].len()) {
                    visited.clear();
                    nodes.clear();
                    get_conflict(self, &counts,lines[i][j], buf, &mut nodes, &mut visited, &mut is_first, &mut next)
                }
                buf.output_line(&[],b"<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
                if i==0 { break } else { i=std::cmp::min(next,i-1) }
            }
        }
    }

    /// Gets the external key corresponding to the given key, returning an
    /// owned vector. If the key is just a patch id, it returns the
    /// corresponding external hash.
    fn external_key(&self,key:&[u8])->ExternalKey {
        unsafe {
            //println!("internal key:{:?}",&key[0..HASH_SIZE]);
            if key.len()>=HASH_SIZE && memcmp(key.as_ptr() as *const c_void,ROOT_KEY.as_ptr() as *const c_void,HASH_SIZE as size_t)==0 {
                //println!("is root key");
                ROOT_KEY.to_vec()
            } else {
                match mdb::get(self.mdb_txn,self.dbi_external,&key[0..HASH_SIZE]) {
                    Ok(pv)=> {
                        let mut result:Vec<u8>=Vec::with_capacity(pv.len()+LINE_SIZE);
                        result.extend(pv);
                        if key.len()==KEY_SIZE { result.extend(&key[HASH_SIZE..KEY_SIZE]) }
                        result
                    },
                    Err(_)=>{
                        println!("internal key:{:?}",key);
                        //dump_table(self.mdb_txn,self.dbi_external);
                        panic!("external key not found !")
                    }
                }
            }
        }
    }


    fn internal_hash<'a>(&self,key:&'a [u8])->Result<&'a [u8],c_int> {
        unsafe {
            if key.len()==HASH_SIZE && memcmp(key.as_ptr() as *const c_void,ROOT_KEY.as_ptr() as *const c_void,HASH_SIZE as size_t)==0 {
                Ok(ROOT_KEY)
            } else {
                mdb::get(self.mdb_txn,self.dbi_internal,key)
            }
        }
    }
    /// Create a new internal patch id, register it in the "external" and
    /// "internal" bases, and write the result in its second argument
    /// ("result").
    pub fn new_internal(&mut self,result:&mut[u8]) {
        let curs=Cursor::new(self.mdb_txn,self.dbi_external).unwrap();
        let root_key=&ROOT_KEY[0..HASH_SIZE];
        let last=
            unsafe {
                let mut k:MDB_val=std::mem::zeroed();
                let mut v:MDB_val=std::mem::zeroed();
                let e=mdb_cursor_get(curs.cursor,&mut k,&mut v, Op::MDB_LAST as c_uint);
                if e==0 && (k.mv_size as usize)>=HASH_SIZE {
                    slice::from_raw_parts(k.mv_data as *const u8, k.mv_size as usize)
                } else {
                    root_key
                }
            };
        fn create_new(r:&mut [u8],last:&[u8],i:usize){
            if i>0 {
                if last[i]==0xff { r[i]=0; create_new(r,last,i-1) }
                else { r[i]=last[i]+1 }
            }
        }
        create_new(result,last,last.len()-1);
    }

    pub fn register_hash(&mut self,internal:&[u8],external:&[u8]){
        unsafe {
            //println!("register_hash: {:?}\n               {:?}",external,internal);
            mdb::put(self.mdb_txn,self.dbi_external,internal,external,0).unwrap();
            mdb::put(self.mdb_txn,self.dbi_internal,external,internal,0).unwrap();
        }
    }




    fn delete_edges<'a>(&self, edges:&mut Vec<Edge>, key:&'a[u8]){
        // Get external key for "key"
        //println!("delete key: {}",key.to_hex());
        let ext_key=self.external_key(key);

        // Then collect edges to delete
        let curs=Cursor::new(self.mdb_txn,self.dbi_nodes).unwrap();
        for c in [PARENT_EDGE, PARENT_EDGE|FOLDER_EDGE].iter() {
            unsafe {
                let mut k = MDB_val{ mv_data:key.as_ptr() as *const c_void, mv_size:key.len() as size_t };
                let mut v = MDB_val{ mv_data:(c as *const c_uchar) as *const c_void, mv_size:1 };
                let mut e= mdb_cursor_get(curs.cursor, &mut k,&mut v,Op::MDB_GET_BOTH_RANGE as c_uint);
                // take all parent or folder-parent edges:
                //println!("e={}",e);
                while e==0 && v.mv_size>0 && *(v.mv_data as (*mut c_uchar)) == *c {
                    if (v.mv_size as usize) < 1+HASH_SIZE+KEY_SIZE {
                        panic!("Wrong encoding in delete_edges")
                    }
                    // look up the external hash up.
                    let pv=slice::from_raw_parts((v.mv_data as *const c_uchar).offset(1), KEY_SIZE as usize);
                    let pp=slice::from_raw_parts((v.mv_data as *const c_uchar).offset(1+KEY_SIZE as isize), HASH_SIZE as usize);
                    //println!("get key pv");
                    let _=self.external_key(pv);
                    //println!("get key pp");
                    let _=self.external_key(pp);

                    edges.push(Edge { from:ext_key.clone(), to:self.external_key(pv), flag:(*c)^DELETED_EDGE, introduced_by:self.external_key(pp) });
                    e= mdb_cursor_get(curs.cursor, &mut k,&mut v,Op::MDB_NEXT_DUP as c_uint);
                }
            }
        }
    }

    fn diff(&self,line_num:&mut usize, actions:&mut Vec<Change>, a:&mut Line, b:&Path)->Result<(),std::io::Error> {
        fn memeq(a:&[u8],b:&[u8])->bool {
            if a.len() == b.len() {
                unsafe { memcmp(a.as_ptr() as *const c_void,b.as_ptr() as *const c_void,
                                b.len() as size_t) == 0 }
            } else { false }
        }
        fn local_diff(repo:&Repository,actions:&mut Vec<Change>,line_num:&mut usize, lines_a:&[&[u8]], contents_a:&[&[u8]], b:&[&[u8]]) {
            let mut opt=vec![vec!();contents_a.len()+1];
            for i in 0..opt.len() { opt[i]=vec![0;b.len()+1] }
            // opt
            for i in (0..contents_a.len()).rev() {
                for j in (0..b.len()).rev() {
                    opt[i][j]=
                        if memeq(contents_a[i],b[j]) {
                            opt[i+1][j+1]+1
                        } else {
                            std::cmp::max(opt[i+1][j], opt[i][j+1])
                        }
                }
            }
            let mut i=1;
            let mut j=0;
            fn add_lines(repo:&Repository,actions:&mut Vec<Change>, line_num:&mut usize,
                         up_context:&[u8],down_context:&[&[u8]],lines:&[&[u8]]){
                actions.push(
                    Change::NewNodes {
                        up_context:vec!(repo.external_key(up_context)),
                        down_context:down_context.iter().map(|x|{repo.external_key(x)}).collect(),
                        line_num: *line_num,
                        flag:0,
                        nodes:lines.iter().map(|x|{x.to_vec()}).collect()
                    });
                *line_num += lines.len()
            }
            fn delete_lines(repo:&Repository,actions:&mut Vec<Change>, lines:&[&[u8]]){
                let mut edges=Vec::with_capacity(lines.len());
                for l in lines {
                    repo.delete_edges(&mut edges,l)
                }
                actions.push(Change::Edges{edges:edges})
            }
            let mut oi=None;
            let mut oj=None;
            while i<contents_a.len() && j<b.len() {
                //println!("i={}, j={}",i,j);
                if memeq(contents_a[i],b[j]) {
                    //unsafe { println!("== {:?} {:?} {} {}",oi,oj,std::str::from_utf8_unchecked(contents_a[i]),std::str::from_utf8_unchecked(b[j])) }
                    if let Some(i0)=oi {
                        delete_lines(repo,actions, &lines_a[i0..i]);
                        oi=None
                    } else if let Some(j0)=oj {
                        add_lines(repo,actions, line_num,
                                  lines_a[i-1], // up context
                                  &lines_a[i..i+1], // down context
                                  &b[j0..j]);
                        oj=None
                    }
                    i+=1; j+=1;
                } else {
                    //println!("!= {:?} {:?} {:?} {:?}",opt[i+1][j],opt[i][j+1], oi,oj);
                    if opt[i+1][j] >= opt[i][j+1] {
                        if let Some(j0)=oj {
                            add_lines(repo,actions, line_num,
                                      lines_a[i-1], // up context
                                      &lines_a[i..i+1], // down context
                                      &b[j0..j]);
                            oj=None
                        }
                        //println!("oi {:?}",oi);
                        if oi.is_none() { oi=Some(i) }
                        i+=1
                    } else {
                        if let Some(i0)=oi {
                            delete_lines(repo,actions, &lines_a[i0..i]);
                            oi=None
                        }
                        if oj.is_none() { oj=Some(j) }
                        j+=1
                    }
                }
            }
            //println!("done i={}/{} j={}/{}",i,contents_a.len(),j,b.len());
            if i < lines_a.len() {
                if let Some(j0)=oj {
                    add_lines(repo,actions, line_num,
                              lines_a[i-1], // up context
                              &lines_a[i..i+1], // down context
                              &b[j0..j])
                }
                delete_lines(repo,actions, &lines_a[i..lines_a.len()])
            } else if j < b.len() {
                if let Some(i0)=oi {
                    delete_lines(repo,actions, &lines_a[i0..i]);
                    add_lines(repo,actions, line_num, lines_a[i0-1], &[], &b[j..b.len()])
                } else {
                    add_lines(repo,actions, line_num, lines_a[i-1], &[], &b[j..b.len()])
                }
            }
        }

        let mut buf_b=Vec::new();
        let mut lines_b=Vec::new();
        let err={
            let err={
                let f = std::fs::File::open(b);
                let mut f = std::io::BufReader::new(f.unwrap());
                f.read_to_end(&mut buf_b)
            };
            let mut i=0;
            let mut j=0;
            //unsafe { println!("buf_b= {}",std::str::from_utf8_unchecked(&buf_b))}

            while j<buf_b.len() {
                if buf_b[j]==0xa {
                    //unsafe {println!("pushing {}",std::str::from_utf8_unchecked(&buf_b[i..j+1]))}
                    lines_b.push(&buf_b[i..j+1]);
                    i=j+1
                }
                j+=1;
            }
            if i<j { lines_b.push(&buf_b[i..j]) }
            err
        };
        match err {
            Ok(_)=>{
                let mut d = Diff { lines_a:Vec::new(), contents_a:Vec::new() };
                self.output_file(&mut d,a);
                local_diff(self,actions, line_num,
                           &d.lines_a,
                           &d.contents_a,
                           &lines_b);
                Ok(())
            },
            Err(e)=>Err(e)
        }
    }

    fn record_all(&self, actions:&mut Vec<Change>,
           line_num:&mut usize,updatables:&mut HashMap<Vec<u8>,Vec<u8>>,
           parent_inode:Option<&[u8]>,
           parent_node:Option<&[u8]>,
           current_inode:&[u8],
           realpath:&mut std::path::PathBuf, basename:&[u8]) {
        //println!("record dfs {}",to_hex(current_inode));
        if parent_inode.is_some() { realpath.push(str::from_utf8(&basename).unwrap()) }

        let mut k = MDB_val { mv_data:ptr::null_mut(), mv_size:0 };
        let mut v = MDB_val { mv_data:ptr::null_mut(), mv_size:0 };
        let mut l2=[0;LINE_SIZE];
        let current_node=
            if parent_inode.is_some() {
                k.mv_data=current_inode.as_ptr() as *const c_void;
                k.mv_size=INODE_SIZE as size_t;
                let e = unsafe { mdb_get(self.mdb_txn,self.dbi_inodes,&mut k, &mut v) };
                if e==0 { // This inode already has a corresponding node
                    let current_node=unsafe { slice::from_raw_parts(v.mv_data as *const u8, v.mv_size as usize) };
                    //println!("Existing node: {}",to_hex(current_node));


                    let old_attr=((current_node[1] as usize) << 8) | (current_node[2] as usize);
                    // Add the new name.
                    let int_attr={
                        let attr=metadata(&realpath).unwrap();
                        let p=(permissions(&attr)) & 0o777;
                        let is_dir= if attr.is_dir() { DIRECTORY_FLAG } else { 0 };
                        //println!("int_attr {:?} : {} {}",realpath,p,is_dir);
                        (if p==0 { old_attr } else { p }) | is_dir
                    };
                    //println!("attributes: {} {}",old_attr,int_attr);
                    if current_node[0]==1 || old_attr!=int_attr {
                        // file moved

                        // Delete all former names.
                        let mut edges=Vec::new();
                        // Now take all grandparents of l2, delete them.
                        let mut curs_parents=Cursor::new(self.mdb_txn,self.dbi_nodes).unwrap();
                        for parent in CursIter::new(&mut curs_parents,&current_node[3..],FOLDER_EDGE,true) {
                            let mut curs_grandparents=Cursor::new(self.mdb_txn,self.dbi_nodes).unwrap();
                            for grandparent in CursIter::new(&mut curs_grandparents,&parent[1..(1+KEY_SIZE)],FOLDER_EDGE,true) {
                                edges.push(Edge {
                                    from:self.external_key(&parent),
                                    to:self.external_key(&grandparent[1..(1+KEY_SIZE)]),
                                    flag:grandparent[0],
                                    introduced_by:self.external_key(&grandparent[1+KEY_SIZE..])
                                });
                            }
                        }
                        actions.push(Change::Edges{edges:edges});

                        let mut name=Vec::with_capacity(basename.len()+2);
                        name.push(((int_attr >> 8) & 0xff) as u8);
                        name.push((int_attr & 0xff) as u8);
                        name.extend(basename);
                        actions.push(
                            Change::NewNodes { up_context: vec!(self.external_key(parent_node.unwrap())),
                                               line_num: *line_num,
                                               down_context: vec!(self.external_key(&current_node[3..])),
                                               nodes: vec!(name),
                                               flag:FOLDER_EDGE }
                            );
                        *line_num += 1;

                        let ret=self.retrieve(&current_node[3..]);
                        self.diff(line_num,actions, &mut ret.unwrap(), realpath.as_path()).unwrap()


                    } else if current_node[0]==2 {
                        // file deleted. delete recursively
                        let mut edges=Vec::new();
                        // Now take all grandparents of l2, delete them.
                        let mut curs_parents=Cursor::new(self.mdb_txn,self.dbi_nodes).unwrap();
                        for parent in CursIter::new(&mut curs_parents,&current_node[3..],FOLDER_EDGE,true) {
                            edges.push(Edge {
                                from:self.external_key(&current_node[3..]),
                                to:self.external_key(&parent[1..(1+KEY_SIZE)]),
                                flag:parent[0],
                                introduced_by:self.external_key(&parent[1+KEY_SIZE..])
                            });
                            let mut curs_grandparents=Cursor::new(self.mdb_txn,self.dbi_nodes).unwrap();
                            for grandparent in CursIter::new(&mut curs_grandparents,&parent[1..(1+KEY_SIZE)],FOLDER_EDGE,true) {
                                edges.push(Edge {
                                    from:self.external_key(&parent),
                                    to:self.external_key(&grandparent[1..(1+KEY_SIZE)]),
                                    flag:grandparent[0],
                                    introduced_by:self.external_key(&grandparent[1+KEY_SIZE..])
                                });
                            }
                        }
                        actions.push(Change::Edges{edges:edges});
                        unimplemented!() // Remove all known vertices from this file, for else "missing context" conflicts will not be detected.
                    } else if current_node[0]==0 {
                        let ret=self.retrieve(&current_node[3..]);
                        self.diff(line_num,actions, &mut ret.unwrap(), realpath.as_path()).unwrap()
                    } else {
                        panic!("record: wrong inode tag (in base INODES) {}", current_node[0])
                    };
                    Some(current_node)
                } else {
                    // File addition, create appropriate Newnodes.
                    match metadata(&realpath) {
                        Ok(attr) => {
                            //println!("file addition, realpath={:?}", realpath);
                            let int_attr={
                                let attr=metadata(&realpath).unwrap();
                                let p=permissions(&attr);
                                let is_dir= if attr.is_dir() { DIRECTORY_FLAG } else { 0 };
                                (if p==0 { 0o755 } else { p }) | is_dir
                            };
                            let mut nodes=Vec::new();
                            let mut lnum= *line_num + 1;
                            for i in 0..(LINE_SIZE-1) { l2[i]=(lnum & 0xff) as u8; lnum=lnum>>8 }

                            let mut name=Vec::with_capacity(basename.len()+2);
                            name.push(((int_attr >> 8) & 0xff) as u8);
                            name.push((int_attr & 0xff) as u8);
                            name.extend(basename);
                            actions.push(
                                Change::NewNodes { up_context: vec!(self.external_key(parent_node.unwrap())),
                                                   line_num: *line_num,
                                                   down_context: vec!(),
                                                   nodes: vec!(name,vec!()),
                                                   flag:FOLDER_EDGE }
                                );
                            *line_num += 2;

                            // Reading the file
                            if !attr.is_dir() {
                                nodes.clear();
                                let mut line=Vec::new();
                                let f = std::fs::File::open(realpath.as_path());
                                let mut f = std::io::BufReader::new(f.unwrap());
                                loop {
                                    match f.read_until('\n' as u8,&mut line) {
                                        Ok(l) => if l>0 { nodes.push(line.clone());line.clear() } else { break },
                                        Err(_) => break
                                    }
                                }
                                updatables.insert(l2.to_vec(),current_inode.to_vec());
                                let len=nodes.len();
                                if !nodes.is_empty() {
                                    actions.push(
                                        Change::NewNodes { up_context:vec!(l2.to_vec()),
                                                           line_num: *line_num,
                                                           down_context: vec!(),
                                                           nodes: nodes,
                                                           flag:0 }
                                        );
                                }
                                *line_num+=len;
                                Some(&l2[..])
                            } else {
                                None
                            }
                        },
                        Err(_)=>{
                            panic!("error adding a file (metadata failed)")
                        }
                    }
                }
            } else {
                Some(ROOT_KEY)
            };

        //println!("current_node={:?}",current_node);
        match current_node {
            None => (), // we just added a file
            Some(current_node)=>{
                k.mv_data=current_inode.as_ptr() as *const c_void;
                k.mv_size=INODE_SIZE as size_t;

                let curs_tree=Cursor::new(self.mdb_txn,self.dbi_tree).unwrap();
                let mut e= unsafe { mdb_cursor_get(curs_tree.cursor, &mut k,&mut v,Op::MDB_SET_RANGE as c_uint) };
                //dump_table(self.mdb_txn,self.dbi_tree);
                while e==0
                    && (k.mv_size>=INODE_SIZE as size_t)
                    && unsafe { memcmp(k.mv_data as *const c_void, current_inode.as_ptr() as *const c_void,
                                       INODE_SIZE as size_t) } == 0 {

                        let kk= unsafe { std::slice::from_raw_parts(k.mv_data as *const u8,k.mv_size as usize) };
                        let vv= unsafe { std::slice::from_raw_parts(v.mv_data as *const u8,v.mv_size as usize) };
                        self.record_all(actions, line_num,updatables,
                            Some(current_inode), // parent_inode
                            Some(current_node), // parent_node
                            vv,// current_inode
                            realpath,
                            &kk[INODE_SIZE..]);

                        unsafe {
                            e=mdb_cursor_get(curs_tree.cursor,&mut k,&mut v,Op::MDB_NEXT as c_uint);
                        }
                    }
            }
        }
        if parent_inode.is_some() { let _=realpath.pop(); }
    }

    /// Records,i.e. produce a patch and a HashMap mapping line numbers to inodes.
    pub fn record(&mut self,working_copy:&std::path::Path)->Result<(Vec<Change>,HashMap<LocalKey,Inode>),Error>{
        let mut actions:Vec<Change>=Vec::new();
        let mut line_num=1;
        let mut updatables:HashMap<Vec<u8>,Vec<u8>>=HashMap::new();
        let mut realpath=PathBuf::from(working_copy);
        self.record_all(&mut actions, &mut line_num,&mut updatables,
            None,None,ROOT_INODE,&mut realpath,
            &[]);
        //println!("record done");
        Ok((actions,updatables))
    }

    fn unsafe_apply(&mut self,changes:&[Change], internal_patch_id:&[u8]){
        for ch in changes {
            match *ch {
                Change::Edges{ref edges} =>
                    for e in edges {
                        // First remove the deleted version of the edge
                        let mut pu:[u8;1+KEY_SIZE+HASH_SIZE]=[0;1+KEY_SIZE+HASH_SIZE];
                        let mut pv:[u8;1+KEY_SIZE+HASH_SIZE]=[0;1+KEY_SIZE+HASH_SIZE];

                        pu[0]=e.flag ^ DELETED_EDGE ^ PARENT_EDGE;
                        pv[0]=e.flag ^ DELETED_EDGE;
                        let _=unsafe {
                            let u=self.internal_hash(&e.from[0..(e.from.len()-LINE_SIZE)]).unwrap();
                            copy_nonoverlapping(e.from.as_ptr().offset((e.from.len()-LINE_SIZE) as isize),
                                                pu.as_mut_ptr().offset(1+HASH_SIZE as isize), LINE_SIZE);
                            copy_nonoverlapping(u.as_ptr(),pu.as_mut_ptr().offset(1), HASH_SIZE)
                        };
                        let _=unsafe {
                            let v=self.internal_hash(&e.to[0..(e.to.len()-LINE_SIZE)]).unwrap();
                            copy_nonoverlapping(e.to.as_ptr().offset((e.to.len()-LINE_SIZE) as isize),
                                                pv.as_mut_ptr().offset(1+HASH_SIZE as isize), LINE_SIZE);
                            copy_nonoverlapping(v.as_ptr(),pv.as_mut_ptr().offset(1), HASH_SIZE)
                        };
                        let _=unsafe {
                            //println!("introduced by {:?}",e.introduced_by);
                            let p=self.internal_hash(&e.introduced_by).unwrap();
                            copy_nonoverlapping(p.as_ptr(),
                                                pu.as_mut_ptr().offset(1+KEY_SIZE as isize),
                                                HASH_SIZE);
                            copy_nonoverlapping(p.as_ptr(),
                                                pv.as_mut_ptr().offset(1+KEY_SIZE as isize),
                                                HASH_SIZE)
                        };
                        unsafe {
                            let _=mdb::del(self.mdb_txn,self.dbi_nodes,&pu[1..(1+KEY_SIZE)], Some(&pv));
                            let _=mdb::del(self.mdb_txn,self.dbi_nodes,&pv[1..(1+KEY_SIZE)], Some(&pu));
                            // Then add the new edges
                            pu[0]=e.flag^PARENT_EDGE;
                            pv[0]=e.flag;
                            mdb::put(self.mdb_txn,self.dbi_nodes,&pu[1..(1+KEY_SIZE)],&pv,MDB_NODUPDATA).unwrap();
                            mdb::put(self.mdb_txn,self.dbi_nodes,&pv[1..(1+KEY_SIZE)],&pu,MDB_NODUPDATA).unwrap();
                        }
                    },
                Change::NewNodes { ref up_context,ref down_context,ref line_num,ref flag,ref nodes } => {
                    assert!(!nodes.is_empty());
                    let mut pu:[u8;1+KEY_SIZE+HASH_SIZE]=[0;1+KEY_SIZE+HASH_SIZE];
                    let mut pv:[u8;1+KEY_SIZE+HASH_SIZE]=[0;1+KEY_SIZE+HASH_SIZE];
                    let mut lnum0= *line_num;
                    for i in 0..LINE_SIZE { pv[1+HASH_SIZE+i]=(lnum0 & 0xff) as u8; lnum0>>=8 }
                    let _= unsafe {
                        copy_nonoverlapping(internal_patch_id.as_ptr(),
                                            pu.as_mut_ptr().offset(1+KEY_SIZE as isize),
                                            HASH_SIZE);
                        copy_nonoverlapping(internal_patch_id.as_ptr(),
                                            pv.as_mut_ptr().offset(1+KEY_SIZE as isize),
                                            HASH_SIZE);
                        copy_nonoverlapping(internal_patch_id.as_ptr(),
                                            pv.as_mut_ptr().offset(1),
                                            HASH_SIZE)
                    };
                    for c in up_context {
                        let u= if c.len()>LINE_SIZE {
                            self.internal_hash(&c[0..(c.len()-LINE_SIZE)]).unwrap()
                        } else {
                            internal_patch_id
                        };
                        pu[0]= (*flag) ^ PARENT_EDGE;
                        pv[0]= *flag;
                        unsafe {
                            copy_nonoverlapping(u.as_ptr() as *const c_char,
                                                pu.as_ptr().offset(1) as *mut c_char,
                                                HASH_SIZE);
                            copy_nonoverlapping(c.as_ptr().offset((c.len()-LINE_SIZE) as isize),
                                                pu.as_mut_ptr().offset((1+HASH_SIZE) as isize),
                                                LINE_SIZE);
                            mdb::put(self.mdb_txn,self.dbi_nodes,&pu[1..(1+KEY_SIZE)],&pv,0).unwrap();
                            mdb::put(self.mdb_txn,self.dbi_nodes,&pv[1..(1+KEY_SIZE)],&pu,0).unwrap();
                        }
                    }
                    //////////////
                    unsafe {
                        copy_nonoverlapping(internal_patch_id.as_ptr() as *const c_char,
                                            pu.as_ptr().offset(1) as *mut c_char,
                                            HASH_SIZE);
                    }
                    let mut lnum= *line_num + 1;
                    unsafe {
                        mdb::put(self.mdb_txn,self.dbi_contents,&pv[1..(1+KEY_SIZE)], &nodes[0],0).unwrap();
                    }
                    for n in &nodes[1..] {
                        let mut lnum0=lnum-1;
                        for i in 0..LINE_SIZE { pu[1+HASH_SIZE+i]=(lnum0 & 0xff) as u8; lnum0 >>= 8 }
                        lnum0=lnum;
                        for i in 0..LINE_SIZE { pv[1+HASH_SIZE+i]=(lnum0 & 0xff) as u8; lnum0 >>= 8 }
                        pu[0]= (*flag)^PARENT_EDGE;
                        pv[0]= *flag;
                        unsafe {
                            mdb::put(self.mdb_txn,self.dbi_nodes,&pu[1..(1+KEY_SIZE)],&pv,MDB_NODUPDATA).unwrap();
                            mdb::put(self.mdb_txn,self.dbi_nodes,&pv[1..(1+KEY_SIZE)],&pu,MDB_NODUPDATA).unwrap();
                            mdb::put(self.mdb_txn,self.dbi_contents,&pv[1..(1+KEY_SIZE)],&n,0).unwrap();
                        }
                        lnum = lnum+1;
                    }
                    //println!("down context");
                    // In this last part, u is that target (downcontext), and v is the last new node.
                    pu[0]= *flag;
                    pv[0]= (*flag) ^ PARENT_EDGE;
                    for c in down_context {
                        let u= if c.len()>LINE_SIZE {
                            self.internal_hash(&c[0..(c.len()-LINE_SIZE)]).unwrap()
                        } else {
                            internal_patch_id
                        };
                        unsafe {
                            copy_nonoverlapping(u.as_ptr(), pu.as_mut_ptr().offset(1), HASH_SIZE);
                            copy_nonoverlapping(c.as_ptr().offset((c.len()-LINE_SIZE) as isize) as *const c_char,
                                                pu.as_ptr().offset((1+HASH_SIZE) as isize) as *mut c_char,
                                                LINE_SIZE);
                            mdb::put(self.mdb_txn,self.dbi_nodes,&pu[1..(1+KEY_SIZE)],&pv,MDB_NODUPDATA).unwrap();
                            mdb::put(self.mdb_txn,self.dbi_nodes,&pv[1..(1+KEY_SIZE)],&pu,MDB_NODUPDATA).unwrap();
                        }
                    }
                }
            }
        }
    }

    pub fn has_patch(&self, branch:&[u8], hash:&[u8])->Result<bool,Error>{
        unsafe {
            if if hash.len()==HASH_SIZE { memcmp(hash.as_ptr() as *const c_void,
                                                 ROOT_KEY.as_ptr() as *const c_void,
                                                 hash.len() as size_t)==0 } else {false} {
                Ok(true)
            } else {
                match self.internal_hash(hash) {
                    Ok(internal)=>{
                        let curs=try!(Cursor::new(self.mdb_txn,self.dbi_branches).map_err(Error::IoError));
                        match mdb::cursor_get(&curs,branch,Some(internal),Op::MDB_GET_BOTH) {
                            Ok(_)=>Ok(true),
                            Err(MDB_NOTFOUND)=>Ok(false),
                            Err(_)=>Ok(false) // TODO
                        }
                    },
                    Err(_)=>{ Ok(false) }
                }
            }
        }
    }

    /// Applies a patch to a repository.
    pub fn apply(&mut self, patch:&Patch, internal:&[u8])->Result<(),Error> {
        {
            let current=self.get_current_branch();
            unsafe {
                let curs=try!(Cursor::new(self.mdb_txn,self.dbi_branches).map_err(Error::IoError));
                match mdb::cursor_get(&curs,&current,Some(internal),Op::MDB_GET_BOTH) {
                    Ok(_)=>return Err(Error::AlreadyApplied),
                    Err(_)=>()
                };
                mdb::put(self.mdb_txn,self.dbi_branches,&current,&internal,MDB_NODUPDATA).unwrap();
            }
        }
        //println!("unsafe apply");
        self.unsafe_apply(&patch.changes,internal);
        //println!("/unsafe apply");
        for ch in patch.changes.iter() {
            match *ch {
                Change::Edges{ref edges} =>{
                    for e in edges {
                        let hu=self.internal_hash(&e.from[0..(e.from.len()-LINE_SIZE)]).unwrap();
                        let hv=self.internal_hash(&e.to[0..(e.to.len()-LINE_SIZE)]).unwrap();
                        let mut u:[u8;KEY_SIZE]=[0;KEY_SIZE];
                        let mut v:[u8;KEY_SIZE]=[0;KEY_SIZE];
                        unsafe {
                            copy_nonoverlapping(hu.as_ptr(),u.as_mut_ptr(),HASH_SIZE);
                            copy_nonoverlapping(hv.as_ptr(),v.as_mut_ptr(),HASH_SIZE);
                            copy_nonoverlapping(e.from.as_ptr().offset((e.from.len()-LINE_SIZE) as isize),
                                                u.as_mut_ptr().offset(HASH_SIZE as isize),LINE_SIZE);
                            copy_nonoverlapping(e.to.as_ptr().offset((e.to.len()-LINE_SIZE) as isize),
                                                v.as_mut_ptr().offset(HASH_SIZE as isize),LINE_SIZE);
                        }
                        if e.flag&DELETED_EDGE!=0 {
                            let (pu,pv)= if e.flag&PARENT_EDGE!=0 { (&v,&u) } else { (&u,&v) };
                            if e.flag&FOLDER_EDGE!=0 {
                                self.connect_down_folders(pu,pv,&internal)
                            } else {
                                // Now if u is alive, connect u to alive descendants of v (following deleted edges from v).
                                let mut cursor=Cursor::new(self.mdb_txn,self.dbi_nodes).unwrap();
                                if nonroot_is_alive(&mut cursor,pu) {
                                    //println!("pu ={} is alive",to_hex(pu));
                                    let mut children=Vec::new();
                                    for w in CursIter::new(&mut cursor,pv,DELETED_EDGE,false) {
                                        children.extend(&w[1..(1+KEY_SIZE)]);
                                    }
                                    let mut i=0;
                                    while i<children.len() {
                                        self.connect_down(pu,&children[i..(i+KEY_SIZE)],&internal);
                                        i+=KEY_SIZE
                                    }
                                }// else {println!("pu ={} is dead",to_hex(pu))}
                            }
                        } else {
                            let (pu,pv) = if e.flag&PARENT_EDGE!=0 { (&v,&u) } else { (&u,&v) };
                            self.connect_up(pu,pv,&internal);
                            if e.flag&FOLDER_EDGE == 0 {
                                // Now connect v to alive descendants of v (following deleted edges from v).
                                let mut cursor=Cursor::new(self.mdb_txn,self.dbi_nodes).unwrap();
                                let mut children=Vec::new();
                                for w in CursIter::new(&mut cursor,pv,DELETED_EDGE,false) {
                                    children.extend(&w[1..(1+KEY_SIZE)]);
                                }
                                let mut i=0;
                                while i<children.len() {
                                    self.connect_down(pv,&children[i..(i+KEY_SIZE)],&internal);
                                    i+=KEY_SIZE
                                }
                            }
                        }
                    }
                },
                Change::NewNodes { ref up_context,ref down_context,ref line_num,flag:_,ref nodes } => {
                    let mut pu:[u8;KEY_SIZE]=[0;KEY_SIZE];
                    let mut pv:[u8;KEY_SIZE]=[0;KEY_SIZE];
                    let mut lnum0= *line_num;
                    for i in 0..LINE_SIZE { pv[HASH_SIZE+i]=(lnum0 & 0xff) as u8; lnum0>>=8 }
                    let _= unsafe {
                        copy_nonoverlapping(internal.as_ptr(),
                                            pv.as_mut_ptr(),
                                            HASH_SIZE)
                    };
                    for c in up_context {
                        unsafe {
                            let u= if c.len()>LINE_SIZE {
                                self.internal_hash(&c[0..(c.len()-LINE_SIZE)]).unwrap()
                            } else {
                                internal as &[u8]
                            };
                            copy_nonoverlapping(u.as_ptr(), pu.as_mut_ptr(), HASH_SIZE);
                            self.connect_up(&pu,&pv,&internal)
                        }
                    }
                    lnum0= (*line_num)+nodes.len()-1;
                    for i in 0..LINE_SIZE { pv[HASH_SIZE+i]=(lnum0 & 0xff) as u8; lnum0>>=8 }
                    for c in down_context {
                        unsafe {
                            let u= if c.len()>LINE_SIZE {
                                self.internal_hash(&c[0..(c.len()-LINE_SIZE)]).unwrap()
                            } else {
                                internal as &[u8]
                            };
                            copy_nonoverlapping(u.as_ptr(), pv.as_mut_ptr(), HASH_SIZE);
                            self.connect_down(&pv,&pu,&internal)
                        }
                    }
                }
            }
        }
        for ref dep in patch.dependencies.iter() {
            let dep_internal=self.internal_hash(&dep).unwrap();
            unsafe {
                mdb::put(self.mdb_txn,self.dbi_revdep,dep_internal,internal,0).unwrap()
            }
        }
        Ok(())
    }


    pub fn write_changes_file(&self,changes_file:&Path)->Result<(),Error> {
        let mut patches=Vec::new();
        unsafe {
            let branch=self.get_current_branch();
            let curs=Cursor::new(self.mdb_txn,self.dbi_branches).unwrap();
            let mut k=MDB_val{mv_data:branch.as_ptr() as *const c_void, mv_size:branch.len() as size_t};
            let mut v:MDB_val=std::mem::zeroed();
            let mut e=mdb_cursor_get(curs.cursor,&mut k,&mut v,Op::MDB_FIRST as c_uint);
            while e==0 {
                if branch.len() as size_t==k.mv_size &&
                    memcmp(branch.as_ptr() as *const c_void, k.mv_data as *const c_void, k.mv_size)==0 {
                        patches.push(self.external_key(slice::from_raw_parts(v.mv_data as *const u8,v.mv_size as usize)));
                        e=mdb_cursor_get(curs.cursor,&mut k,&mut v,Op::MDB_NEXT as c_uint);
                    }
            }
        }
        try!(fs_representation::write_changes(&patches,changes_file).map_err(Error::FsRepresentation));
        Ok(())
    }



    /// Connect b to the alive ancestors of a (adding pseudo-folder edges if necessary).
    fn connect_up(&mut self, a:&[u8], b:&[u8],internal_patch_id:&[u8]) {
        fn connect<'a>(visited:&mut HashSet<&'a[u8]>, repo:&Repository, a:&'a[u8], internal_patch_id:&'a[u8], buf:&mut Vec<u8>, folder_buf:&mut Vec<u8>) {
            if !visited.contains(a) {
                visited.insert(a);
                // Follow parent edges.
                let mut cursor=Cursor::new(repo.mdb_txn,repo.dbi_nodes).unwrap();
                for a1 in CursIter::new(&mut cursor,&a,PARENT_EDGE|DELETED_EDGE,false) {
                    connect(visited,repo,&a1[1..(1+KEY_SIZE)],internal_patch_id,buf,folder_buf);
                }
                // Test for life of the current node
                if nonroot_is_alive(&mut cursor,a) {
                    //println!("key is alive");
                    buf.push(PSEUDO_EDGE|PARENT_EDGE);
                    buf.extend(a);
                    buf.extend(internal_patch_id)
                }

                // Look at all deleted folder parents, and add them.
                for a1 in CursIter::new(&mut cursor,&a,PARENT_EDGE|DELETED_EDGE|FOLDER_EDGE,false) {
                    folder_buf.push(PSEUDO_EDGE|PARENT_EDGE|FOLDER_EDGE);
                    folder_buf.extend(a);
                    folder_buf.extend(internal_patch_id);
                    folder_buf.push(PSEUDO_EDGE|FOLDER_EDGE);
                    folder_buf.extend(a1);
                    folder_buf.extend(internal_patch_id);
                }
            }
        }
        let mut visited=HashSet::new();
        let mut buf=Vec::new();
        let mut folder_buf=Vec::new();
        connect(&mut visited, self,a,internal_patch_id,&mut buf,&mut folder_buf);
        let mut pu:[u8;1+KEY_SIZE+HASH_SIZE]=[0;1+KEY_SIZE+HASH_SIZE];
        unsafe {
            copy_nonoverlapping(b.as_ptr(), pu.as_mut_ptr().offset(1), KEY_SIZE);
            copy_nonoverlapping(internal_patch_id.as_ptr(), pu.as_mut_ptr().offset((1+KEY_SIZE) as isize), HASH_SIZE)
        }
        let mut i=0;
        while i<buf.len(){
            pu[0]=buf[i] ^ PARENT_EDGE;
            unsafe {
                mdb::put(self.mdb_txn,self.dbi_nodes,&pu[1..(1+KEY_SIZE)],&buf[i..(i+1+KEY_SIZE+HASH_SIZE)],MDB_NODUPDATA).unwrap();
                mdb::put(self.mdb_txn,self.dbi_nodes,&buf[(i+1)..(i+1+KEY_SIZE)],&pu,MDB_NODUPDATA).unwrap();
            }
            i+=1+KEY_SIZE+HASH_SIZE
        }
        i=0;
        while i<folder_buf.len(){
            unsafe {
                mdb::put(self.mdb_txn,self.dbi_nodes,
                         &folder_buf[(i+1)..(i+1+KEY_SIZE)],
                         &folder_buf[(i+1+KEY_SIZE+HASH_SIZE)..(i+2*(1+KEY_SIZE+HASH_SIZE))],0).unwrap();
                mdb::put(self.mdb_txn,self.dbi_nodes,
                         &folder_buf[(i+1+KEY_SIZE+HASH_SIZE+1)..(i+1+KEY_SIZE+HASH_SIZE+KEY_SIZE)],
                         &folder_buf[i..(i+1+KEY_SIZE+HASH_SIZE)],0).unwrap();
            }
            i+=2*(1+KEY_SIZE+HASH_SIZE)
        }
    }

    /// Connect a to the alive descendants of b (not including folder descendants).
    fn connect_down(&mut self, a:&[u8], b:&[u8],internal_patch_id:&[u8]) {
        fn connect<'a>(visited:&mut HashSet<&'a[u8]>, repo:&Repository, b:&'a[u8], internal_patch_id:&'a[u8], buf:&mut Vec<u8>) {
            if !visited.contains(b) {
                visited.insert(b);
                let mut cursor=Cursor::new(repo.mdb_txn,repo.dbi_nodes).unwrap();
                for b1 in CursIter::new(&mut cursor,&b,DELETED_EDGE,false) {
                    connect(visited,repo,&b1[1..(1+KEY_SIZE)],internal_patch_id,buf);
                }
                // for all alive descendants
                for b1 in CursIter::new(&mut cursor,&b,0,true) {
                    buf.push(PSEUDO_EDGE|PARENT_EDGE);
                    buf.extend(&b1[1..(1+KEY_SIZE)]);
                    buf.extend(internal_patch_id)
                }
                // if b is a zombie (we got to b through a deleted edge but it also has alive edges)
                if nonroot_is_alive(&mut cursor,b) {
                    buf.push(PSEUDO_EDGE|PARENT_EDGE);
                    buf.extend(b);
                    buf.extend(internal_patch_id)
                }
            }
        }
        let mut visited=HashSet::new();
        let mut buf=Vec::new();
        connect(&mut visited, self,b,internal_patch_id,&mut buf);
        let mut pu:[u8;1+KEY_SIZE+HASH_SIZE]=[0;1+KEY_SIZE+HASH_SIZE];
        unsafe {
            copy_nonoverlapping(a.as_ptr(), pu.as_mut_ptr().offset(1), KEY_SIZE);
            copy_nonoverlapping(internal_patch_id.as_ptr(), pu.as_mut_ptr().offset((1+KEY_SIZE) as isize), HASH_SIZE)
        }
        let mut i=0;
        while i<buf.len(){
            pu[0]=buf[i] ^ PARENT_EDGE;
            unsafe {
                mdb::put(self.mdb_txn,self.dbi_nodes,&pu[1..(1+KEY_SIZE)],&buf[i..(i+1+KEY_SIZE+HASH_SIZE)],MDB_NODUPDATA).unwrap();
                mdb::put(self.mdb_txn,self.dbi_nodes,&buf[(i+1)..(i+1+KEY_SIZE)],&pu,MDB_NODUPDATA).unwrap();
            }
            i+=1+KEY_SIZE+HASH_SIZE
        }
    }



    /// Connect a to the alive descendants of b (not including folder descendants).
    fn connect_down_folders(&mut self, a:&[u8], b:&[u8],internal_patch_id:&[u8]) {
        fn connect<'a>(visited:&mut HashSet<&'a[u8]>, repo:&Repository, a:&'a[u8],b:&'a[u8], internal_patch_id:&'a[u8], buf:&mut Vec<u8>)->bool {
            if !visited.contains(b) {
                visited.insert(b);
                let mut cursor=Cursor::new(repo.mdb_txn,repo.dbi_nodes).unwrap();
                let flag=DELETED_EDGE|FOLDER_EDGE;
                let mut has_alive_descendants=false;
                for b1 in CursIter::new(&mut cursor,&b,flag,true) {
                    has_alive_descendants = has_alive_descendants ||
                        connect(visited,repo,b,b1,internal_patch_id,buf);
                }
                // test for life.
                let flag=[PARENT_EDGE|FOLDER_EDGE];
                let v= unsafe { mdb::cursor_get(&cursor,&b,Some(&flag[..]),Op::MDB_GET_BOTH_RANGE).unwrap() };
                let is_alive= if v.len()<1 { false } else { v[0]==flag[0] };
                if is_alive {
                    true
                } else {
                    if has_alive_descendants {
                        // dead, but with alive descendants: conflict!
                        buf.push(PARENT_EDGE|FOLDER_EDGE|PSEUDO_EDGE);
                        buf.extend(a);
                        buf.extend(internal_patch_id);
                        buf.push(FOLDER_EDGE|PSEUDO_EDGE);
                        buf.extend(b);
                        buf.extend(internal_patch_id);
                        true
                    } else {
                        false
                    }
                }
            } else {
                // This should never happen, since the "folder part" of the graph is a tree.
                // Maybe remove the "visited" hash set altogether after debugging.
                unreachable!()
            }
        }
        let mut visited=HashSet::new();
        let mut buf=Vec::new();
        connect(&mut visited, self,a,b,internal_patch_id,&mut buf);
        let mut i=0;
        while i<buf.len(){
            let sz=1+KEY_SIZE+HASH_SIZE;
            unsafe {
                mdb::put(self.mdb_txn,self.dbi_nodes,
                         &buf[(i+1)..(i+1+KEY_SIZE)],
                         &buf[(i+sz)..(i+2*sz)],MDB_NODUPDATA).unwrap();
                mdb::put(self.mdb_txn,self.dbi_nodes,
                         &buf[(i+sz+1)..(i+sz+1+KEY_SIZE)],
                         &buf[i..(i+sz)],MDB_NODUPDATA).unwrap();
            }
            i+=2*sz
        }
    }






    pub fn sync_file_additions(&mut self, changes:&[Change], updates:&HashMap<LocalKey,Inode>, internal_patch_id:&[u8]){
        for change in changes {
            match *change {
                Change::NewNodes { up_context:_, down_context:_,ref line_num,ref flag,ref nodes } => {
                    if flag&FOLDER_EDGE != 0 {
                        let mut node=[0;3+KEY_SIZE];
                        unsafe { let _=copy_nonoverlapping(internal_patch_id.as_ptr(), node.as_mut_ptr().offset(3), HASH_SIZE); }
                        let mut l0=*line_num + 1;
                        for i in 0..LINE_SIZE { node[3+HASH_SIZE+i]=(l0&0xff) as u8; l0 = l0>>8 }
                        let mut inode=[0;INODE_SIZE];
                        let inode_l2 = match updates.get(&node[(3+HASH_SIZE)..]) {
                            None => {
                                // This file comes from a remote patch
                                self.create_new_inode(&mut inode);
                                &inode[..]
                            },
                            Some(ref inode)=>
                                // This file comes from a local patch
                                &inode[..]
                        };
                        //println!("adding inode: {:?} for node {:?}",inode,node);
                        unsafe {
                            node[1]=(nodes[0][0] & 0xff) as u8;
                            node[2]=(nodes[0][1] & 0xff) as u8;
                            mdb::put(self.mdb_txn,self.dbi_inodes,&inode_l2,&node,0).unwrap();
                            mdb::put(self.mdb_txn,self.dbi_revinodes,&node[3..],&inode_l2,0).unwrap();
                        }
                    }
                },
                Change::Edges{..} => {}
            }
        }
    }


    fn filename_of_inode(&self,inode:&[u8],working_copy:&mut PathBuf)->bool {
        let mut v_inode=MDB_val{mv_data:inode.as_ptr() as *const c_void, mv_size:inode.len() as size_t};
        let mut v_next:MDB_val = unsafe {std::mem::zeroed()};
        let mut components=Vec::new();
        loop {
            let e = unsafe {mdb_get(self.mdb_txn,self.dbi_revtree,&mut v_inode, &mut v_next)};
            if e==0 {
                if unsafe { memcmp(v_next.mv_data, ROOT_INODE.as_ptr() as *const c_void, INODE_SIZE as size_t) } == 0 {
                    break
                } else {
                    components.push(unsafe { slice::from_raw_parts((v_next.mv_data as *const u8).offset(INODE_SIZE as isize),
                                                                   (v_next.mv_size as usize-INODE_SIZE)) });
                    v_inode.mv_data=v_next.mv_data;
                    v_inode.mv_size=v_next.mv_size;
                }
            } else {
                return false
            }
        }
        for c in components.iter().rev() {
            working_copy.push(std::str::from_utf8(c).unwrap());
        }
        true
    }



    fn unsafe_output_repository(&mut self, working_copy:&Path) -> Result<Vec<(Vec<u8>,Vec<u8>)>,Error>{
        fn retrieve_paths<'a> (repo:&'a Repository,
                               working_copy:&Path,
                               key:&'a [u8],path:&mut PathBuf,parent_inode:&'a [u8],
                               paths:&mut HashMap<PathBuf,Vec<(Vec<u8>,Vec<u8>,Vec<u8>,Option<PathBuf>,usize)>>,
                               cache:&mut HashSet<&'a [u8]>,
                               nfiles:&mut usize) {
            if !cache.contains(key) {
                cache.insert(key);
                let mut curs_b=Cursor::new(repo.mdb_txn,repo.dbi_nodes).unwrap();
                for b in CursIter::new(&mut curs_b,key,FOLDER_EDGE,true) {
                    let cont_b=
                        match unsafe { mdb::get(repo.mdb_txn,repo.dbi_contents,&b[1..(1+KEY_SIZE)]) } {
                            Ok(cont_b)=>cont_b,
                            Err(_)=>&[][..]
                        };
                    if cont_b.len()<2 { panic!("node (b) too short") } else {
                        let filename=&cont_b[2..];
                        let perms= (((cont_b[0] as usize) << 8) | (cont_b[1] as usize)) & 0x1ff;
                        let mut curs_c=Cursor::new(repo.mdb_txn,repo.dbi_nodes).unwrap();
                        for c in CursIter::new(&mut curs_c,&b[1..(1+KEY_SIZE)],FOLDER_EDGE,true) {

                            let mut cv=unsafe {MDB_val { mv_data:(c.as_ptr() as *const u8).offset(1) as *const c_void,
                                                         mv_size:KEY_SIZE as size_t }};
                            let mut cont_c:MDB_val=unsafe { std::mem::zeroed() };
                            let e=unsafe {mdb_get(repo.mdb_txn,repo.dbi_contents,&mut cv,&mut cont_c) };
                            if e!=0 {
                                panic!("c contents not found")
                            } else {
                                let mut inode:MDB_val = unsafe { std::mem::zeroed() };
                                let e=unsafe {mdb_get(repo.mdb_txn,repo.dbi_revinodes,&mut cv,&mut inode) };
                                let inode=
                                    if e==0 {
                                        unsafe { slice::from_raw_parts(inode.mv_data as *const u8,
                                                                       inode.mv_size as usize) }
                                    } else {
                                        panic!("inodes not synchronized")
                                    };
                                path.push(std::str::from_utf8(filename).unwrap());
                                {
                                    let vec=paths.entry(path.clone()).or_insert(Vec::new());
                                    let mut buf=PathBuf::from(working_copy);
                                    *nfiles+=1;
                                    vec.push((c[1..(1+KEY_SIZE)].to_vec(),parent_inode.to_vec(),inode.to_vec(),
                                              if repo.filename_of_inode(inode,&mut buf) {Some(buf)} else { None },
                                              perms))
                                }
                                if perms & DIRECTORY_FLAG != 0 { // is_directory
                                    retrieve_paths(repo,working_copy,&c[1..(1+KEY_SIZE)],path,inode,paths,cache,nfiles);
                                }
                                path.pop();
                            }
                        }
                    }
                }
            }
        }
        let mut paths=HashMap::new();
        let mut cache=HashSet::new();
        let mut buf=PathBuf::from(working_copy);
        let mut nfiles=0;
        retrieve_paths(self,working_copy,&ROOT_KEY,&mut buf,ROOT_INODE,&mut paths,&mut cache,&mut nfiles);

        //println!("dropping tree");
        unsafe {
            mdb_drop(self.mdb_txn,self.dbi_tree,0);
            mdb_drop(self.mdb_txn,self.dbi_revtree,0);
        };
        let mut updates=Vec::with_capacity(nfiles);
        for (k,a) in paths {
            let alen=a.len();
            let mut kk=k.clone();
            let mut filename=kk.file_name().unwrap().to_os_string();
            let mut i=0;
            for (node,parent_inode,inode,oldpath,perms) in a {
                if alen>1 { filename.push(format!("~{}",i)) }
                kk.set_file_name(&filename);
                match oldpath {
                    Some(oldpath)=> try!(fs::rename(oldpath,&kk).map_err(Error::IoError)),
                    None => ()
                }
                let mut par=parent_inode.to_vec();
                par.extend(filename.to_str().unwrap().as_bytes());
                updates.push((par,inode.to_vec()));
                // Then (if file) output file
                if perms & DIRECTORY_FLAG == 0 { // this is a real file, not a directory
                    let mut l=self.retrieve(&node).unwrap();
                    let mut f=std::fs::File::create(&kk).unwrap();
                    self.output_file(&mut f,&mut l);
                } else {
                    try!(std::fs::create_dir_all(&kk).map_err(Error::IoError));
                }
                //
                i+=1
            }
        }
        Ok(updates)
    }


    fn update_tree(&mut self,updates:Vec<(Vec<u8>,Vec<u8>)>){
        for (par,inode) in updates {
            unsafe {
                mdb::put(self.mdb_txn,self.dbi_tree,&par,&inode,0).unwrap();
                mdb::put(self.mdb_txn,self.dbi_revtree,&inode,&par,0).unwrap();
            }
        }
    }


    pub fn output_repository(&mut self, working_copy:&Path, pending:&Patch) -> Result<(),Error>{
        unsafe {
            let parent_txn=self.mdb_txn;
            let txn=ptr::null_mut();
            let e=mdb_txn_begin(self.mdb_env,self.mdb_txn,0,std::mem::transmute(&txn));
            if e==0 {
                self.mdb_txn=txn;
                let mut internal=[0;HASH_SIZE];
                self.new_internal(&mut internal[..]);
                try!(self.apply(pending,&internal[..]));
                let updates=try!(self.unsafe_output_repository(working_copy));
                mdb_txn_abort(txn);
                self.mdb_txn=parent_txn;
                self.update_tree(updates);
                Ok(())
            } else {
                Err(Error::IoError(std::io::Error::from_raw_os_error(e)))
            }
        }
    }


    pub fn debug<W>(&mut self,w:&mut W) where W:Write {
        let mut styles=Vec::with_capacity(16);
        for i in 0..15 {
            styles.push(("color=").to_string()
                        +["red","blue","green","black"][(i >> 1)&3]
                        +if (i as u8)&DELETED_EDGE!=0 { ", style=dashed"} else {""}
                        +if (i as u8)&PSEUDO_EDGE!=0 { ", style=dotted"} else {""})
        }
        w.write(b"digraph{\n").unwrap();
        let curs=Cursor::new(self.mdb_txn,self.dbi_nodes).unwrap();
        unsafe {
            let mut k:MDB_val=std::mem::zeroed();
            let mut v:MDB_val=std::mem::zeroed();
            let mut e=mdb_cursor_get(curs.cursor,&mut k,&mut v,Op::MDB_FIRST as c_uint);
            let cur=&[];
            while e==0 {
                let kk=slice::from_raw_parts(k.mv_data as *const u8,k.mv_size as usize);
                let vv=slice::from_raw_parts(v.mv_data.offset(1) as *const u8,KEY_SIZE as usize);
                if kk!=cur {
                    let mut ww:MDB_val=std::mem::zeroed();
                    let f=mdb_get(self.mdb_txn,self.dbi_contents, &mut k, &mut ww);
                    let cont:&[u8]=
                        if f==0 { slice::from_raw_parts(ww.mv_data as *const u8,ww.mv_size as usize) } else { &[] };
                    write!(w,"n_{}[label=\"{}: {}\"];\n", to_hex(&kk), to_hex(&kk),
                           match str::from_utf8(&cont) { Ok(x)=>x.to_string(), Err(_)=> to_hex(&cont) }).unwrap();
                }
                let flag:u8= * (v.mv_data as *const u8);
                write!(w,"n_{}->n_{}[{},label=\"{}\"];\n", to_hex(&kk), to_hex(&vv), styles[(flag&0xff) as usize], flag).unwrap();
                e=mdb_cursor_get(curs.cursor,&mut k,&mut v,Op::MDB_NEXT as c_uint);
            }
        }
        w.write(b"}\n").unwrap();
    }
}
/*
fn dump_table(txn:*mut MdbTxn,dbi:MdbDbi){
    println!("dumping table");
    unsafe {
        let mut k:MDB_val=std::mem::zeroed();
        let mut v:MDB_val=std::mem::zeroed();
        let c=Cursor::new(txn,dbi).unwrap();
        let mut e=mdb_cursor_get(c.cursor,&mut k,&mut v,Op::MDB_FIRST as c_uint);
        while e==0 {
            let kk=std::slice::from_raw_parts(k.mv_data as *const u8, k.mv_size as usize);
            let vv=std::slice::from_raw_parts(v.mv_data as *const u8, v.mv_size as usize);
            println!("key:{:?}, value={:?}",to_hex(kk),to_hex(vv));
            e=mdb_cursor_get(c.cursor,&mut k,&mut v,Op::MDB_NEXT as c_uint)
        }
    }
    println!("/dumping table");
}
*/
