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
extern crate time;
extern crate serde;
#[macro_use]
extern crate log;

mod lmdb;

use self::libc::{c_char,c_uchar,c_void,size_t};
use self::libc::{memcmp};
use std::ptr::{copy_nonoverlapping};
use std::ptr;

use std::str;

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::path::{PathBuf,Path};

use std::io::{Write,BufRead,Read};
use std::collections::HashSet;
use std::fs::{metadata};
pub mod fs_representation;
use self::fs_representation::*;
pub mod patch;
use self::patch::*;

pub mod error;
use self::error::Error;

use std::collections::BTreeSet;
#[cfg(not(windows))]
use std::os::unix::fs::PermissionsExt;

use std::fs;

extern crate rand;

use std::marker::PhantomData;

/// The repository structure, on which most functions work.
pub struct Repository<'a> {
    mdb_env:lmdb::Env,
    mdb_txn:lmdb::Txn<'a>,
    dbi_nodes:lmdb::Dbi,
    dbi_revdep:lmdb::Dbi,
    dbi_contents:lmdb::Dbi,
    dbi_internal:lmdb::Dbi,
    dbi_external:lmdb::Dbi,
    dbi_branches:lmdb::Dbi,
    dbi_tree:lmdb::Dbi,
    dbi_revtree:lmdb::Dbi,
    dbi_inodes:lmdb::Dbi,
    dbi_revinodes:lmdb::Dbi
}

impl <'a>Drop for Repository<'a> {
    fn drop(&mut self){
        unsafe {
            if std::thread::panicking() {
                self.mdb_txn.unsafe_abort()
            } else {
                self.mdb_txn.unsafe_commit().unwrap()
            }
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

const LINE_HALF_DELETED:c_uchar=4;
const LINE_VISITED:c_uchar=2;
const LINE_ONSTACK:c_uchar=1;

struct Line<'a> {
    key:&'a[u8],
    flags:u8,
    children:usize,
    n_children:usize,
    index:usize,
    lowlink:usize,
    scc:usize
}

struct Graph<'a> {
    lines:Vec<Line<'a>>,
    children:Vec<(*const u8,usize)>
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


struct CursIter<'a,'b> {
    cursor: *mut lmdb::MdbCursor,
    op:lmdb::Op,
    edge_flag:u8,
    include_pseudo:bool,
    key:&'b[u8],
    marker:PhantomData<&'a()>
}

impl <'a,'b>CursIter<'a,'b> {
    fn new(curs:*mut lmdb::MdbCursor,key:&'b [u8],flag:u8,include_pseudo:bool)->CursIter<'a,'b>{
        CursIter { cursor:curs,
                   key:key,
                   include_pseudo:include_pseudo,
                   edge_flag:flag,
                   op:lmdb::Op::MDB_GET_BOTH_RANGE,
                   marker:PhantomData }
    }
}

impl <'a,'b>Iterator for CursIter<'a,'b> {
    type Item=&'a [u8];
    fn next(&mut self)->Option<&'a[u8]>{
        match unsafe { lmdb::cursor_get(self.cursor,self.key,Some(&[self.edge_flag][..]),
                                        std::mem::replace(&mut self.op, lmdb::Op::MDB_NEXT_DUP)) } {
            Ok((_,val))=> {
                if val[0] == self.edge_flag || (self.include_pseudo && val[0] == (self.edge_flag|PSEUDO_EDGE)) {
                    Some(val)
                } else {
                    None
                }
            },
            Err(_) => None
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

fn has_edge(cursor:&mut lmdb::MdbCursor,key:&[u8],flag0:u8,include_folder:bool,include_pseudo:bool)->bool {
    let mut flag=[flag0];
    debug!(target:"has_edge", "{:?}",flag[0]);
    while flag[0] <= flag0|PSEUDO_EDGE|FOLDER_EDGE {
        if (flag[0] & PSEUDO_EDGE != 0 && include_pseudo)
            || (flag[0] & FOLDER_EDGE !=0 && include_folder)
            || (flag[0] == flag0)
        {
            match unsafe {lmdb::cursor_get(cursor,&key,Some(&flag[..]),lmdb::Op::MDB_GET_BOTH_RANGE)} {
                Ok((_,v))=>{
                    debug_assert!(v.len()>=1);
                    debug!(target:"has_edge", "{:?} == {:?} ?",flag[0],v[0]);
                    if v[0]==flag[0] { return true }
                },
                _=>{}
            }
        }
        flag[0]+=1
    }
    false
}


fn is_alive(cursor:&mut lmdb::MdbCursor,key:&[u8])->bool {
    (unsafe { memcmp(key.as_ptr() as *const c_void,
                     ROOT_KEY.as_ptr() as *const c_void,
                     ROOT_KEY.len() as size_t) == 0 })
        || has_edge(cursor,key,PARENT_EDGE,true,true)
}


fn has_nondeleted_children(cursor:&mut lmdb::MdbCursor,key:&[u8])->bool {
    let mut flag=[0];
    let alive= {
        match unsafe {lmdb::cursor_get(cursor,&key,Some(&flag[..]),lmdb::Op::MDB_GET_BOTH_RANGE)} {
            Ok((_,v))=>{
                debug_assert!(v.len()>=1);
                v[0]==flag[0]
            },
            _=>false
        }
    };
    alive || {
        flag[0]=FOLDER_EDGE;
        match unsafe { lmdb::cursor_get(cursor,&key,Some(&flag[..]),lmdb::Op::MDB_GET_BOTH_RANGE) } {
            Ok((_,v))=>{
                debug_assert!(v.len()>=1);
                v[0]==flag[0]
            },
            _=>false
        }
    }
}




impl <'a> Repository<'a> {
    pub fn new(path:&std::path::Path)->Result<Repository<'a>,Error>{
        let env=try!(lmdb::Env_::new());
        let _=try!(env.reader_check());
        try!(env.set_maxdbs(10));
        try!(env.set_mapsize( (1 << 30) ));
        let env=try!(env.open(path,0,0o755));
        unsafe {
            let txn=try!(env.unsafe_txn(0));
            let dbi_nodes=try!(txn.unsafe_dbi_open(b"nodes\0",lmdb::MDB_CREATE|lmdb::MDB_DUPSORT|lmdb::MDB_DUPFIXED));
            let dbi_revdep=try!(txn.unsafe_dbi_open(b"revdep\0",lmdb::MDB_CREATE|lmdb::MDB_DUPSORT));
            let dbi_contents=try!(txn.unsafe_dbi_open(b"contents\0",lmdb::MDB_CREATE));
            let dbi_internal=try!(txn.unsafe_dbi_open(b"internal\0",lmdb::MDB_CREATE));
            let dbi_external=try!(txn.unsafe_dbi_open(b"external\0",lmdb::MDB_CREATE));
            let dbi_branches=try!(txn.unsafe_dbi_open(b"branches\0",lmdb::MDB_CREATE|lmdb::MDB_DUPSORT));
            let dbi_tree=try!(txn.unsafe_dbi_open(b"tree\0",lmdb::MDB_CREATE));
            let dbi_revtree=try!(txn.unsafe_dbi_open(b"revtree\0",lmdb::MDB_CREATE));
            let dbi_inodes=try!(txn.unsafe_dbi_open(b"inodes\0",lmdb::MDB_CREATE));
            let dbi_revinodes=try!(txn.unsafe_dbi_open(b"revinodes\0",lmdb::MDB_CREATE));
            let repo=Repository{
                mdb_env:env,
                mdb_txn:txn,
                dbi_nodes:dbi_nodes,
                dbi_revdep:dbi_revdep,
                dbi_contents:dbi_contents,
                dbi_internal:dbi_internal,
                dbi_external:dbi_external,
                dbi_branches:dbi_branches,
                dbi_tree:dbi_tree,
                dbi_revtree:dbi_revtree,
                dbi_inodes:dbi_inodes,
                dbi_revinodes:dbi_revinodes
            };
            Ok(repo)
        }
    }



    fn create_new_inode(&mut self,buf:&mut [u8]){
        let curs_revtree=self.mdb_txn.cursor(self.dbi_revtree).unwrap();
        for i in 0..INODE_SIZE { buf[i]=rand::random() }
        while let Ok((_,x))=curs_revtree.get(&buf,None,lmdb::Op::MDB_SET_RANGE) {
            if unsafe { memcmp(buf.as_ptr() as *const c_void,
                               x.as_ptr() as *const c_void,
                               INODE_SIZE as size_t) } != 0 {
                break
            } else {
                for i in 0..INODE_SIZE { buf[i]=rand::random() }
            }
        }
    }

    fn add_inode(&mut self, inode:&Option<&[u8]>, path:&std::path::Path, is_dir:bool)->Result<(),Error>{
        let mut buf:Inode=vec![0;INODE_SIZE];
        let mut components=path.components();
        let mut cs=components.next();
        while let Some(s)=cs { // need to peek at the next element, so no for.
            cs=components.next();
            let ss=s.as_os_str().to_str().unwrap();
            buf.truncate(INODE_SIZE);
            buf.extend(ss.as_bytes());
            let mut broken=false;
            {
                match self.mdb_txn.get(self.dbi_tree,&buf) {
                    Ok(Some(v))=> {
                        if cs.is_none() {
                            return Err(Error::AlreadyAdded)
                        } else {
                            // replace buf with existing inode
                            buf.clear();
                            buf.extend(v);
                        }
                    },
                    _ =>{
                        broken=true
                    }
                }
            }
            if broken {
                let mut inode_:[u8;INODE_SIZE]=[0;INODE_SIZE];
                let inode = if cs.is_none() && inode.is_some() {
                    inode.unwrap()
                } else {
                    self.create_new_inode(&mut inode_);
                    &inode_[..]
                };
                self.mdb_txn.put(self.dbi_tree,&buf,&inode,0).unwrap();
                self.mdb_txn.put(self.dbi_revtree,&inode,&buf,0).unwrap();
                if cs.is_some() || is_dir {
                    self.mdb_txn.put(self.dbi_tree,&inode,&[],0).unwrap();
                }
                // push next inode onto buf.
                buf.clear();
                buf.extend(inode)
            }
        }
        Ok(())
    }
    /// Adds a file in the repository. Additions need to be recorded in
    /// order to produce a patch.
    pub fn add_file(&mut self, path:&std::path::Path, is_dir:bool)->Result<(),Error>{
        self.add_inode(&None,path,is_dir)
    }


    pub fn move_file(&mut self, path:&std::path::Path, path_:&std::path::Path,is_dir:bool) -> Result<(), Error>{

        let inode= &mut (Vec::new());
        let parent= &mut (Vec::new());

        (*inode).extend(ROOT_INODE);
        for c in path.components() {
            inode.extend(c.as_os_str().to_str().unwrap().as_bytes());
            match self.mdb_txn.get(self.dbi_tree,&inode) {
                Ok(Some(x))=> {
                    std::mem::swap(inode,parent);
                    (*inode).clear();
                    (*inode).extend(x);
                },
                _=>{
                    return Err(Error::FileNotInRepo(path.to_path_buf()))
                }
            }
        }
        // Now the last inode is in "*inode"
        let basename=path.file_name().unwrap();
        (*parent).extend(basename.to_str().unwrap().as_bytes());

        try!(self.mdb_txn.del(self.dbi_tree,parent,None));

        self.add_inode(&Some(inode),path_,is_dir).unwrap();

        let vv=
            match self.mdb_txn.get(self.dbi_inodes,inode) {
                Ok(Some(v))=> {
                    let mut vv=v.to_vec();
                    vv[0]=1;
                    Some(vv)
                },
                _=>None
            };
        if let Some(vv)=vv {
            self.mdb_txn.put(self.dbi_inodes,inode,&vv,0).unwrap();
        };
        Ok(())
    }
    pub fn remove_file(&mut self, path:&std::path::Path) -> Result<(), Error>{
        let mut inode=Vec::new();
        inode.extend(ROOT_INODE);
        let mut comp=path.components();
        let mut c=comp.next();
        loop {
            match c {
                Some(sc)=>{
                    //println!("inode {} + {:?}",to_hex(&inode),sc);
                    inode.extend(sc.as_os_str().to_str().unwrap().as_bytes());
                    match self.mdb_txn.get(self.dbi_tree,&inode) {
                        Ok(Some(x))=> { c=comp.next();
                                  if c.is_some() {inode.clear(); inode.extend(x) }
                        },
                        _ => return Err(Error::FileNotInRepo(path.to_path_buf()))
                    }
                },
                _=>break
            }
        }
        // This function returns a boolean indicating whether the directory we are trying to delete is non-empty, and deletes it if so.
        fn rec_delete(repo:&mut Repository,key:&[u8])->bool {
            //println!("rec_delete {}",to_hex(key));
            let mut children=Vec::new();
            // First, kill the inode itself, if it exists (or mark it deleted)
            //let mut k = MDB_val{ mv_data:key.as_ptr() as *const c_void, mv_size:key.len() as size_t };
            //let mut v : MDB_val = std::mem::zeroed();
            {
                let curs=repo.mdb_txn.cursor(repo.dbi_tree).unwrap();
                let mut result=curs.get(&key,None,lmdb::Op::MDB_SET_RANGE);
                loop {
                    match result {
                        Ok((k,v))=>
                            if unsafe { memcmp(k.as_ptr() as *const c_void,key.as_ptr() as *const c_void,
                                               key.len() as size_t) } ==0 {
                                //debug_assert!(v.mv_size as usize==INODE_SIZE);
                                if v.len()>0 {
                                    children.push((k.to_vec(),v.to_vec()));
                                }
                                result=curs.get(&k,Some(&v),lmdb::Op::MDB_NEXT);
                            } else {
                                break
                            },
                        _=>break
                    }
                }
            }
            {
                for (a,b) in children {
                    if rec_delete(repo,&b) {
                        //println!("deleting {} {}",to_hex(&a),to_hex(&b));
                        repo.mdb_txn.del(repo.dbi_tree,&a,Some(&b)).unwrap();
                        repo.mdb_txn.del(repo.dbi_revtree,&b,Some(&a)).unwrap();
                    }
                }
            }
            let mut node_=[0;3+KEY_SIZE];
            // If the directory is empty, then mark the corresponding node as deleted (flag '2').
            // TODO: this could be done by unsafely mutating the lmdb memory.
            let b=
                match repo.mdb_txn.get(repo.dbi_inodes,key) {
                    Ok(Some(node)) => {
                        debug_assert!(node.len()==KEY_SIZE);
                        unsafe {
                            copy_nonoverlapping(node.as_ptr() as *const c_void,
                                                node_.as_ptr() as *mut c_void,
                                                3+KEY_SIZE);
                        }
                        node_[0]=2;
                        false
                    },
                    Ok(None)=>true,
                    Err(_)=>{
                        panic!("delete panic")
                    }
                };
            if !b {
                repo.mdb_txn.put(repo.dbi_inodes,key,&node_[..],0).unwrap();
            }
            b
        }
        rec_delete(self,&inode);
        Ok(())
    }


    pub fn list_files(&self)->Vec<PathBuf>{
        fn collect(repo:&Repository,key:&[u8],pb:&Path, basename:&[u8],files:&mut Vec<PathBuf>) {
            //println!("collecting {:?},{:?}",to_hex(key),std::str::from_utf8_unchecked(basename));
            let add= match repo.mdb_txn.get(repo.dbi_inodes,key) {
                Ok(Some(node)) => node[0]<2,
                Ok(None)=> true,
                Err(_)=>panic!("list_files panic")
            };
            if add {
                let next_pb=pb.join(std::str::from_utf8(basename).unwrap());
                let next_pb_=next_pb.clone();
                if basename.len()>0 { files.push(next_pb) }
                let curs=repo.mdb_txn.cursor(repo.dbi_tree).unwrap();

                let mut result = curs.get(key,None,lmdb::Op::MDB_SET_RANGE);
                loop {
                    match result {
                        Ok((k,v))=>{
                            if v.len()>0 && unsafe {memcmp(k.as_ptr() as *const c_void,
                                                           key.as_ptr() as *const c_void,
                                                           INODE_SIZE as size_t) }==0 {

                                collect(repo,v,next_pb_.as_path(),&k[INODE_SIZE..],files);
                                result=curs.get(key,Some(v),lmdb::Op::MDB_NEXT_DUP);
                            } else {
                                break
                            }
                        },
                        _=>break
                    }
                }
            }
        }
        let mut files=Vec::new();
        let mut pathbuf=PathBuf::new();
        collect(self,&ROOT_INODE[..], &mut pathbuf, &[], &mut files);
        files
    }


    pub fn get_current_branch(&'a self)->&'a[u8] {
        match self.mdb_txn.get(self.dbi_branches,&[0]) {
            Ok(Some(b))=>b,
            Ok(None)=>DEFAULT_BRANCH.as_bytes(),
            Err(_)=>panic!("get_current_branch")
        }
    }



    fn retrieve(&'a self,key:&'a [u8])->Result<Graph<'a>,()>{
        fn retr<'a,'b,'c>(cache:&mut HashMap<&'a [u8],usize>,
                          curs:&'b mut lmdb::MdbCursor,
                          lines:&mut Vec<Line<'a>>,
                          children:&mut Vec<(*const u8,usize)>,
                          key:&'a[u8])->usize {
            {
                match cache.entry(key) {
                    Entry::Occupied(e) => return *(e.get()),
                    Entry::Vacant(e) => {
                        let idx=lines.len();
                        e.insert(idx);
                        debug!(target:"retrieve","{}",to_hex(key));
                        // Test: is this a zombie line?
                        let is_zombie={
                            let mut tag=PARENT_EDGE|DELETED_EDGE;
                            unsafe {
                                (match lmdb::cursor_get(curs,key,Some(&[tag][..]),lmdb::Op::MDB_GET_BOTH_RANGE) {
                                    Ok((_,v)) if v[0]==tag => true,
                                    _=>false
                                }) ||
                                    ({tag=PARENT_EDGE|DELETED_EDGE|FOLDER_EDGE;
                                      match lmdb::cursor_get(curs,key,Some(&[tag][..]),lmdb::Op::MDB_GET_BOTH_RANGE) {
                                          Ok((_,v)) if v[0]==tag => true,
                                          _=>false
                                      }})
                            }
                        };
                        //
                        let mut l=Line {
                            key:key,flags:if is_zombie {LINE_HALF_DELETED} else {0},
                            children:children.len(),n_children:0,index:0,lowlink:0,scc:0
                        };
                        for child in CursIter::new(curs,key,0,true) {
                            children.push((child.as_ptr(),0));
                            /*
                            unsafe {
                                children.push(std::mem::transmute(child.as_ptr()))
                            }
                            */
                            l.n_children+=1
                        }
                        lines.push(l)
                    }
                }
            }
            let idx=lines.len()-1;
            let l_children=lines[idx].children;
            let n_children=lines[idx].n_children;
            debug!(target:"retrieve", "n_children: {}",n_children);
            for i in 0..n_children {
                let (a,_)=children[l_children+i];
                let child_key = unsafe {
                    std::slice::from_raw_parts(a.offset(1),KEY_SIZE)
                };
                let flag = unsafe { *a };
                children[l_children+i] = (a, retr(cache,curs,lines,children,child_key))
            }
            if n_children==0 {
                children.push((std::ptr::null(),0));
                lines[idx].n_children=1;
            }
            idx
        }
        let mut cache=HashMap::new();
        let mut cursor= unsafe { &mut *self.mdb_txn.unsafe_cursor(self.dbi_nodes).unwrap() };
        let mut lines=Vec::new();
        // Insert last line (so that all lines have a common descendant).
        lines.push(Line {
            key:&b""[..],flags:0,children:0,n_children:0,index:0,lowlink:0,scc:0
        });
        cache.insert(&b""[..],0);
        let mut children=Vec::new();
        retr(&mut cache,cursor,&mut lines,&mut children,key);
        unsafe { lmdb::mdb_cursor_close(cursor) };
        Ok(Graph { lines:lines, children:children })
    }

    fn contents<'b>(&'a self,key:&'b[u8]) -> &'a[u8] {
        debug_assert!(key.len() == KEY_SIZE);
        match self.mdb_txn.get(self.dbi_contents,key) {
            Ok(Some(v))=>v,
            Ok(None)=>&[],
            Err(e) =>{
                debug!("contents error for key {}",to_hex(key));
                panic!("contents error: {:?}", e)
            }
        }
    }


    fn tarjan(&self,line:&mut Graph)->Vec<Vec<usize>> {
        fn dfs<'a>(repo:&Repository,
                   scc:&mut Vec<Vec<usize>>,
                   stack:&mut Vec<usize>,
                   index:&mut usize, g:&mut Graph<'a>, n_l:usize){
            {
                let mut l=&mut (g.lines[n_l]);
                (*l).index = *index;
                (*l).lowlink = *index;
                (*l).flags |= LINE_ONSTACK | LINE_VISITED;
                debug!(target:"tarjan", "{} {} chi",to_hex((*l).key),(*l).n_children);
                //unsafe {println!("contents: {}",std::str::from_utf8_unchecked(repo.contents((*l).key))); }
            }
            stack.push(n_l);
            *index = *index + 1;
            for i in 0..g.lines[n_l].n_children {
                //let mut l=&mut (g.lines[n_l]);

                let (_,n_child) = g.children[g.lines[n_l].children + i];
                //println!("children: {}",to_hex(g.lines[n_child].key));

                if g.lines[n_child].flags & LINE_VISITED == 0 {
                    dfs(repo,scc,stack,index,g,n_child);
                    g.lines[n_l].lowlink=std::cmp::min(g.lines[n_l].lowlink, g.lines[n_child].lowlink);
                } else {
                    if g.lines[n_child].flags & LINE_ONSTACK != 0 {
                        g.lines[n_l].lowlink=std::cmp::min(g.lines[n_l].lowlink, g.lines[n_child].index)
                    }
                }
            }

            if g.lines[n_l].index == g.lines[n_l].lowlink {
                //println!("SCC: {:?}",slice::from_raw_parts((*l).key,KEY_SIZE));
                let mut v=Vec::new();
                loop {
                    match stack.pop() {
                        None=>break,
                        Some(n_p)=>{
                            g.lines[n_p].scc= scc.len();
                            g.lines[n_p].flags = g.lines[n_p].flags ^ LINE_ONSTACK;
                            v.push(n_p);
                            if n_p == n_l { break }
                        }
                    }
                }
                scc.push(v);
                //*scc+=1
            }
        }
        let mut stack=vec!();
        let mut index=0;
        let mut scc=Vec::with_capacity(line.lines.len());
        //let mut scc=0;
        dfs(self,&mut scc, &mut stack, &mut index, line, 1);
        scc
    }





    fn output_file<'b,'c:'b,B:LineBuffer<'c>>(&'c self,buf:&'b mut B,g:Graph<'a>,forward:&mut Vec<u8>) {
        let mut g=g;
        let t0=time::precise_time_s();
        let scc = self.tarjan(&mut g); // in reverse order.
        let t1=time::precise_time_s();
        info!("tarjan took {}s",t1-t0);
        info!("There are {} SCC",scc.len());
        //let mut levels=vec![0;scc];
        let mut last_visit=vec![0;scc.len()];
        let mut first_visit=vec![0;scc.len()];
        let mut step=1;
        fn dfs<'a>(g:&mut Graph<'a>,
                   first_visit:&mut[usize],
                   last_visit:&mut[usize],
                   forward:&mut Vec<u8>,
                   zero:&[u8],
                   step:&mut usize,
                   scc:&[Vec<usize>],
                   mut n_scc:usize) {
            let mut child_components=BTreeSet::new();
            let mut skipped=vec!(n_scc);
            loop {
                first_visit[n_scc] = *step;
                debug!(target:"output_file","step={} scc={}",*step,n_scc);
                *step += 1;
                child_components.clear();
                let mut next_scc=0;
                for cousin in scc[n_scc].iter() {
                    debug!(target:"output_file","cousin: {}",*cousin);
                    let n=g.lines[*cousin].n_children;
                    for i in 0 .. n {
                        let (_,n_child) = g.children[g.lines[*cousin].children + i];
                        let child_component=g.lines[n_child].scc;
                        if child_component < n_scc { // if this is a child and not a sibling.
                            child_components.insert(child_component);
                            next_scc=child_component
                        }
                    }
                }
                if child_components.len() != 1 { break } else {
                    n_scc=next_scc;
                    skipped.push(next_scc);
                }
            }
            let mut forward_scc=HashSet::new();
            for component in child_components.iter().rev() {
                if first_visit[*component] > first_visit[n_scc] { // forward edge
                    debug!(target:"output_file","forward ! {} {}",n_scc,*component);
                    forward_scc.insert(*component);
                } else {
                    debug!(target:"output_file","visiting scc {} {}",*component,to_hex(g.lines[scc[*component][0]].key));
                    dfs(g,first_visit,last_visit,forward,zero,step,scc,*component)
                }
            }
            for cousin in scc[n_scc].iter() {
                let n=g.lines[*cousin].n_children;
                for i in 0 .. n {
                    let (_,n_child) = g.children[g.lines[*cousin].children + i];
                    let child_component=g.lines[n_child].scc;
                    let is_forward=forward_scc.contains(&child_component);
                    if is_forward {
                        if n_child & 1 != 0 {
                            forward.push(PSEUDO_EDGE|PARENT_EDGE);
                            forward.extend(g.lines[*cousin].key);
                            forward.extend(zero);
                            forward.push(PSEUDO_EDGE);
                            forward.extend(g.lines[n_child].key);
                        }
                        // Indicate here that we do not want to follow this edge (it is forward).
                        let (a,_)=g.children[g.lines[*cousin].children+i];
                        g.children[g.lines[*cousin].children + i] = (a,0);
                    }
                }
            }
            for i in skipped.iter().rev() {
                last_visit[*i] = *step;
                *step+=1;
            }
        }
        let zero=[0;HASH_SIZE];
        dfs(&mut g,&mut first_visit,&mut last_visit,forward,&zero[..],&mut step,&scc,scc.len()-1);
        // assumes no conflict for now.
        let mut i=scc.len()-1;
        let mut nodes=vec!();
        let mut selected_zombies=HashMap::new();
        loop {
            // test for conflict
            if scc[i].len() <= 1 && first_visit[i] <= first_visit[0] && last_visit[i] >= last_visit[0] && g.lines[scc[i][0]].flags & LINE_HALF_DELETED == 0 {
                let key=g.lines[scc[i][0]].key;
                //unsafe {println!("key={} contents={}",to_hex(key),std::str::from_utf8_unchecked(self.contents(key))) }
                if key.len()>0 {
                    buf.output_line(&key,self.contents(key));
                }
                if i==0 { break } else { i-=1 }
            } else {
                struct A<'b,'a:'b,'c,B:LineBuffer<'a>> where 'a:'c, B:'b {
                    repo:&'a Repository<'a>,
                    scc:&'c Vec<Vec<usize>>,
                    first_visit:&'c[usize],
                    last_visit:&'c[usize],
                    selected_zombies:&'c mut HashMap<&'a [u8],bool>,
                    forced_zombie:bool,
                    g:&'b Graph<'a>,
                    b:&'b mut B,
                    nodes:&'c mut Vec<&'a[u8]>,
                    i:usize,
                    next:usize,
                    is_first:bool
                }
                fn get_conflict<'b,'a:'b,'c,B:LineBuffer<'a>>(x:&mut A<'b,'a,'c,B>) {

                    if x.scc[x.i].len() <= 1 && x.first_visit[x.i] <= x.first_visit[0] && x.last_visit[x.i] >= x.last_visit[0] {
                        if ! x.is_first {x.b.output_line(&[],b"================================\n");}
                        else{
                            x.is_first=false
                        }
                        for key in x.nodes.iter() {
                            x.b.output_line(key,x.repo.contents(key))
                        }
                        x.next=x.i
                    } else {
                        // Pour chaque permutation de la SCC, ajouter tous les sommets sur la pile, et appel recursif de chaque arete non-forward.
                        /*
                        for cousin in x.scc[i].iter() {
                            let not_zombie=
                                g.lines[*cousin].flags & LINE_HALF_DELETED == 0
                                || forced_zombie;
                            if not_zombie { nodes.push(g.lines[*cousin].key) }
                            let n=g.lines[*cousin].n_children;
                            for i in 0 .. n {
                                let (edge_child,n_child) = g.children[g.lines[*cousin].children + i];
                                if n_child != 0 || edge_child.is_null() {
                                    // This is not a forward edges (forward edges are of the form (!=NULL,0))
                                    let child_component=g.lines[n_child].scc;
                                    let edge_child= if edge_child.is_null() { &ROOT_KEY[0..HASH_SIZE] } else {
                                        unsafe {
                                            std::slice::from_raw_parts(edge_child.offset(1+KEY_SIZE as isize), HASH_SIZE)
                                        }
                                    };
                                    let (forced_zombie,newly_forced)=
                                        match selected_zombies.entry(edge_child) {
                                            Entry::Occupied(v)=>(*v.get(),false),
                                            Entry::Vacant(v)=>{
                                                let child_is_zombie=
                                                    g.lines[n_child].flags & LINE_HALF_DELETED != 0;
                                                v.insert(child_is_zombie);
                                                (child_is_zombie,true)
                                            }
                                        };
                                    get_conflict(repo,scc,first_visit,last_visit,g,
                                                 selected_zombies,
                                                 forced_zombie,
                                                 child_component,
                                                 b,nodes,is_first,next);
                                    if forced_zombie && newly_forced {
                                        // Dans ce cas, aussi essayer avec l'autre.
                                        selected_zombies.insert(edge_child,false);
                                        get_conflict(repo,scc,first_visit,last_visit,g,
                                                     selected_zombies,
                                                     false,
                                                     child_component,
                                                     b,nodes,is_first,next)
                                    }
                                    if newly_forced {
                                        selected_zombies.remove(edge_child);
                                    }
                                }
                            }
                        }*/
                    }
                }
                // TODO: custom conflict outputters (part of the "writer" typeclass?)
                buf.output_line(&[],b">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
                nodes.clear();
                let next={
                    let mut conflict= A {
                        repo:self,
                        scc:&scc,
                        first_visit:&first_visit,
                        last_visit:&last_visit,
                        g:&g,
                        b:buf,
                        next:0,
                        i:i,
                        nodes:&mut nodes,
                        is_first:true,
                        selected_zombies:&mut selected_zombies,
                        forced_zombie:false
                    };
                    get_conflict(&mut conflict);
                    conflict.next
                };
                buf.output_line(&[],b"<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n");
                if i==0 { break } else { i=std::cmp::min(i-1,next) }
            }
        }
    }
    fn remove_redundant_edges(&mut self,forward:&mut Vec<u8>) {
        let mut i=0;
        let cursor=unsafe { self.mdb_txn.unsafe_cursor(self.dbi_nodes).unwrap() };
        while i<forward.len() {
            unsafe {
                let (_,v)=lmdb::cursor_get(cursor,
                                           &forward[(i+1)..(i+1+KEY_SIZE)],
                                           Some(&forward[(i+1+KEY_SIZE+HASH_SIZE)..
                                                         (i+1+KEY_SIZE+HASH_SIZE+1+KEY_SIZE)]),
                                           lmdb::Op::MDB_GET_BOTH_RANGE).unwrap();
                // vérifier que c'est le bon résultat.
                if memcmp(v.as_ptr() as *const c_void,
                          forward.as_ptr().offset((i+1+KEY_SIZE+HASH_SIZE) as isize) as *const c_void,
                          (1+KEY_SIZE) as size_t) == 0 {

                    copy_nonoverlapping(v.as_ptr().offset((1+KEY_SIZE) as isize),
                                        forward.as_mut_ptr().offset((i+1+KEY_SIZE) as isize),
                                        HASH_SIZE);
                }
                lmdb::cursor_del(cursor,0).unwrap();
                self.mdb_txn.del(self.dbi_nodes,
                                 &forward[(i+1+KEY_SIZE+HASH_SIZE+1)..(i+1+KEY_SIZE+HASH_SIZE+1+KEY_SIZE)],
                                 Some(&forward[i..(i+1+KEY_SIZE+HASH_SIZE)])).unwrap();
            }
            i+=(1+HASH_SIZE+KEY_SIZE) + (1+KEY_SIZE)
        }
        unsafe { lmdb::mdb_cursor_close(cursor) };
    }

    /// Gets the external key corresponding to the given key, returning an
    /// owned vector. If the key is just a patch id, it returns the
    /// corresponding external hash.
    fn external_key(&self,key:&[u8])->ExternalKey {
        let mut result= self.external_hash(&key[0..HASH_SIZE]).to_vec();
        if key.len()==KEY_SIZE { result.extend(&key[HASH_SIZE..KEY_SIZE]) };
        result
    }

    fn external_hash(&self,key:&[u8])->&[u8] {
        //println!("internal key:{:?}",&key[0..HASH_SIZE]);
        if key.len()>=HASH_SIZE
            && unsafe {memcmp(key.as_ptr() as *const c_void,ROOT_KEY.as_ptr() as *const c_void,HASH_SIZE as size_t)}==0 {
                //println!("is root key");
                &ROOT_KEY[0..HASH_SIZE]
            } else {
                match self.mdb_txn.get(self.dbi_external,&key[0..HASH_SIZE]) {
                    Ok(Some(pv))=> {
                        pv
                    },
                    Ok(None)=>{
                        println!("internal key:{:?}",key);
                        panic!("external key not found !")
                    },
                    Err(_)=>{
                        println!("internal key:{:?}",key);
                        panic!("LMDB error !")
                    }
                }
            }
    }


    fn internal_hash(&'a self,key:&[u8])->Option<&'a [u8]> {
        debug!("internal_hash: {}, {}",to_hex(key), key.len());
        if key.len()==HASH_SIZE
            && unsafe { memcmp(key.as_ptr() as *const c_void,ROOT_KEY.as_ptr() as *const c_void,HASH_SIZE as size_t) }==0 {
                Some(ROOT_KEY)
            } else {
                self.mdb_txn.get(self.dbi_internal,key).unwrap()
            }
    }
    /// Create a new internal patch id, register it in the "external" and
    /// "internal" bases, and write the result in its second argument
    /// ("result").
    ///
    /// When compiled in debug mode, this function is deterministic
    /// and returns the last registered patch number, plus one (in big
    /// endian binary on HASH_SIZE bytes). Otherwise, it returns a
    /// random patch number not yet registered.
    pub fn new_internal(&mut self,result:&mut[u8]) {

        if cfg!(debug_assertions){
            let curs=self.mdb_txn.cursor(self.dbi_external).unwrap();
            if let Ok((k,_))=curs.get(b"",None,lmdb::Op::MDB_LAST) {
                unsafe { copy_nonoverlapping(k.as_ptr() as *const c_void,result.as_mut_ptr() as *mut c_void, HASH_SIZE) }
            } else {
                for i in 0..HASH_SIZE { result[i]=0 }
            };
            let mut i=HASH_SIZE-1;
            while i>0 && result[i]==0xff {
                result[i]=0;
                i-=1
            }
            if result[i] != 0xff {
                result[i]+=1
            } else {
                panic!("the last patch in the universe has arrived")
            }
        } else {
            for i in 0..result.len() { result[i]=rand::random() }
            loop {
                match self.mdb_txn.get(self.dbi_external,&result) {
                    Ok(None)=>break,
                    Ok(_)=>{for i in 0..result.len() { result[i]=rand::random() }},
                    Err(_)=>panic!("")
                }
            }
        }
    }

    pub fn register_hash(&mut self,internal:&[u8],external:&[u8]){
        self.mdb_txn.put(self.dbi_external,internal,external,0).unwrap();
        self.mdb_txn.put(self.dbi_internal,external,internal,0).unwrap();
    }


    fn delete_edges(&self, cursor:&mut lmdb::MdbCursor,edges:&mut Vec<Edge>, key:&'a[u8],flag:u8){
        if key.len() > 0 {
            let ext_key=self.external_key(key);
            for v in CursIter::new(cursor,key,flag,false) {
                edges.push(Edge { from:ext_key.clone(),
                                  to:self.external_key(&v[1..(1+KEY_SIZE)]),
                                  introduced_by:self.external_key(&v[(1+KEY_SIZE)..]) });
            }
        }
    }

    fn diff(&self,line_num:&mut usize, actions:&mut Vec<Change>, redundant:&mut Vec<u8>,
            a:Graph, b:&Path)->Result<(),std::io::Error> {
        fn memeq(a:&[u8],b:&[u8])->bool {
            if a.len() == b.len() {
                unsafe { memcmp(a.as_ptr() as *const c_void,b.as_ptr() as *const c_void,
                                b.len() as size_t) == 0 }
            } else { false }
        }
        fn local_diff(repo:&Repository,cursor:&mut lmdb::MdbCursor,actions:&mut Vec<Change>,line_num:&mut usize, lines_a:&[&[u8]], contents_a:&[&[u8]], b:&[&[u8]]) {
            debug!("local_diff {} {}",contents_a.len(),b.len());
            let mut opt=vec![vec![0;b.len()+1];contents_a.len()+1];
            if contents_a.len()>0 {
                let mut i=contents_a.len() - 1;
                loop {
                    opt[i]=vec![0;b.len()+1];
                    if b.len()>0 {
                        let mut j=b.len()-1;
                        loop {
                            opt[i][j]=
                                if memeq(contents_a[i],b[j]) {
                                    opt[i+1][j+1]+1
                                } else {
                                    std::cmp::max(opt[i+1][j], opt[i][j+1])
                                };
                            debug!(target:"diff","opt[{}][{}] = {}",i,j,opt[i][j]);
                            if j>0 { j-=1 } else { break }
                        }
                    }
                    if i>0 { i-=1 } else { break }
                }
            }
            let mut i=1;
            let mut j=0;
            fn add_lines(repo:&Repository,actions:&mut Vec<Change>, line_num:&mut usize,
                         up_context:&[u8],down_context:&[&[u8]],lines:&[&[u8]]){
                /*unsafe {
                    println!("u {}",std::str::from_utf8_unchecked(repo.contents(up_context)));
                    for i in lines {
                        println!("+ {}",std::str::from_utf8_unchecked(i));
                    }
                if down_context.len()>0 {
                        println!("d {}",std::str::from_utf8_unchecked(repo.contents(down_context[0])));
                    }
                }*/
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
            fn delete_lines(repo:&Repository,cursor:&mut lmdb::MdbCursor,actions:&mut Vec<Change>, lines:&[&[u8]]){
                let mut edges=Vec::with_capacity(lines.len());
                for i in 0..lines.len() {
                    //unsafe {println!("- {}",std::str::from_utf8_unchecked(repo.contents(lines[i])));}
                    repo.delete_edges(cursor,&mut edges,lines[i],PARENT_EDGE)
                }
                actions.push(Change::Edges{edges:edges,flag:PARENT_EDGE|DELETED_EDGE})
            }
            let mut oi=None;
            let mut oj=None;
            let mut last_alive_context=0;
            while i<contents_a.len() && j<b.len() {
                debug!(target:"diff","i={}, j={}",i,j);
                if memeq(contents_a[i],b[j]) {
                    if let Some(i0)=oi {
                        debug!(target:"diff","deleting from {} to {} / {}",i0,i,lines_a.len());
                        //println!("delete starting from line: \"{}\"",to_hex(lines_a[i0]));
                        //unsafe { println!("contents: \"{}\"",std::str::from_utf8_unchecked(contents_a[i0])); }
                        delete_lines(repo,cursor,actions, &lines_a[i0..i]);
                        oi=None
                    } else if let Some(j0)=oj {
                        /* unsafe {
                            println!("adding with context: {} \"{}\"",last_alive_context,to_hex(lines_a[last_alive_context]));
                            println!("adding with context: +{}",std::str::from_utf8_unchecked(b[j0]));
                        }*/
                        add_lines(repo,actions, line_num,
                                  lines_a[last_alive_context], // up context
                                  &lines_a[i..i+1], // down context
                                  &b[j0..j]);
                        oj=None
                    }
                    last_alive_context=i;
                    i+=1; j+=1;
                } else {
                    //println!("!= {:?} {:?} {:?} {:?}",opt[i+1][j],opt[i][j+1], oi,oj);
                    if opt[i+1][j] >= opt[i][j+1] {
                        // we will delete things starting from i (included).
                        if let Some(j0)=oj {
                            /*unsafe {
                                println!("adding with context: \"{}\"",to_hex(lines_a[last_alive_context]));
                                println!("adding with context: +{}",std::str::from_utf8_unchecked(b[j0]));
                            }*/
                            add_lines(repo,actions, line_num,
                                      lines_a[last_alive_context], // up context
                                      &lines_a[i..i+1], // down context
                                      &b[j0..j]);
                            oj=None
                        }
                        //println!("oi {:?}",oi);
                        if oi.is_none() {
                            oi=Some(i)
                        }
                        i+=1
                    } else {
                        // We will add things starting from j.
                        if let Some(i0)=oi {
                            /*unsafe {
                                println!("deleting: \"{}\"",to_hex(lines_a[i0]));
                            }*/
                            delete_lines(repo,cursor,actions, &lines_a[i0..i]);
                            last_alive_context=i0-1;
                            oi=None
                        }
                        if oj.is_none() {
                            oj=Some(j)
                        }
                        j+=1
                    }
                    //println!("done");
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
                delete_lines(repo,cursor,actions,&lines_a[i..lines_a.len()])
            } else if j < b.len() {
                if let Some(i0)=oi {
                    delete_lines(repo,cursor,actions, &lines_a[i0..i]);
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
                let t0=time::precise_time_s();
                let mut d = Diff { lines_a:Vec::new(), contents_a:Vec::new() };
                self.output_file(&mut d,a,redundant);
                let t1=time::precise_time_s();
                info!("output_file took {}s",t1-t0);
                //println!("output, now calling local_diff");
                let cursor= unsafe {&mut *self.mdb_txn.unsafe_cursor(self.dbi_nodes).unwrap()};
                local_diff(self,cursor,actions, line_num,
                           &d.lines_a,
                           &d.contents_a,
                           &lines_b);
                //println!("/local_diff");
                unsafe {
                    lmdb::mdb_cursor_close(cursor);
                }
                let t2=time::precise_time_s();
                info!("diff took {}s",t2-t1);
                Ok(())
            },
            Err(e)=>Err(e)
        }
    }



    fn record_all(&self,
                  actions:&mut Vec<Change>,
                  line_num:&mut usize,
                  redundant:&mut Vec<u8>,
                  updatables:&mut HashMap<Vec<u8>,Vec<u8>>,
                  parent_inode:Option<&[u8]>,
                  parent_node:Option<&[u8]>,
                  current_inode:&[u8],
                  realpath:&mut std::path::PathBuf,
                  basename:&[u8]) {
        //println!("record dfs {}",to_hex(current_inode));
        if parent_inode.is_some() { realpath.push(str::from_utf8(&basename).unwrap()) }

        //let mut k = MDB_val { mv_data:ptr::null_mut(), mv_size:0 };
        //let mut v = MDB_val { mv_data:ptr::null_mut(), mv_size:0 };
        let mut l2=[0;LINE_SIZE];
        let current_node=
            if parent_inode.is_some() {
                match self.mdb_txn.get(self.dbi_inodes,&current_inode) {
                    Ok(Some(current_node))=>{
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
                            let mut curs_parents=unsafe {
                                &mut *self.mdb_txn.unsafe_cursor(self.dbi_nodes).unwrap()
                            };
                            let mut curs_grandparents=unsafe {
                                &mut *self.mdb_txn.unsafe_cursor(self.dbi_nodes).unwrap()
                            };
                            for parent in CursIter::new(curs_parents,&current_node[3..],FOLDER_EDGE|PARENT_EDGE,true) {
                                for grandparent in CursIter::new(curs_grandparents,&parent[1..(1+KEY_SIZE)],FOLDER_EDGE|PARENT_EDGE,true) {
                                    edges.push(Edge {
                                        from:self.external_key(&parent),
                                        to:self.external_key(&grandparent[1..(1+KEY_SIZE)]),
                                        introduced_by:self.external_key(&grandparent[1+KEY_SIZE..])
                                    });
                                }
                            }
                            unsafe {
                                lmdb::mdb_cursor_close(curs_parents);
                                lmdb::mdb_cursor_close(curs_grandparents);
                            }
                            actions.push(Change::Edges{edges:edges,flag:DELETED_EDGE|FOLDER_EDGE|PARENT_EDGE});

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
                            info!("retrieving");
                            let time0=time::precise_time_s();
                            let ret=self.retrieve(&current_node[3..]);
                            let time1=time::precise_time_s();
                            info!("retrieve took {}s, now calling diff", time1-time0);
                            self.diff(line_num,actions, redundant,ret.unwrap(), realpath.as_path()).unwrap();
                            let time2=time::precise_time_s();
                            info!("total diff took {}s", time2-time1);


                        } else if current_node[0]==2 {
                            // file deleted. delete recursively
                            let mut edges=Vec::new();
                            // Now take all grandparents of l2, delete them.
                            let mut curs_parents=unsafe {
                                &mut *self.mdb_txn.unsafe_cursor(self.dbi_nodes).unwrap()
                            };
                            let mut curs_grandparents=unsafe {
                                &mut *self.mdb_txn.unsafe_cursor(self.dbi_nodes).unwrap()
                            };
                            for parent in CursIter::new(curs_parents,&current_node[3..],FOLDER_EDGE|PARENT_EDGE,true) {
                                edges.push(Edge {
                                    from:self.external_key(&current_node[3..]),
                                    to:self.external_key(&parent[1..(1+KEY_SIZE)]),
                                    introduced_by:self.external_key(&parent[1+KEY_SIZE..])
                                });
                                for grandparent in CursIter::new(curs_grandparents,&parent[1..(1+KEY_SIZE)],FOLDER_EDGE|PARENT_EDGE,true) {
                                    edges.push(Edge {
                                        from:self.external_key(&parent),
                                        to:self.external_key(&grandparent[1..(1+KEY_SIZE)]),
                                        introduced_by:self.external_key(&grandparent[1+KEY_SIZE..])
                                    });
                                }
                            }
                            unsafe {
                                lmdb::mdb_cursor_close(curs_parents);
                                lmdb::mdb_cursor_close(curs_grandparents);
                            }
                            actions.push(Change::Edges{edges:edges,flag:FOLDER_EDGE|PARENT_EDGE|DELETED_EDGE});
                            unimplemented!() // Remove all known vertices from this file, for else "missing context" conflicts will not be detected.
                        } else if current_node[0]==0 {
                            let time0=time::precise_time_s();
                            let ret=self.retrieve(&current_node[3..]);
                            let time1=time::precise_time_s();
                            info!("record: retrieve took {}s, now calling diff", time1-time0);
                            self.diff(line_num,actions, redundant,ret.unwrap(), realpath.as_path()).unwrap();
                            let time2=time::precise_time_s();
                            info!("total diff took {}s", time2-time1);
                        } else {
                            panic!("record: wrong inode tag (in base INODES) {}", current_node[0])
                        };
                        Some(current_node)
                    },
                    Ok(None)=>{
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
                    },
                    Err(_)=>{
                        panic!("lmdb error")
                    }
                }
            } else {
                Some(ROOT_KEY)
            };

        //println!("current_node={:?}",current_node);
        match current_node {
            None => (), // we just added a file
            Some(current_node)=>{

                let cursor=self.mdb_txn.cursor(self.dbi_tree).unwrap();
                let mut op=lmdb::Op::MDB_SET_RANGE;
                while let Ok((k,v))=cursor.get(current_inode,None,op) {
                    if unsafe{memcmp(k.as_ptr() as *const c_void,
                                     current_inode.as_ptr() as *const c_void,
                                     INODE_SIZE as size_t) } != 0 {
                        break
                    } else {
                        self.record_all(actions, line_num,redundant,updatables,
                                        Some(current_inode), // parent_inode
                                        Some(current_node), // parent_node
                                        v,// current_inode
                                        realpath,
                                        &k[INODE_SIZE..]);
                        op=lmdb::Op::MDB_NEXT;
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
        let mut redundant=vec!();
        self.record_all(&mut actions, &mut line_num,&mut redundant,&mut updatables,
            None,None,ROOT_INODE,&mut realpath,
            &[]);
        //println!("record done");
        self.remove_redundant_edges(&mut redundant);
        Ok((actions,updatables))
    }

    fn unsafe_apply(&mut self,changes:&[Change], internal_patch_id:&[u8]){
        let mut pu:[u8;1+KEY_SIZE+HASH_SIZE]=[0;1+KEY_SIZE+HASH_SIZE];
        let mut pv:[u8;1+KEY_SIZE+HASH_SIZE]=[0;1+KEY_SIZE+HASH_SIZE];
        let mut time_newnodes=0f64;
        let mut time_edges=0f64;
        let alive= unsafe { self.mdb_txn.unsafe_cursor(self.dbi_nodes).unwrap() };
        let cursor= unsafe { self.mdb_txn.unsafe_cursor(self.dbi_nodes).unwrap() };
        for ch in changes {
            match *ch {
                Change::Edges{ref flag, ref edges} => {
                    let time0=time::precise_time_s();
                    debug!(target:"libpijul","unsafe_apply:edges");
                    for e in edges {
                        // First remove the deleted version of the edge
                        pu[0]=*flag ^ DELETED_EDGE ^ PARENT_EDGE;
                        pv[0]=*flag ^ DELETED_EDGE;
                        unsafe {
                            let u=self.internal_hash(&e.from[0..(e.from.len()-LINE_SIZE)]).unwrap();
                            copy_nonoverlapping(e.from.as_ptr().offset((e.from.len()-LINE_SIZE) as isize),
                                                pu.as_mut_ptr().offset(1+HASH_SIZE as isize), LINE_SIZE);
                            copy_nonoverlapping(u.as_ptr(),pu.as_mut_ptr().offset(1), HASH_SIZE);

                            let v=self.internal_hash(&e.to[0..(e.to.len()-LINE_SIZE)]).unwrap();
                            copy_nonoverlapping(e.to.as_ptr().offset((e.to.len()-LINE_SIZE) as isize),
                                                pv.as_mut_ptr().offset(1+HASH_SIZE as isize), LINE_SIZE);
                            copy_nonoverlapping(v.as_ptr(),pv.as_mut_ptr().offset(1), HASH_SIZE);

                            //println!("introduced by {:?}",e.introduced_by);
                            let p=self.internal_hash(&e.introduced_by).unwrap();
                            copy_nonoverlapping(p.as_ptr(),
                                                pu.as_mut_ptr().offset(1+KEY_SIZE as isize),
                                                HASH_SIZE);
                            copy_nonoverlapping(p.as_ptr(),
                                                pv.as_mut_ptr().offset(1+KEY_SIZE as isize),
                                                HASH_SIZE)
                        };
                        self.mdb_txn.del(self.dbi_nodes,&pu[1..(1+KEY_SIZE)], Some(&pv)).unwrap();
                        self.mdb_txn.del(self.dbi_nodes,&pv[1..(1+KEY_SIZE)], Some(&pu)).unwrap();
                        // Then add the new edges
                        pu[0]=*flag^PARENT_EDGE;
                        pv[0]=*flag;
                        debug!(target:"libpijul","new edge: {}\n          {}",to_hex(&pu),to_hex(&pv));
                        unsafe {
                            copy_nonoverlapping(internal_patch_id.as_ptr(),pu.as_mut_ptr().offset(1+KEY_SIZE as isize), HASH_SIZE);
                            copy_nonoverlapping(internal_patch_id.as_ptr(),pv.as_mut_ptr().offset(1+KEY_SIZE as isize), HASH_SIZE);
                        }
                        self.mdb_txn.put(self.dbi_nodes,&pu[1..(1+KEY_SIZE)],&pv,lmdb::MDB_NODUPDATA).unwrap();
                        self.mdb_txn.put(self.dbi_nodes,&pv[1..(1+KEY_SIZE)],&pu,lmdb::MDB_NODUPDATA).unwrap();
                    }
                    let time2=time::precise_time_s();
                    time_edges += time2-time0;
                    debug!(target:"libpijul","unsafe_apply:edges.done");
                },
                Change::NewNodes { ref up_context,ref down_context,ref line_num,ref flag,ref nodes } => {
                    assert!(!nodes.is_empty());
                    debug!(target:"libpijul","unsafe_apply: newnodes");
                    let time0=time::precise_time_s();
                    let mut pu:[u8;1+KEY_SIZE+HASH_SIZE]=[0;1+KEY_SIZE+HASH_SIZE];
                    let mut pv:[u8;1+KEY_SIZE+HASH_SIZE]=[0;1+KEY_SIZE+HASH_SIZE];
                    let mut lnum0= *line_num;
                    for i in 0..LINE_SIZE { pv[1+HASH_SIZE+i]=(lnum0 & 0xff) as u8; lnum0>>=8 }
                    unsafe {
                        copy_nonoverlapping(internal_patch_id.as_ptr(),
                                            pu.as_mut_ptr().offset(1+KEY_SIZE as isize),
                                            HASH_SIZE);
                        copy_nonoverlapping(internal_patch_id.as_ptr(),
                                            pv.as_mut_ptr().offset(1+KEY_SIZE as isize),
                                            HASH_SIZE);
                        copy_nonoverlapping(internal_patch_id.as_ptr(),
                                            pv.as_mut_ptr().offset(1),
                                            HASH_SIZE);
                    };
                    for c in up_context {
                        {
                            debug!("newnodes: up_context {:?}",to_hex(&c));

                            let u= if c.len()>LINE_SIZE {
                                self.internal_hash(&c[0..(c.len()-LINE_SIZE)]).unwrap()
                            } else {
                                internal_patch_id
                            };
                            pu[0]= (*flag) ^ PARENT_EDGE;
                            pv[0]= *flag;
                            unsafe {
                                copy_nonoverlapping(u.as_ptr() as *const c_char,
                                                    pu.as_mut_ptr().offset(1) as *mut c_char,
                                                    HASH_SIZE);
                                copy_nonoverlapping(c.as_ptr().offset((c.len()-LINE_SIZE) as isize),
                                                    pu.as_mut_ptr().offset((1+HASH_SIZE) as isize),
                                                    LINE_SIZE);
                            }
                        }
                        self.mdb_txn.put(self.dbi_nodes,&pu[1..(1+KEY_SIZE)],&pv,lmdb::MDB_NODUPDATA).unwrap();
                        self.mdb_txn.put(self.dbi_nodes,&pv[1..(1+KEY_SIZE)],&pu,lmdb::MDB_NODUPDATA).unwrap();
                    }
                    unsafe {
                        copy_nonoverlapping(internal_patch_id.as_ptr() as *const c_char,
                                            pu.as_ptr().offset(1) as *mut c_char,
                                            HASH_SIZE);
                    }
                    debug!("newnodes: inserting");
                    let mut lnum= *line_num + 1;
                    self.mdb_txn.put(self.dbi_contents,&pv[1..(1+KEY_SIZE)], &nodes[0],0).unwrap();
                    for n in &nodes[1..] {
                        let mut lnum0=lnum-1;
                        for i in 0..LINE_SIZE { pu[1+HASH_SIZE+i]=(lnum0 & 0xff) as u8; lnum0 >>= 8 }
                        lnum0=lnum;
                        for i in 0..LINE_SIZE { pv[1+HASH_SIZE+i]=(lnum0 & 0xff) as u8; lnum0 >>= 8 }
                        pu[0]= (*flag)^PARENT_EDGE;
                        pv[0]= *flag;
                        self.mdb_txn.put(self.dbi_nodes,&pu[1..(1+KEY_SIZE)],&pv,lmdb::MDB_NODUPDATA).unwrap();
                        self.mdb_txn.put(self.dbi_nodes,&pv[1..(1+KEY_SIZE)],&pu,lmdb::MDB_NODUPDATA).unwrap();
                        self.mdb_txn.put(self.dbi_contents,&pv[1..(1+KEY_SIZE)],&n,0).unwrap();
                        lnum = lnum+1;
                    }
                    // In this last part, u is that target (downcontext), and v is the last new node.
                    pu[0]= *flag;
                    pv[0]= (*flag) ^ PARENT_EDGE;
                    for c in down_context {
                        {
                            unsafe {
                                let u=if c.len()>LINE_SIZE {
                                    self.internal_hash(&c[0..(c.len()-LINE_SIZE)]).unwrap()
                                } else {
                                    internal_patch_id
                                };
                                copy_nonoverlapping(u.as_ptr(), pu.as_mut_ptr().offset(1), HASH_SIZE);
                                copy_nonoverlapping(c.as_ptr().offset((c.len()-LINE_SIZE) as isize) as *const c_char,
                                                    pu.as_ptr().offset((1+HASH_SIZE) as isize) as *mut c_char,
                                                    LINE_SIZE);
                            }
                        }
                        self.mdb_txn.put(self.dbi_nodes,&pu[1..(1+KEY_SIZE)],&pv,lmdb::MDB_NODUPDATA).unwrap();
                        self.mdb_txn.put(self.dbi_nodes,&pv[1..(1+KEY_SIZE)],&pu,lmdb::MDB_NODUPDATA).unwrap();
                        /*
                        // remove edges between up context and down context.
                        // commented because it could break unrecord.
                        for up in up_context {
                            {
                                unsafe {
                                    let w= if up.len()>LINE_SIZE {
                                        self.internal_hash(&up[0..(up.len()-LINE_SIZE)]).unwrap()
                                    } else {
                                        internal_patch_id
                                    };
                                    copy_nonoverlapping(w.as_ptr() as *const c_char,
                                                        pw.as_mut_ptr().offset(1) as *mut c_char,
                                                        HASH_SIZE);
                                    copy_nonoverlapping(up.as_ptr().offset((c.len()-LINE_SIZE) as isize),
                                                        pw.as_mut_ptr().offset((1+HASH_SIZE) as isize),
                                                        LINE_SIZE);
                                }
                                pw[0]=pu[0]^PARENT_EDGE;
                                info!(target:"libpijul_newnodes","newnodes {} {}",to_hex(&pw[1..(1+KEY_SIZE)]),to_hex(&pu[..]));
                                unsafe {
                                    match lmdb::cursor_get(cursor,&pw[1..(1+KEY_SIZE)],Some(&pu[0..(1+KEY_SIZE)]),lmdb::Op::MDB_GET_BOTH_RANGE) {
                                        Ok((_,b)) if b[0]|PSEUDO_EDGE == pu[0]|PSEUDO_EDGE
                                            && memcmp(b.as_ptr().offset(1) as *const c_void,
                                                      pu.as_ptr().offset(1) as *const c_void,
                                                      KEY_SIZE as size_t) == 0 => {
                                                //info!(target:"libpijul_newnodes","cursor gave {} {}",to_hex(a),to_hex(b));
                                                copy_nonoverlapping(b.as_ptr().offset(1+KEY_SIZE as isize) as *const c_void,
                                                                    pw.as_mut_ptr().offset(1+KEY_SIZE as isize) as *mut c_void,
                                                                    HASH_SIZE);
                                                lmdb::mdb_cursor_del(cursor,0);
                                                self.mdb_txn.del(self.dbi_nodes,&pu[1..(1+KEY_SIZE)],Some(&pw));
                                            },
                                        _ => {}
                                    }
                                }
                            }
                        }
                         */
                    }
                    let time1=time::precise_time_s();
                    time_newnodes += time1-time0;
                    debug!(target:"libpijul","unsafe_apply:newnodes.done");
                }
            }
        }
        unsafe {
            lmdb::mdb_cursor_close(alive);
            lmdb::mdb_cursor_close(cursor)
        };
        info!(target:"libpijul","edges: {} newnodes: {}", time_edges,time_newnodes);
    }

    pub fn has_patch(&self, branch:&[u8], hash:&[u8])->Result<bool,Error>{
        if hash.len()==HASH_SIZE && unsafe {memcmp(hash.as_ptr() as *const c_void,
                                                   ROOT_KEY.as_ptr() as *const c_void,
                                                   hash.len() as size_t)==0 } {
            Ok(true)
        } else {
            match self.internal_hash(hash) {
                Some(internal)=>{
                    let curs=try!(self.mdb_txn.cursor(self.dbi_branches).map_err(Error::IoError));
                    match curs.get(branch,Some(internal),lmdb::Op::MDB_GET_BOTH) {
                        Ok(_)=>Ok(true),
                        Err(_)=>Ok(false)
                    }
                },
                None=>Ok(false),
            }
        }
    }
    // requires pu to be KEY_SIZE, pv to be 1+KEY_SIZE+HASH_SIZE
    fn connected(&mut self,cursor:*mut lmdb::MdbCursor,pu:&[u8],pv:&mut [u8])->bool{
        let pv_0=pv[0];
        pv[0]=0;
        match unsafe { lmdb::cursor_get(cursor,&pu,Some(pv),lmdb::Op::MDB_GET_BOTH_RANGE) } {
            Ok((_,v))=>{
                let x=unsafe {memcmp(pv.as_ptr() as *const c_void,
                                     v.as_ptr() as *const c_void,
                                     (1+KEY_SIZE) as size_t)};
                pv[0]=pv_0;
                x == 0
            },
            _=>{
                pv[0]=PSEUDO_EDGE;
                match unsafe { lmdb::cursor_get(cursor,&pu,Some(pv),lmdb::Op::MDB_GET_BOTH_RANGE) } {
                    Ok((_,v))=>{
                        let x=unsafe {memcmp(pv.as_ptr() as *const c_void,
                                             v.as_ptr() as *const c_void,
                                             (1+KEY_SIZE) as size_t) == 0 };
                        pv[0]=pv_0;
                        x
                    },
                    _=>false
                }
            }
        }
    }
    fn add_pseudo_edge(&mut self,pu:&[u8],pv:&mut [u8]){
        self.mdb_txn.put(self.dbi_nodes,&pu[1..(1+KEY_SIZE)],&pv,lmdb::MDB_NODUPDATA).unwrap();
        self.mdb_txn.put(self.dbi_nodes,&pv[1..(1+KEY_SIZE)],&pu,lmdb::MDB_NODUPDATA).unwrap();
    }

    fn kill_obsolete_pseudo_edges(&mut self,cursor:*mut lmdb::MdbCursor,pv:&[u8]) {
        let mut a:[u8;1+KEY_SIZE+HASH_SIZE]=[0;1+KEY_SIZE+HASH_SIZE];
        let mut b:[u8;KEY_SIZE]=[0;KEY_SIZE];
        unsafe {
            copy_nonoverlapping(pv.as_ptr() as *const c_void,
                                a.as_mut_ptr().offset(1) as *mut c_void,
                                KEY_SIZE);
        }
        for flag in [PSEUDO_EDGE,PARENT_EDGE|PSEUDO_EDGE,
                     FOLDER_EDGE|PSEUDO_EDGE,PARENT_EDGE|PSEUDO_EDGE|FOLDER_EDGE].iter() {
            loop {
                let flag=[*flag];
                match unsafe { lmdb::cursor_get(cursor,&pv,Some(&flag[..]),lmdb::Op::MDB_GET_BOTH_RANGE) } {
                    Ok((_,v))=>{
                        if v[0]==flag[0] {
                            debug!(target:"libpijul","kill_obsolete_pseudo: {}",to_hex(v));
                            unsafe {
                                copy_nonoverlapping((v.as_ptr().offset(1)) as *const c_void,
                                                    b.as_mut_ptr() as *mut c_void,
                                                    KEY_SIZE);
                            }
                            a[0]= v[0] ^ PARENT_EDGE;
                            unsafe { lmdb::mdb_cursor_del(cursor,0) };
                            self.mdb_txn.del(self.dbi_nodes,&b[..],Some(&a[..])).unwrap();
                        } else {
                            debug!(target:"libpijul","not kill_obsolete_pseudo: {}",to_hex(v));
                            break
                        }
                    },
                    Err(_)=>break
                }
            }
        }
    }

    /// Applies a patch to a repository.
    pub fn apply<'b>(mut self, patch:&Patch, internal:&'b [u8], new_patches:&HashSet<&[u8]>)->Result<Repository<'a>,Error> {
        let current=self.get_current_branch().to_vec();
        {
            let curs=self.mdb_txn.cursor(self.dbi_branches).unwrap();
            match curs.get(&current,Some(internal),lmdb::Op::MDB_GET_BOTH) {
                Ok(_)=>return Err(Error::AlreadyApplied),
                Err(_)=>{}
            }
        }
        self.mdb_txn.put(self.dbi_branches,&current,&internal,lmdb::MDB_NODUPDATA).unwrap();
        let time0=time::precise_time_s();
        self.unsafe_apply(&patch.changes,internal);
        let time1=time::precise_time_s();
        info!(target:"libpijul_apply","unsafe_apply took: {}", time1-time0);
        //let mut children=Vec::new();
        let zero:[u8;HASH_SIZE]=[0;HASH_SIZE];
        let mut max_parents=0;
        let mut max_children=0;
        let mut max_parents_single=0;
        let mut max_children_single=0;
        let cursor= unsafe {&mut *self.mdb_txn.unsafe_cursor(self.dbi_nodes).unwrap() };
        let cursor_= unsafe {&mut *self.mdb_txn.unsafe_cursor(self.dbi_nodes).unwrap() };

        for ch in patch.changes.iter() {
            match *ch {
                Change::Edges{ref edges,ref flag} if flag & DELETED_EDGE!=0=>{
                    /*if (*flag)&FOLDER_EDGE!=0 {
                    self.connect_down_folders(pu,pv,&internal)
                } else {*/
                    let mut u:[u8;KEY_SIZE]=[0;KEY_SIZE];
                    let mut v:[u8;KEY_SIZE]=[0;KEY_SIZE];
                    let mut alive_children=HashSet::new();
                    let mut alive_parents=HashSet::new();
                    let mut parents:Vec<u8>=Vec::with_capacity(edges.len()*2);
                    for e in edges {
                        //info!(target:"libpijul_apply","edge={:?}", e);
                        unsafe {
                            let hu=self.internal_hash(&e.from[0..(e.from.len()-LINE_SIZE)]).unwrap();
                            let hv=self.internal_hash(&e.to[0..(e.to.len()-LINE_SIZE)]).unwrap();
                            copy_nonoverlapping(hu.as_ptr(),u.as_mut_ptr(),HASH_SIZE);
                            copy_nonoverlapping(hv.as_ptr(),v.as_mut_ptr(),HASH_SIZE);
                            copy_nonoverlapping(e.from.as_ptr().offset((e.from.len()-LINE_SIZE) as isize),
                                                u.as_mut_ptr().offset(HASH_SIZE as isize),LINE_SIZE);
                            copy_nonoverlapping(e.to.as_ptr().offset((e.to.len()-LINE_SIZE) as isize),
                                                v.as_mut_ptr().offset(HASH_SIZE as isize),LINE_SIZE);
                        }
                        let (pu,pv)= if (*flag)&PARENT_EDGE!=0 { (&v,&u) } else { (&u,&v) };
                        debug!(target:"libpijul_deleting","{} {}", to_hex(pu),to_hex(pv));
                        let mut parents_count=0;
                        if is_alive(cursor_,pu) {
                            //alive_parents.insert(pu);
                            parents.push(PSEUDO_EDGE|PARENT_EDGE);
                            parents.extend(pu);
                            parents.extend(&zero);
                            parents_count+=1;
                        }
                        for parent in CursIter::new(cursor,pv,PARENT_EDGE,true) {
                            if is_alive(cursor_,&parent[1..(1+KEY_SIZE)]) {
                                alive_parents.insert(&parent[1..(1+KEY_SIZE)]);
                                parents_count+=1;
                            }
                        }
                        max_parents_single=std::cmp::max(max_parents_single,parents_count);
                        let mut children_count=0;
                        // pv is being deleted. Look at its alive children.
                        for child in CursIter::new(cursor,pv,0,true) {
                            if is_alive(cursor_,&child[1..(1+KEY_SIZE)]) {
                                debug!(target:"libpijul_deleting","child alive: {}", to_hex(&child[1..(1+KEY_SIZE)]));
                                alive_children.insert(&child[1..(1+KEY_SIZE)]);
                                children_count+=1
                            }
                        }
                        max_children_single=std::cmp::max(max_children_single,children_count);
                    }
                    let mut children:Vec<u8>=Vec::with_capacity(KEY_SIZE*alive_children.len());
                    for child in alive_children {
                        children.push(PSEUDO_EDGE);
                        children.extend(child);
                        children.extend(&zero);
                    }
                    for parent in alive_parents {
                        parents.push(PSEUDO_EDGE|PARENT_EDGE);
                        parents.extend(parent);
                        parents.extend(&zero);
                    }
                    let mut i=0;
                    while i<children.len() {
                        let mut j=0;
                        while j<parents.len() {
                            if !self.connected(cursor,
                                               &parents[j+1 .. j+1+KEY_SIZE],
                                               &mut children[i .. i+1+KEY_SIZE+HASH_SIZE]) {
                                self.add_pseudo_edge(&parents[j..(j+1+KEY_SIZE+HASH_SIZE)],
                                                     &mut children[i..(i+1+KEY_SIZE+HASH_SIZE)]);
                            }
                            j+=1+KEY_SIZE+HASH_SIZE;
                        }
                        i+=1+KEY_SIZE+HASH_SIZE;
                    }
                    for e in edges {
                        let to= if (*flag)&PARENT_EDGE!=0 { &e.from } else { &e.to };
                        unsafe {
                            let hv=self.internal_hash(&to[0..(e.to.len()-LINE_SIZE)]).unwrap();
                            copy_nonoverlapping(hv.as_ptr(),v.as_mut_ptr(),HASH_SIZE);
                            copy_nonoverlapping(to.as_ptr().offset((to.len()-LINE_SIZE) as isize),
                                                v.as_mut_ptr().offset(HASH_SIZE as isize),LINE_SIZE);
                        }
                        self.kill_obsolete_pseudo_edges(cursor,&v);
                    }
                    max_parents=std::cmp::max(max_parents,parents.len()/(1+KEY_SIZE+HASH_SIZE));
                    max_children=std::cmp::max(max_children,children.len()/(1+KEY_SIZE+HASH_SIZE));
                },
                Change::Edges{ref edges,ref flag}=>{
                    unimplemented!()
                },
                Change::NewNodes { ref up_context,ref down_context,ref line_num, ref flag, ref nodes } => {
                    debug!(target:"libpijul","apply: newnodes");
                    let mut pu:[u8;1+KEY_SIZE+HASH_SIZE]=[0;1+KEY_SIZE+HASH_SIZE];
                    let mut pv:[u8;1+KEY_SIZE+HASH_SIZE]=[0;1+KEY_SIZE+HASH_SIZE];
                    let mut context:[u8;KEY_SIZE]=[0;KEY_SIZE];
                    let mut lnum0= *line_num;
                    for i in 0..LINE_SIZE { pv[1+HASH_SIZE+i]=(lnum0 & 0xff) as u8; lnum0>>=8 }
                    let _= unsafe {
                        copy_nonoverlapping(internal.as_ptr(),
                                            pu.as_mut_ptr().offset(1),
                                            HASH_SIZE);
                        copy_nonoverlapping(internal.as_ptr(),
                                            pv.as_mut_ptr().offset(1),
                                            HASH_SIZE);
                    };
                    lnum0= (*line_num);
                    unsafe { copy_nonoverlapping(internal.as_ptr(), pu.as_mut_ptr().offset(1), HASH_SIZE); }
                    for i in 0..LINE_SIZE { pu[1+HASH_SIZE+i]=(lnum0 & 0xff) as u8; lnum0>>=8 }

                    for c in up_context {
                        unsafe {
                            let u= if c.len()>LINE_SIZE {
                                self.internal_hash(&c[0..(c.len()-LINE_SIZE)]).unwrap()
                            } else {
                                internal as &[u8]
                            };
                            copy_nonoverlapping(u.as_ptr(), context.as_mut_ptr(), HASH_SIZE);
                            copy_nonoverlapping(c.as_ptr().offset((c.len()-LINE_SIZE) as isize),
                                                context.as_mut_ptr().offset(HASH_SIZE as isize),
                                                LINE_SIZE);
                        }
                        if ! has_edge(cursor,&context[..],PARENT_EDGE,true,true) {
                            let mut relatives=Vec::new();
                            self.find_alive_relatives(&context[..],DELETED_EDGE|PARENT_EDGE,
                                                      internal,new_patches,&mut relatives);
                            println!("up relatives:{}",to_hex(&relatives));
                            let mut i=0;
                            while i<relatives.len() {
                                pu[i]= (*flag | PSEUDO_EDGE);
                                relatives[0]= (*flag | PSEUDO_EDGE)^PARENT_EDGE;
                                self.mdb_txn.put(self.dbi_nodes,
                                                 &relatives[(i+1)..(i+1+KEY_SIZE)],
                                                 &pu,
                                                 0);
                                self.mdb_txn.put(self.dbi_nodes,
                                                 &pu[1..(1+KEY_SIZE)],
                                                 &relatives[i..(i+1+KEY_SIZE+HASH_SIZE)],
                                                 0);
                                i+=1+KEY_SIZE+HASH_SIZE
                            }
                        }
                    }
                    lnum0= (*line_num)+nodes.len()-1;
                    unsafe { copy_nonoverlapping(internal.as_ptr(), pu.as_mut_ptr(), HASH_SIZE); }
                    for i in 0..LINE_SIZE { pu[HASH_SIZE+i]=(lnum0 & 0xff) as u8; lnum0>>=8 }
                    for c in down_context {
                        unsafe {
                            let u= if c.len()>LINE_SIZE {
                                self.internal_hash(&c[0..(c.len()-LINE_SIZE)]).unwrap()
                            } else {
                                internal as &[u8]
                            };
                            copy_nonoverlapping(u.as_ptr(), pv.as_mut_ptr().offset(1), HASH_SIZE);
                            copy_nonoverlapping(c.as_ptr().offset((c.len()-LINE_SIZE) as isize),
                                                pv.as_mut_ptr().offset(1+HASH_SIZE as isize),
                                                LINE_SIZE);
                        }
                        debug!(target:"missing","{}",to_hex(&pv[..]));
                        if ! has_edge(cursor,&pv[..],0,true,true) {
                            debug!(target:"missing","no_edge !");
                            let mut relatives=Vec::new();
                            self.find_alive_relatives(&pv[1..(1+KEY_SIZE)],DELETED_EDGE,
                                                      internal,new_patches,&mut relatives);
                            println!("down relatives:{}",to_hex(&relatives));
                            let mut i=0;
                            while i<relatives.len() {
                                // TODO: For each child of relative [(i+1)..(i+1+KEY_SIZE)], add an edge.
                                pu[i]= (*flag | PSEUDO_EDGE);
                                relatives[0]= (*flag | PSEUDO_EDGE)^PARENT_EDGE;
                                self.mdb_txn.put(self.dbi_nodes,
                                                 &relatives[(i+1)..(i+1+KEY_SIZE)],
                                                 &pu,
                                                 0);
                                self.mdb_txn.put(self.dbi_nodes,
                                                 &pu[1..(1+KEY_SIZE)],
                                                 &relatives[i..(i+1+KEY_SIZE+HASH_SIZE)],
                                                 0);
                                i+=1+KEY_SIZE+HASH_SIZE
                            }
                        }
                    }
                    debug!(target:"libpijul","apply: newnodes, done");
                }
            }
        }
        unsafe {
            lmdb::mdb_cursor_close(cursor);
            lmdb::mdb_cursor_close(cursor_);
        }
        let time2=time::precise_time_s();
        info!(target:"libpijul_apply","apply took: {}, max_parents:{} (single {}), max_children:{} (single {})", time2-time1,max_parents,max_parents_single,max_children,max_children_single);
        for ref dep in patch.dependencies.iter() {
            let dep_internal=self.internal_hash(&dep).unwrap().to_vec();
            self.mdb_txn.put(self.dbi_revdep,&dep_internal,internal,0).unwrap();
        }
        let time3=time::precise_time_s();
        info!(target:"libpijul","deps took: {}", time3-time2);
        Ok(self)
    }


    fn find_alive_relatives(&self, a:&[u8], direction:u8, patch_id:&[u8], new_patches:&HashSet<&[u8]>,
                            relatives:&mut Vec<u8>) {
        let cursor= unsafe { &mut * self.mdb_txn.unsafe_cursor(self.dbi_nodes).unwrap() };
        fn connect(repo:&Repository,
                   cursor:&mut lmdb::MdbCursor,
                   a:&[u8],
                   direction:u8,
                   result:&mut Vec<u8>,
                   buffer:&mut Vec<u8>,
                   patch_id:&[u8],
                   new_patches:&HashSet<&[u8]>) {
            let mut i=buffer.len();
            let i0=buffer.len();
            for neighbor in CursIter::new(cursor,a,direction,false) {
                let ext=repo.external_hash(&neighbor[(1+KEY_SIZE)..]);
                if new_patches.contains(ext) {
                    buffer.extend(&neighbor[1..(1+KEY_SIZE)]);
                }
            }
            let j=buffer.len();
            if j==i0 && unsafe { memcmp(ROOT_KEY.as_ptr()as *const c_void,a.as_ptr() as *const c_void,KEY_SIZE as size_t)!=0} {
                debug_assert!(a.len()==KEY_SIZE);
                debug_assert!(patch_id.len()==HASH_SIZE);
                result.push(direction|PSEUDO_EDGE);
                result.extend(a);
                result.extend(patch_id);
            } else {
                let mut copy=[0;KEY_SIZE];
                while i < j {
                    unsafe {
                        copy_nonoverlapping(buffer.as_ptr().offset(i as isize),
                                            copy.as_mut_ptr(),
                                            KEY_SIZE);
                    }
                    connect(repo,cursor, &copy[..],direction,result,buffer,patch_id,new_patches);
                    i+= KEY_SIZE
                }
            }
            buffer.truncate(i0)
        }
        let mut buf=Vec::with_capacity(4*KEY_SIZE);
        connect(self,cursor,a,direction,relatives,&mut buf,patch_id,new_patches);
        unsafe { lmdb::mdb_cursor_close(cursor); }
    }



    pub fn write_changes_file(&self,changes_file:&Path)->Result<(),Error> {
        let mut patches=HashSet::new();
        let branch=self.get_current_branch();
        let curs=self.mdb_txn.cursor(self.dbi_branches).unwrap();
        let mut op=lmdb::Op::MDB_SET;
        while let Ok((_,v))=curs.get(&branch,None,op) {
            patches.insert(self.external_hash(v));
            op=lmdb::Op::MDB_NEXT_DUP
        }
        try!(patch::write_changes(&patches,changes_file));
        Ok(())
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
                        node[1]=(nodes[0][0] & 0xff) as u8;
                        node[2]=(nodes[0][1] & 0xff) as u8;
                        self.mdb_txn.put(self.dbi_inodes,&inode_l2,&node,0).unwrap();
                        self.mdb_txn.put(self.dbi_revinodes,&node[3..],&inode_l2,0).unwrap();
                    }
                },
                Change::Edges{..} => {}
            }
        }
    }


    // Climp up the tree (using revtree).
    fn filename_of_inode(&self,inode:&[u8],working_copy:&mut PathBuf)->bool {
        //let mut v_inode=MDB_val{mv_data:inode.as_ptr() as *const c_void, mv_size:inode.len() as size_t};
        //let mut v_next:MDB_val = unsafe {std::mem::zeroed()};
        let mut components=Vec::new();
        let mut current=inode;
        loop {
            match self.mdb_txn.get(self.dbi_revtree,current) {
                Ok(Some(v))=>{
                    components.push(&v[INODE_SIZE..]);
                    current=&v[0..INODE_SIZE];
                    if unsafe { memcmp(current.as_ptr() as *const c_void,
                                       ROOT_INODE.as_ptr() as *const c_void,
                                       INODE_SIZE as size_t) } == 0 {
                        break
                    }
                },
                Ok(None)=> return false,
                Err(_)=>panic!("filename_of_inode")
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
                               nfiles:&mut usize)-> Result<(),Error> {
            if !cache.contains(key) {
                cache.insert(key);
                let mut curs_b= unsafe { &mut *repo.mdb_txn.unsafe_cursor(repo.dbi_nodes).unwrap()};
                for b in CursIter::new(curs_b,key,FOLDER_EDGE,true) {
                    let cont_b=
                        match try!(repo.mdb_txn.get(repo.dbi_contents,&b[1..(1+KEY_SIZE)])) {
                            Some(cont_b)=>cont_b,
                            None=>&[][..]
                        };
                    if cont_b.len()<2 { panic!("node (b) too short") } else {
                        let filename=&cont_b[2..];
                        let perms= (((cont_b[0] as usize) << 8) | (cont_b[1] as usize)) & 0x1ff;
                        let mut curs_c= unsafe { &mut *repo.mdb_txn.unsafe_cursor(repo.dbi_nodes).unwrap()};
                        for c in CursIter::new(curs_c,&b[1..(1+KEY_SIZE)],FOLDER_EDGE,true) {
                            let cv=&c[1..(1+KEY_SIZE)];
                            match try!(repo.mdb_txn.get(repo.dbi_revinodes,cv)) {
                                Some(inode)=>{
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
                                        try!(retrieve_paths(repo,working_copy,&c[1..(1+KEY_SIZE)],path,inode,paths,cache,nfiles));
                                    }
                                    path.pop();
                                },
                                None =>{
                                    panic!("inodes not synchronized")
                                }
                            }
                        }
                        unsafe { lmdb::mdb_cursor_close(curs_c) }
                    }
                }
                unsafe { lmdb::mdb_cursor_close(curs_b) }
            }
            Ok(())
        }
        let mut paths=HashMap::new();
        let mut nfiles=0;
        {
            let mut cache=HashSet::new();
            let mut buf=PathBuf::from(working_copy);
            try!(retrieve_paths(self,working_copy,&ROOT_KEY,&mut buf,ROOT_INODE,&mut paths,&mut cache,&mut nfiles));
        }
        //println!("dropping tree");
        {
            try!(self.mdb_txn.drop(self.dbi_tree,false));
            try!(self.mdb_txn.drop(self.dbi_revtree,false));
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
                    let mut redundant_edges=vec!();
                    {
                        let time0=time::precise_time_s();
                        let l=self.retrieve(&node).unwrap();
                        let time1=time::precise_time_s();
                        info!("unsafe_output_repository: retrieve took {}s", time1-time0);
                        let mut f=std::fs::File::create(&kk).unwrap();
                        self.output_file(&mut f,l,&mut redundant_edges);
                        let time2=time::precise_time_s();
                        info!("unsafe_output_repository: output_file took {}s", time2-time1);
                    }
                    self.remove_redundant_edges(&mut redundant_edges);
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
            self.mdb_txn.put(self.dbi_tree,&par,&inode,0).unwrap();
            self.mdb_txn.put(self.dbi_revtree,&inode,&par,0).unwrap();
        }
    }


    pub fn output_repository(mut self, working_copy:&Path, pending:&Patch) -> Result<Repository<'a>,Error>{
        unsafe {
            let mut internal=[0;HASH_SIZE];
            let parent_txn=self.mdb_txn.txn;
            let txn=ptr::null_mut();

            let e=lmdb::mdb_txn_begin(self.mdb_env.env,self.mdb_txn.txn,0,std::mem::transmute(&txn));
            if e!=0 {
                return Err(Error::IoError(std::io::Error::from_raw_os_error(e)))
            }
            self.mdb_txn.txn=txn;
            self.new_internal(&mut internal[..]);

            let mut repository=self.apply(pending,&internal[..],&HashSet::new()).unwrap();
            let updates=try!(repository.unsafe_output_repository(working_copy));
            lmdb::mdb_txn_abort(txn);
            repository.mdb_txn.txn=parent_txn;
            repository.update_tree(updates);
            Ok(repository)
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
        let curs=self.mdb_txn.cursor(self.dbi_nodes).unwrap();
        let mut op=lmdb::Op::MDB_FIRST;
        let mut cur=&[][..];
        while let Ok((k,v))=curs.get(cur,None,op) {
            op=lmdb::Op::MDB_NEXT;
            if k!=cur {
                let f=self.mdb_txn.get(self.dbi_contents, k);
                let cont:&[u8]=
                    match f {
                        Ok(Some(ww))=>ww,
                        _=>&[]
                    };
                write!(w,"n_{}[label=\"{}: {}\"];\n", to_hex(&k), to_hex(&k),
                       match str::from_utf8(&cont) { Ok(x)=>x.to_string(), Err(_)=> to_hex(&cont) }
                       ).unwrap();
                cur=k;
            }
            let flag=v[0];
            if flag & PARENT_EDGE == 0 {
                write!(w,"n_{}->n_{}[{},label=\"{}\"];\n", to_hex(&k), to_hex(&v[1..(1+KEY_SIZE)]), styles[(flag&0xff) as usize], flag).unwrap();
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
