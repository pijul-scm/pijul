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

use super::backend::*;
use super::patch::*;
use super::error::*;
use super::file_operations::*;
use super::graph::*;
use super::diff;

use std::collections::HashMap;
use std::path::{PathBuf};
use std::fs::metadata;
use std;
use std::io::BufRead;
use rustc_serialize::hex::ToHex;

#[cfg(not(windows))]
use std::os::unix::fs::PermissionsExt;


#[cfg(not(windows))]
fn permissions(attr:&std::fs::Metadata)->Option<usize> {
    Some(attr.permissions().mode() as usize)
}
#[cfg(windows)]
fn permissions(attr:&std::fs::Metadata)->Option<usize> {
    None
}


macro_rules! iterate_parents {
    ($branch:expr, $key:expr) => {
        $branch.iter($key, Some(&[FOLDER_EDGE|PARENT_EDGE][..]))
            .take_while(|&(k,parent)| {
                debug!("take_while: {:?} {:?}", k.to_hex(), parent.to_hex());
                k == $key && parent[0] >= FOLDER_EDGE|PARENT_EDGE && parent[0] <= FOLDER_EDGE|PARENT_EDGE|PSEUDO_EDGE
            })
            .map(|(_,b)| b)
    }
}

struct RecordState {
    line_num: usize,
    updatables: HashMap<Vec<u8>, Inode >,
    actions: Vec<Change>,
    redundant: Vec<u8>
}

fn record_file_addition<T>(st : &mut RecordState, current_inode: Inode, parent_node: &[u8], db_external: &Db<T>, realpath: &mut std::path::PathBuf, basename: &[u8]) -> Option< [u8; LINE_SIZE]> {
    let mut l2=[0;LINE_SIZE];
    // File addition, create appropriate Newnodes.
    debug!("metadata for {:?}", realpath);
    match metadata(&realpath) {
        Ok(attr) => {
            let int_attr={
                let p=permissions(&attr).unwrap_or(0o755);
                let is_dir= if attr.is_dir() { DIRECTORY_FLAG } else { 0 };
                p | is_dir
            };
            let mut nodes=Vec::new();
            unsafe {
                *(l2.as_mut_ptr() as *mut u32) = ((st.line_num+1) as u32).to_le()
            };

            let mut name=Vec::with_capacity(basename.len()+2);
            name.push(((int_attr >> 8) & 0xff) as u8);
            name.push((int_attr & 0xff) as u8);
            name.extend(basename);
            {
                let mut l2_=Vec::with_capacity(LINE_SIZE+2);
                l2_.extend(&name[0..2]);
                l2_.extend(&l2);
                st.updatables.insert(l2_,current_inode);
            }
            st.actions.push(
                Change::NewNodes { up_context: vec!(
                    if parent_node.len()>LINE_SIZE {
                        external_key(&db_external, parent_node)
                    } else {parent_node.to_vec()}
                ),
                                   line_num: st.line_num as u32,
                                   down_context: vec!(),
                                   nodes: vec!(name,vec!()),
                                   flag:FOLDER_EDGE }
            );
            st.line_num += 2;
            st.updatables.insert(l2.to_vec(),current_inode);
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
                let len=nodes.len();
                if !nodes.is_empty() {
                    st.actions.push(
                        Change::NewNodes { up_context:vec!(l2.to_vec()),
                                           line_num: st.line_num as u32,
                                           down_context: vec!(),
                                           nodes: nodes,
                                           flag:0 }
                    );
                }
                st.line_num+=len;
                None
            } else {
                Some(l2)
            }
        },
        Err(_)=>{
            panic!("error adding file {:?} (metadata failed)",realpath);
        }
    }
}

fn record_moved_file<'c, 'b, 'a, T>(branch:&Branch<'c,'b,'a,T>, repository: &Transaction<'a, T>,
                                    realpath: &mut std::path::PathBuf, db_contents: &Db<T>,
                                    db_external: &Db<T>, st: &mut RecordState,
                                    parent_node: &[u8], current_node: &[u8], basename: &[u8], int_attr: usize,
                                    old_attr: usize) -> Result <(), Error>{
    // Delete all former names.
    let mut edges=Vec::new();
    // Now take all grandparents of l2, delete them.

    let mut name=Vec::with_capacity(basename.len()+2);
    name.push(((int_attr >> 8) & 0xff) as u8);
    name.push((int_attr & 0xff) as u8);
    name.extend(basename);
    for parent in iterate_parents!(branch, &current_node[3..]) {
        debug!("iterate_parents: {:?}", parent.to_hex());
        let mut contents_name: Contents<T> = Contents::from_slice(&name[..]);
        let mut previous_name: Contents<T> =
            match db_contents.contents(&parent[1..(1+KEY_SIZE)]) {
                None=>Contents::from_slice(b""),
                Some(n)=>n
            };
        let name_changed = !super::eq(&mut contents_name,
                                      &mut previous_name);
        for grandparent in iterate_parents!(branch, &parent[1..(1+KEY_SIZE)]) {
            debug!("iterate_parents: grandparent = {:?}", grandparent.to_hex());
            if &grandparent[1..(1+KEY_SIZE)] != parent_node
                || name_changed {
                    edges.push(Edge {
                        from:external_key(db_external, &parent[1..(1+KEY_SIZE)]),
                        to:external_key(db_external, &grandparent[1..(1+KEY_SIZE)]),
                        introduced_by:external_key(db_external, &grandparent[1+KEY_SIZE..])
                    })
                }
        }
    }
    debug!("edges:{:?}",edges);
    if !edges.is_empty(){
        st.actions.push(Change::Edges{edges:edges,flag:DELETED_EDGE|FOLDER_EDGE|PARENT_EDGE});
        debug!("parent_node: {:?}",parent_node);
        debug!("ext key: {:?}",external_key(&db_external, parent_node));
        debug!("ext key: {:?}",external_key(&db_external, &current_node[3..]));
        st.actions.push(
            Change::NewNodes { up_context:{
                vec!(if parent_node.len()>LINE_SIZE { external_key(&db_external, parent_node) }
                     else { parent_node.to_vec() })
            },
                               line_num: st.line_num as u32,
                               down_context:{
                                   let p = &current_node[3..];
                                   vec!(if parent_node.len()>LINE_SIZE { external_key(&db_external, &p) }
                                        else { parent_node.to_vec() })
                               },
                               nodes: vec!(name),
                               flag:FOLDER_EDGE }
            );
    }
    st.line_num += 1;
    debug!("directory_flag:{}",old_attr&DIRECTORY_FLAG);
    if old_attr & DIRECTORY_FLAG == 0 {
        info!("retrieving");
        //let time0=time::precise_time_s();
        let ret = retrieve(branch, &current_node[3..]);
        //let time1=time::precise_time_s();
        //info!("retrieve took {}s, now calling diff", time1-time0);
        debug!("diff");
        try!(diff::diff(repository, branch, &mut st.line_num,&mut st.actions, &mut st.redundant,ret, realpath.as_path()));
        //let time2=time::precise_time_s();
        //info!("total diff took {}s", time2-time1);
    };
    Ok(())
}

fn record_deleted_file<T>(st: &mut RecordState, branch: &Branch<T>, current_node: &[u8], db_external: &Db<T>)
                          -> Result<(), Error> {
    debug!("record_deleted_file");
    let mut edges=Vec::new();
    // Now take all grandparents of l2, delete them.
    for parent in iterate_parents!(branch, &current_node[3..]) {
        for grandparent in iterate_parents!(branch, &parent[1..(1+KEY_SIZE)]) {
            edges.push(Edge {
                from: external_key(&db_external, &parent[1..(1+KEY_SIZE)]),
                to: external_key(&db_external, &grandparent[1..(1+KEY_SIZE)]),
                introduced_by: external_key(&db_external, &grandparent[1+KEY_SIZE..])
            })
        }
    }

    // Delete the file recursively
    let mut file_edges=vec!();
    {
        debug!("del={}",current_node.to_hex());
        let ret = retrieve(branch, &current_node[3..]);
        for l in ret.lines {
            if l.key.len()>0 {
                let ext_key = external_key(&db_external, l.key);
                debug!("ext_key={}",ext_key.to_hex());
                for v in iterate_parents!(branch, l.key) {
                    
                    debug!("v={}",v.to_hex());
                    if v[0] & FOLDER_EDGE != 0 { &mut edges } else { &mut file_edges }
                    .push(Edge { from: ext_key.clone(),
                                 to: external_key(&db_external, &v[1..(1+KEY_SIZE)]),
                                 introduced_by: external_key(&db_external, &v[(1+KEY_SIZE)..]) });
                }
            }
        }
    }

    st.actions.push(Change::Edges{edges:edges,flag:FOLDER_EDGE|PARENT_EDGE|DELETED_EDGE});
    if file_edges.len()>0 {
        st.actions.push(Change::Edges{edges:file_edges,flag:PARENT_EDGE|DELETED_EDGE});
    };
    Ok(())
}

fn record_all<'a,'b,'c,T> (
    repository:&Transaction<'a,T>,
    branch:&Branch<'c,'b,'a,T>,
    st: &mut RecordState,
    parent: Option< (&[u8], Inode) >,
    current_inode: Inode,
    realpath:&mut std::path::PathBuf,
    basename:&[u8]
        )->Result<(),Error> {
    let db_inodes = & repository.db_inodes();
    if parent.is_some() { realpath.push(std::str::from_utf8(&basename).unwrap()) }
    debug!("realpath:{:?}",realpath);
    //debug!(target:"record_all","inode:{:?}",current_inode.to_hex());

    let mut l2=[0;LINE_SIZE];
    let db_external = repository.db_external();
    let db_contents = repository.db_contents();
    let current_node=
        if let Some((parent_node, _parent_inode)) = parent {
            match db_inodes.get(current_inode.as_ref()) {
                Some(current_node)=>{
                    let old_attr=((current_node[1] as usize) << 8) | (current_node[2] as usize);
                    // Add the new name.
                    let (int_attr,deleted)={
                        match metadata(&realpath) {
                            Ok(attr)=>{
                                let p=(permissions(&attr).unwrap_or(old_attr)) & 0o777;
                                let is_dir= if attr.is_dir() { DIRECTORY_FLAG } else { 0 };
                                ((p | is_dir),false)
                            },
                            Err(_)=>{
                                (old_attr,true)
                            }
                        }
                    };
                    debug!("current_node={:?}", current_node.to_hex());
                    debug!("current_node[0]={},old_attr={},int_attr={}",
                           current_node[0],old_attr,int_attr);
                    if !deleted && (current_node[0]==1 || old_attr!=int_attr) {
                        try!(record_moved_file(branch, repository, realpath, &db_contents,
                                               &db_external, st, parent_node, current_node, basename, int_attr,
                                               old_attr));
                    } else if deleted || current_node[0]==2 {
                        try!(record_deleted_file(st, branch, current_node, &db_external));
                    } else if current_node[0]==0 {
                        if old_attr & DIRECTORY_FLAG == 0 {
                            //let time0=time::precise_time_s();
                            let ret = retrieve(branch, &current_node[3..]);
                            //let time1=time::precise_time_s();
                            info!("now calling diff");
                            try!(diff::diff(repository, branch, &mut st.line_num, &mut st.actions, &mut st.redundant,
                                            ret, realpath.as_path()));
                            //let time2=time::precise_time_s();
                            //info!(target:"record_all","total diff took {}s", time2-time1);
                        }
                    } else {
                        panic!("record: wrong inode tag (in base INODES) {}", current_node[0])
                    };
                    Some(&current_node[3..])
                },
                None=>{
                    record_file_addition(st, current_inode, parent_node, &db_external, realpath, basename)
                        .map(|ref p| {l2 = p.clone(); &l2[..]})
                }
            }
        } else {
            Some(ROOT_KEY)
        };
    debug!("current_node={:?}",current_node);
    match current_node {
        None => (), // we just added a file
        Some(current_node)=>{
            debug!("children of current_inode {}",current_inode.to_hex());
            let db_tree = repository.db_tree();

            for (k,v) in db_tree.iter(current_inode.as_ref(), None) {

                if &k[0..INODE_SIZE] == current_inode.as_ref() {

                    if v.len()>0 {
                        debug!("  child: {} + {}",&v[0..INODE_SIZE].to_hex(), std::str::from_utf8(&k[INODE_SIZE..]).unwrap());
                        try!(record_all(
                            repository, branch,
                            st,
                            Some((current_node, current_inode)), // parent
                            Inode::from_slice(v),// current_inode
                            realpath,
                            &k[INODE_SIZE..]));
                    }
                } else {
                    break
                }
            }
        }
    }
    if parent.is_some() { let _=realpath.pop(); }
    Ok(())
}

pub fn record<T>(repository:&Transaction<T>,branch_name:&str, working_copy:&std::path::Path)->Result<(Vec<Change>,HashMap<LocalKey,Inode>),Error>{
    let mut branch = try!(repository.db_nodes(branch_name));
    let mut st = RecordState {
        line_num: 1,
        actions: Vec::new(),
        updatables : HashMap::new(),
        redundant : Vec::new(),
    };
    {
        let mut realpath=PathBuf::from(working_copy);
        try!(record_all(repository, &branch, &mut st,
                        None, ROOT_INODE,&mut realpath,
                        &[]));
        debug!("record done, {} changes", st.actions.len());
    }
    try!(super::graph::remove_redundant_edges(&mut branch, &mut st.redundant));
    try!(branch.commit_branch(branch_name));
    //repository.set_db_nodes(branch_name, branch);
    debug!("remove_redundant_edges done");
    Ok((st.actions,st.updatables))
}
