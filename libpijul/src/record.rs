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

use super::backend;
use super::backend::*;
use super::patch::*;
use super::error::*;
use super::file_operations::*;
use super::graph::*;
use super::diff;

use std::collections::HashMap;
use std::path::{Path,PathBuf};
use std::fs::metadata;
use std;
use std::io::BufRead;

#[cfg(not(windows))]
use std::os::unix::fs::PermissionsExt;


#[cfg(not(windows))]
fn permissions(attr:&std::fs::Metadata)->usize{
    attr.permissions().mode() as usize
}
#[cfg(windows)]
fn permissions(attr:&std::fs::Metadata)->usize{
    0
}


macro_rules! iterate_parents {
    ($repository:expr, $branch:expr, $key:expr) => {
        $repository.iter($branch, $key, Some(&[FOLDER_EDGE|PARENT_EDGE][..]))
            .take_while(|&(k,parent)| {
                k == $key && parent[0] >= FOLDER_EDGE|PARENT_EDGE && parent[0] <= FOLDER_EDGE|PARENT_EDGE|PSEUDO_EDGE
            })
            .map(|(a,b)| b)
    }
}


pub fn record_all (
    repository:&Repository,
    branch:&Db,
    actions:&mut Vec<Change>,
    line_num:&mut usize,
    redundant:&mut Vec<u8>,
    updatables:&mut HashMap<Vec<u8>,Inode >,
    parent_inode:Option< Inode >,
    parent_node:Option< &[u8] >,
    current_inode: Inode,
    realpath:&mut std::path::PathBuf,
    basename:&[u8])->Result<(),Error> {

    if parent_inode.is_some() { realpath.push(std::str::from_utf8(&basename).unwrap()) }
    debug!(target:"record_all","realpath:{:?}",realpath);
    //debug!(target:"record_all","inode:{:?}",current_inode.to_hex());

    let mut l2=[0;LINE_SIZE];
    let current_node=
        if parent_inode.is_some() {
            match repository.get(branch, current_inode.as_ref()) {
                Some(current_node)=>{
                    let old_attr=((current_node[1] as usize) << 8) | (current_node[2] as usize);
                    // Add the new name.
                    let (int_attr,deleted)={
                        match metadata(&realpath) {
                            Ok(attr)=>{
                                let p=(permissions(&attr)) & 0o777;
                                let is_dir= if attr.is_dir() { DIRECTORY_FLAG } else { 0 };
                                ((if p==0 { old_attr } else { p }) | is_dir,false)
                            },
                            Err(_)=>{
                                (old_attr,true)
                            }
                        }
                    };
                    debug!(target:"record_all","current_node[0]={},old_attr={},int_attr={}",
                           current_node[0],old_attr,int_attr);
                    if !deleted && (current_node[0]==1 || old_attr!=int_attr) {
                        // file moved

                        // Delete all former names.
                        let mut edges=Vec::new();
                        // Now take all grandparents of l2, delete them.

                        let mut name=Vec::with_capacity(basename.len()+2);
                        name.push(((int_attr >> 8) & 0xff) as u8);
                        name.push((int_attr & 0xff) as u8);
                        name.extend(basename);

                        for parent in iterate_parents!(repository, branch, &current_node[3..]) {
                            let mut previous_name=
                                match repository.contents(&parent[1..(1+KEY_SIZE)]) {
                                    None=>Contents::from_slice(b""),
                                    Some(n)=>n
                                };
                            let name_changed = !super::eq(&mut Contents::from_slice(&name[..]),
                                                          &mut previous_name);
                            for grandparent in iterate_parents!(repository, branch, &parent[1..(1+KEY_SIZE)]) {
                                if &grandparent[1..(1+KEY_SIZE)] != parent_node.unwrap()
                                    || name_changed {
                                        edges.push(Edge {
                                            from:external_key(repository, &parent[1..(1+KEY_SIZE)]),
                                            to:external_key(repository, &grandparent[1..(1+KEY_SIZE)]),
                                            introduced_by:external_key(repository, &grandparent[1+KEY_SIZE..])
                                        })
                                    }
                            }
                        }
                        /*
                        for parent in CursIter::new(curs_parents,&current_node[3..],FOLDER_EDGE|PARENT_EDGE,true,false) {
                            let previous_name=
                                match try!(self.txn.get(self.dbi_contents,&parent[1..(1+KEY_SIZE)])) {
                                    None=>"".as_bytes(),
                                    Some(n)=>n
                                };
                            for grandparent in CursIter::new(curs_grandparents,&parent[1..(1+KEY_SIZE)],FOLDER_EDGE|PARENT_EDGE,true,false) {
                                if &grandparent[1..(1+KEY_SIZE)] != parent_node.unwrap()
                                    || &name[..] != previous_name {
                                        edges.push(Edge {
                                            from:self.external_key(&parent[1..(1+KEY_SIZE)]),
                                            to:self.external_key(&grandparent[1..(1+KEY_SIZE)]),
                                            introduced_by:self.external_key(&grandparent[1+KEY_SIZE..])
                                        });
                                    }
                            }
                        }
                        unsafe {
                            lmdb::mdb_cursor_close(curs_parents);
                            lmdb::mdb_cursor_close(curs_grandparents);
                        }
                         */
                        debug!(target:"record_all", "edges:{:?}",edges);
                        if !edges.is_empty(){
                            actions.push(Change::Edges{edges:edges,flag:DELETED_EDGE|FOLDER_EDGE|PARENT_EDGE});
                            //debug!(target:"record_all","parent_node: {:?}",parent_node.unwrap());
                            //debug!(target:"record_all","ext key: {:?}",self.external_key(parent_node.unwrap()));
                            //debug!(target:"record_all","ext key: {:?}",self.external_key(&current_node[3..]));
                            actions.push(
                                Change::NewNodes { up_context:{
                                    let p=parent_node.unwrap();
                                    vec!(if p.len()>LINE_SIZE { external_key(repository, &p) }
                                         else { p.to_vec() })
                                },
                                                   line_num: *line_num as u32,
                                                   down_context:{
                                                       let p=&current_node[3..];
                                                       vec!(if p.len()>LINE_SIZE { external_key(repository, &p) }
                                                            else { p.to_vec() })
                                                   },
                                                   nodes: vec!(name),
                                                   flag:FOLDER_EDGE }
                            );
                        }
                        *line_num += 1;
                        debug!(target:"record_all", "directory_flag:{}",old_attr&DIRECTORY_FLAG);
                        if old_attr & DIRECTORY_FLAG == 0 {
                            info!("retrieving");
                            //let time0=time::precise_time_s();
                            let ret = retrieve(repository, branch, &current_node[3..]);
                            //let time1=time::precise_time_s();
                            //info!("retrieve took {}s, now calling diff", time1-time0);
                            diff::diff(repository, branch, line_num,actions, redundant,ret.unwrap(), realpath.as_path()).unwrap();
                            //let time2=time::precise_time_s();
                            //info!("total diff took {}s", time2-time1);
                        }

                    } else if deleted || current_node[0]==2 {

                        let mut edges=Vec::new();
                        // Now take all grandparents of l2, delete them.
                        for parent in iterate_parents!(repository, branch, &current_node[3..]) {
                            for grandparent in iterate_parents!(repository, branch, &parent[1..(1+KEY_SIZE)]) {
                                edges.push(Edge {
                                    from: external_key(repository, &parent[1..(1+KEY_SIZE)]),
                                    to: external_key(repository, &grandparent[1..(1+KEY_SIZE)]),
                                    introduced_by: external_key(repository, &grandparent[1+KEY_SIZE..])
                                })
                            }
                        }

                        // Delete the file recursively
                        let mut file_edges=vec!();
                        {
                            //debug!(target:"record_all","del={}",current_node.to_hex());
                            let ret = retrieve(repository, branch, &current_node[3..]).unwrap();
                            for l in ret.lines {
                                if l.key.len()>0 {
                                    let ext_key = external_key(repository, l.key);
                                    //debug!(target:"record_all","ext_key={}",ext_key.to_hex());
                                    for v in iterate_parents!(repository, branch, l.key) {

                                        //debug!(target:"record_all","v={}",v.to_hex());
                                        if v[0] & FOLDER_EDGE != 0 { &mut edges } else { &mut file_edges }
                                        .push(Edge { from: ext_key.clone(),
                                                     to: external_key(repository, &v[1..(1+KEY_SIZE)]),
                                                     introduced_by: external_key(repository, &v[(1+KEY_SIZE)..]) });
                                    }
                                }
                            }
                        }

                        actions.push(Change::Edges{edges:edges,flag:FOLDER_EDGE|PARENT_EDGE|DELETED_EDGE});
                        if file_edges.len()>0 {
                            actions.push(Change::Edges{edges:file_edges,flag:PARENT_EDGE|DELETED_EDGE});
                        }
                    } else if current_node[0]==0 {
                        if old_attr & DIRECTORY_FLAG == 0 {
                            //let time0=time::precise_time_s();
                            let ret = retrieve(repository, branch, &current_node[3..]);
                            //let time1=time::precise_time_s();
                            //info!(target:"record_all","record: retrieve took {}s, now calling diff", time1-time0);
                            diff::diff(repository, branch, line_num, actions, redundant,
                                 ret.unwrap(), realpath.as_path()).unwrap();
                            //let time2=time::precise_time_s();
                            //info!(target:"record_all","total diff took {}s", time2-time1);
                        }
                    } else {
                        panic!("record: wrong inode tag (in base INODES) {}", current_node[0])
                    };
                    Some(&current_node[3..])
                },
                None=>{
                    // File addition, create appropriate Newnodes.
                    debug!(target:"record_all","metadata");
                    match metadata(&realpath) {
                        Ok(attr) => {
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
                            {
                                let mut l2_=Vec::with_capacity(LINE_SIZE+2);
                                l2_.extend(&name[0..2]);
                                l2_.extend(&l2);
                                updatables.insert(l2_,current_inode);
                            }
                            actions.push(
                                Change::NewNodes { up_context: vec!(
                                    if parent_node.unwrap().len()>LINE_SIZE {
                                        external_key(repository, parent_node.unwrap())
                                    } else {parent_node.unwrap().to_vec()}
                                ),
                                                   line_num: *line_num as u32,
                                                   down_context: vec!(),
                                                   nodes: vec!(name,vec!()),
                                                   flag:FOLDER_EDGE }
                            );
                            *line_num += 2;
                            updatables.insert(l2.to_vec(),current_inode);
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
                                    actions.push(
                                        Change::NewNodes { up_context:vec!(l2.to_vec()),
                                                           line_num: *line_num as u32,
                                                           down_context: vec!(),
                                                           nodes: nodes,
                                                           flag:0 }
                                    );
                                }
                                *line_num+=len;
                                None
                            } else {
                                Some(&l2[..])
                            }
                        },
                        Err(_)=>{
                            println!("error adding file {:?} (metadata failed)",realpath);
                            None
                        }
                    }
                }
            }
        } else {
            Some(ROOT_KEY)
        };
    debug!(target:"record_all","current_node={:?}",current_node);
    match current_node {
        None => (), // we just added a file
        Some(current_node)=>{
            //debug!(target:"record_all","children of current_inode {}",current_inode.to_hex());
            let db_tree = repository.db_tree();
            for (k,v) in repository.iter(&db_tree, current_inode.as_ref(), None) {

                if k == current_inode.as_ref() {

                    if v.len()>0 {
                        //debug!(target:"record_all","  child: {} + {}",&v[0..INODE_SIZE].to_hex(), std::str::from_utf8(&k[INODE_SIZE..]).unwrap());
                        try!(record_all(
                            repository, branch,
                            actions, line_num,redundant,updatables,
                            Some(current_inode), // parent_inode
                            Some(current_node), // parent_node
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
    if parent_inode.is_some() { let _=realpath.pop(); }
    Ok(())
}
