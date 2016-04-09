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
//! This module defines the data structures representing contents of a
//! pijul repository at any point in time. It is a Graph of Lines.
//! Each Line corresponds to either a bit of contents of a file, or a
//! bit of information about fs layout within the working directory
//! (files and directories).
//!
//! Lines are organised in a Graph, which encodes which line belongs to what
//! file, in what order they appear, and any conflict.

extern crate libc;
use self::libc::{c_uchar};

extern crate rustc_serialize;
use rustc_serialize::hex::ToHex;

use super::backend::*;
use super::Len;

use std::collections::{HashMap, HashSet, BTreeSet};
use std::collections::hash_map::Entry;


pub const PSEUDO_EDGE:u8=1;
pub const FOLDER_EDGE:u8=2;
pub const PARENT_EDGE:u8=4;
pub const DELETED_EDGE:u8=8;

use patch::{HASH_SIZE, KEY_SIZE};
use std;

pub const DIRECTORY_FLAG:usize = 0x200;

pub const LINE_HALF_DELETED:c_uchar=4;
pub const LINE_VISITED:c_uchar=2;
pub const LINE_ONSTACK:c_uchar=1;

/// The elementary datum in the representation of the repository state
/// at any given point in time.
pub struct Line<'a> {
    pub key:&'a[u8], /// A unique identifier for the line. It is
                 /// guaranteed to be universally unique if the line
                 /// appears in a commit, and locally unique
                 /// otherwise.

    pub flags:u8,    /// The status of the line with respect to a dfs of
                 /// a graph it appears in. This is 0 or
                 /// LINE_HALF_DELETED unless some dfs is being run.

    pub children:usize,
    pub n_children:usize,
    pub index:usize,
    pub lowlink:usize,
    pub scc:usize
}


impl <'a>Line<'a> {
    pub fn is_zombie(&self)->bool {
        self.flags & LINE_HALF_DELETED != 0
    }
}

/// A graph, representing the whole content of a state of the repository at a point in time.
/// Vertices are Lines.
pub struct Graph<'a> {
    pub lines:Vec<Line<'a>>,
    pub children:Vec<(*const u8,usize)> // raw pointer because we might need the edge address. We need the first element anyway, replace "*const u8" by "u8" if the full address is not needed. The index is the index in the array of lines.
}

pub trait LineBuffer<'a> {

    fn output_line(&mut self, key:&'a [u8], contents: Contents<'a>);

    fn begin_conflict(&mut self) {
        let mut l = Contents::from_slice(b">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
        self.output_line(&[], l);
    }
    fn conflict_next(&mut self) {
        let mut l = Contents::from_slice(b"================================\n");
        self.output_line(&[], l);
    }
    fn end_conflict(&mut self) {
        let mut l = Contents::from_slice(b"<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n");
        self.output_line(&[], l);
    }
}



pub fn retrieve<'a>(repository:&'a Repository, branch:&Db, key:&'a [u8])->Result<Graph<'a>,()>{

    fn retr<'a>(
        repository: &'a Repository,
        db_nodes: &Db,
        cache: &mut HashMap<&'a [u8],usize>,
        lines: &mut Vec<Line<'a>>,
        children: &mut Vec<(*const u8,usize)>,
        key: &'a [u8])->usize {

        match cache.entry(key) {
            Entry::Occupied(e) => return *(e.get()),
            Entry::Vacant(e) => {
                let idx=lines.len();
                e.insert(idx);
                debug!(target:"retrieve","{}",key.to_hex());
                let is_zombie={
                    let mut tag=PARENT_EDGE|DELETED_EDGE;
                    let mut is_zombie = false;
                    for (k,v) in repository.iter(db_nodes, key, Some(&[tag][..])) {
                        if k==key && v[0] == tag {
                            is_zombie = true
                        }
                        break
                    }
                    tag=PARENT_EDGE|DELETED_EDGE|FOLDER_EDGE;
                    for (k,v) in repository.iter(db_nodes, key, Some(&[tag][..])) {
                        if k==key && v[0] == tag {
                            is_zombie = true
                        }
                        break
                    }
                    is_zombie
                };
                let mut l=Line {
                    key:key,flags:if is_zombie {LINE_HALF_DELETED} else {0},
                    children:children.len(),n_children:0,index:0,lowlink:0,scc:0
                };
                for (k,v) in repository.iter(db_nodes, key, Some(&[0][..])) {
                    if k == key && v[0] <= PSEUDO_EDGE|FOLDER_EDGE {
                        children.push((v.as_ptr(),0));
                        l.n_children += 1;
                    } else {
                        break
                    }
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
            children[l_children+i] = (a, retr(repository,db_nodes,cache,lines,children,child_key))
        }
        if n_children==0 {
            children.push((std::ptr::null(),0));
            lines[idx].n_children=1;
        }
        idx
    }
    let mut cache=HashMap::new();
    let mut lines=Vec::new();
    // Insert last line (so that all lines have a common descendant).
    lines.push(Line {
        key:&b""[..],flags:0,children:0,n_children:0,index:0,lowlink:0,scc:0
    });
    cache.insert(&b""[..],0);
    let mut children=Vec::new();
    retr(repository, &branch, &mut cache, &mut lines, &mut children, key);
    Ok(Graph { lines:lines, children:children })
}

fn tarjan(line:&mut Graph)->Vec<Vec<usize>> {
    fn dfs<'a>(scc:&mut Vec<Vec<usize>>,
               stack:&mut Vec<usize>,
               index:&mut usize, g:&mut Graph<'a>, n_l:usize){
        {
            let mut l=&mut (g.lines[n_l]);
            (*l).index = *index;
            (*l).lowlink = *index;
            (*l).flags |= LINE_ONSTACK | LINE_VISITED;
            debug!(target:"tarjan", "{} {} chi",(*l).key.to_hex(),(*l).n_children);
            //unsafe {println!("contents: {}",std::str::from_utf8_unchecked(repo.contents((*l).key))); }
        }
        stack.push(n_l);
        *index = *index + 1;
        for i in 0..g.lines[n_l].n_children {
            //let mut l=&mut (g.lines[n_l]);

            let (_,n_child) = g.children[g.lines[n_l].children + i];
            //println!("children: {}",to_hex(g.lines[n_child].key));

            if g.lines[n_child].flags & LINE_VISITED == 0 {
                dfs(scc,stack,index,g,n_child);
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
    dfs(&mut scc, &mut stack, &mut index, line, 1);
    scc
}




pub fn output_file<'a,B:LineBuffer<'a>>(repository:&'a Repository,branch:&Db, buf:&mut B,mut graph:Graph<'a>,forward:&mut Vec<u8>) {
    debug!(target:"conflict","output_file");

    //let t0=time::precise_time_s();
    let mut scc = tarjan(&mut graph); // SCCs are given here in reverse order.
    //let t1=time::precise_time_s();
    //info!("tarjan took {}s",t1-t0);
    info!("There are {} SCC",scc.len());
    //let mut levels=vec![0;scc];
    let mut last_visit=vec![0;scc.len()];
    let mut first_visit=vec![0;scc.len()];
    let mut step=1;
    fn dfs<'a>(
        graph:&mut Graph<'a>,
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
                let n=graph.lines[*cousin].n_children;
                for i in 0 .. n {
                    let (_,n_child) = graph.children[graph.lines[*cousin].children + i];
                    let child_component=graph.lines[n_child].scc;
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
                debug!(target:"output_file","visiting scc {} {}",*component,graph.lines[scc[*component][0]].key.to_hex());
                dfs(graph,first_visit,last_visit,forward,zero,step,scc,*component)
            }
        }
        for cousin in scc[n_scc].iter() {
            let n=graph.lines[*cousin].n_children;
            for i in 0 .. n {
                let (flag_child,n_child) = graph.children[graph.lines[*cousin].children + i];
                let child_component=graph.lines[n_child].scc;
                let is_forward=forward_scc.contains(&child_component);
                if is_forward {
                    if unsafe {*flag_child} & 1 != 0 {
                        forward.push(PSEUDO_EDGE|PARENT_EDGE);
                        forward.extend(graph.lines[*cousin].key);
                        forward.extend(zero);
                        forward.push(PSEUDO_EDGE);
                        forward.extend(graph.lines[n_child].key);
                    }
                    // Indicate here that we do not want to follow this edge (it is forward).
                    let (a,_)=graph.children[graph.lines[*cousin].children+i];
                    graph.children[graph.lines[*cousin].children + i] = (a,0);
                }
            }
        }
        for i in skipped.iter().rev() {
            last_visit[*i] = *step;
            *step+=1;
        }
    }
    let zero=[0;HASH_SIZE];
    dfs(&mut graph,&mut first_visit,&mut last_visit,forward,&zero[..],&mut step,&scc,scc.len()-1);
    debug!("dfs done");
    // assumes no conflict for now.
    let mut i=scc.len()-1;
    let mut nodes=vec!();
    let mut selected_zombies=HashMap::new();
    // let cursor= unsafe { &mut *self.txn.unsafe_cursor(self.dbi_nodes).unwrap() };
    loop {
        // test for conflict
        // scc[i] has at least one element (from tarjan).
        if scc[i].len() == 1 && first_visit[i] <= first_visit[0] && last_visit[i] >= last_visit[0]  && graph.lines[scc[i][0]].flags & LINE_HALF_DELETED == 0 {
            //debug!(target:"conflict","/flag = {} {}",graph.lines[scc[i][0]].flags,LINE_HALF_DELETED);
            let key=graph.lines[scc[i][0]].key;
            debug!(target:"conflict","key = {}",key.to_hex());
            if key.len()>0 {
                if let Some(cont) = repository.contents(key) {
                    buf.output_line(key, cont)
                }
            }
            if i==0 { break } else { i-=1 }
        } else {
            debug!(target:"conflict","flag = {} {}",graph.lines[scc[i][0]].flags,LINE_HALF_DELETED);
            let key=graph.lines[scc[i][0]].key;
            debug!(target:"conflict","key = {}",key.to_hex());

            fn get_conflict<'a,B:LineBuffer<'a>>(
                repo:&'a Repository,
                branch:&Db,
                graph:&Graph<'a>,
                first_visit: &mut [usize],
                last_visit: &mut [usize],
                scc:&mut Vec<Vec<usize>>,
                nodes:&mut Vec<&'a [u8]>,
                b:&mut B,
                is_first:&mut bool,
                selected_zombies:&mut HashMap<&'a [u8],bool>,
                next:&mut usize,
                i:usize) {
                // x.scc[i] has at least one element (from tarjan).
                if scc[i].len() == 1 && first_visit[i] <= first_visit[0] && last_visit[i] >= last_visit[0] && graph.lines[scc[i][0]].flags & LINE_HALF_DELETED == 0 {
                    // End of conflict.
                    debug!(target:"conflict","end of conflict");
                    let mut first=false; // Detect the first line
                    for key in nodes.iter() {
                        if let Some(cont) = repo.contents(key) {
                            if cont.len() > 0 && !first { // If this is the first non-empty line of this side of the conflict
                                first=true;
                                // Either we've had another side of the conflict before
                                if ! *is_first {b.conflict_next();}
                                // Or not
                                else {
                                    b.begin_conflict();
                                    *is_first=false
                                }
                            }
                            b.output_line(key,cont)
                        }
                    }
                    *next=i
                } else {
                    // Pour chaque permutation de la SCC, ajouter tous les sommets sur la pile, et appel recursif de chaque arete non-forward.
                    fn permutations<'a, B:LineBuffer<'a>>(
                        repo:&'a Repository,
                        branch:&Db,
                        graph:&Graph<'a>,
                        first_visit: &mut [usize],
                        last_visit: &mut [usize],
                        scc:&mut Vec<Vec<usize>>,
                        nodes:&mut Vec<&'a[u8]>,
                        b:&mut B,
                        is_first:&mut bool,
                        selected_zombies:&mut HashMap<&'a [u8],bool>,
                        next:&mut usize,
                        
                        i:usize,
                        j:usize,
                        next_vertices:&mut HashSet<usize>) {
                        
                        debug!(target:"conflict","permutations:j={}, nodes={:?}",j,nodes);
                        if j<scc[i].len() {
                            debug!(target:"conflict","next? j={} {}",j,next_vertices.len());
                            let n=graph.lines[scc[i][j]].n_children;
                            debug!(target:"conflict","n={}",n);
                            for c in 0 .. n {
                                let (edge_child,n_child) = graph.children[graph.lines[scc[i][j]].children + c];
                                if n_child != 0 || edge_child.is_null() {
                                    // Not a forward edge (forward edges are (!=NULL, 0)).
                                    debug!(target:"conflict","n_child={}",n_child);
                                    next_vertices.insert(graph.lines[n_child].scc);
                                }
                            }
                            for k in j..scc[i].len() {
                                scc[i].swap(j,k);
                                let mut newly_forced = Vec::new();
                                let key = graph.lines[scc[i][j]].key;
                                let mut key_is_present = true;
                                if graph.lines[scc[i][j]].is_zombie() {
                                    let mut is_forced:bool = false;
                                    let mut is_defined:bool = false;


                                    for (k,v) in repo.iter(branch, key, Some(&[PARENT_EDGE][..])) {
                                        if v[0] <= PARENT_EDGE|PSEUDO_EDGE|FOLDER_EDGE && k == key {
                                            let f=&v[(1+KEY_SIZE)..(1+KEY_SIZE+HASH_SIZE)];
                                            match selected_zombies.get(f) {
                                                Some(force)=>{
                                                    is_defined = true;
                                                    is_forced = *force
                                                },
                                                None => {
                                                    newly_forced.push(f)
                                                }
                                            }
                                        } else {
                                            break
                                        }
                                    }
                                    debug!(target:"conflict","forced:{:?}",is_forced);
                                    // If this zombie line is not forced in, try without it.
                                    if !is_defined {
                                        // pas defini, on le definit.
                                        for f in newly_forced.iter() {
                                            selected_zombies.insert(f,false);
                                        }
                                    } else {
                                        key_is_present = is_forced
                                    }
                                    if !is_forced {
                                        permutations(repo,branch,graph,first_visit,last_visit,
                                                     scc,nodes,b,is_first,selected_zombies,next,
                                                     i,j+1,next_vertices)
                                    }
                                    if key_is_present {
                                        for f in newly_forced.iter() {
                                            selected_zombies.insert(f,true);
                                        }
                                    }
                                }
                                if key_is_present {
                                    nodes.push(key);
                                    permutations(repo,branch,graph,first_visit,last_visit,
                                                 scc,nodes,b,is_first,selected_zombies,next,
                                                 i,j+1,next_vertices);
                                    nodes.pop();
                                }
                                if newly_forced.len()>0 {
                                    // Unmark here.
                                    for f in &newly_forced {
                                        selected_zombies.remove(f);
                                    }
                                }
                            }
                        } else {
                            debug!(target:"conflict","next? {}",next_vertices.len());
                            for chi in next_vertices.iter() {
                                debug!(target:"conflict","rec: get_conflict {}",*chi);
                                get_conflict(repo,branch,graph,first_visit,last_visit,scc,nodes,b,is_first,selected_zombies,next,*chi);
                            }
                        }
                    }
                    let mut next_vertices=HashSet::new();
                    debug!(target:"conflict","permutations");
                    permutations(repo,branch,graph,first_visit,last_visit,scc,nodes,b,is_first,selected_zombies,next,i,0,&mut next_vertices);
                }
            }
            nodes.clear();
            let (next,is_first)={
                let mut is_first = true;
                let mut next = 0;
                get_conflict(repository,branch,&graph,&mut first_visit[..],&mut last_visit[..],&mut scc, &mut nodes,
                             buf,
                             &mut is_first,
                             &mut selected_zombies,
                             &mut next, i);
                (next,is_first)
            };
            if !is_first { buf.end_conflict() }
            if i==0 { break } else { i=std::cmp::min(i-1,next) }
        }
    }
    debug!(target:"conflict","/output_file");
}

/*
fn remove_redundant_edges<R:Backend>(repository:&mut R,forward:&mut Vec<u8>) {
    let mut i=0;
    let cursor=unsafe { self.txn.unsafe_cursor(self.dbi_nodes).unwrap() };
    while i<forward.len() {
        debug!(target:"remove_redundant_edges","i={},forward.len={}",i,forward.len());
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
            self.txn.del(self.dbi_nodes,
                         &forward[(i+1+KEY_SIZE+HASH_SIZE+1)..(i+1+KEY_SIZE+HASH_SIZE+1+KEY_SIZE)],
                         Some(&forward[i..(i+1+KEY_SIZE+HASH_SIZE)])).unwrap();
        }
        i+=(1+HASH_SIZE+KEY_SIZE) + (1+KEY_SIZE)
    }
    unsafe { lmdb::mdb_cursor_close(cursor) };
}
*/
