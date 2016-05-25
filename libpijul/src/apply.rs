// Copyright Florent Becker and Pierre-Etienne Meunier 2015.
//
// This file is part of Pijul.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.
//

use super::backend::*;
use super::error::Error;
use super::patch::{Change, HASH_SIZE, KEY_SIZE, LINE_SIZE, ROOT_KEY, EDGE_SIZE, InternalKey,
                   internal_hash, external_hash, Patch, LocalKey, new_internal, register_hash};
use super::graph::{PSEUDO_EDGE, FOLDER_EDGE, PARENT_EDGE, DELETED_EDGE};
use super::file_operations::Inode;
use super::Len;

use std::collections::{HashSet, HashMap};
use std::ptr::copy_nonoverlapping;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use time;
use std::sync::Arc;
use std::thread;
use super::fs_representation::patches_dir;
use rustc_serialize::hex::ToHex;

/// Test whether a node has edges unknown to the patch we're applying.
fn has_exclusive_edge<T>(branch: &Branch<T>,
                         db_external: &Db<T>,
                         internal_patch_id: &InternalKey,
                         key: &[u8],
                         flag0: u8,
                         dependencies: &HashSet<Vec<u8>>)
                         -> bool {
    for (k, neighbor) in branch.iter(&key[1..(1 + KEY_SIZE)], Some(&[flag0][..])) {
        // ,include_folder,include_pseudo) {
        if k == &key[1..(1 + KEY_SIZE)] && neighbor[0] <= flag0 | PSEUDO_EDGE | FOLDER_EDGE {

            if &neighbor[1 + KEY_SIZE..] != internal_patch_id.as_slice() {

                let ext = external_hash(&db_external,
                                        &neighbor[(1 + KEY_SIZE)..(1 + KEY_SIZE + HASH_SIZE)]);
                if !dependencies.contains(ext) {
                    return true;
                }/* else {
                    for p in dependencies.iter() {
                        debug!(target:"exclusive","p={}",p.to_hex());
                    }
                }*/
            }
        } else {
            break;
        }
    }
    false
}


/// "intro" is the internal patch number of the patch that introduced this edge.
fn internal_edge<T>(internal: &Db<T>,
                    flag: u8,
                    to: &[u8],
                    intro: &InternalKey,
                    result: &mut [u8])
                    -> Result<(), Error> {
    debug_assert!(result.len() >= 1 + KEY_SIZE + HASH_SIZE);
    debug_assert!(intro.contents.len() == HASH_SIZE);
    result[0] = flag;
    let int_to = try!(internal_hash(&internal, &to[0..(to.len() - LINE_SIZE)]));
    unsafe {
        copy_nonoverlapping(int_to.contents.as_ptr(),
                            result.as_mut_ptr().offset(1),
                            HASH_SIZE);
        copy_nonoverlapping(to.as_ptr().offset((to.len() - LINE_SIZE) as isize),
                            result.as_mut_ptr().offset((1 + HASH_SIZE) as isize),
                            LINE_SIZE);
        copy_nonoverlapping(intro.contents.as_ptr(),
                            result.as_mut_ptr().offset(1 + KEY_SIZE as isize),
                            HASH_SIZE);
    }
    Ok(())
}


fn unsafe_apply<T>(db_internal: &Db<T>,
                   db_external: &Db<T>,
                   branch: &mut Branch<T>,
                   db_contents: &mut Db<T>,
                   changes: &[Change],
                   internal_patch_id: &InternalKey,
                   dependencies: &HashSet<Vec<u8>>)
                   -> Result<(), Error> {

    debug!("unsafe_apply");
    let mut pu: [u8; 1 + KEY_SIZE + HASH_SIZE] = [0; 1 + KEY_SIZE + HASH_SIZE];
    let mut pv: [u8; 1 + KEY_SIZE + HASH_SIZE] = [0; 1 + KEY_SIZE + HASH_SIZE];
    // let alive= unsafe { &mut *self.txn.unsafe_cursor(self.dbi_nodes).unwrap() };
    // let cursor= unsafe { &mut *self.txn.unsafe_cursor(self.dbi_nodes).unwrap() };
    let mut parents: Vec<u8> = Vec::new();
    let mut children: Vec<u8> = Vec::new();
    // let db_internal = repository.db_internal();
    // debug!(target:"apply","unsafe_apply (patch {})",internal_patch_id.contents.to_hex());
    for ch in changes {
        match *ch {
            Change::Edges { ref flag, ref edges } => {
                // If this hunk deletes nodes that are not known to
                // the author of the current patch, add pseudo-edges
                // (zombie lines) to each edge of this hunk.
                debug!("edges");
                let mut add_zombies = false;
                for e in edges {
                    // First remove the deleted version of the edge
                    // debug!(target:"conflictdiff","e:{:?}",e);
                    {
                        let p = try!(internal_hash(&db_internal, &e.introduced_by));
                        try!(internal_edge(db_internal,
                                           *flag ^ DELETED_EDGE ^ PARENT_EDGE,
                                           &e.from,
                                           &p,
                                           &mut pu));
                        try!(internal_edge(db_internal, *flag ^ DELETED_EDGE, &e.to, p, &mut pv));
                        // debug!(target:"exclusive","pu={}\npv={}",pu.to_hex(),pv.to_hex());
                    }
                    try!(branch.del(&pu[1..(1 + KEY_SIZE)], Some(&pv)));
                    try!(branch.del(&pv[1..(1 + KEY_SIZE)], Some(&pu)));

                    if *flag & DELETED_EDGE != 0 {
                        // Will we need zombies?  We need
                        // internal_patch_id here: previous hunks of
                        // this patch could have added edges to us.
                        if has_exclusive_edge(branch,
                                              &db_external,
                                              internal_patch_id,
                                              &pv,
                                              PARENT_EDGE,
                                              dependencies) ||
                           has_exclusive_edge(branch,
                                              &db_external,
                                              internal_patch_id,
                                              &pu,
                                              0,
                                              dependencies) {
                            add_zombies = true;
                        } else {
                            debug!("not add zombies: {}", add_zombies);
                        }
                        //
                        try!(kill_obsolete_pseudo_edges(branch,
                                                        if *flag & PARENT_EDGE == 0 {
                                                            &mut pv
                                                        } else {
                                                            &mut pu
                                                        }))
                    }
                }
                // Then add the new edges.
                // Then add zombies and pseudo-edges if needed.
                // debug!(target:"apply","edges (patch {})",internal_patch_id.to_hex());
                parents.clear();
                children.clear();
                for e in edges {
                    try!(internal_edge(db_internal,
                                       *flag ^ PARENT_EDGE,
                                       &e.from,
                                       internal_patch_id,
                                       &mut pu));
                    try!(internal_edge(db_internal, *flag, &e.to, internal_patch_id, &mut pv));
                    debug!("new edge:\n  {}\n  {}", pu.to_hex(), pv.to_hex());
                    try!(branch.put(&pu[1..(1 + KEY_SIZE)], &pv));
                    try!(branch.put(&pv[1..(1 + KEY_SIZE)], &pu));
                    // Here, there are two options: either we need
                    // zombie lines because the currently applied
                    // patch doesn't know about some of our edges, or
                    // else we just need to reconnect parents and
                    // children of a deleted portion of the graph.
                    if *flag & DELETED_EDGE != 0 {
                        if add_zombies {
                            pu[0] ^= DELETED_EDGE;
                            pv[0] ^= DELETED_EDGE;
                            // debug!(target:"apply","zombie:\n  {}\n  {}",pu.to_hex(),pv.to_hex());
                            try!(branch.put(&pu[1..(1 + KEY_SIZE)], &pv));
                            try!(branch.put(&pv[1..(1 + KEY_SIZE)], &pu));
                        } else if *flag & FOLDER_EDGE == 0 {
                            // collect alive parents/children of hunk
                            let (pu, pv) = if *flag & PARENT_EDGE == 0 {
                                (&pu, &pv)
                            } else {
                                (&pv, &pu)
                            };
                            if has_edge(branch, &pu[1..(1 + KEY_SIZE)], PARENT_EDGE, true) {
                                let i = parents.len();
                                parents.extend(&pu[..]);
                                parents[i] ^= PSEUDO_EDGE | DELETED_EDGE;
                            }
                            for (k, neighbor) in branch.iter(&pv[1..(1 + KEY_SIZE)],
                                                             Some(&[PARENT_EDGE][..])) {

                                if k == &pv[1..(1 + KEY_SIZE)] &&
                                   neighbor[0] <= PARENT_EDGE | PSEUDO_EDGE | FOLDER_EDGE {
                                    if has_edge(branch,
                                                &neighbor[1..(1 + KEY_SIZE)],
                                                PARENT_EDGE,
                                                true) {
                                        let i = parents.len();
                                        parents.extend(neighbor);
                                        parents[i] ^= PSEUDO_EDGE;
                                    }
                                } else {
                                    break;
                                }
                            }
                            for (k, neighbor) in branch.iter(&pv[1..(1 + KEY_SIZE)],
                                                             Some(&[0][..])) {

                                if k == &pv[1..(1 + KEY_SIZE)] &&
                                   neighbor[0] <= PSEUDO_EDGE | FOLDER_EDGE {
                                    if has_edge(branch,
                                                &neighbor[1..(1 + KEY_SIZE)],
                                                PARENT_EDGE,
                                                true) {
                                        let i = children.len();
                                        children.extend(neighbor);
                                        children[i] ^= PSEUDO_EDGE;
                                    }
                                } else {
                                    break;
                                }
                            }
                        }
                    }
                }
                debug!("/edges");
                // Finally: reconnect
                if *flag & DELETED_EDGE != 0 {
                    let mut i = 0;
                    while i < children.len() {
                        let mut j = 0;
                        while j < parents.len() {
                            if !connected(branch,
                                          &parents[j + 1..j + 1 + KEY_SIZE],
                                          &mut children[i..i + 1 + KEY_SIZE + HASH_SIZE]) {
                                // debug!(target:"apply","reconnect:\n  {}\n  {}",
                                // &parents[j..(j+1+KEY_SIZE+HASH_SIZE)].to_hex(),
                                // &mut children[i..(i+1+KEY_SIZE+HASH_SIZE)].to_hex());
                                if &parents[(j + 1)..(j + 1 + KEY_SIZE)] !=
                                   &children[(i + 1)..(i + 1 + KEY_SIZE)] {
                                    try!(add_edge(branch,
                                                  &parents[j..(j + 1 + KEY_SIZE + HASH_SIZE)],
                                                  &mut children[i..(i + 1 + KEY_SIZE +
                                                                    HASH_SIZE)]));
                                }
                            }
                            j += 1 + KEY_SIZE + HASH_SIZE;
                        }
                        i += 1 + KEY_SIZE + HASH_SIZE;
                    }
                }
                debug!("unsafe_apply:edges.done");
            }
            Change::NewNodes { ref up_context,
                               ref down_context,
                               ref line_num,
                               ref flag,
                               ref nodes } => {
                assert!(!nodes.is_empty());
                debug!("unsafe_apply: newnodes");
                let mut pu: [u8; 1 + KEY_SIZE + HASH_SIZE] = [0; 1 + KEY_SIZE + HASH_SIZE];
                let mut pv: [u8; 1 + KEY_SIZE + HASH_SIZE] = [0; 1 + KEY_SIZE + HASH_SIZE];
                let mut lnum0 = *line_num;
                for i in 0..LINE_SIZE {
                    pv[1 + HASH_SIZE + i] = (lnum0 & 0xff) as u8;
                    lnum0 >>= 8
                }
                unsafe {
                    copy_nonoverlapping(internal_patch_id.contents.as_ptr(),
                                        pu.as_mut_ptr().offset(1 + KEY_SIZE as isize),
                                        HASH_SIZE);
                    copy_nonoverlapping(internal_patch_id.contents.as_ptr(),
                                        pv.as_mut_ptr().offset(1 + KEY_SIZE as isize),
                                        HASH_SIZE);
                    copy_nonoverlapping(internal_patch_id.contents.as_ptr(),
                                        pv.as_mut_ptr().offset(1),
                                        HASH_SIZE);
                };
                for c in up_context {
                    {
                        // debug!("newnodes: up_context {:?}",c.to_hex());

                        let u = if c.len() > LINE_SIZE {
                            let u = try!(internal_hash(&db_internal, &c[0..(c.len() - LINE_SIZE)]));
                            u
                        } else {
                            internal_patch_id
                        };
                        pu[0] = (*flag) ^ PARENT_EDGE;
                        pv[0] = *flag;
                        unsafe {
                            copy_nonoverlapping(u.contents.as_ptr(),
                                                pu.as_mut_ptr().offset(1),
                                                HASH_SIZE);
                            copy_nonoverlapping(c.as_ptr().offset((c.len() - LINE_SIZE) as isize),
                                                pu.as_mut_ptr().offset((1 + HASH_SIZE) as isize),
                                                LINE_SIZE);
                        }
                    }
                    try!(branch.put(&pu[1..(1 + KEY_SIZE)], &pv));
                    try!(branch.put(&pv[1..(1 + KEY_SIZE)], &pu));
                }
                unsafe {
                    copy_nonoverlapping(internal_patch_id.contents.as_ptr(),
                                        pu.as_mut_ptr().offset(1),
                                        HASH_SIZE);
                }
                debug!("newnodes: inserting");
                let mut lnum = *line_num + 1;
                try!(db_contents.put(&pv[1..(1 + KEY_SIZE)], &nodes[0]));
                for n in &nodes[1..] {
                    let mut lnum0 = lnum - 1;
                    for i in 0..LINE_SIZE {
                        pu[1 + HASH_SIZE + i] = (lnum0 & 0xff) as u8;
                        lnum0 >>= 8
                    }
                    lnum0 = lnum;
                    for i in 0..LINE_SIZE {
                        pv[1 + HASH_SIZE + i] = (lnum0 & 0xff) as u8;
                        lnum0 >>= 8
                    }
                    pu[0] = (*flag) ^ PARENT_EDGE;
                    pv[0] = *flag;
                    try!(branch.put(&pu[1..(1 + KEY_SIZE)], &pv));
                    try!(branch.put(&pv[1..(1 + KEY_SIZE)], &pu));
                    try!(db_contents.put(&pv[1..(1 + KEY_SIZE)], &n));
                    lnum = lnum + 1;
                }
                // repository.set_db_contents(db_contents);
                // In this last part, u is that target (downcontext), and v is the last new node.
                pu[0] = *flag;
                pv[0] = (*flag) ^ PARENT_EDGE;
                for c in down_context {
                    {
                        unsafe {
                            let u = if c.len() > LINE_SIZE {
                                try!(internal_hash(&db_internal, &c[0..(c.len() - LINE_SIZE)]))
                            } else {
                                internal_patch_id
                            };
                            copy_nonoverlapping(u.contents.as_ptr(),
                                                pu.as_mut_ptr().offset(1),
                                                HASH_SIZE);
                            copy_nonoverlapping(c.as_ptr().offset((c.len() - LINE_SIZE) as isize),
                                                pu.as_mut_ptr().offset((1 + HASH_SIZE) as isize),
                                                LINE_SIZE);
                        }
                    }
                    try!(branch.put(&pu[1..(1 + KEY_SIZE)], &pv));
                    try!(branch.put(&pv[1..(1 + KEY_SIZE)], &pu));
                    // There was something here before, to remove
                    // existing edges between up and down context, but
                    // it would break unrecord.
                }
            }
        }
    }
    Ok(())
}


/// Test whether `key` has a neighbor with flag `flag0`. If
/// `include_pseudo`, this includes pseudo-neighbors.
pub fn has_edge<T>(branch: &Branch<T>, key: &[u8], flag0: u8, include_pseudo: bool) -> bool {

    for (k, v) in branch.iter(key, Some(&[flag0][..])) {
        return k == key && (v[0] == flag0 || (include_pseudo && v[0] <= flag0 | PSEUDO_EDGE));
    }
    false
}


pub fn has_patch<T>(repository: &Transaction<T>,
                    branch_name: &str,
                    hash: &[u8])
                    -> Result<bool, Error> {
    if hash.len() == HASH_SIZE && hash == ROOT_KEY {
        Ok(true)
    } else {
        let db_internal = repository.db_internal();
        match internal_hash(&db_internal, hash) {
            Ok(internal) => {
                let db_branches = repository.db_branches();
                for (k, v) in db_branches.iter(branch_name.as_bytes(), Some(internal.as_slice())) {
                    return Ok(k == branch_name.as_bytes() && internal.as_slice() == v);
                }
                Ok(false)
            }
            Err(Error::InternalHashNotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }
}

// requires pu to be KEY_SIZE, pv to be 1+KEY_SIZE+HASH_SIZE
fn connected<T>(branch: &Branch<T>, pu: &[u8], pv: &mut [u8]) -> bool {
    let pv_0 = pv[0];
    pv[0] = 0;
    for (k, v) in branch.iter(pu, Some(pv)) {
        if k == pu && &v[1..(1 + KEY_SIZE)] == &pv[1..(1 + KEY_SIZE)] &&
           v[0] | PSEUDO_EDGE == pv[0] | PSEUDO_EDGE {
            pv[0] = pv_0;
            return true;
        }
        break;
    }
    pv[0] = pv_0;
    false
}

fn add_edge<T>(branch: &mut Branch<T>, pu: &[u8], pv: &[u8]) -> Result<(), Error> {
    try!(branch.put(&pu[1..(1 + KEY_SIZE)], &pv)); // ,lmdb::MDB_NODUPDATA));
    branch.put(&pv[1..(1 + KEY_SIZE)], &pu) // ,lmdb::MDB_NODUPDATA)
}

fn kill_obsolete_pseudo_edges<T>(branch: &mut Branch<T>, pv: &[u8]) -> Result<(), Error> {
    debug_assert!(pv.len() == 1 + KEY_SIZE + HASH_SIZE);
    let mut a: [u8; 1 + KEY_SIZE + HASH_SIZE] = [0; 1 + KEY_SIZE + HASH_SIZE];
    let mut b: [u8; 1 + KEY_SIZE + HASH_SIZE] = [0; 1 + KEY_SIZE + HASH_SIZE];
    unsafe {
        copy_nonoverlapping(pv.as_ptr().offset(1), b.as_mut_ptr().offset(1), KEY_SIZE);
    }
    for flag in [PSEUDO_EDGE,
                 PARENT_EDGE | PSEUDO_EDGE,
                 FOLDER_EDGE | PSEUDO_EDGE,
                 PARENT_EDGE | PSEUDO_EDGE | FOLDER_EDGE]
                    .iter() {

        loop {
            let mut found = false;
            for (k, v) in branch.iter(&pv[1..(1 + KEY_SIZE)], Some(&[*flag][..])) {
                if k == &pv[1..(1 + KEY_SIZE)] && v[0] == *flag {
                    unsafe {
                        copy_nonoverlapping(v.as_ptr(), a.as_mut_ptr(), 1 + KEY_SIZE + HASH_SIZE);
                        copy_nonoverlapping(v.as_ptr().offset(1 + KEY_SIZE as isize),
                                            b.as_mut_ptr().offset(1 + KEY_SIZE as isize),
                                            HASH_SIZE);
                    }
                    b[0] = v[0] ^ PARENT_EDGE;
                    found = true
                }
                break;
            }
            if found {
                try!(branch.del(&a[1..(1 + KEY_SIZE)], Some(&b[..])));
                try!(branch.del(&b[1..(1 + KEY_SIZE)], Some(&a[..])));
            } else {
                break;
            }
        }
    }
    Ok(())
}


/// Applies a patch to a repository. "new_patches" are patches that
/// just this repository has, and the remote repository doesn't have.
pub fn apply<'b, T>(repository: &mut Transaction<T>,
                    branch_name: &str,
                    patch: &Patch,
                    internal: &'b InternalKey,
                    new_patches: &HashSet<&[u8]>)
                    -> Result<(), Error> {
    let mut db_branches = repository.db_branches();
    if db_branches.get(internal.as_slice()).is_some() {
        return Err(Error::AlreadyApplied);
    }
    let db_internal = repository.db_internal();
    let db_external = repository.db_external();
    let mut db_nodes = try!(repository.db_nodes(branch_name));
    let mut db_contents = repository.db_contents();
    {
        debug!("apply: registering {:?} in branch {:?}",
               internal.as_slice().to_hex(),
               branch_name);
        try!(db_branches.put(branch_name.as_bytes(), internal.as_slice()));
        // repository.set_db_branches(db_branches);
        debug!("done");
        try!(unsafe_apply(&db_internal,
                          &db_external,
                          &mut db_nodes,
                          &mut db_contents,
                          &patch.changes,
                          &internal,
                          &patch.dependencies));
    }
    // let cursor= unsafe {&mut *self.txn.unsafe_cursor(self.dbi_nodes).unwrap() };
    // let cursor_= unsafe {&mut *self.txn.unsafe_cursor(self.dbi_nodes).unwrap() };
    {
        let mut relatives = Vec::new();
        // repair_missing_context adds all zombie edges needed.
        let mut repair_missing_context = |db_nodes: &mut Branch<T>,
                                          direction_up: bool,
                                          c: &[u8]|
                                          -> Result<(), Error> {
            let mut context: [u8; KEY_SIZE] = [0; KEY_SIZE];
            unsafe {
                let u: &InternalKey = if c.len() > LINE_SIZE {
                    internal_hash(&db_internal, &c[0..(c.len() - LINE_SIZE)]).unwrap()
                } else {
                    internal // as &[u8]
                };
                copy_nonoverlapping(u.contents.as_ptr(), context.as_mut_ptr(), HASH_SIZE);
                copy_nonoverlapping(c.as_ptr().offset((c.len() - LINE_SIZE) as isize),
                                    context.as_mut_ptr().offset(HASH_SIZE as isize),
                                    LINE_SIZE);
            }
            // debug!(target:"missing context","{} context:{}",direction_up,context.to_hex());
            if if direction_up {
                !has_edge(&db_nodes, &context[..], PARENT_EDGE, true)
            } else {
                has_edge(&db_nodes, &context[..], PARENT_EDGE | DELETED_EDGE, true)
            } {
                relatives.clear();
                find_alive_relatives(repository,
                                     db_nodes,
                                     &context[..],
                                     if direction_up {
                                         DELETED_EDGE | PARENT_EDGE
                                     } else {
                                         DELETED_EDGE
                                     },
                                     internal,
                                     new_patches,
                                     &mut relatives);
                let mut i = 0;
                while i < relatives.len() {
                    try!(db_nodes.put(&relatives[(i + 1)..(i + 1 + KEY_SIZE)],
                                      &relatives[(i + EDGE_SIZE)..(i + 2 * EDGE_SIZE)]));
                    try!(db_nodes.put(&relatives[(i + EDGE_SIZE + 1)..(i + EDGE_SIZE + 1 +
                                                                       KEY_SIZE)],
                                      &relatives[i..(i + EDGE_SIZE)]));
                    i += 2 * EDGE_SIZE
                }
            }
            Ok(())
        };

        let mut u = [0; KEY_SIZE];
        let mut v = [0; KEY_SIZE];
        for ch in patch.changes.iter() {
            match *ch {
                Change::Edges { ref flag, ref edges } => {

                    if (*flag) & DELETED_EDGE == 0 {
                        // Handle missing context (up and down)
                        // Untested (how to generate non-deleted Change::Edges?)
                        for e in edges {
                            {
                                let int_from = try!(internal_hash(&db_internal,
                                                                  &e.from[0..(e.from.len() -
                                                                              LINE_SIZE)]));
                                let int_to = try!(internal_hash(&db_internal,
                                                                &e.to[0..(e.to.len() -
                                                                          LINE_SIZE)]));
                                unsafe {
                                    copy_nonoverlapping(int_from.contents.as_ptr(),
                                                        u.as_mut_ptr(),
                                                        HASH_SIZE);
                                    copy_nonoverlapping(e.from.as_ptr().offset((e.from.len()-LINE_SIZE) as isize),
                                                        u.as_mut_ptr().offset(HASH_SIZE as isize),
                                                        LINE_SIZE);
                                    copy_nonoverlapping(int_to.contents.as_ptr(),
                                                        v.as_mut_ptr(),
                                                        HASH_SIZE);
                                    copy_nonoverlapping(e.to.as_ptr().offset((e.to.len()-LINE_SIZE) as isize),
                                                        v.as_mut_ptr().offset(HASH_SIZE as isize),
                                                        LINE_SIZE);
                                }
                            }
                            try!(repair_missing_context(&mut db_nodes,
                                                        (*flag) & PARENT_EDGE != 0,
                                                        &u[..]));
                            try!(repair_missing_context(&mut db_nodes,
                                                        (*flag) & PARENT_EDGE == 0,
                                                        &v[..]));
                        }
                    } else
                    // DELETED_EDGE
                    if (*flag) & FOLDER_EDGE != 0 {
                        for e in edges {
                            {
                                let dest = if *flag & PARENT_EDGE != 0 {
                                    &e.from
                                } else {
                                    &e.to
                                };
                                let int_dest = try!(internal_hash(&db_internal,
                                                                  &dest[0..(dest.len() -
                                                                            LINE_SIZE)]));
                                unsafe {
                                    copy_nonoverlapping(int_dest.contents.as_ptr(),
                                                        u.as_mut_ptr(),
                                                        HASH_SIZE);
                                    copy_nonoverlapping(dest.as_ptr().offset((dest.len()-LINE_SIZE) as isize),
                                                            u.as_mut_ptr().offset(HASH_SIZE as isize),
                                                            LINE_SIZE);
                                }
                            }
                            let u_is_empty = match db_contents.contents(&u[..]) {
                                Some(cont) => cont.len() == 0,
                                None => true,
                            };
                            if u_is_empty && has_edge(&db_nodes, &u[..], 0, true) {
                                // If a deleted folder edge has alive
                                // children, reconnect it to the root.
                                try!(reconnect_zombie_folder(&mut db_nodes, &u[..], internal));
                            }
                        }
                    }
                }
                Change::NewNodes { ref up_context, ref down_context, .. } => {
                    // Handle missing contexts.
                    for c in up_context {
                        try!(repair_missing_context(&mut db_nodes, true, c))
                    }
                    for c in down_context {
                        try!(repair_missing_context(&mut db_nodes, false, c))
                    }
                    debug!("apply: newnodes, done");
                }
            }
        }
    }
    // unsafe {
    // lmdb::mdb_cursor_close(cursor);
    // lmdb::mdb_cursor_close(cursor_);
    // }
    // let time2=time::precise_time_s();
    let mut db_revdep = repository.db_revdep();
    for ref dep in patch.dependencies.iter() {
        let dep_internal = try!(internal_hash(&db_internal, &dep)).contents.to_vec();
        try!(db_revdep.put(&dep_internal, &internal.contents));
    }

    try!(db_nodes.commit_branch(branch_name));
    // let time3=time::precise_time_s();
    // info!(target:"libpijul","deps took: {}", time3-time2);
    Ok(())
}


fn find_alive_relatives<T>(repository: &Transaction<T>,
                           branch: &Branch<T>,
                           a: &[u8],
                           direction: u8,
                           patch_id: &InternalKey,
                           new_patches: &HashSet<&[u8]>,
                           relatives: &mut Vec<u8>) {
    // let cursor= unsafe { &mut * self.txn.unsafe_cursor(self.dbi_nodes).unwrap() };
    fn connect<T>(repository: &Transaction<T>,
                  branch: &Branch<T>,
                  a: &[u8],
                  direction: u8,
                  result: &mut Vec<u8>,
                  // buffer:&mut Vec<u8>,
                  patch_id: &InternalKey,
                  new_patches: &HashSet<&[u8]>) {
        // different from root
        if ROOT_KEY != a {
            let db_external = repository.db_external();
            let mut i = result.len();
            for (k, neighbor) in branch.iter(a, Some(&[direction][..])) {
                if k == a && neighbor[0] <= direction | PSEUDO_EDGE {
                    // Is this neighbor from one of the newly applied patches?
                    let is_new = if &neighbor[(1 + KEY_SIZE)..] == patch_id.as_slice() {
                        false
                    } else {
                        let ext = external_hash(&db_external, &neighbor[(1 + KEY_SIZE)..]);
                        new_patches.contains(ext)
                    };
                    if is_new {
                        result.push((neighbor[0] ^ PARENT_EDGE) ^ DELETED_EDGE);
                        result.extend(a);
                        result.extend(&patch_id.contents);

                        result.push(neighbor[0] ^ DELETED_EDGE);
                        result.extend(&neighbor[1..(1 + KEY_SIZE)]);
                        result.extend(&patch_id.contents);
                    }
                } else {
                    break;
                }
            }
            let j = result.len();
            debug_assert!(a.len() == KEY_SIZE);
            debug_assert!(patch_id.contents.len() == HASH_SIZE);
            // debug!(target:"alive","a={}",a.to_hex());
            let mut copy = [0; KEY_SIZE];
            while i < j {
                unsafe {
                    copy_nonoverlapping(result.as_ptr()
                                              .offset((i + 2 + KEY_SIZE + HASH_SIZE) as isize),
                                        copy.as_mut_ptr(),
                                        KEY_SIZE);
                }
                connect(repository,
                        branch,
                        &copy[..],
                        direction,
                        result,
                        // buffer,
                        patch_id,
                        new_patches);
                i += 2 * (1 + KEY_SIZE + HASH_SIZE)
            }
            // buffer.truncate(i0)
        }
    }
    // let mut buf=Vec::with_capacity(4*KEY_SIZE);
    connect(repository,
            branch,
            a,
            direction,
            relatives,
            patch_id,
            new_patches);
    // unsafe { lmdb::mdb_cursor_close(cursor); }
}


fn reconnect_zombie_folder<T>(branch: &mut Branch<T>,
                              a: &[u8],
                              patch_id: &InternalKey)
                              -> Result<(), Error> {
    fn connect<T>(branch: &mut Branch<T>, a: &[u8], patch_id: &InternalKey, edges: &mut Vec<u8>) {

        if a != ROOT_KEY && !has_edge(branch, &a, PARENT_EDGE | FOLDER_EDGE, false) {
            let i = edges.len();
            for (k, neighbor) in
                branch.iter(a, Some(&[PARENT_EDGE | DELETED_EDGE | FOLDER_EDGE][..])) {
                if k == a && neighbor[0] <= PARENT_EDGE | DELETED_EDGE | FOLDER_EDGE | PSEUDO_EDGE {
                    // debug!(target:"missing context","pushing from {}",a.to_hex());
                    // debug!(target:"missing context","pushing {}",neighbor.to_hex());
                    edges.push(FOLDER_EDGE);
                    edges.extend(a);
                    edges.extend(&patch_id.contents);
                    edges.push(PARENT_EDGE | FOLDER_EDGE);
                    edges.extend(&neighbor[1..(1 + KEY_SIZE)]);
                    edges.extend(&patch_id.contents);
                } else {
                    break;
                }
            }
            let mut j = i;
            let l = edges.len();
            let mut neighbor = [0; KEY_SIZE];
            while j < l {
                unsafe {
                    copy_nonoverlapping(edges.as_ptr().offset((j + EDGE_SIZE + 1) as isize),
                                        neighbor.as_mut_ptr(),
                                        KEY_SIZE)
                }
                connect(branch, &neighbor[..], patch_id, edges);
                j += 2 * EDGE_SIZE
            }
        }
        // debug!(target:"missing context","/connect zombie: {}",a.to_hex());
    }
    // let mut buf=Vec::with_capacity(4*KEY_SIZE);
    let mut edges = Vec::new();
    // let cursor= unsafe { &mut * self.txn.unsafe_cursor(self.dbi_nodes).unwrap() };
    connect(branch, a, patch_id, &mut edges);
    debug!("edges.len()={}", edges.len());
    let mut i = 0;
    while i < edges.len() {
        try!(branch.put(&edges[(i + 1)..(i + 1 + KEY_SIZE)],
                        &edges[(i + EDGE_SIZE)..(i + 2 * EDGE_SIZE)]));
        try!(branch.put(&edges[(i + EDGE_SIZE + 1)..(i + EDGE_SIZE + 1 + KEY_SIZE)],
                        &edges[i..(i + EDGE_SIZE)]));
        i += 2 * EDGE_SIZE
    }
    // unsafe { lmdb::mdb_cursor_close(cursor); }
    Ok(())
}

/// Assumes all patches have been downloaded. The third argument
/// `remote_patches` needs to contain at least all the patches we want
/// to apply, and the fourth one `local_patches` at least all the patches the other
/// party doesn't have.
pub fn apply_patches<T>(repository: &mut Transaction<T>,
                        branch_name: &str,
                        r: &Path,
                        remote_patches: &HashSet<Vec<u8>>,
                        local_patches: &HashSet<Vec<u8>>)
                        -> Result<(), Error> {
    debug!("local {}, remote {}",
           local_patches.len(),
           remote_patches.len());
    let pullable = remote_patches.difference(&local_patches);
    let only_local = {
        let mut only_local: HashSet<&[u8]> = HashSet::new();
        for i in local_patches.difference(&remote_patches) {
            only_local.insert(&i[..]);
        }
        only_local
    };
    fn apply_patches<'a, T>(repository: &mut Transaction<'a, T>,
                            branch_name: &str,
                            repo_root: &Path,
                            patch_hash: &[u8],
                            patches_were_applied: &mut bool,
                            only_local: &HashSet<&[u8]>)
                            -> Result<(), Error> {
        if !try!(has_patch(repository, branch_name, patch_hash)) {
            let patch = try!(Patch::from_repository(repo_root, patch_hash));
            debug!("Applying patch {:?}", patch_hash.to_hex());
            for dep in patch.dependencies.iter() {
                debug!("Applying dependency {:?}", dep.to_hex());
                try!(apply_patches(repository,
                                   branch_name,
                                   repo_root,
                                   &dep,
                                   patches_were_applied,
                                   only_local))
            }
            debug!("Now applying patch {:?}", patch);
            let internal = new_internal(repository);
            // println!("pulling and applying patch {}",to_hex(patch_hash));
            try!(apply(repository, branch_name, &patch, &internal, only_local));
            *patches_were_applied = true;
            // This is not necessary anymore, output_files does this.
            // sync_file_additions(repository, &patch.changes[..],&HashMap::new(), &internal);
            try!(register_hash(repository, &internal, patch_hash));
            Ok(())
        } else {
            debug!("Patch {:?} has already been applied", patch_hash);
            Ok(())
        }
    }
    // let current_branch=self.get_current_branch().to_vec();
    // let branch = repository.db_nodes(branch_name);
    let pending = {
        let (changes, _) = try!(super::record::record(repository, branch_name, &r));
        let mut p = Patch::empty();
        p.changes = changes;
        p
    };
    let mut patches_were_applied = false;
    for p in pullable {
        try!(apply_patches(repository,
                           branch_name,
                           &r,
                           p,
                           &mut patches_were_applied,
                           &only_local))
    }
    debug!("patches applied? {}", patches_were_applied);
    if cfg!(debug_assertions) {
        debug!("debugging");
        let mut buffer = BufWriter::new(File::create(r.join("debug_")).unwrap());
        repository.debug(branch_name, &mut buffer);
        debug!("/debugging");
    }
    if patches_were_applied {
        try!(repository.write_changes_file(branch_name, r));
        debug!("output_repository");
        try!(super::output::output_repository(repository, branch_name, &r, &pending));
        debug!("done outputting_repository");
    }
    let mut f = File::create(r.join("dump")).unwrap();
    try!(repository.dump(&mut f));
    debug!("finished apply_patches");
    Ok(())
}

/// Apply a patch from a local record: register it, give it a hash, and then apply.
pub fn apply_local_patch<T>(repository: &mut Transaction<T>,
                            branch_name: &str,
                            location: &Path,
                            patch: Patch,
                            inode_updates: &HashMap<LocalKey, Inode>)
                            -> Result<(), Error> {
    info!("registering a patch with {} changes: {:?}",
          patch.changes.len(),
          patch);
    let patch = Arc::new(patch);
    let child_patch = patch.clone();
    let patches_dir = patches_dir(location);
    let hash_child = thread::spawn(move || {
        let t0 = time::precise_time_s();
        let hash = child_patch.save(&patches_dir);
        let t1 = time::precise_time_s();
        info!("hashed patch in {}s", t1 - t0);
        hash
    });
    let join = hash_child.join();

    let t0 = time::precise_time_s();
    let internal: &InternalKey = &new_internal(repository);// InternalKey::new( &internal );
    debug!("applying patch");
    try!(apply(repository, branch_name, &patch, internal, &HashSet::new()));
    debug!("synchronizing tree: {:?}", inode_updates);
    {
        let branch = try!(repository.db_nodes(branch_name));
        let mut db_inodes = repository.db_inodes();
        let mut db_revinodes = repository.db_revinodes();
        {
            let mut key = [0; 3 + KEY_SIZE];
            unsafe {
                copy_nonoverlapping(internal.contents.as_ptr(),
                                    key.as_mut_ptr().offset(3),
                                    HASH_SIZE)
            }
            for (local_key, inode) in inode_updates.iter() {
                unsafe {
                    copy_nonoverlapping(local_key.as_ptr().offset(2),
                                        key.as_mut_ptr().offset(3 + HASH_SIZE as isize),
                                        LINE_SIZE);
                    copy_nonoverlapping(local_key.as_ptr(), key.as_mut_ptr().offset(1), 2);
                }
                // If this file addition was finally recorded (i.e. in dbi_nodes)
                debug!("apply_local_patch: {:?}", key.to_hex());
                if branch.get(&key[3..]).is_some() {
                    debug!("it's in here!: {:?} {:?}", key.to_hex(), inode.to_hex());
                    try!(db_inodes.put(inode.as_ref(), &key[..]));
                    try!(db_revinodes.put(&key[3..], inode.as_ref()));
                }
            }
        }
        debug!("committing branch");
        try!(branch.commit_branch(branch_name));
        if cfg!(debug_assertions) {
            debug!("debugging");
            let mut buffer = BufWriter::new(File::create(location.join("debug_")).unwrap());
            repository.debug(branch_name, &mut buffer);

            let mut f = File::create(location.join("dump")).unwrap();
            try!(repository.dump(&mut f));
            debug!("/debugging");
        }
    }
    let t2 = time::precise_time_s();
    info!("applied patch in {}s", t2 - t0);
    match join {
        Ok(Ok(hash)) => {
            try!(register_hash(repository, internal, &hash[..]));
            debug!("hash={}, local={}", hash.to_hex(), internal.to_hex());
            try!(repository.write_changes_file(branch_name, location));
            let t3 = time::precise_time_s();
            info!("changes files took {}s to write", t3 - t2);
            Ok(())
        }
        Ok(Err(x)) => Err(x),
        Err(_) => panic!("saving patch"),
    }
}
