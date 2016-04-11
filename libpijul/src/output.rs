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
use super::patch::{Patch,KEY_SIZE,ROOT_KEY,HASH_SIZE,LINE_SIZE,InternalKey,LocalKey, new_internal, register_hash};
use super::graph::{PSEUDO_EDGE,FOLDER_EDGE,PARENT_EDGE, DIRECTORY_FLAG,LineBuffer, retrieve, output_file};
use super::file_operations::{INODE_SIZE,ROOT_INODE, create_new_inode, Inode};
use super::error::Error;
use super::fs_representation::patches_dir;
use super::apply::{apply,has_edge};
use super::Len;

use rustc_serialize::hex::ToHex;
use std::path::{Path,PathBuf};
use std::collections::{HashMap,HashSet};
use std::collections::hash_map::Entry;
use std;
use std::fs;
use std::fs::File;
use std::ptr;
use std::ptr::copy_nonoverlapping;
use time;
use std::sync::Arc;
use std::thread;
use std::io::BufWriter;

// Used between functions of unsafe_output_repository (Rust does not allow enum inside the class)
enum Tree {
    Move { tree_key:Vec<u8>,tree_value:Vec<u8> },
    Addition { tree_key:Vec<u8>,tree_value:Vec<u8> },
    NameConflict { inode:Vec<u8> }
}


// Climp up the tree (using revtree).
fn filename_of_inode(db_revtree:&Db,inode:&[u8],working_copy:&mut PathBuf)->bool {
    //let mut v_inode=MDB_val{mv_data:inode.as_ptr() as *const c_void, mv_size:inode.len() as size_t};
    //let mut v_next:MDB_val = unsafe {std::mem::zeroed()};
    let mut components=Vec::new();
    let mut current=inode;
    loop {
        match db_revtree.get(current) {
            Some(v) => {
                /*debug!(target:"output_repository","filename_of_inode {}{}",
                &v[0..INODE_SIZE].to_hex(),
                std::str::from_utf8(&v[INODE_SIZE..]).unwrap());*/
                components.push(&v[INODE_SIZE..]);
                current=&v[0..INODE_SIZE];
                if current == ROOT_INODE.as_ref() {
                    break
                }
            },
            None => return false
        }
    }
    for c in components.iter().rev() {
        working_copy.push(std::str::from_utf8(c).unwrap());
    }
    true
}

pub fn retrieve_paths<'a,'b>(branch:&'a Db<'a,'b>, db_contents:&'a Db<'a,'b>,
                             key:&[u8], flag:u8) -> Vec<(Contents<'a>,&'a[u8])> {
    let mut result=Vec::new();
    for (k,b) in branch.iter(key,Some(&[flag][..])) {
        if k==key && b[0] <= flag|PSEUDO_EDGE {

            if let Some(cont_b) = db_contents.contents(&b[1..(1+KEY_SIZE)]) {

                //let filename=&cont_b[2..];
                //let perms= ((cont_b[0] as usize) << 8) | (cont_b[1] as usize);
                for (k,c) in branch.iter(&b[1..(1+KEY_SIZE)], Some(&[flag][..])) {
                    if k==&b[1..(1+KEY_SIZE)] && c[0] <= flag|PSEUDO_EDGE {
                        let cv=&c[1..(1+KEY_SIZE)];
                        result.push((cont_b.clone(),cv))
                    } else {
                        break
                    }
                }
            } else {
                panic!("file without contents: {:?}", (&b[1..(1+KEY_SIZE)]).to_hex())
            }
        } else {
            break
        }
    }
    result
}

/// Returns the path's inode
pub fn follow_path(db_tree:&Db, path:&[&[u8]])->Result<Option<Vec<u8>>,Error> {
    // follow in tree, return inode
    let mut buf=vec![0;INODE_SIZE];
    for p in path {
        buf.extend(*p);
        //println!("follow: {:?}",buf.to_hex());
        match db_tree.get(&buf) {
            Some(v)=> {
                //println!("some: {:?}",v.to_hex());
                buf.clear();
                buf.extend(v)
            },
            None => {
                //println!("none");
                return Ok(None)
            }
        }
    }
    Ok(Some(buf))
}

/// Returns the node's properties
pub fn node_of_inode<'a,'b>(db_inodes:&'a Db<'a,'b>, inode:&[u8]) -> Option<&'a [u8]> {
    // follow in tree, return inode
    if inode == ROOT_INODE.as_ref() {
        Some(ROOT_KEY)
    } else {
        let node=db_inodes.get(&inode);
        node
    }
}

fn output_aux(branch:&Db,
              db_contents:&Db,
              db_inodes:&mut Db,
              db_revinodes:&mut Db,
              db_tree:&mut Db,
              db_revtree:&mut Db,
              working_copy:&Path,
              do_output:bool,
              visited:&mut HashMap<Vec<u8>,Vec<PathBuf>>,
              path:&mut PathBuf,
              key:&[u8],
              inode:&[u8],
              moves:&mut Vec<Tree>)->Result<(),Error> {
    debug!(target:"output_repository","visited {}",key.to_hex());
    moves.clear();
    debug_assert!(inode.len()==INODE_SIZE);
    debug_assert!(key.len()==KEY_SIZE);
    let mut recursive_calls=Vec::new();
    let mut new_inodes:HashMap<Vec<u8>,(usize,&[u8])>=HashMap::new();
    // This function is globally a DFS, but has two phases,
    // one for collecting actions (and moving files around on
    // the filesystem), and the other one for updating and
    // preparing the next level.
    //
    // This is because the database cannot be updated while being iterated over.
    let mut filename_buffer = Vec::new();
    for (k,b) in branch.iter(key,Some(&[FOLDER_EDGE][..]))
        .take_while(|&(k,b)| k==key && b[0]<=FOLDER_EDGE|PSEUDO_EDGE) {

            //debug!(target:"output_repository","b={}",to_hex(b));
            let cont_b = db_contents.contents(&b[1..(1+KEY_SIZE)]).unwrap();
            debug_assert!(cont_b.len()>=2);
            filename_buffer.clear();
            for i in cont_b {
                filename_buffer.extend(i);
            }
            let filename_bytes=&filename_buffer[2..];
            let filename=std::str::from_utf8(filename_bytes).unwrap();
            let perms= ((filename_buffer[0] as usize) << 8) | (filename_buffer[1] as usize);

            for (k,c) in branch.iter(&b[1..(1+KEY_SIZE)], Some(&[FOLDER_EDGE][..]))
                .take_while(|&(k,c)| k==&b[1..(1+KEY_SIZE)] && c[0]<=FOLDER_EDGE|PSEUDO_EDGE) {

                    let cv=&c[1..(1+KEY_SIZE)];
                    debug!(target:"output_repository","cv={}",cv.to_hex());
                    let c_inode=
                        match db_revinodes.get(cv) {
                            Some(c_inode) => c_inode.to_vec(),
                            None => {
                                let mut v=vec![0;INODE_SIZE];
                                loop {
                                    create_new_inode(db_revtree, &mut v);
                                    if new_inodes.get(&v).is_none() { break }
                                }
                                new_inodes.insert(v.clone(),(perms,cv));
                                v
                            }
                        };
                    path.push(filename);
                    let mut inode_v=inode.to_vec();
                    inode_v.extend(filename_bytes);
                    match visited.entry(cv.to_vec()){
                        Entry::Occupied(mut e)=>{
                            // Help! A name conflict!
                            e.get_mut().push(path.clone());
                            println!("Name conflict between {:?}",e.get());
                            inode_v.truncate(INODE_SIZE);
                            if inode_v.iter().any(|&x| { x!=0 }) {
                                moves.push(Tree::NameConflict {
                                    inode:inode_v,
                                })
                            }
                        }
                        Entry::Vacant(e)=>{
                            e.insert(vec!(path.clone()));
                            debug!(target:"output_repository","inode={}",c_inode.to_hex());
                            {
                                let mut buf=PathBuf::from(working_copy);
                                if filename_of_inode(db_revtree, &c_inode,&mut buf) {
                                    debug!(target:"output_repository","former_path={:?}",buf);
                                    if buf.as_os_str() != path.as_os_str() {
                                        // move on filesystem
                                        debug!(target:"output_repository","moving {:?} to {:?}",buf,path);
                                        if fs::rename(&buf,&path).is_err() {
                                            let mut filename=path.file_name().unwrap().to_str().unwrap().to_string();
                                            let l=filename.len();
                                            let mut i=0;
                                            loop {
                                                filename.truncate(l);
                                                filename = filename + &format!("~{}",i);
                                                path.set_file_name(&filename);
                                                if fs::rename(&buf,&path).is_ok() { break }
                                                i+=1
                                            }
                                        }
                                        debug!(target:"output_repository","done");
                                        moves.push(Tree::Move { tree_key:inode_v,tree_value:c_inode.to_vec() })
                                    }
                                } else {
                                    debug!(target:"output_repository","no former_path");
                                    moves.push(Tree::Addition { tree_key:inode_v,tree_value:c_inode.to_vec() });
                                    if perms&DIRECTORY_FLAG==0 {
                                        std::fs::File::create(&path).unwrap();
                                    } else {
                                        std::fs::create_dir_all(&path).unwrap();
                                    }
                                };
                            }
                            if perms&DIRECTORY_FLAG==0 {
                                if do_output {
                                    let mut redundant_edges=vec!();
                                    let l = retrieve(branch, &cv).unwrap();
                                    debug!(target:"output_repository","creating file {:?}",path);
                                    let mut f=std::fs::File::create(&path).unwrap();
                                    debug!(target:"output_repository","done");
                                    output_file(branch, db_contents, &mut f,l,&mut redundant_edges);
                                }
                            } else {
                                recursive_calls.push((filename.to_string(),cv.to_vec(),c_inode.to_vec()));
                            }
                        }
                    }
                    path.pop();
                }
                debug!(target:"output_repository","/b");
    }

    // Update inodes: add files that were not on the filesystem before this output.
    let mut key=[0;3+KEY_SIZE];
    for (inode,&(perm,k)) in new_inodes.iter() {
        unsafe {
            copy_nonoverlapping(k.as_ptr(),key.as_mut_ptr().offset(3),KEY_SIZE)
        }
        key[0]=0;
        key[1]=((perm>>8) & 0xff) as u8;
        key[2]=(perm & 0xff) as u8;
        debug!(target:"output_repository","updating dbi_(rev)inodes: {} {}",inode.to_hex(),k.to_hex());
        try!(db_inodes.put(&inode,&key));
        try!(db_revinodes.put(&key[3..],&inode));
    }
    // Update the tree: add the last file moves.
    for update in &moves[..] {
        match update {
            &Tree::Move { ref tree_key,ref tree_value } => { // tree_key = inode_v
                debug!(target:"output_repository","updating move {}{} {}{}",
                       &tree_key[0..INODE_SIZE].to_hex(),std::str::from_utf8(&tree_key[INODE_SIZE..]).unwrap(),
                       &tree_value[0..INODE_SIZE].to_hex(),std::str::from_utf8(&tree_value[INODE_SIZE..]).unwrap());

                let current_parent_inode = db_revtree.get(&tree_value).unwrap().to_vec();
                debug!(target:"output_repository","current parent {}{}",
                       &current_parent_inode[0..INODE_SIZE].to_hex(),
                       std::str::from_utf8(&current_parent_inode[INODE_SIZE..]).unwrap());
                try!(db_tree.del(&current_parent_inode,Some(&tree_value)));
                try!(db_revtree.del(&tree_value,Some(&current_parent_inode)));
                try!(db_tree.put(&tree_key,&tree_value));
                try!(db_revtree.put(&tree_value,&tree_key));
            }
            &Tree::Addition { ref tree_key,ref tree_value } =>{
                try!(db_tree.put(&tree_key,&tree_value));
                try!(db_revtree.put(&tree_value,&tree_key));
            }
            &Tree::NameConflict { ref inode } => {
                // Mark the file as moved.
                let mut current_key={
                    db_inodes.get(&inode).unwrap().to_vec()
                };
                current_key[0]=1;
                try!(db_inodes.put(&current_key,&inode));
            }
        }
    }

    // Now do all the recursive calls
    for (filename,cv,c_inode) in recursive_calls {
        path.push(filename);
        debug!(target:"output_repository","> {:?}",path);
        try!(output_aux(branch,db_contents,db_inodes, db_revinodes, db_tree, db_revtree,
                        working_copy,do_output,visited,path,&cv,&c_inode,moves));
        debug!(target:"output_repository","< {:?}",path);
        path.pop();
    }
    Ok(())
}

fn unsafe_output_repository(branch:&Db, db_contents:&Db, db_inodes:&mut Db, db_revinodes:&mut Db, db_tree:&mut Db, db_revtree:&mut Db,
                            working_copy:&Path,do_output:bool)->Result<(),Error> {
    let mut visited=HashMap::new();
    let mut p=PathBuf::from(working_copy);

    let mut moves=Vec::new();

    /*let db_contents = repository.db_contents();
    let mut db_inodes = repository.db_inodes();
    let mut db_revinodes = repository.db_inodes();
    let mut db_tree = repository.db_tree();
    let mut db_revtree = repository.db_revtree();*/
    try!(output_aux(branch, db_contents, db_inodes, db_revinodes, db_tree, db_revtree,
                    working_copy,do_output,&mut visited,&mut p,ROOT_KEY,ROOT_INODE.as_ref(),&mut moves));

    // Now, garbage collect dead inodes.
    let mut dead=Vec::new();
    {
        //let curs = try!(self.txn.cursor(self.dbi_inodes));
        for (u,v) in db_inodes.iter(b"",None) {
            if ! has_edge(branch, &v[3..],PARENT_EDGE|FOLDER_EDGE,true) {
                // v is dead.
                debug!(target:"output_repository","dead:{:?} {:?}",u.to_hex(),v.to_hex());
                dead.push((u.to_vec(),(&v[3..]).to_vec()))
            }
        }
    }


    // Now, "kill the deads"
    {
        //let mut curs_tree= unsafe { &mut *try!(self.txn.unsafe_cursor(self.dbi_tree)) };
        //let mut curs_revtree= unsafe { &mut *try!(self.txn.unsafe_cursor(self.dbi_revtree)) };

        let mut uu = Vec::new();
        let mut vv = Vec::new();
        for (ref inode,ref key) in dead {
            try!(db_inodes.del(inode,None));
            try!(db_revinodes.del(key,None));
            let mut kills = Vec::new();
            // iterate through inode's relatives.
            for (k,v) in db_revtree.iter(&inode, None).take_while(|&(k,v)| k==&inode[..]) {
                kills.push((k.to_vec(), v.to_vec()));
            }
            for &(ref k,ref v) in kills.iter() {
                try!(db_tree.del(&v,Some(&k[..])));
                try!(db_tree.del(&k,Some(&v[..])));
            }

            loop {
                let mut found = false;
                for (u,v) in db_tree.iter(inode, None) {
                    found = true;
                    uu.clear();
                    uu.extend(u);
                    vv.clear();
                    vv.extend(v);
                    break
                }
                try!(db_tree.del(&uu[..], Some(&vv[..])));
                try!(db_revtree.del(&vv[..], Some(&uu[..])));
                if !found { break }
            }
        }
    }
    Ok(())
}



pub fn retrieve_and_output<'a, 'b, 'c, L:LineBuffer<'a>>(branch:&'a Db<'a,'b>, db_contents:&'a Db<'a,'c>, key:&'a [u8],l:&mut L) {
    let mut redundant_edges=vec!();
    let graph = retrieve(branch, key).unwrap();
    output_file(branch, db_contents, l, graph, &mut redundant_edges);
}

pub fn output_repository<T>(repository:&mut Transaction<T>, branch_name:&str, working_copy:&Path, pending:&Patch) -> Result<(),Error>{
    debug!(target:"output_repository","begin output repository");
    // First output the repository to change the trees/inodes tables (and their revs).
    // Do not output the files (do_output = false).
    {
        let branch = repository.db_nodes(branch_name);
        let db_contents = repository.db_contents();
        let mut db_inodes = repository.db_inodes();
        let mut db_revinodes = repository.db_inodes();
        let mut db_tree = repository.db_tree();
        let mut db_revtree = repository.db_revtree();

        try!(unsafe_output_repository(&branch, &db_contents,
                                      &mut db_inodes, &mut db_revinodes, &mut db_tree, &mut db_revtree,
                                      working_copy,false))
    };
    // Then, apply pending and output in an aborted transaction.
    let mut child_repository = repository.child();
    let internal = new_internal(&mut child_repository);
    //debug!(target:"output_repository","pending patch: {}",internal.to_hex());
    try!(apply(&mut child_repository, branch_name, pending,&internal,&HashSet::new()));
    // Now output all files (do_output=true)
    {
        let branch = child_repository.db_nodes(branch_name);
        let db_contents = child_repository.db_contents();
        let mut db_inodes = child_repository.db_inodes();
        let mut db_revinodes = child_repository.db_inodes();
        let mut db_tree = child_repository.db_tree();
        let mut db_revtree = child_repository.db_revtree();

        try!(unsafe_output_repository(&branch, &db_contents,
                                      &mut db_inodes, &mut db_revinodes, &mut db_tree, &mut db_revtree,
                                      working_copy, true))
    }
    child_repository.abort();
    Ok(())
}
