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

use libc::{c_char,c_int,c_void,c_uint};
extern crate libpijul;
use libpijul::*;
use libpijul::backend::Branch;
use libpijul::patch::{KEY_SIZE,LocalKey,Patch};
use std::ffi::CString;
use std::path::{Path};
use std::collections::{HashMap,HashSet};

use std::os::unix::io::{FromRawFd};


#[no_mangle]
pub extern "C" fn pijul_open_repository(path:*const c_char,repository:*mut *mut c_void) -> c_int {
    unsafe {
        let p=std::str::from_utf8_unchecked(std::ffi::CStr::from_ptr(path).to_bytes());
        let path=Path::new(p);
        match Repository::open(&path){
            Ok(repo)=>{
                *repository = std::mem::transmute(Box::new(repo));
                0
            },
            Err(_)=>{
                -1
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn pijul_close_repository(repository:*const c_void) {
    unsafe {
        let r:Box<Repository>=std::mem::transmute(repository);
        std::mem::drop(*r)
    }
}

#[no_mangle]
pub unsafe extern "C" fn pijul_mut_txn_begin(repository:*const c_void, transaction:*mut *mut c_void) -> c_int {
    let r:Box<Repository>=std::mem::transmute(repository);
    let result = match r.mut_txn_begin() {
        Ok(t) => {
            let m = libc::malloc(std::mem::size_of::<usize>()) as *mut *mut c_void;
            *m = std::mem::transmute(Box::new(t));
            *transaction = std::mem::transmute(m);
            0
        },
        _ => {
            -1
        }
    };
    std::mem::forget(r);
    result
}


#[no_mangle]
pub unsafe extern "C" fn pijul_mut_txn_destroy(transaction:*mut c_void) {
    let r:*mut *mut c_void = std::mem::transmute(transaction);
    let t:*mut c_void = *r;
    if !t.is_null() {
        let r:Box<Transaction> = std::mem::transmute(*r);
        println!("free: abort/destroy");
        std::mem::drop(r)
    }
    println!("free: destroy");
    libc::free(transaction)
}

#[no_mangle]
pub unsafe extern "C" fn pijul_mut_txn_commit(transaction:*mut c_void) -> c_int {
    let r:*mut *mut c_void = std::mem::transmute(transaction);
    if (*r).is_null() {
        -1
    } else {
        let t:Box<Transaction>=std::mem::transmute(*r);
        let result = if let Ok(()) = t.commit() {
            0
        } else {
            -1
        };
        println!("free: commit");
        *r = std::ptr::null_mut();
        result
    }
}



#[no_mangle]
pub unsafe extern "C" fn pijul_empty_patch()->*mut c_void {
    std::mem::transmute(Box::new(Patch::empty()))
}
#[no_mangle]
pub unsafe extern "C" fn pijul_destroy_patch(patch:*mut c_void) {
    let r:Box<Patch> = std::mem::transmute(patch);
    std::mem::drop(r)
}




#[no_mangle]
pub unsafe extern "C" fn pijul_add_file(transaction:*mut c_void,path:*const c_char,is_dir:c_int)->c_int {
    let p=std::str::from_utf8_unchecked(std::ffi::CStr::from_ptr(path).to_bytes());
    let path=Path::new(p);
    let mut repository:Box<Transaction>=std::mem::transmute(transaction);
    let result = match repository.add_file(&path,is_dir!=0) {
        Ok(_) => 0,
        _ => -1
    };
    std::mem::forget(repository);
    result
}

#[no_mangle]
pub unsafe extern "C" fn pijul_list_files(transaction:*mut c_void, p_c_list:*mut *mut *mut c_char, c_len:*mut c_uint)->c_int {
    let repository:Box<Transaction>=std::mem::transmute(transaction);
    let result = match repository.list_files() {
        Ok(list) => {

            let c_list = libc::malloc(list.len() * std::mem::size_of::<*const c_void>()) as *mut *mut c_char;
            *p_c_list = c_list;

            let mut i = 0;
            for x in list.iter() {
                if let Some(y) = x.to_str().and_then(|y| CString::new(y).ok()) {
                    *(c_list.offset(i)) = y.into_raw()
                } else {
                    for _ in 0..i {
                        libc::free(*(c_list.offset(i)) as *mut c_void)
                    }
                    libc::free(c_list as *mut c_void);
                    *p_c_list = std::ptr::null_mut();
                    return -1
                }
                i+=1
            }
            *c_len = list.len() as c_uint;
            0
        },
        _ => -1
    };
    std::mem::forget(repository);
    result
}

#[no_mangle]
pub extern "C" fn pijul_move_file(repository:*mut c_void,patha:*const c_char,pathb:*const c_char,is_dir:c_int)->c_int {
    unsafe {
        let pa=std::str::from_utf8_unchecked(std::ffi::CStr::from_ptr(patha).to_bytes());
        let patha=Path::new(pa);
        let pb=std::str::from_utf8_unchecked(std::ffi::CStr::from_ptr(pathb).to_bytes());
        let pathb=Path::new(pb);
        let mut repository:Box<Transaction>=std::mem::transmute(repository);
        let result = match repository.move_file(&patha,&pathb,is_dir!=0) {
            Ok(_) => 0,
            _ => -1
        };
        std::mem::forget(repository);
        result
    }
}

#[no_mangle]
pub extern "C" fn pijul_remove_file(repository:*mut c_void,path:*const c_char)->c_int {
    unsafe {
        let p=std::str::from_utf8_unchecked(std::ffi::CStr::from_ptr(path).to_bytes());
        let path=Path::new(p);
        let mut repository:Box<Transaction>=std::mem::transmute(repository);
        let result = match repository.remove_file(&path) {
            Ok(_) => 0,
            _ => -1
        };
        std::mem::forget(repository);
        result
    }
}

#[no_mangle]
pub unsafe extern "C" fn pijul_get_branch(repository:*mut c_void, c_branch:*const c_char, r:*mut *mut c_void) -> c_int {
    
    let repository:Box<Transaction>=std::mem::transmute(repository);
    let branch=std::str::from_utf8_unchecked(std::ffi::CStr::from_ptr(c_branch).to_bytes());
    let result = if let Ok(branch) = repository.db_nodes(branch) {
        *r = std::mem::transmute(Box::new(branch));
        0
    } else {
        -1
    };
    std::mem::forget(repository);
    result
}


#[no_mangle]
pub unsafe extern "C" fn pijul_retrieve_and_output(repository:*mut c_void, c_branch:*const c_void, c_key:*const c_char, output:c_int) -> c_int {
    
    let repository:Box<Transaction>=std::mem::transmute(repository);
    let branch:Box<Branch<_>>=std::mem::transmute(c_branch);
    // let branch=std::str::from_utf8_unchecked(std::ffi::CStr::from_ptr(c_branch).to_bytes());
    let key=std::slice::from_raw_parts(c_key as *const u8, KEY_SIZE);
    let mut file = std::fs::File::from_raw_fd(output);
    let result = if let Ok(_) = repository.retrieve_and_output(&branch, key, &mut file) {
        0
    } else {
        -1
    };
    std::mem::forget(repository);
    std::mem::forget(branch);
    result
}

#[no_mangle]
pub unsafe extern "C" fn pijul_apply_patches(repository:*mut c_void,
                                             c_branch:*const c_char,
                                             c_path:*const c_char,
                                             c_remote_patches:*const c_void,
                                             c_local_patches:*const c_void) -> c_int {
    
    let mut repository:Box<Transaction>=std::mem::transmute(repository);
    let branch=std::str::from_utf8_unchecked(std::ffi::CStr::from_ptr(c_branch).to_bytes());
    let path=std::str::from_utf8_unchecked(std::ffi::CStr::from_ptr(c_path).to_bytes());
    let remote_patches:Box<HashSet<Vec<u8>>> =
        if c_remote_patches.is_null() {
            Box::new(HashSet::new())
        } else {
            std::mem::transmute(c_remote_patches)
        };
    let local_patches:Box<HashSet<Vec<u8>>> =
        if c_remote_patches.is_null() {
            Box::new(HashSet::new())
        } else {
            std::mem::transmute(c_local_patches)
        };
    let result = if let Ok(()) = repository.apply_patches(branch, path, &remote_patches, &local_patches) {
        0
    } else {
        -1
    };
    std::mem::forget(repository);
    if !c_remote_patches.is_null() {
        std::mem::forget(remote_patches);
    }
    if !c_local_patches.is_null() {
        std::mem::forget(local_patches);
    }
    result
}

#[no_mangle]
pub unsafe extern "C" fn pijul_apply_local_patch(transaction:*mut c_void,
                                                 c_branch:*const c_char,
                                                 c_path:*const c_char,
                                                 c_patch:*const c_void,
                                                 c_inode_updates:*const c_void) -> c_int {
    
    let mut transaction:Box<Transaction>=std::mem::transmute(transaction);
    let branch=std::str::from_utf8_unchecked(std::ffi::CStr::from_ptr(c_branch).to_bytes());
    let path=std::str::from_utf8_unchecked(std::ffi::CStr::from_ptr(c_path).to_bytes());
    let patch:Box<Patch> = std::mem::transmute(c_patch);
    let inode_updates:Box<HashMap<LocalKey, Inode>> = std::mem::transmute(c_inode_updates);

    let result = if let Ok(()) = transaction.apply_local_patch(branch, path, *patch, &inode_updates) {
        0
    } else {
        -1
    };
    std::mem::forget(transaction);
    std::mem::forget(inode_updates);
    result
}


#[no_mangle]
pub unsafe extern "C" fn pijul_record(repository:*mut c_void,
                                      c_branch:*const c_char,
                                      c_path:*const c_char,
                                      c_patch:*mut *mut c_void,
                                      c_inode_updates: *mut *mut c_void) -> c_int {
    
    let mut repository:Box<Transaction>=std::mem::transmute(repository);
    let branch=std::str::from_utf8_unchecked(std::ffi::CStr::from_ptr(c_branch).to_bytes());
    let path=std::str::from_utf8_unchecked(std::ffi::CStr::from_ptr(c_path).to_bytes());
    let result = if let Ok((patch, inode_updates)) = repository.record(path, branch) {
        *c_patch = std::mem::transmute(Box::new(patch));
        *c_inode_updates = std::mem::transmute(Box::new(inode_updates));
        0
    } else {
        -1
    };
    std::mem::forget(repository);
    result
}


#[no_mangle]
pub unsafe extern "C" fn pijul_output_repository(repository:*mut c_void,
                                                 c_branch:*const c_char,
                                                 c_working_copy:*const c_char,
                                                 c_pending:*const c_void) -> c_int {
    
    let mut repository:Box<Transaction>=std::mem::transmute(repository);
    let pending:Box<Patch> = std::mem::transmute(c_pending);
    let branch=std::str::from_utf8_unchecked(std::ffi::CStr::from_ptr(c_branch).to_bytes());
    let working_copy=std::str::from_utf8_unchecked(std::ffi::CStr::from_ptr(c_working_copy).to_bytes());
    let result = if let Ok(()) = repository.output_repository(branch, working_copy, &pending) {
        0
    } else {
        -1
    };
    std::mem::forget(repository);
    std::mem::forget(pending);
    result
}

