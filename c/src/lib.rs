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

use libc::{c_char,c_int,c_void,malloc,size_t};
extern crate libpijul;
use libpijul::*;
use std::ffi::CStr;
use std::ffi::CString;
use std::path::{Path};
use std::ptr::copy_nonoverlapping;
use std::collections::HashMap;
#[no_mangle]
pub extern "C" fn pijul_open_repository(path:*const c_char,repository:*mut *mut c_void) -> c_int {
    unsafe {
        let p=std::str::from_utf8_unchecked(std::ffi::CStr::from_ptr(path).to_bytes());
        let path=Path::new(p);
        match Repository::new(&path){
            Ok(repo)=>{
                *repository=std::mem::transmute(Box::new(repo));
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
        let repository:Box<Repository>=std::mem::transmute(repository);
    }
}


#[no_mangle]
pub extern "C" fn pijul_add_file(repository:*mut c_void,path:*const c_char,is_dir:c_int) {
    unsafe {
        let p=std::str::from_utf8_unchecked(std::ffi::CStr::from_ptr(path).to_bytes());
        let path=Path::new(p);
        let repository:&mut Repository=std::mem::transmute(repository);
        repository.add_file(&path,is_dir!=0);
    }
}

#[no_mangle]
pub extern "C" fn pijul_move_file(repository:*mut c_void,patha:*const c_char,pathb:*const c_char,is_dir:c_int) {
    unsafe {
        let pa=std::str::from_utf8_unchecked(std::ffi::CStr::from_ptr(patha).to_bytes());
        let patha=Path::new(pa);
        let pb=std::str::from_utf8_unchecked(std::ffi::CStr::from_ptr(pathb).to_bytes());
        let pathb=Path::new(pb);
        let repository:&mut Repository=std::mem::transmute(repository);
        repository.move_file(&patha,&pathb,is_dir!=0);
    }
}


#[no_mangle]
pub extern "C" fn pijul_remove_file(repository:*mut c_void,path:*const c_char) {
    unsafe {
        let p=std::str::from_utf8_unchecked(std::ffi::CStr::from_ptr(path).to_bytes());
        let path=Path::new(p);
        let repository:&mut Repository=std::mem::transmute(repository);
        repository.remove_file(&path);
    }
}


#[no_mangle]
pub extern "C" fn pijul_get_current_branch(repository:*mut c_void)->*mut c_char {
    unsafe {
        let repository:&mut Repository=std::mem::transmute(repository);
        let cur=repository.get_current_branch();
        let p:*mut c_char=malloc(cur.len()+1) as *mut c_char;
        *(p.offset(cur.len() as isize))=0;
        copy_nonoverlapping(cur.as_ptr() as *const c_void,p as *mut c_void,cur.len() as size_t);
        p
    }
}

#[no_mangle]
pub extern "C" fn pijul_new_internal(repository:*mut c_void,result:*mut c_char) {
    unsafe {
        let repository:&mut Repository=std::mem::transmute(repository);
        repository.new_internal(std::slice::from_raw_parts_mut(result as *mut u8,HASH_SIZE));
    }
}


#[no_mangle]
pub extern "C" fn pijul_register_hash(repository:*mut c_void,internal:*mut c_char,external:*mut c_char,external_len:size_t) {
    unsafe {
        let repository:&mut Repository=std::mem::transmute(repository);
        repository.register_hash(std::slice::from_raw_parts_mut(internal as *mut u8,HASH_SIZE),
                                 std::slice::from_raw_parts_mut(external as *mut u8,external_len))
    }
}


#[no_mangle]
pub extern "C" fn pijul_record(repository:*mut c_void,working_copy:*const c_char,changes:*mut *mut c_void, updates:*mut*mut c_void)->c_int {
    unsafe {
        let repository:&mut Repository=std::mem::transmute(repository);
        let p=std::str::from_utf8_unchecked(std::ffi::CStr::from_ptr(working_copy).to_bytes());
        let path=Path::new(p);
        match repository.record(&path) {
            Ok((a,b))=>{
                *changes=std::mem::transmute(Box::new(a));
                *updates=std::mem::transmute(Box::new(b));
                0
            },
            Err(_)=> (-1)
        }
    }
}


#[no_mangle]
pub extern "C" fn pijul_has_patch(repository:*mut c_void,branch:*const c_char, external_hash:*const c_char, external_hash_len:size_t)->c_int {
    unsafe {
        let repository:&mut Repository=std::mem::transmute(repository);
        let branch=std::ffi::CStr::from_ptr(branch).to_bytes();
        let hash=std::slice::from_raw_parts(external_hash as *const u8,external_hash_len as usize);
        match repository.has_patch(branch,hash) {
            Ok(true)=>1,
            Ok(false)=>0,
            _=> (-1)
        }
    }
}



#[no_mangle]
pub extern "C" fn pijul_new_patch(changes:*const c_void)->*const c_void {
    unsafe {
        let changes:Box<Vec<Change>>=std::mem::transmute(changes);
        std::mem::transmute(Box::new(Patch::new(*changes)))
    }
}


#[no_mangle]
pub extern "C" fn pijul_apply(repository:*mut c_void,patch:*const c_void,internal:*const c_char)->c_int {
    unsafe {
        let repository:&mut Repository=std::mem::transmute(repository);
        let patch:&Patch=std::mem::transmute(patch);
        let internal:&[u8]=std::slice::from_raw_parts(internal as *const u8, HASH_SIZE as usize);
        match repository.apply(patch,internal) {
            Ok(())=>0,
            Err(_)=>(-1)
        }
    }
}

#[no_mangle]
pub extern "C" fn pijul_write_changes_file(repository:*mut c_void,path:*const c_char)->c_int {
    unsafe {
        let repository:&mut Repository=std::mem::transmute(repository);
        let p=std::str::from_utf8_unchecked(std::ffi::CStr::from_ptr(path).to_bytes());
        let path=Path::new(p);
        match repository.write_changes_file(path) {
            Ok(())=>0,
            Err(_)=>(-1)
        }
    }
}


#[no_mangle]
pub extern "C" fn pijul_sync_file_additions(repository:*mut c_void,changes:*const c_void,updates:*const c_void,internal:*const c_char) {
    unsafe {
        let repository:&mut Repository=std::mem::transmute(repository);
        let changes:&Vec<Change>=std::mem::transmute(changes);
        let updates:&HashMap<LocalKey,Inode>=std::mem::transmute(updates);
        let internal=std::slice::from_raw_parts(internal as *const u8,HASH_SIZE as usize);
        repository.sync_file_additions(changes,updates,internal)
    }
}


#[no_mangle]
pub extern "C" fn pijul_output_repository(repository:*mut c_void,working_copy:*const c_char,pending:*const c_void)->c_int {
    unsafe {
        let repository:&mut Repository=std::mem::transmute(repository);
        let p=std::str::from_utf8_unchecked(std::ffi::CStr::from_ptr(working_copy).to_bytes());
        let path=Path::new(p);
        let pending:&Patch=std::mem::transmute(pending);

        match repository.output_repository(path,pending) {
            Ok(())=>0,
            Err(_)=>(-1)
        }
    }
}
