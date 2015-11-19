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

use libc::{c_char,c_int,c_void};
extern crate libpijul;
use libpijul::*;
use std::ffi::CStr;
use std::path::{Path};

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
        let repository:Box<Repository>=std::mem::transmute(repository);
        add_file(&mut repository,&path,is_dir!=0);
    }
}
