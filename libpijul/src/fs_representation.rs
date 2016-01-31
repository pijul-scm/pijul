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

use std::path::{Path,PathBuf,MAIN_SEPARATOR};
use std::fs::{metadata,create_dir_all};
extern crate rustc_serialize;
use self::rustc_serialize::hex::ToHex;
use std;

pub const PIJUL_DIR_NAME:&'static str=".pijul";

pub fn repo_dir<P:AsRef<Path>>(p : P) -> PathBuf {
    p.as_ref().join(PIJUL_DIR_NAME)
}

pub fn pristine_dir<P:AsRef<Path>>(p : P) -> PathBuf {
    return p.as_ref().join(PIJUL_DIR_NAME).join("pristine")
}

pub const PATCHES_DIR_NAME:&'static str="patches";

pub fn patches_dir<P:AsRef<Path>>(p : P) -> PathBuf {
    return p.as_ref().join(PIJUL_DIR_NAME).join(PATCHES_DIR_NAME)
}

pub fn branch_changes_base_path(b:&[u8])->String {
    "changes.".to_string() + &b.to_hex()
}

pub fn branch_changes_file(p : &Path, b: &[u8]) -> PathBuf {
    p.join(PIJUL_DIR_NAME).join(branch_changes_base_path(b))
}

pub fn meta_file(p : &Path) -> PathBuf {
    p.join(PIJUL_DIR_NAME).join("meta.toml")
}

pub fn find_repo_root<'a>(dir : &'a Path) -> Option<PathBuf> {
    let c:Vec<&std::ffi::OsStr>=dir.iter().collect();
    let mut i=c.len();
    while i>0 {
        let mut p=PathBuf::new();
        for j in 0..i {
            p.push(c[j])
        }
        p.push(PIJUL_DIR_NAME);
        debug!("trying {:?}",p);
        match metadata(&p) {
            Ok (ref attr) if attr.is_dir() => {
                p.pop();
                return Some(p)
            },
            _=>{}
        }
        p.pop();
        i-=1;
    }
    None
}

pub fn create(dir : &Path) -> std::io::Result<()> {
    let mut repo_dir = repo_dir(dir);
    try!(create_dir_all(&repo_dir));
    repo_dir.push("pristine");
    try!(create_dir_all(&repo_dir));
    repo_dir.pop();
    repo_dir.push("patches");
    try!(create_dir_all(&repo_dir));
    Ok(())
}


pub fn patch_path(root:&Path,h:&[u8])->Option<PathBuf> {
    for p in patch_path_iter(h,MAIN_SEPARATOR) {
        let p=root.join(p);
        debug!("patch_path: trying {:?}",p);
        if std::fs::metadata(&p).is_ok() {
            return Some(p)
        }
    }
    None
}

const PATCH_EXTENSIONS:[&'static str;3]=["cbor.gpg","cbor.gz","cbor"];

pub struct PatchPath<'a> {
    h:&'a [u8],
    i:usize,
    sep:char
}

impl<'a> Iterator for PatchPath<'a> {
    type Item=String;
    fn next(&mut self)->Option<String> {
        if self.i>=PATCH_EXTENSIONS.len() {
            None
        } else {
            let mut p=PIJUL_DIR_NAME.to_string();
            p.push(self.sep);
            p.push_str(PATCHES_DIR_NAME);
            p.push(self.sep);
            p.push_str(&self.h.to_hex());
            p.push('.');
            p.push_str(PATCH_EXTENSIONS[self.i]);
            self.i+=1;
            Some(p)
        }
    }
}

pub fn patch_path_iter<'a>(h:&'a[u8],sep:char)->PatchPath<'a> {
    PatchPath { h:h,i:0,sep:sep }
}

