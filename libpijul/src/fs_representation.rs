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

use std::path::Path;
use std::path::PathBuf;
use std::fs::{metadata,create_dir_all};

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
    "changes.".to_string() + &to_hex(b)
}

pub fn branch_changes_file(p : &Path, b: &[u8]) -> PathBuf {
    p.join(PIJUL_DIR_NAME).join(branch_changes_base_path(b))
}

pub fn meta_file(p : &Path) -> PathBuf {
    p.join(PIJUL_DIR_NAME).join("meta.toml")
}

pub fn find_repo_root(dir : &Path) -> Option<&Path> {
    let pijul_dir = repo_dir(dir);
    match metadata(pijul_dir) {
        Ok (attr) =>
            if attr.is_dir() {Some(dir)} else {None},
        Err(_) =>
            dir.parent().and_then(find_repo_root)
    }
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

pub fn to_hex(x:&[u8]) -> String {
    let mut v = Vec::with_capacity(x.len() * 2);
    for &byte in x.iter() {
        v.push(CHARS[(byte >> 4) as usize]);
        v.push(CHARS[(byte & 0xf) as usize]);
    }

    unsafe {
        String::from_utf8_unchecked(v)
    }
}
static CHARS: &'static[u8] = b"0123456789abcdef";
