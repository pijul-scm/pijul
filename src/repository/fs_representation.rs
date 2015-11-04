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
use std::fs::{metadata,create_dir};
use std::io;

pub fn pijul_dir_name() -> &'static Path {
    return Path::new(".pijul")
}

pub fn repo_dir(p : &Path) -> PathBuf {
    return p.join(pijul_dir_name())
}

pub fn find_repo_root(dir : &Path) -> Option<&Path> {
    let pijul_dir = repo_dir(dir);
    match (metadata(pijul_dir)) {
        Ok (attr) =>
            if attr.is_dir() {Some(dir)} else {None},
        Err(_) =>
            dir.parent().and_then(find_repo_root)
    }
}

pub fn create(dir : &Path) -> io::Result<()> {
    let repo_dir = repo_dir(dir);
    create_dir(&repo_dir)
}
