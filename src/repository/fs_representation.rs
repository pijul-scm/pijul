use std::path::Path;
use std::fs::{metadata,create_dir};
use std::io;

pub fn pijul_dir_name() -> &'static Path {
    return Path::new(".pijul")
}

pub fn find_repo_root(dir : &Path) -> Option<&Path> {
    let pijul_dir = dir.join(pijul_dir_name());
    match (metadata(pijul_dir)) {
        Ok (attr) =>
            if attr.is_dir() {Some(dir)} else {None},
        Err(_) =>
            dir.parent().and_then(find_repo_root)
    }
}

pub fn create(dir : &Path) -> io::Result<()> {
    let repo_dir = dir.join(pijul_dir_name());
    create_dir(&repo_dir)
}
