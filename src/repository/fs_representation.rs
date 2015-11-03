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
