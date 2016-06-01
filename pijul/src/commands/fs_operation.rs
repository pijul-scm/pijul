
extern crate clap;
extern crate libpijul;
use clap::ArgMatches;
use self::libpijul::{Repository};
use self::libpijul::fs_representation::{repo_dir, pristine_dir, find_repo_root};
use std::path::{Path};
use std::fs::{metadata, canonicalize};
use commands::error;
use super::get_wd;
#[derive(Debug)]
pub struct Params<'a> {
    pub touched_files : Vec<&'a Path>,
    pub repository : Option<&'a Path>
}
use super::error::Error;

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    let paths =
        match args.values_of("files") {
            Some(l) => l.map(|p| { Path::new(p) }).collect(),
            None => vec!()
        };
    let repository = args.value_of("repository").and_then(|x| {Some(Path::new(x))});
    Params { repository : repository, touched_files : paths }
}

#[derive(Debug)]
pub enum Operation { Add,
                     Remove }

pub fn run<'a>(args : &Params<'a>, op : Operation)
               -> Result<Option<()>, error::Error> {
    debug!("fs_operation {:?}",op);
    let files = &args.touched_files;
    let wd=try!(get_wd(args.repository));
    match find_repo_root(&wd) {
        None => return Err(error::Error::NotInARepository),
        Some(ref r) =>
        {
            debug!("repo {:?}",r);
            let repo_dir=pristine_dir(r);
            let repo = try!(Repository::open(&repo_dir).map_err(error::Error::Repository));
            let mut txn = try!(repo.mut_txn_begin());
            match op {
                Operation::Add =>{
                    for file in &files[..] {
                        let p=try!(canonicalize(wd.join(*file)));
                        let m=try!(metadata(&p));
                        if let Some(file)=iter_after(p.components(), r.components()) {
                            try!(txn.add_file(file.as_path(),m.is_dir()))
                        } else {
                            return Err(Error::InvalidPath(file.to_string_lossy().into_owned()))
                        }
                    }
                },
                Operation::Remove => {
                    for file in &files[..] {
                        let p=try!(canonicalize(wd.join(*file)));
                        if let Some(file)=iter_after(p.components(), r.components()) {
                            try!(txn.remove_file(file.as_path()))
                        } else {
                            return Err(Error::InvalidPath(file.to_string_lossy().into_owned()))
                        }
                    }
                }
            }
            try!(txn.commit());
            Ok(Some(()))
        }
    }
}

/// Ce morceau vient de path.rs du projet Rust, sous licence Apache/MIT.
fn iter_after<A, I, J>(mut iter: I, mut prefix: J) -> Option<I> where
    I: Iterator<Item=A> + Clone, J: Iterator<Item=A>, A: PartialEq
{
    loop {
        let mut iter_next = iter.clone();
        match (iter_next.next(), prefix.next()) {
            (Some(x), Some(y)) => {
                if x != y { return None }
            }
            (Some(_), None) => return Some(iter),
            (None, None) => return Some(iter),
            (None, Some(_)) => return None,
        }
        iter = iter_next;
    }
}
