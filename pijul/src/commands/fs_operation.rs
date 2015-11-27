
extern crate clap;
extern crate libpijul;
use clap::ArgMatches;
use self::libpijul::{Repository};
use self::libpijul::fs_representation::{repo_dir, pristine_dir, find_repo_root};
use std::path::{Path};
use std::fs::{metadata};
use commands::error;

pub struct Params<'a> {
    pub touched_files : Vec<&'a Path>,
    pub repository : &'a Path
}


pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    let paths =
        match args.values_of("files") {
            Some(l) => l.iter().map(|&p| { Path::new(p) }).collect(),
            None => vec!()
        };
    let repository = Path::new(args.value_of("repository").unwrap_or("."));
    Params { repository : repository, touched_files : paths }
}


pub enum Operation { Add,
                     Remove }

pub fn run<'a>(args : &Params<'a>, op : Operation)
               -> Result<Option<()>, error::Error<'a>> {
    let files = &args.touched_files;
    let pwd = args.repository;
    match find_repo_root(&pwd){
        None => return Err(error::Error::NotInARepository),
        Some(r) =>
        {
            let repo_dir=pristine_dir(r);
            for file in &files[..] {
                match metadata(file) {
                    Ok(_)=>
                        if iter_after((pwd.join(*file)).components(), r.components()).is_none() {
                            return Err(error::Error::NotInARepository)
                        },
                    Err(_) =>
                        return Err(error::Error::PathNotFound(*file))
                }
            }
            for file in &files[..] {
                let m=metadata(file).unwrap();
                let p=pwd.join(*file);
                let file=iter_after(p.components(), r.components()).unwrap();
                let mut repo = try!(Repository::new(&repo_dir).map_err(error::Error::Repository));
                match op {
                    Operation::Add => repo.add_file(file.as_path(),m.is_dir()).unwrap(),
                    Operation::Remove =>
                        match repo.remove_file(file.as_path()) {
                            Ok(_) => (),
                            Err(e) => return Err(error::Error::Repository(e))
                        }
                }
            }
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
