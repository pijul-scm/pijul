
extern crate clap;
extern crate libpijul;
use clap::ArgMatches;
use self::libpijul::{Repository};
use self::libpijul::fs_representation::{repo_dir, pristine_dir, find_repo_root};
use std;
use std::io;
use std::fmt;
use std::error;
use std::path::{Path};
use std::fs::{metadata};


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


#[derive(Debug)]
pub enum Error <'a>{
    NotInARepository,
    PathNotFound(&'a Path),
    IoError(io::Error),
    Repository(libpijul::Error)
}

impl <'a> fmt::Display for Error<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::NotInARepository => write!(f, "Not in a repository"),
            Error::PathNotFound(p) => write!(f, "Path not found: {}", p.to_string_lossy()),
            Error::IoError(ref err) => write!(f, "IO error: {}", err),
            Error::Repository(ref err) => write!(f, "Repository error: {}", err),
        }
    }
}

impl <'a> error::Error for Error<'a> {
    fn description(&self) -> &str {
        match *self {
            Error::NotInARepository => "not in a repository",
            Error::PathNotFound(_) => "path not found",
            Error::IoError(ref err) => error::Error::description(err),
            Error::Repository(ref err) => libpijul::Error::description(err),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::IoError(ref err) => Some(err),
            Error::Repository(ref err) => Some(err),
            Error::PathNotFound(_) => None,
            Error::NotInARepository => None
        }
    }
}

impl <'a> From<io::Error> for Error<'a> {
    fn from(err: io::Error) -> Error<'a> {
        Error::IoError(err)
    }
}

pub enum Operation { Add,
                     Remove }

pub fn run<'a>(args : &Params<'a>, op : Operation)
               -> Result<Option<()>, Error<'a>> {
    let files = &args.touched_files;
    let pwd = args.repository;
    match find_repo_root(&pwd){
        None => return Err(Error::NotInARepository),
        Some(r) =>
        {
            let repo_dir=pristine_dir(r);
            for file in &files[..] {
                match metadata(file) {
                    Ok(_)=>
                        if iter_after((pwd.join(*file)).components(), r.components()).is_none() {
                            return Err(Error::NotInARepository)
                        },
                    Err(_) =>
                        return Err(Error::PathNotFound(*file))
                }
            }
            for file in &files[..] {
                let m=metadata(file).unwrap();
                let p=pwd.join(*file);
                let file=iter_after(p.components(), r.components()).unwrap();
                let mut repo = try!(Repository::new(&repo_dir).map_err(Error::Repository));
                match op {
                    Operation::Add => repo.add_file(file.as_path(),m.is_dir()).unwrap(),
                    Operation::Remove => repo.remove_file(file.as_path())
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
