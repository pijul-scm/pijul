
extern crate clap;
extern crate libpijul;
use clap::ArgMatches;
use self::libpijul::{Repository};
use self::libpijul::fs_representation::{repo_dir, pristine_dir, find_repo_root};
use std::path::{Path,PathBuf};
use std::fs::{metadata,rename};
use commands::error;

#[derive(Debug)]
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
                     Move,
                     Remove }

pub fn run<'a>(args : &Params<'a>, op : Operation)
               -> Result<Option<()>, error::Error> {
    let files = &args.touched_files;
    let pwd = args.repository;
    match find_repo_root(&pwd){
        None => return Err(error::Error::NotInARepository),
        Some(r) =>
        {
            let repo_dir=pristine_dir(r);
            let mut repo = try!(Repository::new(&repo_dir).map_err(error::Error::Repository));
            match op {
                Operation::Add =>{
                    for file in &files[..] {
                        let m=try!(metadata(file));
                        let p=pwd.join(*file);
                        let file=iter_after(p.components(), r.components()).unwrap();
                        repo.add_file(file.as_path(),m.is_dir()).unwrap()
                    }
                },
                Operation::Move => {
                    debug!(target:"mv","moving {:?}",args.touched_files);
                    if args.touched_files.len() <=1 {
                        return Err(error::Error::NotEnoughArguments)
                    } else {
                        let target_file=args.touched_files.last().unwrap();
                        let p=pwd.join(&target_file);
                        let target={
                            iter_after(p.components(), r.components()).unwrap()
                        };
                        let target_is_dir=target_file.is_dir();
                        if args.touched_files.len() > 2 || (args.touched_files.len()==2 && target_is_dir) {
                            if !target_is_dir {
                                return Err(error::Error::MoveTargetNotDirectory)
                            } else {
                                let mut i=0;
                                while i<args.touched_files.len()-2 {
                                    let file=args.touched_files[i];
                                    let p=pwd.join(file);
                                    let file=iter_after(p.components(), r.components()).unwrap();

                                    let full_target_name ={
                                        let target_basename = file.as_path().file_name().unwrap();
                                        target.as_path().join(&target_basename)
                                    };
                                    let m=try!(metadata(args.touched_files[i]));
                                    try!(repo.move_file(&file.as_path(),
                                                        &full_target_name.as_path(),
                                                        m.is_dir()));
                                    i+=1
                                }
                                i=0;
                                while i<args.touched_files.len()-2 {
                                    let target_basename = args.touched_files[i].file_name().unwrap();
                                    let full_target_name = (args.touched_files.last().unwrap()).join(&target_basename);
                                    try!(rename(&args.touched_files[i],
                                                &full_target_name));
                                    i+=1
                                }
                            }
                        } else {
                            let file=args.touched_files[0];
                            let p=pwd.join(file);
                            let file=iter_after(p.components(), r.components()).unwrap();

                            let file_=args.touched_files[1];
                            let p_=pwd.join(file_);
                            let file_=iter_after(p_.components(), r.components()).unwrap();

                            try!(repo.move_file(&file.as_path(),&file_.as_path(),target_is_dir));
                            try!(rename(&args.touched_files[0],
                                        &args.touched_files[1]))
                        }
                    }
                },
                Operation::Remove => {
                    for file in &files[..] {
                        let m=metadata(file).unwrap();
                        let p=pwd.join(*file);
                        let file=iter_after(p.components(), r.components()).unwrap();
                        match repo.remove_file(file.as_path()) {
                            Ok(_) => (),
                            Err(e) => return Err(error::Error::Repository(e))
                        }
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
