
extern crate clap;
extern crate libpijul;
use clap::ArgMatches;
use self::libpijul::{Repository};
use self::libpijul::fs_representation::{repo_dir, pristine_dir, find_repo_root};
use std::path::{Path};
use std::fs::{metadata,rename,canonicalize};
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
            Some(l) => l.iter().map(|&p| { Path::new(p) }).collect(),
            None => vec!()
        };
    let repository = args.value_of("repository").and_then(|x| {Some(Path::new(x))});
    Params { repository : repository, touched_files : paths }
}

#[derive(Debug)]
pub enum Operation { Add,
                     Move,
                     Remove }

pub fn run<'a>(args : &Params<'a>, op : Operation)
               -> Result<Option<()>, error::Error> {
    debug!(target:"mv","fs_operation {:?}",op);
    let files = &args.touched_files;
    let wd=try!(get_wd(args.repository));
    match find_repo_root(&wd) {
        None => return Err(error::Error::NotInARepository),
        Some(ref r) =>
        {
            debug!(target:"mv","repo {:?}",r);
            let repo_dir=pristine_dir(r);
            let mut repo = try!(Repository::new(&repo_dir).map_err(error::Error::Repository));
            match op {
                Operation::Add =>{
                    for file in &files[..] {
                        let p=try!(canonicalize(wd.join(*file)));
                        let m=try!(metadata(&p));
                        if let Some(file)=iter_after(p.components(), r.components()) {
                            try!(repo.add_file(file.as_path(),m.is_dir()))
                        } else {
                            return Err(Error::InvalidPath(file.to_string_lossy().into_owned()))
                        }
                    }
                },
                Operation::Move => {
                    debug!(target:"mv","moving {:?}",args.touched_files);
                    if args.touched_files.len() <=1 {
                        return Err(error::Error::NotEnoughArguments)
                    } else {
                        let target_file=args.touched_files.last().unwrap();
                        let p=try!(canonicalize(wd.join(&target_file)));
                        if let Some(target)=iter_after(p.components(), r.components()) {
                            let target_is_dir=target_file.is_dir();
                            if args.touched_files.len() > 2 || (args.touched_files.len()==2 && target_is_dir) {
                                if !target_is_dir {
                                    return Err(error::Error::MoveTargetNotDirectory)
                                } else {
                                    let mut i=0;
                                    while i<args.touched_files.len()-1 {
                                        let file=args.touched_files[i];
                                        let p=wd.join(file);
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
                                    while i<args.touched_files.len()-1 {
                                        let target_basename = args.touched_files[i].file_name().unwrap();
                                        let full_target_name = (args.touched_files.last().unwrap()).join(&target_basename);
                                        try!(rename(&args.touched_files[i],
                                                    &full_target_name));
                                        i+=1
                                    }
                                }
                            } else {
                                let file=args.touched_files[0];
                                let p=wd.join(file);
                                let file=iter_after(p.components(), r.components()).unwrap();

                                let file_=args.touched_files[1];
                                let p_=wd.join(file_);
                                let file_=iter_after(p_.components(), r.components()).unwrap();

                                try!(repo.move_file(&file.as_path(),&file_.as_path(),target_is_dir));
                                try!(rename(&args.touched_files[0],
                                            &args.touched_files[1]))
                            }
                        } else {
                            return Err(Error::InvalidPath(target_file.to_string_lossy().into_owned()))
                        }
                    }
                },
                Operation::Remove => {
                    for file in &files[..] {
                        let p=try!(canonicalize(wd.join(*file)));
                        if let Some(file)=iter_after(p.components(), r.components()) {
                            try!(repo.remove_file(file.as_path()))
                        } else {
                            return Err(Error::InvalidPath(file.to_string_lossy().into_owned()))
                        }
                    }
                }
            }
            try!(repo.commit());
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
