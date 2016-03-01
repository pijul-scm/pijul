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

use commands::fs_operation;
use commands::fs_operation::Operation;
use commands::StaticSubcommand;
use clap::{SubCommand, ArgMatches,Arg};
use commands::error;
use std::path::{PathBuf, Path};
use std::fs::{rename, metadata};

extern crate libpijul;
use self::libpijul::fs_representation::{repo_dir, pristine_dir, find_repo_root, repo_path};
use self::libpijul::Repository;

use super::get_wd;

pub fn invocation() -> StaticSubcommand {
    return 
        SubCommand::with_name("mv")
        .about("Change file names")
        .arg(Arg::with_name("files")
             .multiple(true)
             .help("Files to move.")
             .required(true)
             .min_values(2)
             )
        .arg(Arg::with_name("repository")
             .long("repository")
             .help("Repository where the files are.")
             );
}

#[derive(Debug)]
pub enum Movement {
    IntoDir { from: Vec<PathBuf>, to: PathBuf},
    FileToFile { from: PathBuf, to: PathBuf}
}

#[derive(Debug)]
pub struct Params<'a> {
    pub repository: Option<&'a Path>,
    pub movement: Movement
}

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    let repository = args.value_of("repository").and_then(|x| {Some(Path::new(x))});
    let wd = get_wd(repository).unwrap();
    let repo_root = find_repo_root(&wd).unwrap();
    let mut repo_paths = Vec::new();
    for fname in args.values_of("files").unwrap() {
        let p = Path::new(fname);
        let r = repo_path(&repo_root, p).unwrap();
        repo_paths.push(r);
    }
    let repo_paths = repo_paths;
    let (dest, origs) = repo_paths.split_last().unwrap();
    let target_path = repo_root.join(&dest);
    let to_dir = target_path.exists() && target_path.is_dir();
    
    if to_dir
    {
        Params { repository: repository,
                 movement: Movement::IntoDir { from: Vec::from(origs), to: dest.clone()}}
    }
    else
    {
        if origs.len() == 1
        {
            Params { repository: repository,
                     movement: Movement::FileToFile { from: origs[0].clone(), to: dest.clone() }}
        }
        else
        {
            panic!("Cannot move files into {}: it is not a valid directory", dest.to_string_lossy());
        }
    }
}


pub fn run<'a>(args : &Params<'a>) -> Result<(), error::Error> {
    let repo_root = try!(get_wd(args.repository));
    let pristine = pristine_dir(&repo_root);
    let mut repo = try!(Repository::new(&pristine).map_err(error::Error::Repository));
    match args.movement {
        Movement::FileToFile { from : ref orig_path, to : ref dest_path } =>
        {
            try!(repo.move_file(orig_path.as_path(), dest_path.as_path(), false));
            try!(rename(repo_root.join(orig_path.as_path()), repo_root.join(dest_path.as_path())));
            Ok(())
        },
        Movement::IntoDir { from : ref orig_paths, to : ref dest_dir } =>
        {
            for file in orig_paths {
                let f = &file.as_path();
                let repo_target_name = {
                    let target_basename = try!(f.file_name()
                                               .ok_or(error::Error::InvalidPath(f.to_string_lossy().into_owned())));
                    dest_dir.as_path().join(&target_basename)
                };
                let is_dir = try!(metadata(&repo_root.join(f))).is_dir();
                try!(repo.move_file(f, &repo_target_name.as_path(), is_dir));
            };
            for file in orig_paths {
                let f = & file.as_path();
                let full_target_name = {
                    let target_basename = try!(f.file_name()
                                               .ok_or(error::Error::InvalidPath(f.to_string_lossy().into_owned())));
                    dest_dir.as_path().join(&target_basename)
                };
                try!(rename(&repo_root.join(f), repo_root.join(full_target_name.as_path())));
            };
            Ok(())
        }
    }
}

