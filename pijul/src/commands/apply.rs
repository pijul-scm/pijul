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
extern crate clap;
use clap::{SubCommand, ArgMatches, Arg};

extern crate libpijul;
use commands::StaticSubcommand;
use self::libpijul::{Repository};
use self::libpijul::patch::{Patch,HASH_SIZE};
use self::libpijul::fs_representation::{repo_dir, pristine_dir, patches_dir, find_repo_root, branch_changes_file};

use commands::error::Error;
use std::collections::{HashSet,HashMap};

extern crate rand;
use std::path::{Path};

use std::io::{BufReader};
use std::fs::File;

extern crate rustc_serialize;
use self::rustc_serialize::hex::{FromHex};
extern crate time;

pub fn invocation() -> StaticSubcommand {
    return
        SubCommand::with_name("apply")
        .about("apply a patch")
        .arg(Arg::with_name("patch")
             .help("Hash of the patch to apply, in hexadecimal.")
             .multiple(true)
             .required(true))
        .arg(Arg::with_name("repository")
             .long("repository")
             .help("The repository where to record, defaults to the current directory.")
             .required(false))
}

pub struct Params<'a> {
    pub repository : &'a Path,
    pub hex_hash : &'a str
}

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a>
{
    Params {
        repository : Path::new(args.value_of("repository").unwrap_or(".")),
        hex_hash : args.value_of("patch").unwrap()
    }
}

pub fn run(params : &Params) -> Result<Option<()>, Error> {
    match find_repo_root(&params.repository){
        None => return Err(Error::NotInARepository),
        Some(r) =>
        {
            let repo_dir=pristine_dir(r);
            let t0=time::precise_time_s();
            let patch={
                let path=patches_dir(r).join(params.hex_hash).with_extension("cbor");
                let file=try!(File::open(&path));
                let mut r = BufReader::new(file);
                try!(Patch::from_reader(&mut r))
            };//Patch::new(changes);
            let mut internal=[0;HASH_SIZE];
            let mut repo = try!(Repository::new(&repo_dir).map_err(Error::Repository));
            repo.new_internal(&mut internal);
            debug!(target:"pijul","applying patch");

            let mut repo=repo.apply(&patch, &internal, &HashSet::new()).unwrap();
            repo.sync_file_additions(&patch.changes[..],&HashMap::new(), &internal);

            let t2=time::precise_time_s();
            info!("applied patch in {}s", t2-t0);
            let hash=try!(params.hex_hash.from_hex());
            repo.register_hash(&internal[..],&hash);
            repo.write_changes_file(&branch_changes_file(r,repo.get_current_branch())).unwrap();
            let t3=time::precise_time_s();
            info!("changes files took {}s to write", t3-t2);
            try!(repo.output_repository(&r,&Patch::empty()));
            Ok(Some(()))
        }
    }
}
