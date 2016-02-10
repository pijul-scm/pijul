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
use clap::{SubCommand, ArgMatches,Arg};

use super::StaticSubcommand;

use super::error::Error;

extern crate hyper;
use std::io::prelude::*;
use std::process::{Command,Stdio};

pub fn invocation() -> StaticSubcommand {
    return
        SubCommand::with_name("login")
        .about("Get an authentication link to a remote server.")
        .arg(Arg::with_name("remote")
             .help("Remote server.")
             .required(true)
             )
}
#[derive(Debug)]
pub struct Params<'a> {
    pub remote:&'a str
}

pub fn parse_args<'a>(args: &'a ArgMatches) -> Params<'a> {
    Params { remote:args.value_of("remote").unwrap() }
}

const PREFIX:&'static [u8]=b"__pijul_auth";
const PREFIX_LEN:usize=12;


pub fn run<'a>(args : &Params<'a>) -> Result<(), Error> {
    debug!("{:?}",args);
    let client=hyper::Client::new();
    let mut remote=args.remote.to_string();


    fn find_key()->Option<String> {
        let output = Command::new("gpg")
            .arg("--list-secret-keys")
            .arg("-q")
            .arg("--with-colons")
            .output()
            .unwrap_or_else(|e| { panic!("failed to execute process: {}", e) });
        for l in output.stdout.lines() {
            let l=l.unwrap();
            let mut fields=l.split(':').skip(4);
            if let Some(ref keyid)=fields.next() {
                let output = Command::new("gpg")
                    .arg("--list-keys")
                    .arg("-q")
                    .arg("--with-colons")
                    .arg(keyid)
                    .output()
                    .unwrap_or_else(|e| { panic!("failed to execute process: {}", e) });
                for l in output.stdout.lines() {
                    let l=l.unwrap();
                    let mut fields=l.split(':').skip(1);
                    if let Some(confidence)=fields.next() {
                        if confidence=="u" {
                            return Some(keyid.to_string())
                        }
                    }
                }
            }
        }
        None
    }

    if let Some(keyid)=find_key() {
        //println!("{:?}",keyid);
        if let Some(c)=remote.pop() { if c!='/' { remote.push(c) } }
        remote.push('/');
        remote.push_str(&keyid);
        //println!("url: {:?}",remote);
        let mut res = try!(client.get(&remote)
                           .header(hyper::header::Connection::keep_alive())
                           .send());
        if let Some(&hyper::header::ContentLength(len))=res.headers.get() {
            let mut encrypted=Vec::with_capacity(len as usize);
            try!(res.read_to_end(&mut encrypted));
            let child = Command::new("gpg")
                .arg("-d")
                .arg("--batch")
                .arg("--no-tty")
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap_or_else(|e| { panic!("failed to execute process: {}", e) });

            try!(child.stdin.unwrap().write(&encrypted));
            let mut clear=String::with_capacity(len as usize);
            try!(child.stdout.unwrap().read_to_string(&mut clear));

            let mut err=String::new();
            try!(child.stderr.unwrap().read_to_string(&mut err));
            info!("{}",err);

            let bclear=clear.as_bytes();
            if bclear.len()>PREFIX_LEN {
                if &bclear[0..PREFIX_LEN]==PREFIX {
                    remote.push('/');
                    remote.push_str(&clear[PREFIX_LEN..]);
                    println!("Visit the following address:\n {}to complete authentication",remote);
                    Ok(())
                } else {
                    panic!("The server is trying to trick you into decoding other stuff")
                }
            } else {
                panic!("Empty clear")
            }
        } else {
            panic!("no content length")
        }
    } else {
        panic!("no pgp key")
    }
}
