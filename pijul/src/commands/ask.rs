extern crate term;
use std::io::prelude::*;
extern crate rustc_serialize;

use self::rustc_serialize::hex::{ToHex};
extern crate libpijul;
//use self::libpijul::fs_representation::{patches_dir};
use self::libpijul::patch::Patch;
extern crate time;
use self::time::{Duration};
//use std::path::Path;
use std::io::{stdout};
use std::collections::{HashMap,HashSet};
#[cfg(not(windows))]
extern crate termios;
#[cfg(not(windows))]
use self::termios::{tcsetattr,ICANON,ECHO};

use super::error::Error;

use std::io::stdin;
use std::char::from_u32_unchecked;
use super::super::languages::*;

const EPOCH:time::Tm = time::Tm {
    tm_sec:0,
    tm_min:0,
    tm_hour:0,
    tm_mday:1,
    tm_mon:0,
    tm_year:70,
    tm_wday:4,
    tm_yday:0,
    tm_isdst:0,
    tm_utcoff:0,
    tm_nsec:0
};




#[cfg(windows)]
extern "C" {
    fn _getch()->c_int;
}


#[cfg(windows)]
fn init_getch()->Result<(),Error> {Ok(())}
#[cfg(windows)]
fn end_getch()->Result<(),Error> {Ok(())}
#[cfg(windows)]
fn getch()->Result<u32,Error> {
    loop {
        let k= unsafe { _getch() as u64 };
        if k==0 {
            unsafe { _getch() as u64 };
        } else {
            return Ok(k)
        }
    }
}

#[cfg(not(windows))]
fn init_getch()->Result<(),Error> {
    let mut termios=try!(self::termios::Termios::from_fd(0));
    termios.c_lflag &= !(ICANON|ECHO);
    try!(tcsetattr(0,self::termios::TCSADRAIN,&termios));
    Ok(())
}
#[cfg(not(windows))]
fn end_getch()->Result<(),Error> {
    let mut termios=try!(self::termios::Termios::from_fd(0));
    termios.c_lflag |= ICANON|ECHO;
    try!(tcsetattr(0,self::termios::TCSADRAIN,&termios));
    Ok(())
}


#[cfg(not(windows))]
fn getch()->Result<u32,Error> {
    let mut r:[u8;1]=[0];
    loop {
        if try!(stdin().read(&mut r[..])) == 0 { return Ok(0) }
        else {
            if r[0]==27 {
                if try!(stdin().read(&mut r[..])) == 0 { return Ok(0) }
                else {
                    if r[0]==91 {
                        if try!(stdin().read(&mut r[..])) == 0 { return Ok(0) }
                    }
                }
            } else {
                // TODO: accept utf-8
                return Ok(r[0] as u32)
            }
        }
    }
}

#[derive(Clone,Copy)]
pub enum Command {
    Pull,
    Push
}
impl Translate for Command {
    fn trans(&self,l:Language,v:Option<Territory>)->&'static str {
        match *self {
            Command::Pull => {
                match (l,v) {
                    (Language::FR,Some(Territory::BE)) => "Voulez-vous tirer ce patch, une fois ?",
                    (Language::FR,_) => "Voulez-vous tirer ce patch ?",
                    (Language::EN,_) => "Do you want to pull this patch?"
                }
            },
            Command::Push => {
                match (l,v) {
                    (Language::FR,_) => "Voulez-vous pousser ce patch ?",
                    (Language::EN,_) => "Do you want to push this patch?"
                }
            }
        }
    }
}

#[derive(Clone,Copy)]
pub enum P {
    Hash,
    Authors,
    Timestamp
}
impl Translate for P {
    fn trans(&self,l:Language,v:Option<Territory>)->&'static str {
        match *self {
            P::Hash => {
                match (l,v) {
                    (Language::FR,_) => "Empreinte :",
                    (Language::EN,_) => "Hash:"
                }
            },
            P::Authors => {
                match (l,v) {
                    (Language::FR,_) => "Auteurs :",
                    (Language::EN,_) => "Authors:"
                }
            },
            P::Timestamp => {
                match (l,v) {
                    (Language::FR,_) => "Horodatage :",
                    (Language::EN,_) => "Timestamp:"
                }
            }
        }
    }
}


fn print_patch_descr(l:Language,t:Option<Territory>,hash:&[u8],patch:&Patch) {
    let time=EPOCH + Duration::seconds(patch.timestamp);
    println!("{} {}",P::Hash.trans(l,t), hash.to_hex());
    println!("{} {:?}",P::Authors.trans(l,t), patch.authors);
    println!("{} {}",P::Timestamp.trans(l,t), time.to_local().rfc822z());
    println!("  * {}",patch.name);
    match patch.description { Some(ref d)=>println!("  {}",d), None=>{} };
}

/// Patches might have a dummy "changes" field here.
pub fn ask_apply<'a>(command_name:Command, language:Language,territory:Option<Territory>,
                     patches:&'a [(&'a[u8],Patch)])->Result<HashSet<Vec<u8>>,Error> {
    //let patches_path=patches_dir(repo_path);
    try!(init_getch());
    let mut i=0;
    let mut choices=HashMap::new();
    let mut rev_dependencies:HashMap<&[u8],Vec<&[u8]>>=HashMap::new();
    let mut final_decision=None;
    while i < patches.len() {
        let (ref a,ref b)=patches[i];
        let decision= {
            if match rev_dependencies.get(*a) { Some(x)=>x.iter().any(|y| *(choices.get(y).unwrap_or(&false))), None=>false } {
                // First case: this patch is a dependency of a selected patch.
                // We must select it.

                // x is the list of patches that depend on a.
                // if any of these is selected, select a.
                Some(true)
            } else if b.dependencies.iter().any(|x| { ! *(choices.get(&x[..]).unwrap_or(&true)) }) {
                // Second case: this patch dependends on an unselected patch.
                // We must unselect it.
                Some(false)
            } else {
                None
            }
        };
        let e=match decision {
            Some(true)=>'Y',
            Some(false)=>'N',
            None=>{
                match final_decision {
                    None => {
                        print_patch_descr(language,territory,a,b);
                        print!("{} [ynkad] ",command_name.trans(language,territory));
                        try!(stdout().flush());
                        match getch() {
                            Ok(e)=> {
                                let e= unsafe { from_u32_unchecked(e) };
                                println!("{}",e);
                                let e=e.to_uppercase().next().unwrap_or('\0');
                                match e {
                                    'A'=> { final_decision=Some('Y'); 'Y' },
                                    'D'=> { final_decision=Some('N'); 'N' },
                                    e=>e
                                }
                            },
                            _=>{
                                unsafe { from_u32_unchecked(0) }
                            }
                        }
                    },
                    Some(d)=>d
                }
            }
        };
        match e {
            'Y' => {
                choices.insert(*a,true);
                for ref dep in b.dependencies.iter() {
                    let d=rev_dependencies.entry(dep).or_insert(vec!());
                    d.push(*a)
                }
                i+=1
            },
            'N' => {
                choices.insert(*a,false);
                i+=1
            },
            'K' if i>0 => {
                let (a,_)=patches[i];
                choices.remove(a);
                i-=1
            },
            _=>{}
        }
    }
    try!(end_getch());
    let mut selected=HashSet::new();
    for p in patches.iter() {
        let (ref a,ref b)=*p;
        if *(choices.get(*a).unwrap_or(&false)) {
            selected.insert(a.to_vec());
            for d in b.dependencies.iter() {
                selected.insert(d.to_vec());
            }
        }
    }
    Ok(selected)
}
