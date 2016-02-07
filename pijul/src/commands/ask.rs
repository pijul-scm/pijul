extern crate term;
use std::io::prelude::*;
extern crate rustc_serialize;

use self::rustc_serialize::hex::{ToHex};
extern crate libpijul;
//use self::libpijul::fs_representation::{patches_dir};
use self::libpijul::patch::{Change,Value,Patch,LINE_SIZE,HASH_SIZE,KEY_SIZE};
extern crate time;
use self::time::{Duration};
//use std::path::Path;
use std::io::{stdout};
use std::collections::{HashMap,HashSet,BTreeMap};
#[cfg(not(windows))]
extern crate termios;
#[cfg(not(windows))]
use self::termios::{tcsetattr,ICANON,ECHO};

use super::error::Error;
use self::libpijul::Repository;
use self::libpijul::contents::{FOLDER_EDGE,PARENT_EDGE};
use std::io::stdin;
use std::char::from_u32_unchecked;
use std::str;
use std::ptr::copy_nonoverlapping;
use std;


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

fn print_patch_descr(hash:&[u8],patch:&Patch) {
    let time=EPOCH + Duration::seconds(patch.timestamp);
    println!("Hash: {}",hash.to_hex());
    println!("Authors: {:?}",patch.authors);
    println!("Timestamp {}",time.to_local().rfc822z());
    println!("  * {}",patch.name);
    match patch.description { Some(ref d)=>println!("  {}",d), None=>{} };
}

/// Patches might have a dummy "changes" field here.
pub fn ask_apply<'a>(command_name:Command,
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
                        print_patch_descr(a,b);
                        print!("{} [ynkad] ",
                               match command_name {
                                   Command::Push => "Shall I push this patch?",
                                   Command::Pull => "Shall I pull this patch?"
                               });
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


fn change_deps(id:usize,c:&Change,provided_by:&mut HashMap<u32,usize>)->HashSet<u32> {
    let mut s=HashSet::new();
    match *c {
        Change::NewNodes{ref up_context,ref down_context,ref line_num,ref nodes,..}=>{
            for cont in up_context.iter().chain(down_context) {
                if cont.len() == LINE_SIZE {
                    let x=(cont[0] as u32)
                        | ((cont[1] as u32) << 8)
                        | ((cont[2] as u32) << 16)
                        | ((cont[3] as u32) << 24);
                    s.insert(x);
                }
            }
            for i in *line_num..(*line_num+nodes.len() as u32) {
                provided_by.insert(i,id);
            }
        },
        Change::Edges { ref edges,.. } => {
            for e in edges {
                if e.from.len() == LINE_SIZE {
                    let cont=&e.from;
                    let x=(cont[0] as u32)
                        | ((cont[1] as u32) << 8)
                        | ((cont[2] as u32) << 16)
                        | ((cont[3] as u32) << 24);
                    s.insert(x);
                }
                if e.to.len() == LINE_SIZE {
                    let cont=&e.to;
                    let x=(cont[0] as u32)
                        | ((cont[1] as u32) << 8)
                        | ((cont[2] as u32) << 16)
                        | ((cont[3] as u32) << 24);
                    s.insert(x);
                }
            }
        }
    }
    s
}

fn print_change<'a>(repo:&Repository<'a>,c:&Change)->Result<(),Error> {
    match *c {
        Change::NewNodes{/*ref up_context,ref down_context,ref line_num,*/ref flag,ref nodes,..}=>{
            for n in nodes {
                if *flag & FOLDER_EDGE != 0 {
                    if n.len()>=2 {
                        println!("new file {}",str::from_utf8(&n[2..]).unwrap_or(""));
                    }
                } else {
                    print!("+ {}",str::from_utf8(n).unwrap_or(""));
                }
            }
            Ok(())
        },
        Change::Edges {ref edges,ref flag,..}=>{
            let mut h_targets=HashSet::with_capacity(edges.len());
            for e in edges {
                let target=
                    if *flag & PARENT_EDGE == 0 {
                        if h_targets.insert(&e.to) { Some(&e.to) } else { None }
                    } else {
                        if h_targets.insert(&e.from) { Some(&e.from) } else { None }
                    };
                if let Some(target)=target {
                    let int=try!(repo.internal_hash(&target[0..target.len()-LINE_SIZE]));
                    let mut internal=[0;KEY_SIZE];
                    unsafe {
                        copy_nonoverlapping(int.contents.as_ptr(),internal.as_mut_ptr(),HASH_SIZE);
                        copy_nonoverlapping(target.as_ptr().offset((target.len() - LINE_SIZE) as isize),
                                            internal.as_mut_ptr().offset(HASH_SIZE as isize),
                                            LINE_SIZE)
                    };
                    print!("- {}",str::from_utf8(repo.contents(&internal[..])).unwrap_or(""));
                }
            }
            Ok(())
        }
    }
}

pub fn ask_record<'a>(repository:&Repository<'a>,changes:&[Change])->Result<HashMap<usize,bool>,Error> {
    try!(init_getch());
    let mut i=0;
    let mut choices:HashMap<usize,bool>=HashMap::new();
    let mut final_decision=None;
    let mut provided_by=HashMap::new();
    let mut line_deps=Vec::with_capacity(changes.len());
    for i in 0..changes.len() {
        line_deps.push(change_deps(i,&changes[i],&mut provided_by));
    }
    let mut deps:HashMap<usize,Vec<usize>>=HashMap::new();
    let mut rev_deps:HashMap<usize,Vec<usize>>=HashMap::new();
    for i in 0..changes.len() {
        for dep in line_deps[i].iter() {
            debug!("provided: i {}, dep {}",i,dep);
            let p=provided_by.get(dep).unwrap();
            debug!("provided: p= {}",p);

            let e=deps.entry(i).or_insert(Vec::new());
            e.push(*p);

            let e=rev_deps.entry(*p).or_insert(Vec::new());
            e.push(i);
        }
    }
    let empty_deps=Vec::new();
    while i < changes.len() {
        let decision=
            // If one of our dependencies has been unselected (with "n")
            if deps.get(&i).unwrap_or(&empty_deps).iter().any(|x| { ! *(choices.get(x).unwrap_or(&true)) }) {
                Some(false)
            } else if rev_deps.get(&i).unwrap_or(&empty_deps).iter().any(|x| { *(choices.get(x).unwrap_or(&false)) }) {
                // If we are a dependency of someone selected (with "y").
                Some(true)
            } else {
                None
            };
        let e=match decision {
            Some(true)=>'Y',
            Some(false)=>'N',
            None=>{
                match final_decision {
                    None => {
                        try!(print_change(repository,&changes[i]));
                        print!("Shall I record this change? [ynkad] ");
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
                choices.insert(i,true);
                i+=1
            },
            'N' => {
                choices.insert(i,false);
                i+=1
            },
            'K' if i>0 => {
                choices.remove(&i);
                i-=1
            },
            _=>{}
        }
    }
    try!(end_getch());
    Ok(choices)
}

pub fn ask_authors()->Result<Vec<BTreeMap<String,Value>>,Error> {
    print!("What is your name <and email address>? ");
    try!(std::io::stdout().flush());
    let mut input = String::new();
    try!(stdin().read_line(&mut input));
    if let Some(c)=input.pop() { if c!='\n' { input.push(c) } }
    let mut auth=BTreeMap::new();
    auth.insert("name".to_string(),Value::String(input));
    Ok(vec!(auth))
}


pub fn ask_patch_name()->Result<String,Error> {
    print!("What is the name of this patch? ");
    try!(std::io::stdout().flush());
    let mut input = String::new();
    try!(stdin().read_line(&mut input));
    if let Some(c)=input.pop() { if c!='\n' { input.push(c) } }
    Ok(input)
}

pub fn ask_learn_ssh(host:&str,fingerprint:&str)->Result<bool,Error> {
    print!("The authenticity of host {:?} cannot be established.\nThe fingerprint is {:?}.\nAre you sure you want to continue (yes/no)? ",host,fingerprint);
    try!(std::io::stdout().flush());
    let mut input = String::new();
    try!(stdin().read_line(&mut input));
    let mut input=input.to_uppercase();
    input.pop();
    if let Some(c)=input.pop() { if c!='\n' { input.push(c) } }
    println!("input={:?}",input);
    Ok(input=="YES")
}
