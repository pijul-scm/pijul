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

#[cfg(not(windows))]
extern crate termios;
#[cfg(not(windows))]
use self::termios::{tcsetattr};

use super::error::Error;

use std::io::stdin;
use std::char::from_u32_unchecked;

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
    Ok((unsafe {_getch()}) as u32)
}

#[cfg(not(windows))]
fn init_getch()->Result<(),Error> {
    let mut termios=try!(self::termios::Termios::from_fd(0));
    termios.c_lflag &= !(self::termios::ICANON);
    try!(tcsetattr(0,self::termios::TCSADRAIN,&termios));
    Ok(())
}
#[cfg(not(windows))]
fn end_getch()->Result<(),Error> {
    let mut termios=try!(self::termios::Termios::from_fd(0));
    termios.c_lflag |= self::termios::ICANON;
    try!(tcsetattr(0,self::termios::TCSADRAIN,&termios));
    Ok(())
}


#[cfg(not(windows))]
fn getch()->Result<u32,Error> {
    match stdin().bytes().next() {
        Some(Ok(u))=>Ok(u as u32),
        Some(Err(e))=>Err(Error::from(e)),
        None => Ok(0)
    }
}


/// Patches might have a dummy "changes" field here.
pub fn ask_apply(command_name:&str, deps:&[(&[u8],Patch)])->Result<(),Error> {
    let mut selected_patches = Vec::new();
    //let patches_path=patches_dir(repo_path);
    try!(init_getch());
    let mut i=0;
    while i < deps.len() {
        let (ref a,ref b)=deps[i];
        let time=EPOCH + Duration::seconds(b.timestamp);
        println!("patch {}",a.to_hex());
        println!("Authors: {:?}",b.authors);
        println!("Date: {}",time.to_local().rfc822z());
        println!("  * {}",b.name);
        match b.description { Some(ref d)=>println!("  {}",d), None=>{} };
        print!("\nShall I {} this patch? [ynkad] ",command_name);
        try!(stdout().flush());
        let e= unsafe { from_u32_unchecked(try!(getch())).to_uppercase().next().unwrap() };
        println!("");
        match e {
            'Y'|'N' => {
                selected_patches.push((a,e=='Y'));
                i+=1
            },
            'A'|'D' => {
                while i<deps.len() {
                    selected_patches.push((a,e=='A'));
                    i+=1
                }
            },
            'K' => {
                selected_patches.pop();
                if i>0 { i-=1 }
            },
            _=>{}
        }
    }
    try!(end_getch());
    //println!("{:?}",stdin().bytes().next());
    Ok(())
    /*
    let mut t = term::stdout().unwrap();

    t.fg(term::color::GREEN).unwrap();
    write!(t, "hello, ").unwrap();

    t.fg(term::color::RED).unwrap();
    writeln!(t, "world!").unwrap();

    t.reset().unwrap();
     */
}
