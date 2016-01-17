/// This module defines the data structures representing contents of a
/// pijul repository at any point in time. It is a Graph of Lines.
/// Each Line corresponds to either a bit of contents of a file, or a
/// bit of information about fs layout within the working directory
/// (files and directories).
///
/// Lines are organised in a Graph, which encodes which line belongs to what
/// file, in what order they appear, and any conflict.

extern crate libc;
use self::libc::{c_uchar};

pub const PSEUDO_EDGE:u8=1;
pub const FOLDER_EDGE:u8=2;
pub const PARENT_EDGE:u8=4;
pub const DELETED_EDGE:u8=8;

/// An Inode is a handle to a file; it is attached to a Line.
pub type Inode=Vec<u8>;
pub const DIRECTORY_FLAG:usize = 0x200;

pub const LINE_HALF_DELETED:c_uchar=4;
pub const LINE_VISITED:c_uchar=2;
pub const LINE_ONSTACK:c_uchar=1;

/// The elementary datum in the representation of the repository state
/// at any given point in time.
pub struct Line<'a> {
    pub key:&'a[u8], /// A unique identifier for the line. It is
                 /// guaranteed to be universally unique if the line
                 /// appears in a commit, and locally unique
                 /// otherwise.
    
    pub flags:u8,    /// The status of the line with respect to a dfs of
                 /// a graph it appears in. This is 0 or
                 /// LINE_HALF_DELETED unless some dfs is being run.
    
    pub children:usize,
    pub n_children:usize,
    pub index:usize,
    pub lowlink:usize,
    pub scc:usize
}


impl <'a>Line<'a> {
    pub fn is_zombie(&self)->bool {
        self.flags & LINE_HALF_DELETED != 0
    }
}

/// A graph, representing the whole content of a state of the repository at a point in time.
/// Vertices are Lines.
pub struct Graph<'a> {
    pub lines:Vec<Line<'a>>,
    pub children:Vec<(*const u8,usize)> // raw pointer because we might need the edge address. We need the first element anyway, replace "*const u8" by "u8" if the full address is not needed.
}


pub trait LineBuffer<'a> {
    fn output_line(&mut self,&'a[u8],&'a[u8]) -> ();
}

