extern crate std;

use lmdb;
use contents;

pub struct LmdbRepository<'a> {
    pub env : lmdb::Env,
    pub txn : lmdb::Txn<'a>,
    pub dbi_nodes : lmdb::Dbi,
    pub dbi_revdep : lmdb::Dbi,
    pub dbi_contents : lmdb::Dbi,
    pub dbi_internal : lmdb::Dbi,
    pub dbi_external : lmdb::Dbi,
    pub dbi_branches : lmdb::Dbi,
    pub dbi_tree : lmdb::Dbi,
    pub dbi_revtree : lmdb::Dbi,
    pub dbi_inodes : lmdb::Dbi,
    pub dbi_revinodes : lmdb::Dbi
}

impl <'a>Drop for LmdbRepository<'a> {
    fn drop(& mut self){
        unsafe {
            self.txn.unsafe_abort()
        }
    }
}

impl <'a> LmdbRepository<'a> {
    pub fn get_file_content<'b>(self : &'b LmdbRepository<'a>, inode: contents::Inode)
                                -> Result<Option<&'b[u8]>, std::io::Error>
    {
        self.txn.get(self.dbi_tree, &(inode.inode_contents))
    }
}
