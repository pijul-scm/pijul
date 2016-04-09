extern crate std;

/*
use std::collections::HashSet;

use std::io::Write;
use std::path::{Path};

use patch::{Patch, FileIndex, Change};
*/

use super::error::*;

pub trait Backend:Sized {
    type Db;
    type Error:std::fmt::Debug;
    
    fn db_tree(&self) -> Self::Db;
    fn set_db_tree(&mut self, db:Self::Db);
    fn db_revtree(&self) -> Self::Db;
    fn set_db_revtree(&mut self, db:Self::Db);

    fn commit(self) -> Result<(),Error>;

    fn put(&mut self, &mut Self::Db, key:&[u8], value:&[u8]) -> Result<(),Error>;
    fn get<'a>(&'a self, &Self::Db, key:&[u8]) -> Result<Option<&'a [u8]>,Error>;
    fn iterate<'a,F:FnMut(&'a[u8],&'a[u8]) -> bool>(&'a self, db:&Self::Db, key:&[u8], value:Option<&[u8]>, f:F);
}




/*

/// RepositoryT is a trait containing the core operations on a repository
pub trait RepositoryT<'b> where Self : Sized {
    type Error;
    type InternalKey;
    
    /// Opens the repository stored at a given path.
    fn open(path : &std::path::Path) -> Result<Self, Self::Error>;

    /// Prints out a representation of `self` on `w`.
    fn debug<W:Write>(&mut self, w:&mut W);

    /// Updates the working copy at `working_copy` to reflect the state of pristine.
    ///
    /// This includes the "pending patch" `pending`.
    fn update_working_copy(&mut self, working_copy: &std::path::Path, pending: &Patch)
                           -> Result<(), Self::Error>;

    /// Applies a patch to the repository. `new_patches` is the patches we have that are unknown to the
    /// source of `patch`.
    fn apply(&mut self, patch: &Patch, internal:Self::InternalKey, new_patches: &HashSet<&[u8]>)
                 -> Result<(), Self::Error>;
    
    /// Applies a bunch of patches to the repository.
    ///
    /// WARNING: because pijul does not use patch commutation, local_patches needs to contain
    /// the set of patches that were known to us, but not to the repository we got `remote_patches`
    /// from.
    ///
    /// TODO: this is probably not a primitive, why can't it just be a loop over apply?
    fn apply_patches(&mut self, repo_root: &Path, remote_patches: &HashSet<Vec<u8>>,
                     local_patches: &HashSet<Vec<u8>>)
                     -> Result<(), Self::Error>;

    /// TODO: calling this from time to time is par for the course
    /// This should probably not be public
    /// It is also not a core operation
    fn sync_file_additions(&mut self, changes: &[Change], updates: &FileIndex, internal_patch_id: Self::InternalKey);

    fn record(&mut self, working_copy: &Path) -> Result<(Vec<Change>, FileIndex), Self::Error>;

    /// Converts an external patch key into an internal patch hash
    fn internal_hash(&'b self, key:&[u8]) -> Result< Self::InternalKey, Self::Error>;

    /// Gets the contents at an (internal?) key (most often, a line)
    fn contents<'c>(&'c self, key: &[u8]) -> &'c[u8];
    
    
    // write_changes_files is probably not a primitive

    // has_patch is probably not a primitive

    // register_hash may be or not be a primitive

    // new_internal may be or not be a primitive
}
*/
