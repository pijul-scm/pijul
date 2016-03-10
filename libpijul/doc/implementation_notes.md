% Implementation Notes for Pijul

# Repository tables

Each repository contains a bunch of (currently lmdb) tables. Each of these tables defines a mapping from keys to (sets of) values. The following tables exist, with their `key -> values` mappings; the meaning of the keys and values will be explained below.

- `dbi_nodes`
- `dbi_revdep`
- `dbi_contents`
- `dbi_internal`
- `dbi_external`
- `dbi_branches`
- `dbi_tree`: FileId -> Inode
- `dbi_revtree`: Inode -> FileId
- `dbi_inodes`: Inode -> LineId (?)
- `dbi_revinodes`: LineId (?) -> Inode

In each of these tables, keys and values are currently of type `[u8]`, but they represent different things. They should be replaced with specific structs at some point.

## Inodes, DirKeys and Files

Getting to a file in these tables is a little involved.

Each **file** corresponds to an entry in `dbi_tree`. The key for that entry is an `FileRef`. The value is an `Inode`, consisting of `INODE_SIZE` bytes. Let us now look at how `FileRef`s are formed. Each file that has been added to the repository has a path, starting from the root of the repository. For instance, this file has path `/libpijul/doc/implementation_notes.md`. The `/libpijul/doc` part of that path is its _parent_, and `implementation_nodes.md` is its _filename_. The _parent_ part will be represented by the `INODE_SIZE` first bits of the `FileRef` (which we'll call the _dirKey_), and the _filename_ will be reflected in the remaining bits of the `FileRef`. These filename bytes correspond to the utf-8 representation of the filename. The `Inode` for the root of the repository is `[0; INODE_SIZE]`. Thus, a file such as `default.nix`, present at the root of the repository is represented in `dbi_tree` by an entry whose key contains INODE_SIZE zero-bytes, then bytes 0x64 (d), 0x65 (e), 0x66 (f), 0x61 (a)…

Each **plain file** also corresponds to an entry in `dbi_inodes`, where the key is the file's `Inode`, and the value is the id of the first line of the file.

Each **directory** corresponds to an entry in `dbi_tree`. The key for that entry is the `FileRef` of that directory (ie, the `Inode` of its parent concatenated with its own name), and the value is its inode (ie, the `dirKey` that will be used to build `FileRefs` for all entries of this directory).

In order to know whether an `Inode` corresponds to plain file or a directory, one needs check whether there is an entry with that key in `dbi_inodes`. If so, it is a plain file, else it is a directory.

To sum up, in order to look for `/libpijul/doc/implementation_notes.md`, the following steps are necessary (we're going to assume INODE_SIZE = 1 for the sake of the example). They are done by the `get_file_entry` method of the repository.
1. Look for the directory key for `libpijul`: `get(dbi_tree(0x00libpijul) -> 0x01`
2. Look for the directory key for `libpijul/doc`: `get(dbi_tree(0x01doc) -> 0x02`
3. Look for the entry of `libpijul/doc/implementation_noted.md`:
    `get(dbi_tree(0x02implementation_notes.md)) -> 0x03`

`dbi_revtree` and `dbi_revinodes` store the same information, but keys and values are reversed compared with `dbi_tree` and `dbi_inodes` respectively.
