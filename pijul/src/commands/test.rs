extern crate tempdir;

//use commands::{init, info, record, add, remove};
//use commands::error;

#[test]
fn init_creates_repo() -> ()
{
    let dir = tempdir::TempDir::new("pijul").unwrap();
    let init_params = init::Params { location : &dir.path(), allow_nested : false};
    init::run(&init_params).unwrap();
    let info_params = info::Params { directory : &dir.path() };
    info::run(&info_params).unwrap();
}

#[test]
fn init_nested_forbidden() {
    let dir = tempdir::TempDir::new("pijul").unwrap();
    let init_params = init::Params { location : &dir.path(), allow_nested : false};
    init::run(&init_params).unwrap();
    let subdir = dir.path().join("subdir");
    fs::create_dir(&subdir);
    let sub_init_params = init::Params { location : &subdir, allow_nested : false};
    match init::run(&sub_init_params) {
        Ok(_) => panic!("Creating a forbidden nested repository"),

        Err(init::Error::InARepository) => (),
        Err(_) => panic!("Failed in a funky way while creating a nested repository")       
    }
}


#[test]
fn init_nested_allowed() {
    let dir = tempdir::TempDir::new("pijul").unwrap();
    let init_params = init::Params { location : &dir.path(), allow_nested : false};
    init::run(&init_params).unwrap();
    let subdir = dir.path().join("subdir");
    fs::create_dir(&subdir);
    let sub_init_params = init::Params { location : &subdir, allow_nested : true};
    init::run(&sub_init_params).unwrap()
}

#[test]
fn in_empty_dir_nothing_to_record() {
    let dir = tempdir::TempDir::new("pijul").unwrap();
    let init_params = init::Params { location : &dir.path(), allow_nested : false};
    init::run(&init_params).unwrap();
    let record_params = record::Params { repository : &dir.path() };
    match record::run(&record_params).unwrap() {
        None => (),
        Some(()) => panic!("found something to record in an empty repository")
    }
}

#[test]
fn with_changes_sth_to_record() {
    let dir = tempdir::TempDir::new("pijul").unwrap();
    let init_params = init::Params { location : &dir.path(), allow_nested : false};
    init::run(&init_params).unwrap();
    let fpath = &dir.path().join("toto");
    let file = fs::File::create(&fpath).unwrap();
    let add_params = add::Params { repository : &dir.path(), touched_files : vec![&fpath] };
    match add::run(&add_params).unwrap() {
        Some (()) => (),
        None => panic!("no file added")        
    };
    let record_params = record::Params { repository : &dir.path() };
    match record::run(&record_params).unwrap() {
        None => panic!("file add is not going to be recorded"),
        Some(()) => ()
    }
}


#[test]
fn add_remove_nothing_to_record() {
    let dir = tempdir::TempDir::new("pijul").unwrap();
    let init_params = init::Params { location : &dir.path(), allow_nested : false};
    init::run(&init_params).unwrap();
    let fpath = &dir.path().join("toto");
    let file = fs::File::create(&fpath).unwrap();
    let add_params = add::Params { repository : &dir.path(), touched_files : vec![&fpath] };
    match add::run(&add_params).unwrap() {
        Some (()) => (),
        None => panic!("no file added")        
    };
    match remove::run(&add_params).unwrap() {
        Some (()) => (),
        None => panic!("no file removed")
    };

    let record_params = record::Params { repository : &dir.path() };
    match record::run(&record_params).unwrap() {
        None => (),
        Some(()) => panic!("add remove left a trace")
    }
}

#[test]
fn no_remove_without_add() {
    let dir = tempdir::TempDir::new("pijul").unwrap();
    let init_params = init::Params { location : &dir.path(), allow_nested : false};
    init::run(&init_params).unwrap();
    let fpath = &dir.path().join("toto");
    let file = fs::File::create(&fpath).unwrap();
    let rem_params = remove::Params { repository : &dir.path(), touched_files : vec![&fpath] };
    match remove::run(&rem_params) {
        Ok(_) => panic!("inexistant file can be removed"),
        Err(error::Error::Repository(FileNotInRepo)) => (),
        Err(_) => panic!("funky error when trying to remove inexistant file")
    }
}

// #[test]
// fn add_record_remove_pull() {
//     let dir = tempdir::TempDir::new("pijul").unwrap();
//     let init_params = init::Params { location : &dir.path(), allow_nested : false};
//     init::run(&init_params).unwrap();
//     let fpath = &dir.path().join("toto");
//     let file = fs::File::create(&fpath).unwrap();
//     let add_params = add::Params { repository : &dir.path(), added_files : vec![&fpath] };
//     match add::run(&add_params).unwrap() {
//         Some (()) => (),
//         None => panic!("no file added")        
//     };
//     let record_params = record::Params { repository : &dir.path() };
//     match record::run(&record_params).unwrap() {
//         None => panic!("file add is not going to be recorded"),
//         Some(()) => ()
//     }
// }
