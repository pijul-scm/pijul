extern crate tempdir;

use commands::{init, info, record, add, remove, pull, remote, mv};
use commands::error;
use std::fs;
use std::path::PathBuf;

#[test]
fn init_creates_repo() -> ()
{
    let dir = tempdir::TempDir::new("pijul").unwrap();
    let init_params = init::Params { location : &dir.path(), allow_nested : false};
    init::run(&init_params).unwrap();
    let info_params = info::Params { repository : Some(&dir.path()) };
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

        Err(error::Error::InARepository) => (),
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
    let record_params = record::Params { repository : Some(&dir.path()),
                                         yes_to_all : true,
                                         patch_name : Some(""),
                                         authors : Some(vec![]) };
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
    let add_params = add::Params { repository : Some(&dir.path()), touched_files : vec![&fpath] };
    match add::run(&add_params).unwrap() {
        Some (()) => (),
        None => panic!("no file added")        
    };
    let record_params = record::Params { repository : Some(&dir.path()),
                                         yes_to_all : true,
                                         patch_name : Some(""),
                                         authors : Some(vec![])
    };
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
    let add_params = add::Params { repository : Some(&dir.path()), touched_files : vec![&fpath] };
    match add::run(&add_params).unwrap() {
        Some (()) => (),
        None => panic!("no file added")        
    };
    match remove::run(&add_params).unwrap() {
        Some (()) => (),
        None => panic!("no file removed")
    };
    
    let record_params = record::Params { repository : Some(&dir.path()),
                                         yes_to_all : true,
                                         authors : Some(vec![]),
                                         patch_name : Some("")
    };
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
    let rem_params = remove::Params { repository : Some(&dir.path()), touched_files : vec![&fpath] };
    match remove::run(&rem_params) {
        Ok(_) => panic!("inexistant file can be removed"),
        Err(error::Error::Repository(FileNotInRepo)) => (),
        Err(_) => panic!("funky error when trying to remove inexistant file")
    }
}

#[test]
fn add_record_pull() {
    let dir = tempdir::TempDir::new("pijul").unwrap();
    let dir_a = &dir.path().join("a");
    let dir_b = &dir.path().join("b");
    fs::create_dir(dir_a);
    fs::create_dir(dir_b);
    let init_params_a = init::Params { location : &dir_a, allow_nested : false};
    let init_params_b = init::Params { location : &dir_b, allow_nested : false};
    init::run(&init_params_a).unwrap();
    init::run(&init_params_b).unwrap();
    let fpath = &dir_a.join("toto");
    let file = fs::File::create(&fpath).unwrap();
    let add_params = add::Params { repository : Some(&dir_a),
                                   touched_files : vec![&fpath] };
    match add::run(&add_params).unwrap() {
        Some (()) => (),
        None => panic!("no file added")
    };
    let record_params = record::Params { repository : Some(&dir_a),
                                         yes_to_all : true,
                                         authors : Some(vec![]),
                                         patch_name : Some("nothing")
    };
    match record::run(&record_params).unwrap() {
        None => panic!("file add is not going to be recorded"),
        Some(()) => ()
    }
    let pull_params = pull::Params { repository : Some(&dir_b),
                                     remote_id : Some(dir_a.to_str().unwrap()),
                                     set_default : false,
                                     port : None,
                                     yes_to_all : true };
    pull::run(&pull_params).unwrap();
    let fpath_b = &dir_b.join("toto");
    let metadata = fs::metadata(fpath_b).unwrap();
    assert!(metadata.is_file());
}

#[test]
fn move_to_file() {
    let dir = tempdir::TempDir::new("pijul").unwrap();
    let dir_a = &dir.path().join("a");
    let dir_b = &dir.path().join("b");
    fs::create_dir(dir_a);
    fs::create_dir(dir_b);
    let init_params_a = init::Params { location : &dir_a, allow_nested : false};
    let init_params_b = init::Params { location : &dir_b, allow_nested : false};
    init::run(&init_params_a).unwrap();
    init::run(&init_params_b).unwrap();
    let toto_path = &dir_a.join("toto");
    let titi_path = &dir_b.join("titi");
    let file = fs::File::create(&toto_path).unwrap();
    let add_params = add::Params { repository : Some(&dir_a),
                                   touched_files : vec![&toto_path] };
    match add::run(&add_params).unwrap() {
        Some (()) => (),
        None => panic!("no file added")
    };
    let record_params = record::Params { repository : Some(&dir_a),
                                         yes_to_all : true,
                                         authors : Some(vec![]),
                                         patch_name : Some("file add")
    };
    match record::run(&record_params).unwrap() {
        None => panic!("file add is not going to be recorded"),
        Some(()) => ()
    };


    let mv_params = mv::Params { repository : Some(&dir_a),
                                 movement : mv::Movement::FileToFile {from: PathBuf::from("toto"),
                                                                      to: PathBuf::from("titi")}
    };
    mv::run(&mv_params).unwrap();
    match record::run(&record_params).unwrap() {
        None => panic!("file move is not going to be recorded"),
        Some(()) => ()
    };
    println!("record command finished");

    println!("Checking the contents of {:?}", &dir_a);
    let paths = fs::read_dir(dir_a).unwrap();

    for path in paths {
        println!("Name: {:?}", path.unwrap().path())
    }
    
    let pull_params = pull::Params { repository : Some(&dir_b),
                                     remote_id : Some(dir_a.to_str().unwrap()),
                                     set_default : false,
                                     port : None,
                                     yes_to_all : true };
    pull::run(&pull_params).unwrap();
    println!("pull command finished");

    let fpath_b = dir_b.join("titi");

    let paths = fs::read_dir(&dir_b).unwrap();

    for path in paths {
        println!("Name: {:?}", path.unwrap().path())
    }

    println!("Checking {:?}", &fpath_b);
    let metadata = fs::metadata(fpath_b).unwrap();
    assert!(metadata.is_file());
}

#[test]
fn move_to_dir() {
    let dir = tempdir::TempDir::new("pijul").unwrap();
    let dir_a = &dir.path().join("a");
    let dir_b = &dir.path().join("b");
    fs::create_dir(dir_a);
    fs::create_dir(dir_b);
    let init_params_a = init::Params { location : &dir_a, allow_nested : false};
    let init_params_b = init::Params { location : &dir_b, allow_nested : false};
    init::run(&init_params_a).unwrap();
    init::run(&init_params_b).unwrap();
    let toto_path = &dir_a.join("toto");
    let titi_path = &dir_b.join("titi");
    let file = fs::File::create(&toto_path).unwrap();
    let add_params = add::Params { repository : Some(&dir_a),
                                   touched_files : vec![&toto_path] };
    match add::run(&add_params).unwrap() {
        Some (()) => (),
        None => panic!("no file added")
    };
    let record_params = record::Params { repository : Some(&dir_a),
                                         yes_to_all : true,
                                         authors : Some(vec![]),
                                         patch_name : Some("file add")
    };
    match record::run(&record_params).unwrap() {
        None => panic!("file add is not going to be recorded"),
        Some(()) => ()
    };

    let subdir_a = &dir_a.join("d");
    fs::create_dir(subdir_a);
    let add_params = add::Params { repository : Some(&dir_a),
                                   touched_files : vec![subdir_a] };
    match add::run(&add_params).unwrap() {
        Some (()) => (),
        None => panic!("no dir added")
    };

    let record_params = record::Params { repository : Some(&dir_a),
                                         yes_to_all : true,
                                         authors : Some(vec![]),
                                         patch_name : Some("dir add")
    };
    match record::run(&record_params).unwrap() {
        None => panic!("file add is not going to be recorded"),
        Some(()) => ()
    };

    let mv_params = mv::Params { repository : Some(&dir_a),
                                 movement : mv::Movement::IntoDir {from: vec![PathBuf::from("toto")],
                                                                   to: PathBuf::from("d")}
    };
    mv::run(&mv_params).unwrap();

    match record::run(&record_params).unwrap() {
        None => panic!("file move is not going to be recorded"),
        Some(()) => ()
    };

    let paths = fs::read_dir(&subdir_a).unwrap();

    for path in paths {
        println!("Name: {:?}", path.unwrap().path())
    }

    
    let pull_params = pull::Params { repository : Some(&dir_b),
                                     remote_id : Some(dir_a.to_str().unwrap()),
                                     set_default : false,
                                     port : None,
                                     yes_to_all : true };
    pull::run(&pull_params).unwrap();

    let subdir_b = &dir_b.join("d");

    let metadata = fs::metadata(&subdir_b).unwrap();
    assert!(metadata.is_dir());
    
    let paths = fs::read_dir(&dir_b).unwrap();

    println!("enumerating {:?}", &subdir_b);
    
    for path in paths {
        println!("Name: {:?}", path.unwrap().path())
    }

    println!("enumeration done");
    
    let fpath_b = &dir_b.join("d/toto");
    let metadata = fs::metadata(fpath_b).unwrap();
    assert!(metadata.is_file());
}
