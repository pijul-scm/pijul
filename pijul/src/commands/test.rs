extern crate tempdir;
extern crate env_logger;
extern crate rand;
use self::rand::Rng;
use commands::{init, info, record, add, remove, pull, mv};
use commands::error;
use std::fs;
use std::path::PathBuf;
use std;
use std::io::prelude::*;
use self::rand::distributions::{IndependentSample, Range};
use libpijul;

fn mk_tmp_repo() -> tempdir::TempDir {
    env_logger::init().unwrap_or(());
    let dir = tempdir::TempDir::new("pijul").unwrap();
    {
        let init_params = init::Params {
            location: &dir.path(),
            allow_nested: false,
        };
        init::run(&init_params).unwrap();
    }
    dir
}

fn mk_tmp_repo_pair() -> (tempdir::TempDir, std::path::PathBuf, std::path::PathBuf) {
    env_logger::init().unwrap_or(());
    let dir = tempdir::TempDir::new("pijul").unwrap();
    let dir_a = dir.path().join("a");
    let dir_b = dir.path().join("b");
    {
        fs::create_dir(&dir_a).unwrap();
        fs::create_dir(&dir_b).unwrap();
        let init_params_a = init::Params {
            location: &dir_a,
            allow_nested: false,
        };
        let init_params_b = init::Params {
            location: &dir_b,
            allow_nested: false,
        };
        init::run(&init_params_a).unwrap();
        init::run(&init_params_b).unwrap();
    }
    (dir, dir_a, dir_b)
}

fn add_one_file(repo: &std::path::Path, file: &std::path::Path) -> Result< Option<()>, error::Error>
{

    let add_params = add::Params {
        repository: Some(&repo),
        touched_files: vec![&file],
    };
    add::run(&add_params)
}

fn record_all(repo: &std::path::Path, name: Option<&str>) -> Result<Option<()>, error::Error> {
    let record_params = record::Params {
        repository: Some(repo),
        yes_to_all: true,
        authors: Some(vec![]),
        patch_name: name,
    };
    debug!("recording file remove!");
    record::run(&record_params)
}

fn pull_all(from: &std::path::Path, to: &std::path::Path) -> Result<(), error::Error> {

    let pull_params = pull::Params {
        repository: Some(from),
        remote_id: Some(to.to_str().unwrap()),
        set_default: true,
        port: None,
        yes_to_all: true,
    };
    pull::run(&pull_params)

}

#[test]
fn add_grandchild() -> () {
    let dir = mk_tmp_repo();
    let subdir = &dir.path().join("subdir");
    fs::create_dir(&subdir).unwrap();
    let fpath = &subdir.join("toto");
    {
        fs::File::create(&fpath).unwrap();
    }

    add_one_file(&dir.path(), &fpath).unwrap() {
        Some(()) => (),
        None => panic!("no file added"),
    };

    match record_all(&dir.path(), Some("")).unwrap() {
        None => panic!("file add is not going to be recorded"),
        Some(()) => (),
    }
}


#[test]
fn info_only_in_repo() -> () {
    let dir = tempdir::TempDir::new("pijul").unwrap();
    let info_params = info::Params { repository: Some(&dir.path()) };
    match info::run(&info_params) {
        Err(error::Error::NotInARepository) => (),
        Ok(_) => panic!("getting info from a non-repository"),
        Err(_) => panic!("funky failure while getting info from a non-repository"),
    }
}

#[test]
fn add_only_in_repo() -> () {
    let dir = tempdir::TempDir::new("pijul").unwrap();
    let fpath = &dir.path().join("toto");
    let add_params = add::Params {
        repository: Some(&dir.path()),
        touched_files: vec![&fpath],
    };
    match add::run(&add_params) {
        Err(error::Error::NotInARepository) => (()),
        Ok(_) => panic!("Wait, I can add in a non-repository???"),
        Err(_) => panic!("funky failure while adding a file into a non-repository"),
    }
}

#[test]
fn add_outside_repo() -> () {
    let repo_dir = mk_tmp_repo();
    let not_repo_dir = tempdir::TempDir::new("pijul_not_repo").unwrap();
    let fpath = &not_repo_dir.path().join("toto");
    fs::File::create(&fpath).unwrap();
    let add_params = add::Params {
        repository: Some(&repo_dir.path()),
        touched_files: vec![&fpath],
    };
    match add::run(&add_params) {
        Err(error::Error::InvalidPath(ref p)) if p == fpath.to_str().unwrap() => (()),
        Ok(_) => panic!("Wait, I can add in a non-repository???"),
        Err(e) => {
            panic!("funky failure {} while adding a file into a non-repository",
                   e)
        }
    }
}

#[test]
fn init_creates_repo() -> () {
    let dir = mk_tmp_repo();
    let info_params = info::Params { repository: Some(&dir.path()) };
    info::run(&info_params).unwrap();
}

#[test]
fn init_nested_forbidden() {
    let dir = mk_tmp_repo();
    let subdir = dir.path().join("subdir");
    fs::create_dir(&subdir).unwrap();
    let sub_init_params = init::Params {
        location: &subdir,
        allow_nested: false,
    };
    match init::run(&sub_init_params) {
        Ok(_) => panic!("Creating a forbidden nested repository"),

        Err(error::Error::InARepository) => (),
        Err(_) => panic!("Failed in a funky way while creating a nested repository")
    }
}


#[test]
fn init_nested_allowed() {
    let dir = mk_tmp_repo();
    let subdir = dir.path().join("subdir");
    fs::create_dir(&subdir).unwrap();
    let sub_init_params = init::Params {
        location: &subdir,
        allow_nested: true,
    };
    init::run(&sub_init_params).unwrap()
}

#[test]
fn in_empty_dir_nothing_to_record() {
    let dir = mk_tmp_repo();

    match record_all(&dir.path(), Some("")).unwrap() {
        None => (),
        Some(()) => panic!("found something to record in an empty repository"),
    }
}

#[test]
fn with_changes_sth_to_record() {
    let dir = mk_tmp_repo();
    let fpath = &dir.path().join("toto");
    {
        let text0 = random_text();
        let mut file = fs::File::create(&fpath).unwrap();
        for line in text0.iter() {
            file.write_all(line.as_bytes()).unwrap();
        }
    }

    let add_params = add::Params { repository : Some(&dir.path()), touched_files : vec![&fpath] };
    match add_one_file(&dir.path(), &fpath).unwrap() {
        Some(()) => (),
        None => panic!("no file added"),
    };

    match record_all(&dir.path(), Some("")).unwrap() {
        None => panic!("file add is not going to be recorded"),
        Some(()) => (),
    }
}


#[test]
fn add_remove_nothing_to_record() {
    let dir = mk_tmp_repo();
    let fpath = &dir.path().join("toto");
    {
        fs::File::create(&fpath).unwrap();
    }

    match add_one_file(&dir.path(), &fpath()).unwrap() {
        Some(()) => (),
        None => panic!("no file added"),
    };
    println!("added");
    match remove::run(&add_params).unwrap() {
        Some(()) => (),
        None => panic!("no file removed"),
    };

    println!("removed");

    match record_all(&dir.path(), Some("").unwrap()) {
        None => (),
        Some(()) => panic!("add remove left a trace"),
    }
}

#[test]
fn no_remove_without_add() {
    let dir = mk_tmp_repo();
    let fpath = &dir.path().join("toto");
    {
        fs::File::create(&fpath).unwrap();
    }
    let rem_params = remove::Params {
        repository: Some(&dir.path()),
        touched_files: vec![&fpath],
    };
    match remove::run(&rem_params) {
        Ok(_) => panic!("inexistant file can be removed"),
        Err(error::Error::Repository(libpijul::error::Error::FileNotInRepo(_))) => (),
        Err(_) => panic!("funky error when trying to remove inexistant file"),
    }
}

#[test]
fn add_record_pull_stop() {
    env_logger::init().unwrap_or(());
    let dir = tempdir::TempDir::new("pijul").unwrap();
    let dir_a = &dir.path().join("a");
    let dir_b = &dir.path().join("b");
    std::mem::forget(dir);
    fs::create_dir(dir_a).unwrap();
    fs::create_dir(dir_b).unwrap();
    let init_params_a = init::Params {
        location: &dir_a,
        allow_nested: false,
    };
    let init_params_b = init::Params {
        location: &dir_b,
        allow_nested: false,
    };
    init::run(&init_params_a).unwrap();
    init::run(&init_params_b).unwrap();
    let fpath = &dir_a.join("toto");

    let text0 = random_text();
    {
        let mut file = fs::File::create(&fpath).unwrap();
        for line in text0.iter() {
            file.write_all(line.as_bytes()).unwrap();
        }
    }

    let add_params = add::Params {
        repository: Some(&dir_a),
        touched_files: vec![&fpath],
    };
    match add::run(&add_params).unwrap() {
        Some(()) => (),
        None => panic!("no file added"),
    };
    let record_params = record::Params {
        repository: Some(&dir_a),
        yes_to_all: true,
        authors: Some(vec![]),
        patch_name: Some("nothing"),
    };
    match record::run(&record_params).unwrap() {
        None => panic!("file add is not going to be recorded"),
        Some(()) => (),
    }
    let pull_params = pull::Params {
        repository: Some(&dir_b),
        remote_id: Some(dir_a.to_str().unwrap()),
        set_default: false,
        port: None,
        yes_to_all: true,
    };
    pull::run(&pull_params).unwrap();
    let fpath_b = &dir_b.join("toto");
    let metadata = fs::metadata(&fpath_b).unwrap();
    assert!(metadata.is_file());
    assert!(file_eq(&fpath_b, &text0));
}

fn file_eq(path: &std::path::Path, text: &[String]) -> bool {
    let mut f = fs::File::open(&path).unwrap();
    let mut s = String::new();
    f.read_to_string(&mut s).unwrap();
    let mut fulltext = String::new();
    for line in text.iter() {
        fulltext.push_str(&line);
    }
    if fulltext == s {
        true
    } else {
        println!("{:?}, {:?}", fulltext, s);
        false
    }
}

#[test]
fn add_record_pull_edit_record_pull() {
    add_record_pull_edit_record_pull_(false, true)
}

#[test]
fn add_record_pull_noedit_record_pull() {
    add_record_pull_edit_record_pull_(false, false)
}
#[test]
fn add_record_pull_edit_record_pull_from_empty() {
    add_record_pull_edit_record_pull_(true, true)
}

#[test]
fn add_record_pull_noedit_record_pull_from_empty() {
    add_record_pull_edit_record_pull_(true, false)
}

fn add_record_pull_edit_record_pull_(empty_file: bool, really_edit: bool) {
    env_logger::init().unwrap_or(());
    let dir = tempdir::TempDir::new("pijul").unwrap();
    let dir_a = &dir.path().join("a");
    let dir_b = &dir.path().join("b");
    fs::create_dir(dir_a).unwrap();
    fs::create_dir(dir_b).unwrap();
    let init_params_a = init::Params {
        location: &dir_a,
        allow_nested: false,
    };
    let init_params_b = init::Params {
        location: &dir_b,
        allow_nested: false,
    };
    init::run(&init_params_a).unwrap();
    init::run(&init_params_b).unwrap();
    let fpath = &dir_a.join("toto");

    let text0 = if empty_file {
        Vec::new()
    } else {
        random_text()
    };
    {
        let mut file = fs::File::create(&fpath).unwrap();
        for line in text0.iter() {
            file.write_all(line.as_bytes()).unwrap();
        }
    }
    let add_params = add::Params {
        repository: Some(&dir_a),
        touched_files: vec![&fpath],
    };
    match add::run(&add_params).unwrap() {
        Some(()) => (),
        None => panic!("no file added"),
    };
    let record_params = record::Params {
        repository: Some(&dir_a),
        yes_to_all: true,
        authors: Some(vec![]),
        patch_name: Some("nothing"),
    };
    match record::run(&record_params).unwrap() {
        None => panic!("file add is not going to be recorded"),
        Some(()) => (),
    }
    let pull_params = pull::Params {
        repository: Some(&dir_b),
        remote_id: Some(dir_a.to_str().unwrap()),
        set_default: false,
        port: None,
        yes_to_all: true,
    };
    pull::run(&pull_params).unwrap();
    let text1 = if really_edit {
        edit(&text0, 5, 2)
    } else {
        text0.clone()
    };
    {
        let mut file = fs::File::create(&fpath).unwrap();
        for line in text1.iter() {
            file.write_all(line.as_bytes()).unwrap();
        }
    }
    let record_params = record::Params {
        repository: Some(&dir_a),
        yes_to_all: true,
        authors: Some(vec![]),
        patch_name: Some("edit"),
    };

    match record::run(&record_params).unwrap() {
        None if text0 != text1 => panic!("file edition is not going to be recorded"),
        _ => (),
    }
    let pull_params = pull::Params {
        repository: Some(&dir_b),
        remote_id: Some(dir_a.to_str().unwrap()),
        set_default: false,
        port: None,
        yes_to_all: true,
    };
    pull::run(&pull_params).unwrap();

    let fpath_b = &dir_b.join("toto");
    let metadata = fs::metadata(&fpath_b).unwrap();
    assert!(metadata.is_file());
    assert!(file_eq(&fpath_b, &text1));
}


#[test]
fn cannot_move_unadded_file() {
    let repo_dir = mk_tmp_repo();
    let mv_params = mv::Params {
        repository: Some(repo_dir.path()),
        movement: mv::Movement::FileToFile {
            from: PathBuf::from("toto"),
            to: PathBuf::from("titi"),
        },
    };
    match mv::run(&mv_params) {
        Err(error::Error::Repository(libpijul::error::Error::FileNotInRepo(ref s)))
            if s.as_path() == std::path::Path::new("toto") => (),
        Err(_) => panic!("funky error"),
        Ok(()) => panic!("Unexpectedly able to move unadded file"),
    }
}


fn edit(input: &[String], percent_add: usize, percent_del: usize) -> Vec<String> {
    let mut text = Vec::new();

    let mut rng = rand::thread_rng();
    let range = Range::new(0, 100);

    for i in input {
        if range.ind_sample(&mut rng) < percent_add {
            let mut s: String = rand::thread_rng()
                                    .gen_ascii_chars()
                                    .take(20)
                                    .collect();
            s.push('\n');
            text.push(s)
        }
        if range.ind_sample(&mut rng) >= percent_del {
            text.push(i.clone())
        }
    }
    text
}

#[test]
fn move_to_file() {
    move_to_file_(false)
}

#[test]
fn move_to_file_editing() {
    move_to_file_(true)
}

fn random_text() -> Vec<String> {
    let mut text = Vec::new();
    for _ in 0..20 {
        let mut s: String = rand::thread_rng()
                                .gen_ascii_chars()
                                .take(20)
                                .collect();
        s.push('\n');
        text.push(s)
    }
    text
}

fn create_file_random_content(path: &std::path::Path) -> Vec<String> {
    let text0 = random_text();
    {
        let mut file = fs::File::create(&path).unwrap();
        for line in text0.iter() {
            file.write_all(line.as_bytes()).unwrap();
        }
    };
    text0
}

fn move_to_file_(edit_file: bool) {
    env_logger::init().unwrap_or(());
    let dir = tempdir::TempDir::new("pijul").unwrap();
    let dir_a = dir.path().join("a");
    let dir_b = dir.path().join("b");
    std::mem::forget(dir);
    fs::create_dir(&dir_a).unwrap();
    fs::create_dir(&dir_b).unwrap();
    let init_params_a = init::Params {
        location: &dir_a,
        allow_nested: false,
    };
    let init_params_b = init::Params {
        location: &dir_b,
        allow_nested: false,
    };
    init::run(&init_params_a).unwrap();
    init::run(&init_params_b).unwrap();
    let toto_path = &dir_a.join("toto");

    let text0 = create_file_random_content(&toto_path);
    println!("Checking {:?}", toto_path);
    {
        let metadata = fs::metadata(toto_path);
        println!("metadata {:?}", metadata.is_ok());
    }


    let add_params = add::Params {
        repository: Some(&dir_a),
        touched_files: vec![&toto_path],
    };
    match add::run(&add_params).unwrap() {
        Some(()) => (),
        None => panic!("no file added"),
    };
    let record_params = record::Params {
        repository: Some(&dir_a),
        yes_to_all: true,
        authors: Some(vec![]),
        patch_name: Some("file add"),
    };
    match record::run(&record_params).unwrap() {
        None => panic!("file add is not going to be recorded"),
        Some(()) => (),
    };


    let mv_params = mv::Params {
        repository: Some(&dir_a),
        movement: mv::Movement::FileToFile {
            from: PathBuf::from("toto"),
            to: PathBuf::from("titi"),
        },
    };
    mv::run(&mv_params).unwrap();

    println!("moved successfully");
    let text1 = if edit_file {
        edit(&text0, 0, 20)
    } else {
        text0.clone()
    };

    {
        let titi_path = &dir_a.join("titi");
        let mut file = fs::File::create(&titi_path).unwrap();
        for line in text1.iter() {
            println!("line={:?}", line);
            file.write_all(line.as_bytes()).unwrap();
        }
    }
    let record_params = record::Params {
        repository: Some(&dir_a),
        yes_to_all: true,
        authors: Some(vec![]),
        patch_name: Some("edition"),
    };

    match record::run(&record_params).unwrap() {
        None if text0 != text1 => panic!("file move is not going to be recorded"),
        _ => (),
    };
    println!("record command finished");

    println!("Checking the contents of {:?}", &dir_a);
    let paths = fs::read_dir(&dir_a).unwrap();

    for path in paths {
        println!("Name: {:?}", path.unwrap().path())
    }

    let pull_params = pull::Params {
        repository: Some(&dir_b),
        remote_id: Some(dir_a.to_str().unwrap()),
        set_default: false,
        port: None,
        yes_to_all: true,
    };
    pull::run(&pull_params).unwrap();
    println!("pull command finished");

    let fpath_b = dir_b.join("titi");

    let paths = fs::read_dir(&dir_b).unwrap();

    for path in paths {
        println!("Name: {:?}", path.unwrap().path())
    }

    {
        let mut f = fs::File::open(&fpath_b).unwrap();
        let mut s = String::new();
        f.read_to_string(&mut s).unwrap();
        let mut fulltext = String::new();
        for line in text1.iter() {
            fulltext.push_str(&line);
        }
        println!("{:?}\n{:?}", fulltext, s);
        assert!(fulltext == s);
    }
    println!("Checking {:?}", &fpath_b);
    let metadata = fs::metadata(fpath_b).unwrap();
    assert!(metadata.is_file());
}

#[test]
fn move_to_dir() {
    move_to_dir_editing_(false, false)
}

#[test]
fn move_to_dir_edit() {
    move_to_dir_editing_(false, true)
}
#[test]
fn move_to_dir_empty() {
    move_to_dir_editing_(true, false)
}

#[test]
fn move_to_dir_edit_empty() {
    move_to_dir_editing_(true, true)
}


fn move_to_dir_editing_(empty_file: bool, edit_file: bool) {
    let (tmp_dir, dir_a, dir_b) = mk_tmp_repo_pair();

    let toto_path = &dir_a.join("toto");

    let text0 = if empty_file {
        Vec::new()
    } else {
        random_text()
    };
    {
        let mut file = fs::File::create(&toto_path).unwrap();
        for line in text0.iter() {
            file.write_all(line.as_bytes()).unwrap();
        }
    }

    let add_params = add::Params {
        repository: Some(&dir_a),
        touched_files: vec![&toto_path],
    };
    match add::run(&add_params).unwrap() {
        Some(()) => (),
        None => panic!("no file added"),
    };
    let record_params = record::Params {
        repository: Some(&dir_a),
        yes_to_all: true,
        authors: Some(vec![]),
        patch_name: Some("file add"),
    };
    match record::run(&record_params).unwrap() {
        None => panic!("file add is not going to be recorded"),
        Some(()) => (),
    };
    println!("record 1 done");
    let subdir_a = &dir_a.join("d");
    fs::create_dir(subdir_a).unwrap();
    let add_params = add::Params {
        repository: Some(&dir_a),
        touched_files: vec![subdir_a],
    };
    match add::run(&add_params).unwrap() {
        Some(()) => (),
        None => panic!("no dir added"),
    };

    let record_params = record::Params {
        repository: Some(&dir_a),
        yes_to_all: true,
        authors: Some(vec![]),
        patch_name: Some("dir add"),
    };
    match record::run(&record_params).unwrap() {
        None => panic!("file add is not going to be recorded"),
        Some(()) => (),
    };
    println!("record 2 done");

    let mv_params = mv::Params {
        repository: Some(&dir_a),
        movement: mv::Movement::IntoDir {
            from: vec![PathBuf::from("toto")],
            to: PathBuf::from("d"),
        },
    };
    mv::run(&mv_params).unwrap();
    let text1 = if edit_file {
        edit(&text0, 0, 20)
    } else {
        text0.clone()
    };
    if edit_file {
        let toto_path = &dir_a.join("d").join("toto");
        let mut file = fs::File::create(&toto_path).unwrap();
        for line in text1.iter() {
            println!("line={:?}", line);
            file.write_all(line.as_bytes()).unwrap();
        }
    }

    match record::run(&record_params).unwrap() {
        None => panic!("file move is not going to be recorded"),
        Some(()) => (),
    };

    let paths = fs::read_dir(&subdir_a).unwrap();

    for path in paths {
        println!("Name: {:?}", path.unwrap().path())
    }


    let pull_params = pull::Params {
        repository: Some(&dir_b),
        remote_id: Some(dir_a.to_str().unwrap()),
        set_default: false,
        port: None,
        yes_to_all: true,
    };
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

#[test]
fn add_edit_remove_pull() {
    let (tmp_dir, dir_a, dir_b) = mk_tmp_repo_pair();

    let toto_path = &dir_a.join("toto");

    create_file_random_content(&toto_path);

    let add_params = add::Params {
        repository: Some(&dir_a),
        touched_files: vec![&toto_path],
    };
    match add_one_file(&dir_a, &toto_path).unwrap() {
        Some(()) => (),
        None => panic!("no file added"),
    };
    let record_params = record::Params {
        repository: Some(&dir_a),
        yes_to_all: true,
        authors: Some(vec![]),
        patch_name: Some("file add"),
    };
    match record::run(&record_params).unwrap() {
        None => panic!("file add is not going to be recorded"),
        Some(()) => (),
    };
    println!("done recording add of toto");

    let pull_params = pull::Params {
        repository: Some(&dir_b),
        remote_id: Some(dir_a.to_str().unwrap()),
        set_default: true,
        port: None,
        yes_to_all: true,
    };
    pull::run(&pull_params).unwrap();

    let remove_params = remove::Params {
        repository: Some(&dir_b),
        touched_files: vec![std::path::Path::new("toto")],
    };
    remove::run(&remove_params).unwrap();

    let record_params = record::Params {
        repository: Some(&dir_b),
        yes_to_all: true,
        authors: Some(vec![]),
        patch_name: Some("file remove"),
    };
    match record::run(&record_params).unwrap() {
        None => panic!("file remove is not going to be recorded"),
        Some(()) => (),
    };
    println!("done recording add of toto");

    let pull_params = pull::Params {
        repository: Some(&dir_a),
        remote_id: Some(dir_b.to_str().unwrap()),
        set_default: true,
        port: None,
        yes_to_all: true,
    };
    pull::run(&pull_params).unwrap();

    match fs::metadata(toto_path) {
        Ok(_) => panic!("pulling a remove fails to delete the file"),
        Err(_) => (),
    }
}

#[test]
fn pull_merge() {
    let (tmp_dir, dir_a, dir_b) = mk_tmp_repo_pair();

    let toto_path = &dir_a.join("toto");

    let _ = create_file_random_content(toto_path);

    let _ = add_one_file(&dir_a, toto_path).unwrap();

    let _ = record_all(&dir_a, Some("add toto")).unwrap();

    let titi_path = &dir_b.join("titi");

    let _ = create_file_random_content(titi_path);

    let _ = add_one_file(&dir_b, titi_path).unwrap();

    let _ = record_all(&dir_b, Some("add titi")).unwrap();

    pull_all(&dir_a, &dir_b).unwrap();
}
