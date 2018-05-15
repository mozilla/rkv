extern crate rkv;
extern crate tempdir;

use rkv::{
	Manager,
	Rkv,
};

use self::tempdir::TempDir;

use std::fs;

use std::sync::{
    Arc,
};

#[test]
// Identical to the same-named unit test, but this one confirms that it works
// via public Manager APIs.
fn test_same() {
    let root = TempDir::new("test_same").expect("tempdir");
    fs::create_dir_all(root.path()).expect("dir created");

    let mut manager = Manager::new();

    let p = root.path();
    assert!(manager.get(p).expect("success").is_none());

    let created_arc = manager.get_or_create(p, Rkv::new).expect("created");
    let fetched_arc = manager.get(p).expect("success").expect("existed");
    assert!(Arc::ptr_eq(&created_arc, &fetched_arc));
}
