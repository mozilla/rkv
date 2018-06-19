// Any copyright is dedicated to the Public Domain.
// http://creativecommons.org/publicdomain/zero/1.0/

//! A simple rkv demo that showcases the basic usage (put/get/delete) of rkv.
//!
//! You can test this out by running:
//!
//!     cargo run --example simple-store

extern crate rkv;
extern crate tempfile;

use rkv::{
    Manager,
    Rkv,
    Store,
    Value,
};
use tempfile::Builder;

use std::fs;

fn main() {
    let root = Builder::new().prefix("simple-db").tempdir().unwrap();
    fs::create_dir_all(root.path()).unwrap();
    let p = root.path();

    // The manager enforces that each process opens the same lmdb environment at most once
    let created_arc = Manager::singleton().write().unwrap().get_or_create(p, Rkv::new).unwrap();
    let k = created_arc.read().unwrap();

    // Creates a store called "store"
    let store: Store<&str> = k.create_or_open("store").unwrap();

    println!("Inserting data...");
    {
        // Use a write transaction to mutate the store
        let mut writer = store.write(&k).unwrap();
        writer.put("int", &Value::I64(1234)).unwrap();
        writer.put("uint", &Value::U64(1234_u64)).unwrap();
        writer.put("float", &Value::F64(1234.0.into())).unwrap();
        writer.put("instant", &Value::Instant(1528318073700)).unwrap();
        writer.put("boolean", &Value::Bool(true)).unwrap();
        writer.put("string", &Value::Str("héllo, yöu")).unwrap();
        writer.put("json", &Value::Json(r#"{"foo":"bar", "number": 1}"#)).unwrap();
        writer.put("blob", &Value::Blob(b"blob")).unwrap();
        writer.commit().unwrap();
    }

    println!("Looking up keys...");
    {
        // Use a read transaction to query the store
        let r = &k.read().unwrap();
        println!("Get int {:?}", store.get(r, "int").unwrap());
        println!("Get uint {:?}", store.get(r, "uint").unwrap());
        println!("Get float {:?}", store.get(r, "float").unwrap());
        println!("Get instant {:?}", store.get(r, "instant").unwrap());
        println!("Get boolean {:?}", store.get(r, "boolean").unwrap());
        println!("Get string {:?}", store.get(r, "string").unwrap());
        println!("Get json {:?}", store.get(r, "json").unwrap());
        println!("Get blob {:?}", store.get(r, "blob").unwrap());
    }

    println!("Aborting transaction...");
    {
        // Aborting a write transaction rollbacks the change(s)
        let mut writer = store.write(&k).unwrap();
        writer.put("foo", &Value::Str("bar")).unwrap();
        writer.abort();

        let r = &k.read().unwrap();
        println!("It should be None! ({:?})", store.get(r, "foo").unwrap());
        // Explicitly aborting a transaction is not required unless an early
        // abort is desired, since both read and write transactions will
        // implicitly be aborted once they go out of scope.
    }

    println!("Deleting keys...");
    {
        // Deleting a key/value also requires a write transaction
        let mut writer = store.write(&k).unwrap();
        writer.put("foo", &Value::Str("bar")).unwrap();
        writer.delete("foo").unwrap();
        // Write transaction also supports read
        println!("It should be None! ({:?})", writer.get("foo").unwrap());
        writer.commit().unwrap();
    }
}
