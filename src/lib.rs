// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

#[macro_use] extern crate arrayref;
#[macro_use] extern crate failure;

extern crate bincode;
extern crate lmdb;
extern crate ordered_float;
extern crate uuid;

use std::os::raw::{
    c_uint,
};

use std::path::{
    Path,
    PathBuf,
};

use lmdb::{
    Environment,
    RoTransaction,
    RwTransaction,
};

pub use lmdb::{
    DatabaseFlags,
    EnvironmentBuilder,
    EnvironmentFlags,
    WriteFlags,
};

pub mod value;
pub mod error;
mod readwrite;

pub use error::{
    DataError,
    StoreError,
};

pub use value::{
    Value,
};

pub use readwrite::{
    Reader,
    Writer,
    Store,
};

static DEFAULT_MAX_DBS: c_uint = 5;

// TODO: integer key support.
pub struct U32Key(u32);
pub struct I32Key(i32);
pub struct U64Key(u64);
pub struct I64Key(i64);

/// Wrapper around an `lmdb::Environment`.
#[derive(Debug)]
pub struct Kista {
    path: PathBuf,
    env: Environment,
}

impl Kista {
    pub fn environment_builder() -> EnvironmentBuilder {
        Environment::new()
    }

    pub fn from_env(env: EnvironmentBuilder, path: &Path) -> Result<Kista, StoreError> {
        Ok(Kista {
            path: path.into(),
            env: env.open(path)
                    .map_err(|e|
                        match e {
                            lmdb::Error::Other(2) => StoreError::DirectoryDoesNotExistError(path.into()),
                            e => StoreError::LmdbError(e),
                        })?,
        })
    }

    /// Return a new Kista environment that supports up to `DEFAULT_MAX_DBS` open databases.
    pub fn new(path: &Path) -> Result<Kista, StoreError> {
        let mut builder = Environment::new();
        builder.set_max_dbs(DEFAULT_MAX_DBS);

        // Future: set flags, maximum size, etc. here if necessary.
        Kista::from_env(builder, path)
    }

    pub fn create_or_open_default(&self) -> Result<Store<&str>, StoreError> {
        self.create_or_open(None)
    }

    pub fn create_or_open<'s, T, K>(&self, name: T) -> Result<Store<K>, StoreError>
    where T: Into<Option<&'s str>>,
          K: AsRef<[u8]> {
        let flags = DatabaseFlags::empty();
        self.create_or_open_with_flags(name, flags)
    }

    pub fn create_or_open_with_flags<'s, T, K>(&self, name: T, flags: DatabaseFlags) -> Result<Store<K>, StoreError>
    where T: Into<Option<&'s str>>,
          K: AsRef<[u8]> {
        let db = self.env.create_db(name.into(), flags).map_err(StoreError::LmdbError)?;
        Ok(Store::new(db))
    }

    pub fn read(&self) -> Result<RoTransaction, lmdb::Error> {
        self.env.begin_ro_txn()
    }

    pub fn write(&self) -> Result<RwTransaction, lmdb::Error> {
        self.env.begin_rw_txn()
    }
}

#[cfg(test)]
mod tests {
    extern crate tempdir;

    use self::tempdir::TempDir;
    use std::fs;

    use super::*;

    /// We can't open a directory that doesn't exist.
    #[test]
    fn test_open_fails() {
        let root = TempDir::new("test_open_fails").expect("tempdir");
        assert!(root.path().exists());

        let nope = root.path().join("nope/");
        assert!(!nope.exists());

        let pb = nope.to_path_buf();
        match Kista::new(nope.as_path()).err() {
            Some(StoreError::DirectoryDoesNotExistError(p)) => {
                assert_eq!(pb, p);
            },
            _ => panic!("expected error"),
        };
    }

    #[test]
    fn test_open() {
        let root = TempDir::new("test_open").expect("tempdir");
        println!("Root path: {:?}", root.path());
        fs::create_dir_all(root.path()).expect("dir created");
        assert!(root.path().is_dir());

        let k = Kista::new(root.path()).expect("new succeeded");
        let _ = k.create_or_open_default().expect("created default");

        let yyy: Store<&str> = k.create_or_open("yyy").expect("opened");
        let reader = yyy.read(&k).expect("reader");

        let result = reader.get("foo");
        assert_eq!(None, result.expect("success but no value"));
    }

    #[test]
    fn test_round_trip_and_transactions() {
        let root = TempDir::new("test_round_trip_and_transactions").expect("tempdir");
        fs::create_dir_all(root.path()).expect("dir created");
        let k = Kista::new(root.path()).expect("new succeeded");

        let mut sk: Store<&str> = k.create_or_open("sk").expect("opened");

        {
            let mut writer = sk.write(&k).expect("writer");
            writer.put("foo", &Value::I64(1234)).expect("wrote");
            writer.put("noo", &Value::F64(1234.0.into())).expect("wrote");
            writer.put("bar", &Value::Bool(true)).expect("wrote");
            writer.put("baz", &Value::Str("héllo, yöu")).expect("wrote");
            assert_eq!(writer.get("foo").expect("read"), Some(Value::I64(1234)));
            assert_eq!(writer.get("noo").expect("read"), Some(Value::F64(1234.0.into())));
            assert_eq!(writer.get("bar").expect("read"), Some(Value::Bool(true)));
            assert_eq!(writer.get("baz").expect("read"), Some(Value::Str("héllo, yöu")));

            // Isolation. Reads won't return values.
            let r = &k.read().unwrap();
            assert_eq!(sk.get(r, "foo").expect("read"), None);
            assert_eq!(sk.get(r, "bar").expect("read"), None);
            assert_eq!(sk.get(r, "baz").expect("read"), None);
        }

        // Dropped: tx rollback. Reads will still return nothing.

        {
            let r = &k.read().unwrap();
            assert_eq!(sk.get(r, "foo").expect("read"), None);
            assert_eq!(sk.get(r, "bar").expect("read"), None);
            assert_eq!(sk.get(r, "baz").expect("read"), None);
        }

        {
            let mut writer = sk.write(&k).expect("writer");
            writer.put("foo", &Value::I64(1234)).expect("wrote");
            writer.put("bar", &Value::Bool(true)).expect("wrote");
            writer.put("baz", &Value::Str("héllo, yöu")).expect("wrote");
            assert_eq!(writer.get("foo").expect("read"), Some(Value::I64(1234)));
            assert_eq!(writer.get("bar").expect("read"), Some(Value::Bool(true)));
            assert_eq!(writer.get("baz").expect("read"), Some(Value::Str("héllo, yöu")));

            writer.commit().expect("committed");
        }

        // Committed. Reads will succeed.
        {
            let r = &k.read().unwrap();
            assert_eq!(sk.get(r, "foo").expect("read"), Some(Value::I64(1234)));
            assert_eq!(sk.get(r, "bar").expect("read"), Some(Value::Bool(true)));
            assert_eq!(sk.get(r, "baz").expect("read"), Some(Value::Str("héllo, yöu")));
        }
    }

    #[test]
    fn test_concurrent_read_transactions_prohibited() {
        let root = TempDir::new("test_concurrent_reads_prohibited").expect("tempdir");
        fs::create_dir_all(root.path()).expect("dir created");
        let k = Kista::new(root.path()).expect("new succeeded");
        let s: Store<&str> = k.create_or_open("s").expect("opened");

        let _first = s.read(&k).expect("reader");
        let second = s.read(&k);

        match second {
            Err(StoreError::ReadTransactionAlreadyExists(t)) => {
                println!("Thread was {:?}", t);
            },
            _ => {
                panic!("Expected error.");
            },
        }
    }

    #[test]
    fn test_isolation() {
        let root = TempDir::new("test_isolation").expect("tempdir");
        fs::create_dir_all(root.path()).expect("dir created");
        let k = Kista::new(root.path()).expect("new succeeded");
        let mut s: Store<&str> = k.create_or_open("s").expect("opened");

        // Add one field.
        {
            let mut writer = s.write(&k).expect("writer");
            writer.put("foo", &Value::I64(1234)).expect("wrote");
            writer.commit().expect("committed");
        }

        // Both ways of reading see the value.
        {
            let reader = &k.read().unwrap();
            assert_eq!(s.get(reader, "foo").expect("read"), Some(Value::I64(1234)));
        }
        {
            let reader = s.read(&k).unwrap();
            assert_eq!(reader.get("foo").expect("read"), Some(Value::I64(1234)));
        }

        // Establish a long-lived reader that outlasts a writer.
        let reader = s.read(&k).expect("reader");
        assert_eq!(reader.get("foo").expect("read"), Some(Value::I64(1234)));

        // Start a write transaction.
        let mut writer = s.write(&k).expect("writer");
        writer.put("foo", &Value::I64(999)).expect("wrote");

        // The reader and writer are isolated.
        assert_eq!(reader.get("foo").expect("read"), Some(Value::I64(1234)));
        assert_eq!(writer.get("foo").expect("read"), Some(Value::I64(999)));

        // If we commit the writer, we still have isolation.
        writer.commit().expect("committed");
        assert_eq!(reader.get("foo").expect("read"), Some(Value::I64(1234)));

        // A new reader sees the committed value. Note that LMDB doesn't allow two
        // read transactions to exist in the same thread, so we abort the previous one.
        reader.abort();
        let reader = s.read(&k).expect("reader");
        assert_eq!(reader.get("foo").expect("read"), Some(Value::I64(999)));
    }
}
