// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::os::raw::{
    c_uint,
};

use std::path::{
    Path,
    PathBuf,
};

use lmdb;

use lmdb::{
    DatabaseFlags,
    Environment,
    EnvironmentBuilder,
    RoTransaction,
    RwTransaction,
};

use error::{
    StoreError,
};

use integer::{
    IntegerStore,
    PrimitiveInt,
};

use readwrite::{
    Store,
};

pub static DEFAULT_MAX_DBS: c_uint = 5;

/// Wrapper around an `lmdb::Environment`.
#[derive(Debug)]
pub struct Rkv {
    path: PathBuf,
    env: Environment,
}

/// Static methods.
impl Rkv {
    pub fn environment_builder() -> EnvironmentBuilder {
        Environment::new()
    }

    /// Return a new Rkv environment from the provided builder.
    pub fn from_env(env: EnvironmentBuilder, path: &Path) -> Result<Rkv, StoreError> {
        if !path.is_dir() {
            return Err(StoreError::DirectoryDoesNotExistError(path.into()));
        }

        Ok(Rkv {
            path: path.into(),
            env: env.open(path)
                    .map_err(|e|
                        match e {
                            lmdb::Error::Other(2) => StoreError::DirectoryDoesNotExistError(path.into()),
                            e => StoreError::LmdbError(e),
                        })?,
        })
    }

    /// Return a new Rkv environment that supports up to `DEFAULT_MAX_DBS` open databases.
    pub fn new(path: &Path) -> Result<Rkv, StoreError> {
        Rkv::with_capacity(path, DEFAULT_MAX_DBS)
    }

    /// Return a new Rkv environment that supports the specified number of open databases.
    pub fn with_capacity(path: &Path, max_dbs: c_uint) -> Result<Rkv, StoreError> {
        if !path.is_dir() {
            return Err(StoreError::DirectoryDoesNotExistError(path.into()));
        }

        let mut builder = Environment::new();
        builder.set_max_dbs(max_dbs);

        // Future: set flags, maximum size, etc. here if necessary.
        Rkv::from_env(builder, path)
    }
}

/// Store creation methods.
impl Rkv {
    pub fn create_or_open_default(&self) -> Result<Store<&str>, StoreError> {
        self.create_or_open(None)
    }

    pub fn create_or_open<'s, T, K>(&self, name: T) -> Result<Store<K>, StoreError>
    where T: Into<Option<&'s str>>,
          K: AsRef<[u8]> {
        let flags = DatabaseFlags::empty();
        self.create_or_open_with_flags(name, flags)
    }

    pub fn create_or_open_integer<'s, T, K>(&self, name: T) -> Result<IntegerStore<K>, StoreError>
    where T: Into<Option<&'s str>>,
          K: PrimitiveInt {
        let mut flags = DatabaseFlags::empty();
        flags.toggle(lmdb::INTEGER_KEY);
        let db = self.env.create_db(name.into(), flags)
                         .map_err(|e| match e {
                             lmdb::Error::BadRslot => StoreError::open_during_transaction(),
                             _ => e.into(),
                         })?;
        Ok(IntegerStore::new(db))
    }

    pub fn create_or_open_with_flags<'s, T, K>(&self, name: T, flags: DatabaseFlags) -> Result<Store<K>, StoreError>
    where T: Into<Option<&'s str>>,
          K: AsRef<[u8]> {
        let db = self.env.create_db(name.into(), flags)
                         .map_err(|e| match e {
                             lmdb::Error::BadRslot => StoreError::open_during_transaction(),
                             _ => e.into(),
                         })?;
        Ok(Store::new(db))
    }
}

/// Read and write accessors.
impl Rkv {
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
    extern crate byteorder;

    use self::byteorder::{
        ByteOrder,
        LittleEndian,
    };

    use self::tempdir::{
        TempDir,
    };

    use std::{
        fs,
        str,
    };

    use super::*;
    use ::*;

    /// We can't open a directory that doesn't exist.
    #[test]
    fn test_open_fails() {
        let root = TempDir::new("test_open_fails").expect("tempdir");
        assert!(root.path().exists());

        let nope = root.path().join("nope/");
        assert!(!nope.exists());

        let pb = nope.to_path_buf();
        match Rkv::new(nope.as_path()).err() {
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

        let k = Rkv::new(root.path()).expect("new succeeded");
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
        let k = Rkv::new(root.path()).expect("new succeeded");

        let sk: Store<&str> = k.create_or_open("sk").expect("opened");

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

        {
            let mut writer = sk.write(&k).expect("writer");
            writer.delete("foo").expect("deleted");
            writer.delete("bar").expect("deleted");
            writer.delete("baz").expect("deleted");
            assert_eq!(writer.get("foo").expect("read"), None);
            assert_eq!(writer.get("bar").expect("read"), None);
            assert_eq!(writer.get("baz").expect("read"), None);

            // Isolation. Reads still return values.
            let r = &k.read().unwrap();
            assert_eq!(sk.get(r, "foo").expect("read"), Some(Value::I64(1234)));
            assert_eq!(sk.get(r, "bar").expect("read"), Some(Value::Bool(true)));
            assert_eq!(sk.get(r, "baz").expect("read"), Some(Value::Str("héllo, yöu")));
        }

        // Dropped: tx rollback. Reads will still return values.

        {
            let r = &k.read().unwrap();
            assert_eq!(sk.get(r, "foo").expect("read"), Some(Value::I64(1234)));
            assert_eq!(sk.get(r, "bar").expect("read"), Some(Value::Bool(true)));
            assert_eq!(sk.get(r, "baz").expect("read"), Some(Value::Str("héllo, yöu")));
        }

        {
            let mut writer = sk.write(&k).expect("writer");
            writer.delete("foo").expect("deleted");
            writer.delete("bar").expect("deleted");
            writer.delete("baz").expect("deleted");
            assert_eq!(writer.get("foo").expect("read"), None);
            assert_eq!(writer.get("bar").expect("read"), None);
            assert_eq!(writer.get("baz").expect("read"), None);

            writer.commit().expect("committed");
        }

        // Committed. Reads will succeed but return None to indicate a missing value.
        {
            let r = &k.read().unwrap();
            assert_eq!(sk.get(r, "foo").expect("read"), None);
            assert_eq!(sk.get(r, "bar").expect("read"), None);
            assert_eq!(sk.get(r, "baz").expect("read"), None);
        }
    }

    #[test]
    fn test_read_before_write_num() {
        let root = TempDir::new("test_read_before_write_num").expect("tempdir");
        fs::create_dir_all(root.path()).expect("dir created");
        let k = Rkv::new(root.path()).expect("new succeeded");
        let sk: Store<&str> = k.create_or_open("sk").expect("opened");

        // Test reading a number, modifying it, and then writing it back.
        // We have to be done with the Value::I64 before calling Writer::put,
        // as the Value::I64 borrows an immutable reference to the Writer.
        // So we extract and copy its primitive value.

        fn get_existing_foo(writer: &Writer<&str>) -> Option<i64> {
            match writer.get("foo").expect("read") {
                Some(Value::I64(val)) => Some(val),
                _ => None,
            }
        }

        let mut writer = sk.write(&k).expect("writer");
        let mut existing = get_existing_foo(&writer).unwrap_or(99);
        existing += 1;
        writer.put("foo", &Value::I64(existing)).expect("success");

        let updated = get_existing_foo(&writer).unwrap_or(99);
        assert_eq!(updated, 100);
        writer.commit().expect("commit");
    }

    #[test]
    fn test_read_before_write_str() {
        let root = TempDir::new("test_read_before_write_str").expect("tempdir");
        fs::create_dir_all(root.path()).expect("dir created");
        let k = Rkv::new(root.path()).expect("new succeeded");
        let sk: Store<&str> = k.create_or_open("sk").expect("opened");

        // Test reading a string, modifying it, and then writing it back.
        // We have to be done with the Value::Str before calling Writer::put,
        // as the Value::Str (and its underlying &str) borrows an immutable
        // reference to the Writer.  So we copy it to a String.

        let mut writer = sk.write(&k).expect("writer");
        let mut existing = match writer.get("foo").expect("read") {
            Some(Value::Str(val)) => val,
            _ => "",
        }.to_string();
        existing.push('…');
        writer.put("foo", &Value::Str(&existing)).expect("write");
        writer.commit().expect("commit");
    }

    #[test]
    fn test_concurrent_read_transactions_prohibited() {
        let root = TempDir::new("test_concurrent_reads_prohibited").expect("tempdir");
        fs::create_dir_all(root.path()).expect("dir created");
        let k = Rkv::new(root.path()).expect("new succeeded");
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
        let k = Rkv::new(root.path()).expect("new succeeded");
        let s: Store<&str> = k.create_or_open("s").expect("opened");

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

    #[test]
    fn test_blob() {
        let root = TempDir::new("test_round_trip_blob").expect("tempdir");
        fs::create_dir_all(root.path()).expect("dir created");
        let k = Rkv::new(root.path()).expect("new succeeded");
        let sk: Store<&str> = k.create_or_open("sk").expect("opened");
        let mut writer = sk.write(&k).expect("writer");

        assert_eq!(writer.get("foo").expect("read"), None);
        writer.put("foo", &Value::Blob(&[1, 2, 3, 4])).expect("wrote");
        assert_eq!(writer.get("foo").expect("read"), Some(Value::Blob(&[1, 2, 3, 4])));

        fn u16_to_u8(src: &[u16]) -> Vec<u8> {
            let mut dst = vec![0; 2 * src.len()];
            LittleEndian::write_u16_into(src, &mut dst);
            dst
        }

        fn u8_to_u16(src: &[u8]) -> Vec<u16> {
            let mut dst = vec![0; src.len() / 2];
            LittleEndian::read_u16_into(src, &mut dst);
            dst
        }

        // When storing UTF-16 strings as blobs, we'll need to convert
        // their [u16] backing storage to [u8].  Test that converting, writing,
        // reading, and converting back works as expected.
        let u16_array = [1000, 10000, 54321, 65535];
        assert_eq!(writer.get("bar").expect("read"), None);
        writer.put("bar", &Value::Blob(&u16_to_u8(&u16_array))).expect("wrote");
        let u8_array = match writer.get("bar").expect("read") {
            Some(Value::Blob(val)) => val,
            _ => &[],
        };
        assert_eq!(u8_to_u16(u8_array), u16_array);
    }

    #[test]
    #[should_panic(expected = "not yet implemented")]
    fn test_delete_value() {
        let root = TempDir::new("test_delete_value").expect("tempdir");
        fs::create_dir_all(root.path()).expect("dir created");
        let k = Rkv::new(root.path()).expect("new succeeded");
        let sk: Store<&str> = k.create_or_open_with_flags("sk", lmdb::DUP_SORT).expect("opened");

        let mut writer = sk.write(&k).expect("writer");
        writer.put("foo", &Value::I64(1234)).expect("wrote");
        writer.put("foo", &Value::I64(1235)).expect("wrote");
        writer.delete_value("foo", &Value::I64(1234)).expect("deleted");
    }

    #[test]
    fn test_iter() {
        let root = TempDir::new("test_iter").expect("tempdir");
        fs::create_dir_all(root.path()).expect("dir created");
        let k = Rkv::new(root.path()).expect("new succeeded");
        let sk: Store<&str> = k.create_or_open("sk").expect("opened");

        // An iterator over an empty store returns no values.
        {
            let reader = sk.read(&k).unwrap();
            let mut iter = reader.iter_start().unwrap();
            assert!(iter.next().is_none());
        }

        let mut writer = sk.write(&k).expect("writer");
        writer.put("foo", &Value::I64(1234)).expect("wrote");
        writer.put("noo", &Value::F64(1234.0.into())).expect("wrote");
        writer.put("bar", &Value::Bool(true)).expect("wrote");
        writer.put("baz", &Value::Str("héllo, yöu")).expect("wrote");
        writer.put("héllò, töűrîst", &Value::Str("Emil.RuleZ!")).expect("wrote");
        writer.put("你好，遊客", &Value::Str("米克規則")).expect("wrote");
        writer.commit().expect("committed");

        let reader = sk.read(&k).unwrap();

        // Reader.iter() returns (key, value) tuples ordered by key.
        let mut iter = reader.iter_start().unwrap();
        let (key, val) = iter.next().unwrap();
        assert_eq!(str::from_utf8(key).expect("key"), "bar");
        assert_eq!(val.expect("value"), Some(Value::Bool(true)));
        let (key, val) = iter.next().unwrap();
        assert_eq!(str::from_utf8(key).expect("key"), "baz");
        assert_eq!(val.expect("value"), Some(Value::Str("héllo, yöu")));
        let (key, val) = iter.next().unwrap();
        assert_eq!(str::from_utf8(key).expect("key"), "foo");
        assert_eq!(val.expect("value"), Some(Value::I64(1234)));
        let (key, val) = iter.next().unwrap();
        assert_eq!(str::from_utf8(key).expect("key"), "héllò, töűrîst");
        assert_eq!(val.expect("value"), Some(Value::Str("Emil.RuleZ!")));
        let (key, val) = iter.next().unwrap();
        assert_eq!(str::from_utf8(key).expect("key"), "noo");
        assert_eq!(val.expect("value"), Some(Value::F64(1234.0.into())));
        let (key, val) = iter.next().unwrap();
        assert_eq!(str::from_utf8(key).expect("key"), "你好，遊客");
        assert_eq!(val.expect("value"), Some(Value::Str("米克規則")));
        assert!(iter.next().is_none());

        // Iterators don't loop.  Once one returns None, additional calls
        // to its next() method will always return None.
        assert!(iter.next().is_none());

        // Reader.iter_from() begins iteration at the first key equal to
        // or greater than the given key.
        let mut iter = reader.iter_from("moo").unwrap();
        let (key, val) = iter.next().unwrap();
        assert_eq!(str::from_utf8(key).expect("key"), "noo");
        assert_eq!(val.expect("value"), Some(Value::F64(1234.0.into())));
        let (key, val) = iter.next().unwrap();
        assert_eq!(str::from_utf8(key).expect("key"), "你好，遊客");
        assert_eq!(val.expect("value"), Some(Value::Str("米克規則")));
        assert!(iter.next().is_none());

        // Reader.iter_from() works as expected when the given key is a prefix
        // of a key in the store.
        let mut iter = reader.iter_from("no").unwrap();
        let (key, val) = iter.next().unwrap();
        assert_eq!(str::from_utf8(key).expect("key"), "noo");
        assert_eq!(val.expect("value"), Some(Value::F64(1234.0.into())));
        let (key, val) = iter.next().unwrap();
        assert_eq!(str::from_utf8(key).expect("key"), "你好，遊客");
        assert_eq!(val.expect("value"), Some(Value::Str("米克規則")));
        assert!(iter.next().is_none());
    }

    #[test]
    #[should_panic(expected = "called `Result::unwrap()` on an `Err` value: NotFound")]
    fn test_iter_from_key_greater_than_existing() {
        let root = TempDir::new("test_iter_from_key_greater_than_existing").expect("tempdir");
        fs::create_dir_all(root.path()).expect("dir created");
        let k = Rkv::new(root.path()).expect("new succeeded");
        let sk: Store<&str> = k.create_or_open("sk").expect("opened");

        let mut writer = sk.write(&k).expect("writer");
        writer.put("foo", &Value::I64(1234)).expect("wrote");
        writer.put("noo", &Value::F64(1234.0.into())).expect("wrote");
        writer.put("bar", &Value::Bool(true)).expect("wrote");
        writer.put("baz", &Value::Str("héllo, yöu")).expect("wrote");
        writer.commit().expect("committed");

        let reader = sk.read(&k).unwrap();

        // There is no key greater than "nuu", so the underlying LMDB API panics
        // when calling iter_from.  This is unfortunate, and I've requested
        // https://github.com/danburkert/lmdb-rs/pull/29 to make the underlying
        // API return a Result instead.
        //
        // Also see alternative https://github.com/danburkert/lmdb-rs/pull/30.
        //
        reader.iter_from("nuu").unwrap();
    }
}
