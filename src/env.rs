// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::os::raw::c_uint;

use std::path::{
    Path,
    PathBuf,
};

use lmdb;

use lmdb::{
    DatabaseFlags,
    Environment,
    EnvironmentBuilder,
};

use error::StoreError;

use integer::{
    IntegerReader,
    IntegerWriter,
    Key,
    PrimitiveInt,
};

use readwrite::{
    Reader,
    Store,
    Writer,
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
            env: env.open(path).map_err(|e| match e {
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
    pub fn open_or_create_default(&self) -> Result<Store, StoreError> {
        self.open_or_create(None)
    }

    pub fn open_or_create<'s, T>(&self, name: T) -> Result<Store, StoreError>
    where
        T: Into<Option<&'s str>>,
    {
        let flags = DatabaseFlags::empty();
        self.open_or_create_with_flags(name, flags)
    }

    pub fn open_or_create_integer<'s, T>(&self, name: T) -> Result<Store, StoreError>
    where
        T: Into<Option<&'s str>>,
    {
        let mut flags = DatabaseFlags::empty();
        flags.toggle(DatabaseFlags::INTEGER_KEY);
        self.open_or_create_with_flags(name, flags)
    }

    pub fn open_or_create_with_flags<'s, T>(&self, name: T, flags: DatabaseFlags) -> Result<Store, StoreError>
    where
        T: Into<Option<&'s str>>,
    {
        let db = self.env.create_db(name.into(), flags).map_err(|e| match e {
            lmdb::Error::BadRslot => StoreError::open_during_transaction(),
            _ => e.into(),
        })?;
        Ok(Store::new(db))
    }

    /// Open an existing database, unlike other `open_or_create_*` functions, it
    /// opens the given database by using a read transaction, which means other
    /// in-flight write transaction will not block this call. This is preferred
    /// to be used in the read_only scenarios.
    pub fn open<'s, T, K>(&self, name: T) -> Result<Store<K>, StoreError>
    where
        T: Into<Option<&'s str>>,
        K: AsRef<[u8]>,
    {
        let db = self.env.open_db(name.into()).map_err(|e| match e {
            lmdb::Error::BadRslot => StoreError::open_during_transaction(),
            _ => e.into(),
        })?;
        Ok(Store::new(db))
    }
}

/// Read and write accessors.
impl Rkv {
    pub fn read<K>(&self) -> Result<Reader<K>, StoreError>
    where
        K: AsRef<[u8]>,
    {
        let txn = self.env.begin_ro_txn()?;
        Ok(Reader::new(txn))
    }

    pub fn write<K>(&self) -> Result<Writer<K>, StoreError>
    where
        K: AsRef<[u8]>,
    {
        let txn = self.env.begin_rw_txn()?;
        Ok(Writer::new(txn))
    }

    pub fn read_int<K>(&self) -> Result<IntegerReader<K>, StoreError>
    where
        K: PrimitiveInt,
    {
        let reader = self.read::<Key<K>>()?;
        Ok(IntegerReader::new(reader))
    }

    pub fn write_int<K>(&self) -> Result<IntegerWriter<K>, StoreError>
    where
        K: PrimitiveInt,
    {
        let write = self.write::<Key<K>>()?;
        Ok(IntegerWriter::new(write))
    }
}

#[cfg(test)]
mod tests {
    extern crate byteorder;
    extern crate tempfile;

    use self::byteorder::{
        ByteOrder,
        LittleEndian,
    };

    use self::tempfile::Builder;

    use std::{
        fs,
        str,
    };

    use super::*;
    use *;

    /// We can't open a directory that doesn't exist.
    #[test]
    fn test_open_fails() {
        let root = Builder::new().prefix("test_open_fails").tempdir().expect("tempdir");
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
        let root = Builder::new().prefix("test_open").tempdir().expect("tempdir");
        println!("Root path: {:?}", root.path());
        fs::create_dir_all(root.path()).expect("dir created");
        assert!(root.path().is_dir());

        let k = Rkv::new(root.path()).expect("new succeeded");
        let _ = k.open_or_create_default().expect("created default");

        let yyy = k.open_or_create("yyy").expect("opened");
        let reader = k.read::<&str>().expect("reader");

        let result = reader.get(&yyy, "foo");
        assert_eq!(None, result.expect("success but no value"));
    }

    #[test]
    fn test_round_trip_and_transactions() {
        let root = Builder::new().prefix("test_round_trip_and_transactions").tempdir().expect("tempdir");
        fs::create_dir_all(root.path()).expect("dir created");
        let k = Rkv::new(root.path()).expect("new succeeded");

        let sk: Store = k.open_or_create("sk").expect("opened");

        {
            let mut writer = k.write::<&str>().expect("writer");
            writer.put(&sk, "foo", &Value::I64(1234)).expect("wrote");
            writer.put(&sk, "noo", &Value::F64(1234.0.into())).expect("wrote");
            writer.put(&sk, "bar", &Value::Bool(true)).expect("wrote");
            writer.put(&sk, "baz", &Value::Str("héllo, yöu")).expect("wrote");
            assert_eq!(writer.get(&sk, "foo").expect("read"), Some(Value::I64(1234)));
            assert_eq!(writer.get(&sk, "noo").expect("read"), Some(Value::F64(1234.0.into())));
            assert_eq!(writer.get(&sk, "bar").expect("read"), Some(Value::Bool(true)));
            assert_eq!(writer.get(&sk, "baz").expect("read"), Some(Value::Str("héllo, yöu")));

            // Isolation. Reads won't return values.
            let r = &k.read::<&str>().unwrap();
            assert_eq!(r.get(&sk, "foo").expect("read"), None);
            assert_eq!(r.get(&sk, "bar").expect("read"), None);
            assert_eq!(r.get(&sk, "baz").expect("read"), None);
        }

        // Dropped: tx rollback. Reads will still return nothing.

        {
            let r = &k.read::<&str>().unwrap();
            assert_eq!(r.get(&sk, "foo").expect("read"), None);
            assert_eq!(r.get(&sk, "bar").expect("read"), None);
            assert_eq!(r.get(&sk, "baz").expect("read"), None);
        }

        {
            let mut writer = k.write::<&str>().expect("writer");
            writer.put(&sk, "foo", &Value::I64(1234)).expect("wrote");
            writer.put(&sk, "bar", &Value::Bool(true)).expect("wrote");
            writer.put(&sk, "baz", &Value::Str("héllo, yöu")).expect("wrote");
            assert_eq!(writer.get(&sk, "foo").expect("read"), Some(Value::I64(1234)));
            assert_eq!(writer.get(&sk, "bar").expect("read"), Some(Value::Bool(true)));
            assert_eq!(writer.get(&sk, "baz").expect("read"), Some(Value::Str("héllo, yöu")));

            writer.commit().expect("committed");
        }

        // Committed. Reads will succeed.
        {
            let r = k.read::<&str>().unwrap();
            assert_eq!(r.get(&sk, "foo").expect("read"), Some(Value::I64(1234)));
            assert_eq!(r.get(&sk, "bar").expect("read"), Some(Value::Bool(true)));
            assert_eq!(r.get(&sk, "baz").expect("read"), Some(Value::Str("héllo, yöu")));
        }

        {
            let mut writer = k.write::<&str>().expect("writer");
            writer.delete(&sk, "foo").expect("deleted");
            writer.delete(&sk, "bar").expect("deleted");
            writer.delete(&sk, "baz").expect("deleted");
            assert_eq!(writer.get(&sk, "foo").expect("read"), None);
            assert_eq!(writer.get(&sk, "bar").expect("read"), None);
            assert_eq!(writer.get(&sk, "baz").expect("read"), None);

            // Isolation. Reads still return values.
            let r = k.read::<&str>().unwrap();
            assert_eq!(r.get(&sk, "foo").expect("read"), Some(Value::I64(1234)));
            assert_eq!(r.get(&sk, "bar").expect("read"), Some(Value::Bool(true)));
            assert_eq!(r.get(&sk, "baz").expect("read"), Some(Value::Str("héllo, yöu")));
        }

        // Dropped: tx rollback. Reads will still return values.

        {
            let r = k.read::<&str>().unwrap();
            assert_eq!(r.get(&sk, "foo").expect("read"), Some(Value::I64(1234)));
            assert_eq!(r.get(&sk, "bar").expect("read"), Some(Value::Bool(true)));
            assert_eq!(r.get(&sk, "baz").expect("read"), Some(Value::Str("héllo, yöu")));
        }

        {
            let mut writer = k.write::<&str>().expect("writer");
            writer.delete(&sk, "foo").expect("deleted");
            writer.delete(&sk, "bar").expect("deleted");
            writer.delete(&sk, "baz").expect("deleted");
            assert_eq!(writer.get(&sk, "foo").expect("read"), None);
            assert_eq!(writer.get(&sk, "bar").expect("read"), None);
            assert_eq!(writer.get(&sk, "baz").expect("read"), None);

            writer.commit().expect("committed");
        }

        // Committed. Reads will succeed but return None to indicate a missing value.
        {
            let r = k.read::<&str>().unwrap();
            assert_eq!(r.get(&sk, "foo").expect("read"), None);
            assert_eq!(r.get(&sk, "bar").expect("read"), None);
            assert_eq!(r.get(&sk, "baz").expect("read"), None);
        }
    }

    #[test]
    fn test_open_store_for_read() {
        let root = Builder::new().prefix("test_open_store_for_read").tempdir().expect("tempdir");
        fs::create_dir_all(root.path()).expect("dir created");
        let k = Rkv::new(root.path()).expect("new succeeded");
        // First create the store, and start a write transaction on it.
        let sk: Store<&str> = k.open_or_create("sk").expect("opened");
        let mut writer = sk.write(&k).expect("writer");
        writer.put("foo", &Value::Str("bar")).expect("write");

        // Open the same store for read, note that the write transaction is still in progress,
        // it should not block the reader though.
        let sk_readonly: Store<&str> = k.open("sk").expect("opened");
        writer.commit().expect("commit");
        // Now the write transaction is committed, any followed reads should see its change.
        let reader = sk_readonly.read(&k).expect("reader");
        assert_eq!(reader.get("foo").expect("read"), Some(Value::Str("bar")));
    }

    #[test]
    #[should_panic(expected = "open a missing store")]
    fn test_open_a_missing_store() {
        let root = Builder::new().prefix("test_open_a_missing_store").tempdir().expect("tempdir");
        fs::create_dir_all(root.path()).expect("dir created");
        let k = Rkv::new(root.path()).expect("new succeeded");
        let _sk: Store<&str> = k.open("sk").expect("open a missing store");
    }

    #[test]
    fn test_open_fail_with_badrslot() {
        let root = Builder::new().prefix("test_open_fail_with_badrslot").tempdir().expect("tempdir");
        fs::create_dir_all(root.path()).expect("dir created");
        let k = Rkv::new(root.path()).expect("new succeeded");
        // First create the store
        let sk: Store<&str> = k.open_or_create("sk").expect("opened");
        // Open a reader on this store
        let _reader = sk.read(&k).expect("reader");
        // Open the same store for read while the reader is in progress will panic
        let store: Result<Store<&str>, StoreError> = k.open("sk");
        match store {
            Err(StoreError::OpenAttemptedDuringTransaction(_thread_id)) => assert!(true),
            _ => panic!("should panic"),
        }
    }

    #[test]
    fn test_read_before_write_num() {
        let root = Builder::new().prefix("test_read_before_write_num").tempdir().expect("tempdir");
        fs::create_dir_all(root.path()).expect("dir created");
        let k = Rkv::new(root.path()).expect("new succeeded");
        let sk: Store = k.open_or_create("sk").expect("opened");

        // Test reading a number, modifying it, and then writing it back.
        // We have to be done with the Value::I64 before calling Writer::put,
        // as the Value::I64 borrows an immutable reference to the Writer.
        // So we extract and copy its primitive value.

        fn get_existing_foo(writer: &Writer<&str>, store: &Store) -> Option<i64> {
            match writer.get(store, "foo").expect("read") {
                Some(Value::I64(val)) => Some(val),
                _ => None,
            }
        }

        let mut writer = k.write::<&str>().expect("writer");
        let mut existing = get_existing_foo(&writer, &sk).unwrap_or(99);
        existing += 1;
        writer.put(&sk, "foo", &Value::I64(existing)).expect("success");

        let updated = get_existing_foo(&writer, &sk).unwrap_or(99);
        assert_eq!(updated, 100);
        writer.commit().expect("commit");
    }

    #[test]
    fn test_read_before_write_str() {
        let root = Builder::new().prefix("test_read_before_write_str").tempdir().expect("tempdir");
        fs::create_dir_all(root.path()).expect("dir created");
        let k = Rkv::new(root.path()).expect("new succeeded");
        let sk: Store = k.open_or_create("sk").expect("opened");

        // Test reading a string, modifying it, and then writing it back.
        // We have to be done with the Value::Str before calling Writer::put,
        // as the Value::Str (and its underlying &str) borrows an immutable
        // reference to the Writer.  So we copy it to a String.

        let mut writer = k.write::<&str>().expect("writer");
        let mut existing = match writer.get(&sk, "foo").expect("read") {
            Some(Value::Str(val)) => val,
            _ => "",
        }.to_string();
        existing.push('…');
        writer.put(&sk, "foo", &Value::Str(&existing)).expect("write");
        writer.commit().expect("commit");
    }

    #[test]
    fn test_concurrent_read_transactions_prohibited() {
        let root = Builder::new().prefix("test_concurrent_reads_prohibited").tempdir().expect("tempdir");
        fs::create_dir_all(root.path()).expect("dir created");
        let k = Rkv::new(root.path()).expect("new succeeded");

        let _first = k.read::<&str>().expect("reader");
        let second = k.read::<&str>();

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
        let root = Builder::new().prefix("test_isolation").tempdir().expect("tempdir");
        fs::create_dir_all(root.path()).expect("dir created");
        let k = Rkv::new(root.path()).expect("new succeeded");
        let s: Store = k.open_or_create("s").expect("opened");

        // Add one field.
        {
            let mut writer = k.write::<&str>().expect("writer");
            writer.put(&s, "foo", &Value::I64(1234)).expect("wrote");
            writer.commit().expect("committed");
        }

        {
            let reader = k.read::<&str>().unwrap();
            assert_eq!(reader.get(&s, "foo").expect("read"), Some(Value::I64(1234)));
        }

        // Establish a long-lived reader that outlasts a writer.
        let reader = k.read::<&str>().expect("reader");
        assert_eq!(reader.get(&s, "foo").expect("read"), Some(Value::I64(1234)));

        // Start a write transaction.
        let mut writer = k.write::<&str>().expect("writer");
        writer.put(&s, "foo", &Value::I64(999)).expect("wrote");

        // The reader and writer are isolated.
        assert_eq!(reader.get(&s, "foo").expect("read"), Some(Value::I64(1234)));
        assert_eq!(writer.get(&s, "foo").expect("read"), Some(Value::I64(999)));

        // If we commit the writer, we still have isolation.
        writer.commit().expect("committed");
        assert_eq!(reader.get(&s, "foo").expect("read"), Some(Value::I64(1234)));

        // A new reader sees the committed value. Note that LMDB doesn't allow two
        // read transactions to exist in the same thread, so we abort the previous one.
        reader.abort();
        let reader = k.read::<&str>().expect("reader");
        assert_eq!(reader.get(&s, "foo").expect("read"), Some(Value::I64(999)));
    }

    #[test]
    fn test_blob() {
        let root = Builder::new().prefix("test_round_trip_blob").tempdir().expect("tempdir");
        fs::create_dir_all(root.path()).expect("dir created");
        let k = Rkv::new(root.path()).expect("new succeeded");
        let sk: Store = k.open_or_create("sk").expect("opened");
        let mut writer = k.write::<&str>().expect("writer");

        assert_eq!(writer.get(&sk, "foo").expect("read"), None);
        writer.put(&sk, "foo", &Value::Blob(&[1, 2, 3, 4])).expect("wrote");
        assert_eq!(writer.get(&sk, "foo").expect("read"), Some(Value::Blob(&[1, 2, 3, 4])));

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
        assert_eq!(writer.get(&sk, "bar").expect("read"), None);
        writer.put(&sk, "bar", &Value::Blob(&u16_to_u8(&u16_array))).expect("wrote");
        let u8_array = match writer.get(&sk, "bar").expect("read") {
            Some(Value::Blob(val)) => val,
            _ => &[],
        };
        assert_eq!(u8_to_u16(u8_array), u16_array);
    }

    #[test]
    #[should_panic(expected = "not yet implemented")]
    fn test_delete_value() {
        let root = Builder::new().prefix("test_delete_value").tempdir().expect("tempdir");
        fs::create_dir_all(root.path()).expect("dir created");
        let k = Rkv::new(root.path()).expect("new succeeded");
        let sk: Store = k.open_or_create_with_flags("sk", DatabaseFlags::DUP_SORT).expect("opened");

        let mut writer = k.write::<&str>().expect("writer");
        writer.put(&sk, "foo", &Value::I64(1234)).expect("wrote");
        writer.put(&sk, "foo", &Value::I64(1235)).expect("wrote");
        writer.delete_value(&sk, "foo", &Value::I64(1234)).expect("deleted");
    }

    #[test]
    fn test_iter() {
        let root = Builder::new().prefix("test_iter").tempdir().expect("tempdir");
        fs::create_dir_all(root.path()).expect("dir created");
        let k = Rkv::new(root.path()).expect("new succeeded");
        let sk: Store = k.open_or_create("sk").expect("opened");

        // An iterator over an empty store returns no values.
        {
            let reader = k.read::<&str>().unwrap();
            let mut iter = reader.iter_start(&sk).unwrap();
            assert!(iter.next().is_none());
        }

        let mut writer = k.write::<&str>().expect("writer");
        writer.put(&sk, "foo", &Value::I64(1234)).expect("wrote");
        writer.put(&sk, "noo", &Value::F64(1234.0.into())).expect("wrote");
        writer.put(&sk, "bar", &Value::Bool(true)).expect("wrote");
        writer.put(&sk, "baz", &Value::Str("héllo, yöu")).expect("wrote");
        writer.put(&sk, "héllò, töűrîst", &Value::Str("Emil.RuleZ!")).expect("wrote");
        writer.put(&sk, "你好，遊客", &Value::Str("米克規則")).expect("wrote");
        writer.commit().expect("committed");

        let reader = k.read::<&str>().unwrap();

        // Reader.iter() returns (key, value) tuples ordered by key.
        let mut iter = reader.iter_start(&sk).unwrap();
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
        let mut iter = reader.iter_from(&sk, "moo").unwrap();
        let (key, val) = iter.next().unwrap();
        assert_eq!(str::from_utf8(key).expect("key"), "noo");
        assert_eq!(val.expect("value"), Some(Value::F64(1234.0.into())));
        let (key, val) = iter.next().unwrap();
        assert_eq!(str::from_utf8(key).expect("key"), "你好，遊客");
        assert_eq!(val.expect("value"), Some(Value::Str("米克規則")));
        assert!(iter.next().is_none());

        // Reader.iter_from() works as expected when the given key is a prefix
        // of a key in the store.
        let mut iter = reader.iter_from(&sk, "no").unwrap();
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
        let root = Builder::new().prefix("test_iter_from_key_greater_than_existing").tempdir().expect("tempdir");
        fs::create_dir_all(root.path()).expect("dir created");
        let k = Rkv::new(root.path()).expect("new succeeded");
        let sk: Store = k.open_or_create("sk").expect("opened");

        let mut writer = k.write::<&str>().expect("writer");
        writer.put(&sk, "foo", &Value::I64(1234)).expect("wrote");
        writer.put(&sk, "noo", &Value::F64(1234.0.into())).expect("wrote");
        writer.put(&sk, "bar", &Value::Bool(true)).expect("wrote");
        writer.put(&sk, "baz", &Value::Str("héllo, yöu")).expect("wrote");
        writer.commit().expect("committed");

        let reader = k.read::<&str>().unwrap();

        // There is no key greater than "nuu", so the underlying LMDB API panics
        // when calling iter_from.  This is unfortunate, and I've requested
        // https://github.com/danburkert/lmdb-rs/pull/29 to make the underlying
        // API return a Result instead.
        //
        // Also see alternative https://github.com/danburkert/lmdb-rs/pull/30.
        //
        reader.iter_from(&sk, "nuu").unwrap();
    }

    #[test]
    fn test_mutilpe_store_read_write() {
        let root = Builder::new().prefix("test_mutilpe_store_read_write").tempdir().expect("tempdir");
        fs::create_dir_all(root.path()).expect("dir created");
        let k = Rkv::new(root.path()).expect("new succeeded");

        let s1: Store = k.open_or_create("store_1").expect("opened");
        let s2: Store = k.open_or_create("store_2").expect("opened");
        let s3: Store = k.open_or_create("store_3").expect("opened");

        let mut writer = k.write::<&str>().expect("writer");
        writer.put(&s1, "foo", &Value::Str("bar")).expect("wrote");
        writer.put(&s2, "foo", &Value::I64(123)).expect("wrote");
        writer.put(&s3, "foo", &Value::Bool(true)).expect("wrote");

        assert_eq!(writer.get(&s1, "foo").expect("read"), Some(Value::Str("bar")));
        assert_eq!(writer.get(&s2, "foo").expect("read"), Some(Value::I64(123)));
        assert_eq!(writer.get(&s3, "foo").expect("read"), Some(Value::Bool(true)));

        writer.commit().expect("committed");

        let reader = k.read::<&str>().expect("unbound_reader");
        assert_eq!(reader.get(&s1, "foo").expect("read"), Some(Value::Str("bar")));
        assert_eq!(reader.get(&s2, "foo").expect("read"), Some(Value::I64(123)));
        assert_eq!(reader.get(&s3, "foo").expect("read"), Some(Value::Bool(true)));
        reader.abort();

        // test delete across multiple stores
        let mut writer = k.write::<&str>().expect("writer");
        writer.delete(&s1, "foo").expect("deleted");
        writer.delete(&s2, "foo").expect("deleted");
        writer.delete(&s3, "foo").expect("deleted");
        writer.commit().expect("committed");

        let reader = k.read::<&str>().expect("reader");
        assert_eq!(reader.get(&s1, "key").expect("value"), None);
        assert_eq!(reader.get(&s2, "key").expect("value"), None);
        assert_eq!(reader.get(&s3, "key").expect("value"), None);
    }

    #[test]
    fn test_multiple_store_iter() {
        let root = Builder::new().prefix("test_multiple_store_iter").tempdir().expect("tempdir");
        fs::create_dir_all(root.path()).expect("dir created");
        let k = Rkv::new(root.path()).expect("new succeeded");
        let s1: Store = k.open_or_create("store_1").expect("opened");
        let s2: Store = k.open_or_create("store_2").expect("opened");

        let mut writer = k.write::<&str>().expect("writer");
        // Write to "s1"
        writer.put(&s1, "foo", &Value::I64(1234)).expect("wrote");
        writer.put(&s1, "noo", &Value::F64(1234.0.into())).expect("wrote");
        writer.put(&s1, "bar", &Value::Bool(true)).expect("wrote");
        writer.put(&s1, "baz", &Value::Str("héllo, yöu")).expect("wrote");
        writer.put(&s1, "héllò, töűrîst", &Value::Str("Emil.RuleZ!")).expect("wrote");
        writer.put(&s1, "你好，遊客", &Value::Str("米克規則")).expect("wrote");
        // Writer to "s2"
        writer.put(&s2, "foo", &Value::I64(1234)).expect("wrote");
        writer.put(&s2, "noo", &Value::F64(1234.0.into())).expect("wrote");
        writer.put(&s2, "bar", &Value::Bool(true)).expect("wrote");
        writer.put(&s2, "baz", &Value::Str("héllo, yöu")).expect("wrote");
        writer.put(&s2, "héllò, töűrîst", &Value::Str("Emil.RuleZ!")).expect("wrote");
        writer.put(&s2, "你好，遊客", &Value::Str("米克規則")).expect("wrote");
        writer.commit().expect("committed");

        let reader = k.read::<&str>().unwrap();

        // Iterate through the whole store in "s1"
        let mut iter = reader.iter_start(&s1).unwrap();
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

        // Iterate through the whole store in "s2"
        let mut iter = reader.iter_start(&s2).unwrap();
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

        // Iterate from a given key in "s1"
        let mut iter = reader.iter_from(&s1, "moo").unwrap();
        let (key, val) = iter.next().unwrap();
        assert_eq!(str::from_utf8(key).expect("key"), "noo");
        assert_eq!(val.expect("value"), Some(Value::F64(1234.0.into())));
        let (key, val) = iter.next().unwrap();
        assert_eq!(str::from_utf8(key).expect("key"), "你好，遊客");
        assert_eq!(val.expect("value"), Some(Value::Str("米克規則")));
        assert!(iter.next().is_none());

        // Iterate from a given key in "s2"
        let mut iter = reader.iter_from(&s2, "moo").unwrap();
        let (key, val) = iter.next().unwrap();
        assert_eq!(str::from_utf8(key).expect("key"), "noo");
        assert_eq!(val.expect("value"), Some(Value::F64(1234.0.into())));
        let (key, val) = iter.next().unwrap();
        assert_eq!(str::from_utf8(key).expect("key"), "你好，遊客");
        assert_eq!(val.expect("value"), Some(Value::Str("米克規則")));
        assert!(iter.next().is_none());

        // Iterate from a given prefix in "s1"
        let mut iter = reader.iter_from(&s1, "no").unwrap();
        let (key, val) = iter.next().unwrap();
        assert_eq!(str::from_utf8(key).expect("key"), "noo");
        assert_eq!(val.expect("value"), Some(Value::F64(1234.0.into())));
        let (key, val) = iter.next().unwrap();
        assert_eq!(str::from_utf8(key).expect("key"), "你好，遊客");
        assert_eq!(val.expect("value"), Some(Value::Str("米克規則")));
        assert!(iter.next().is_none());

        // Iterate from a given prefix in "s2"
        let mut iter = reader.iter_from(&s2, "no").unwrap();
        let (key, val) = iter.next().unwrap();
        assert_eq!(str::from_utf8(key).expect("key"), "noo");
        assert_eq!(val.expect("value"), Some(Value::F64(1234.0.into())));
        let (key, val) = iter.next().unwrap();
        assert_eq!(str::from_utf8(key).expect("key"), "你好，遊客");
        assert_eq!(val.expect("value"), Some(Value::Str("米克規則")));
        assert!(iter.next().is_none());
    }
}
