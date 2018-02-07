// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]
#![allow(unused_imports)]

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

use bincode::{
    Infinite,
    deserialize,
    serialize,
};

use failure::Error;

use lmdb::{
    Database,
    Cursor,
    RoCursor,
    RwCursor,
    Environment,
    Transaction,
    RoTransaction,
    RwTransaction,
};

use ordered_float::{
    OrderedFloat,
};

use uuid::{
    Uuid,
    UuidBytes,
};

pub use lmdb::{
    DatabaseFlags,
    EnvironmentBuilder,
    EnvironmentFlags,
    WriteFlags,
};

/// We define a set of types, associated with simple integers, to annotate values
/// stored in LMDB. This is to avoid an accidental 'cast' from a value of one type
/// to another. For this reason we don't simply use `deserialize` from the `bincode`
/// crate.
#[repr(u8)]
#[derive(Debug, PartialEq, Eq)]
pub enum Type {
    Bool    = 1,
    U64     = 2,
    I64     = 3,
    F64     = 4,
    Instant = 5,    // Millisecond-precision timestamp.
    Uuid    = 6,
    Str     = 7,
    Json    = 8,
}

/// We use manual tagging, because <https://github.com/serde-rs/serde/issues/610>.
impl Type {
    fn from_tag(tag: u8) -> Result<Type, DataError> {
        Type::from_primitive(tag).ok_or(DataError::UnknownType(tag))
    }

    fn to_tag(self) -> u8 {
        self as u8
    }

    fn from_primitive(p: u8) -> Option<Type> {
        match p {
            1 => Some(Type::Bool),
            2 => Some(Type::U64),
            3 => Some(Type::I64),
            4 => Some(Type::F64),
            5 => Some(Type::Instant),
            6 => Some(Type::Uuid),
            7 => Some(Type::Str),
            8 => Some(Type::Json),
            _ => None,
        }
    }
}

impl std::fmt::Display for Type {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        f.write_str(match *self {
            Type::Bool    => "bool",
            Type::U64     => "u64",
            Type::I64     => "i64",
            Type::F64     => "f64",
            Type::Instant => "instant",
            Type::Uuid    => "uuid",
            Type::Str     => "str",
            Type::Json    => "json",
        })
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum Value<'s> {
    Bool(bool),
    U64(u64),
    I64(i64),
    F64(OrderedFloat<f64>),
    Instant(i64),    // Millisecond-precision timestamp.
    Uuid(&'s UuidBytes),
    Str(&'s str),
    Json(&'s str),
}

// TODO: implement conversion between the two types of `Value` wrapper.
// This might be unnecessary: we'll probably jump straight to primitives.
enum OwnedValue {
    Bool(bool),
    U64(u64),
    I64(i64),
    F64(f64),
    Instant(i64),    // Millisecond-precision timestamp.
    Uuid(Uuid),
    Str(String),
    Json(String),    // TODO
}

#[derive(Debug, Fail)]
pub enum DataError {
    #[fail(display = "unknown type tag: {}", _0)]
    UnknownType(u8),

    #[fail(display = "unexpected type tag: expected {}, got {}", expected, actual)]
    UnexpectedType {
        expected: Type,
        actual: Type,
    },

    #[fail(display = "empty data; expected tag")]
    Empty,

    #[fail(display = "invalid value for type {}: {}", value_type, err)]
    DecodingError {
        value_type: Type,
        err: Box<bincode::ErrorKind>,
    },

    #[fail(display = "couldn't encode value: {}", _0)]
    EncodingError(Box<bincode::ErrorKind>),

    #[fail(display = "invalid uuid bytes")]
    InvalidUuid,
}

fn uuid<'s>(bytes: &'s [u8]) -> Result<Value<'s>, DataError> {
    if bytes.len() == 16 {
        Ok(Value::Uuid(array_ref![bytes, 0, 16]))
    } else {
        Err(DataError::InvalidUuid)
    }
}

impl<'s> Value<'s> {
    fn expected_from_tagged_slice(expected: Type, slice: &'s [u8]) -> Result<Value<'s>, DataError> {
        let (tag, data) = slice.split_first().ok_or(DataError::Empty)?;
        let t = Type::from_tag(*tag)?;
        if t == expected {
            return Err(DataError::UnexpectedType { expected: expected, actual: t });
        }
        Value::from_type_and_data(t, data)
    }

    fn from_tagged_slice(slice: &'s [u8]) -> Result<Value<'s>, DataError> {
        let (tag, data) = slice.split_first().ok_or(DataError::Empty)?;
        let t = Type::from_tag(*tag)?;
        Value::from_type_and_data(t, data)
    }

    fn from_type_and_data(t: Type, data: &'s [u8]) -> Result<Value<'s>, DataError> {
        if t == Type::Uuid {
            return deserialize(data).map_err(|e| DataError::DecodingError { value_type: t, err: e })
                                    .map(uuid)?;
        }

        match t {
            Type::Bool => {
                deserialize(data).map(Value::Bool)
            },
            Type::U64 => {
                deserialize(data).map(Value::U64)
            },
            Type::I64 => {
                deserialize(data).map(Value::I64)
            },
            Type::F64 => {
                deserialize(data).map(OrderedFloat).map(Value::F64)
            },
            Type::Instant => {
                deserialize(data).map(Value::Instant)
            },
            Type::Str => {
                deserialize(data).map(Value::Str)
            },
            Type::Json => {
                deserialize(data).map(Value::Json)
            },
            Type::Uuid => {
                // Processed above to avoid verbose duplication of error transforms.
                unreachable!()
            },
        }.map_err(|e| DataError::DecodingError { value_type: t, err: e })
    }

    fn to_bytes(&self) -> Result<Vec<u8>, DataError> {
        match self {
            &Value::Bool(ref v) => {
                serialize(&(Type::Bool.to_tag(), *v), Infinite)
            },
            &Value::U64(ref v) => {
                serialize(&(Type::U64.to_tag(), *v), Infinite)
            },
            &Value::I64(ref v) => {
                serialize(&(Type::I64.to_tag(), *v), Infinite)
            },
            &Value::F64(ref v) => {
                serialize(&(Type::F64.to_tag(), v.0), Infinite)
            },
            &Value::Instant(ref v) => {
                serialize(&(Type::Instant.to_tag(), *v), Infinite)
            },
            &Value::Str(ref v) => {
                serialize(&(Type::Str.to_tag(), v), Infinite)
            },
            &Value::Json(ref v) => {
                serialize(&(Type::Json.to_tag(), v), Infinite)
            },
            &Value::Uuid(ref v) => {
                // Processed above to avoid verbose duplication of error transforms.
                serialize(&(Type::Uuid.to_tag(), v), Infinite)
            },
        }.map_err(DataError::EncodingError)
    }
}

trait AsValue {
    fn as_value(&self) -> Result<Value, DataError>;
}

impl<'a> AsValue for &'a [u8] {
    fn as_value(&self) -> Result<Value, DataError> {
        Value::from_tagged_slice(self)
    }
}

#[derive(Debug, Fail)]
pub enum StoreError {
    #[fail(display = "directory does not exist: {:?}", _0)]
    DirectoryDoesNotExistError(PathBuf),

    #[fail(display = "data error: {:?}", _0)]
    DataError(DataError),

    #[fail(display = "lmdb error: {}", _0)]
    LmdbError(lmdb::Error),
}

impl From<DataError> for StoreError {
    fn from(e: DataError) -> StoreError {
        StoreError::DataError(e)
    }
}

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

static DEFAULT_MAX_DBS: c_uint = 5;

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
        Ok(Store {
            db: db,
            phantom: ::std::marker::PhantomData,
        })
    }

    pub fn read(&self) -> Result<RoTransaction, lmdb::Error> {
        self.env.begin_ro_txn()
    }

    pub fn write(&self) -> Result<RwTransaction, lmdb::Error> {
        self.env.begin_rw_txn()
    }
}

fn read_transform<'x>(val: Result<&'x [u8], lmdb::Error>) -> Result<Option<Value<'x>>, StoreError> {
    match val {
        Ok(bytes) => Value::from_tagged_slice(bytes).map(Some)
                                                    .map_err(StoreError::DataError),
        Err(lmdb::Error::NotFound) => Ok(None),
        Err(e) => Err(StoreError::LmdbError(e)),
    }
}

pub struct Writer<'env, K> where K: AsRef<[u8]> {
    tx: RwTransaction<'env>,
    db: Database,
    phantom: ::std::marker::PhantomData<K>,
}

pub struct Reader<'env, K> where K: AsRef<[u8]> {
    tx: RoTransaction<'env>,
    db: Database,
    phantom: ::std::marker::PhantomData<K>,
}

impl<'env, K> Writer<'env, K> where K: AsRef<[u8]> {
    fn get<'s>(&'s self, k: K) -> Result<Option<Value<'s>>, StoreError> {
        let bytes = self.tx.get(self.db, &k.as_ref());
        read_transform(bytes)
    }

    // TODO: flags
    fn put<'s>(&'s mut self, k: K, v: &Value) -> Result<(), StoreError> {
        // TODO: don't allocate twice.
        let bytes = v.to_bytes()?;
        self.tx
            .put(self.db, &k.as_ref(), &bytes, WriteFlags::empty())
            .map_err(StoreError::LmdbError)
    }

    fn commit(self) -> Result<(), StoreError> {
        self.tx.commit().map_err(StoreError::LmdbError)
    }
}

impl<'env, K> Reader<'env, K> where K: AsRef<[u8]> {
    fn get<'s>(&'s self, k: K) -> Result<Option<Value<'s>>, StoreError> {
        let bytes = self.tx.get(self.db, &k.as_ref());
        read_transform(bytes)
    }
}

/// Wrapper around an `lmdb::Database`.
pub struct Store<K> where K: AsRef<[u8]> {
    db: Database,
    phantom: ::std::marker::PhantomData<K>,
}

impl<K> Store<K> where K: AsRef<[u8]> {
    fn read<'env>(&self, env: &'env Kista) -> Result<Reader<'env, K>, lmdb::Error> {
        let tx = env.read()?;
        Ok(Reader {
            tx: tx,
            db: self.db,
            phantom: ::std::marker::PhantomData,
        })
    }

    fn write<'env>(&mut self, env: &'env Kista) -> Result<Writer<'env, K>, lmdb::Error> {
        let tx = env.write()?;
        Ok(Writer {
            tx: tx,
            db: self.db,
            phantom: ::std::marker::PhantomData,
        })
    }

    fn get<'env, 'tx>(&self, tx: &'tx RoTransaction<'env>, k: K) -> Result<Option<Value<'tx>>, StoreError> {
        let bytes = tx.get(self.db, &k.as_ref());
        read_transform(bytes)
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
    fn test_round_trip() {
        let root = TempDir::new("test_open").expect("tempdir");
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
}
