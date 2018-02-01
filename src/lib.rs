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
extern crate uuid;

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
    DatabaseFlags,
    Cursor,
    RoCursor,
    RwCursor,
    Environment,
    EnvironmentBuilder,
    EnvironmentFlags,
    Transaction,
    RoTransaction,
    RwTransaction,
};

use uuid::{
    Uuid,
    UuidBytes,
};

/// We define a set of types, associated with simple integers, to annotate values
/// stored in LMDB. This is to avoid an accidental 'cast' from a value of one type
/// to another. For this reason we don't simply use `deserialize` from the `bincode`
/// crate.
#[repr(u8)]
#[derive(Debug, PartialEq, Eq)]
enum Type {
    Bool    = 1,
    U64     = 2,
    I64     = 3,
    F64     = 4,
    Instant = 5,    // Millisecond-precision timestamp.
    Uuid    = 6,
    Str     = 7,
    Json    = 8,
}

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

enum Value<'s> {
    Bool(bool),
    U64(u64),
    I64(i64),
    F64(f64),
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
enum DataError {
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
    EncodingError {
        value_type: Type,
        err: Box<bincode::ErrorKind>,
    },

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
            return deserialize(data).map_err(|e| DataError::EncodingError { value_type: t, err: e })
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
                deserialize(data).map(Value::F64)
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
        }.map_err(|e| DataError::EncodingError { value_type: t, err: e })
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

#[derive(Debug, Eq, Fail, PartialEq)]
enum StoreError {
    #[fail(display = "directory does not exist: {:?}", _0)]
    DirectoryDoesNotExistError(PathBuf),

    #[fail(display = "lmdb error: {}", _0)]
    LmdbError(lmdb::Error),
}

/// Wrapper around an `lmdb::Environment`.
pub struct Kista {
    path: PathBuf,
    env: Environment,
}

impl Kista {
    fn new(path: &Path) -> Result<Kista, StoreError> {
        let mut builder = Environment::new();
        builder.set_max_dbs(10);

        // Future: set flags, maximum size, etc. here if necessary.
        let environment =
            builder.open(path)
                   .map_err(|e|
                       match e {
                           lmdb::Error::Other(2) => StoreError::DirectoryDoesNotExistError(path.into()),
                           e => StoreError::LmdbError(e),
                       })?;
        Ok(Kista {
            path: path.into(),
            env: environment,
        })
    }

    fn create_or_open_default(&self) -> Result<Store, StoreError> {
        self.create_or_open(None)
    }

    fn create_or_open<'s, T>(&self, name: T) -> Result<Store, StoreError>
    where T: Into<Option<&'s str>> {
        let flags = DatabaseFlags::empty();
        let db = self.env.create_db(name.into(), flags).map_err(StoreError::LmdbError)?;
        Ok(Store {
            db: db,
        })
    }
}

/// Wrapper around an `lmdb::Database`.
pub struct Store {
    db: Database,
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

        assert_eq!(Some(StoreError::DirectoryDoesNotExistError(nope.to_path_buf())),
                   Kista::new(nope.as_path()).err());
    }

    #[test]
    fn test_open() {
        let root = TempDir::new("test_open").expect("tempdir");
        println!("Root path: {:?}", root.path());
        fs::create_dir_all(root.path()).expect("dir created");
        assert!(root.path().is_dir());

        let k = Kista::new(root.path()).expect("new succeeded");
        assert!(k.create_or_open_default().is_ok());
        assert!(k.create_or_open("yyy").is_ok());
    }
}
