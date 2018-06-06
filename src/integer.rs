// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::marker::{
    PhantomData,
};

use bincode::{
    Infinite,
    serialize,
};

use lmdb::{
    Database,
    RoTransaction,
};

use serde::{
    Serialize,
};

use error::{
    DataError,
    StoreError,
};

use value::{
    Value,
};

use readwrite::{
    Reader,
    Store,
    Writer,
};

use ::Rkv;


pub trait EncodableKey {
    fn to_bytes(&self) -> Result<Vec<u8>, DataError>;
}

pub trait PrimitiveInt: EncodableKey {}

impl PrimitiveInt for u32 {}

impl<T> EncodableKey for T where T: Serialize {
    fn to_bytes(&self) -> Result<Vec<u8>, DataError> {
        serialize(self, Infinite)         // TODO: limited key length.
        .map_err(|e| e.into())
    }
}

struct Key<K> {
    bytes: Vec<u8>,
    phantom: PhantomData<K>,
}

impl<K> AsRef<[u8]> for Key<K> where K: EncodableKey {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

impl<K> Key<K> where K: EncodableKey {
    fn new(k: K) -> Result<Key<K>, DataError> {
        Ok(Key {
            bytes: k.to_bytes()?,
            phantom: PhantomData,
        })
    }
}

pub struct IntegerStore<K> where K: PrimitiveInt {
    inner: Store<Key<K>>,
}

pub struct IntegerReader<'env, K> where K: PrimitiveInt {
    inner: Reader<'env, Key<K>>,
}

impl<'env, K> IntegerReader<'env, K> where K: PrimitiveInt {
    pub fn get<'s>(&'s self, k: K) -> Result<Option<Value<'s>>, StoreError> {
        self.inner.get(Key::new(k)?)
    }

    pub fn abort(self) {
        self.inner.abort();
    }
}

pub struct IntegerWriter<'env, K> where K: PrimitiveInt {
    inner: Writer<'env, Key<K>>,
}

impl<'env, K> IntegerWriter<'env, K> where K: PrimitiveInt {
    pub fn get<'s>(&'s self, k: K) -> Result<Option<Value<'s>>, StoreError> {
        self.inner.get(Key::new(k)?)
    }

    pub fn put<'s>(&'s mut self, k: K, v: &Value) -> Result<(), StoreError> {
        self.inner.put(Key::new(k)?, v)
    }

    fn abort(self) {
        self.inner.abort();
    }
}

impl<K> IntegerStore<K> where K: PrimitiveInt {
    pub fn new(db: Database) -> IntegerStore<K> {
        IntegerStore {
            inner: Store::new(db),
        }
    }

    pub fn read<'env>(&self, env: &'env Rkv) -> Result<IntegerReader<'env, K>, StoreError> {
        Ok(IntegerReader {
            inner: self.inner.read(env)?,
        })
    }

    pub fn write<'env>(&mut self, env: &'env Rkv) -> Result<IntegerWriter<'env, K>, StoreError> {
        Ok(IntegerWriter {
            inner: self.inner.write(env)?,
        })
    }

    pub fn get<'env, 'tx>(&self, tx: &'tx RoTransaction<'env>, k: K) -> Result<Option<Value<'tx>>, StoreError> {
        let key = Key::new(k)?;
        self.inner.get(tx, key)
    }
}

#[cfg(test)]
mod tests {
    extern crate tempfile;

    use self::tempfile::{
        Builder,
    };
    use std::fs;

    use super::*;

    #[test]
    fn test_integer_keys() {
        let root = Builder::new().prefix("test_integer_keys").tempdir().expect("tempdir");
        fs::create_dir_all(root.path()).expect("dir created");
        let k = Rkv::new(root.path()).expect("new succeeded");
        let mut s: IntegerStore<u32> = k.create_or_open_integer("s").expect("open");

        let mut writer = s.write(&k).expect("writer");

        writer.put(123, &Value::Str("hello!")).expect("write");
        assert_eq!(writer.get(123).expect("read"), Some(Value::Str("hello!")));
    }
}
