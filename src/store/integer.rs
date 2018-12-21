// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::marker::PhantomData;

use bincode::serialize;

use serde::Serialize;

use lmdb::{
    Database,
    Transaction, 
    RwTransaction,
};

use crate::error::{
    DataError,
    StoreError,
};

use crate::value::Value;

use crate::store::single::{
    SingleStore,
};

pub trait EncodableKey {
    fn to_bytes(&self) -> Result<Vec<u8>, DataError>;
}

pub trait PrimitiveInt: EncodableKey {}

impl PrimitiveInt for u32 {}

impl<T> EncodableKey for T
where
    T: Serialize,
{
    fn to_bytes(&self) -> Result<Vec<u8>, DataError> {
        serialize(self) // TODO: limited key length.
            .map_err(|e| e.into())
    }
}

pub(crate) struct Key<K> {
    bytes: Vec<u8>,
    phantom: PhantomData<K>,
}

impl<K> AsRef<[u8]> for Key<K>
where
    K: EncodableKey,
{
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

impl<K> Key<K>
where
    K: EncodableKey,
{
    fn new(k: K) -> Result<Key<K>, DataError> {
        Ok(Key {
            bytes: k.to_bytes()?,
            phantom: PhantomData,
        })
    }
}

pub struct IntegerStore<K>
where
    K: PrimitiveInt,
{
    inner: SingleStore,
}

impl<'env, K> IntegerStore<K>
where
    K: PrimitiveInt,
{
    pub(crate) fn new(store: SingleStore) -> IntegerStore<K> {
        IntegerStore {
            inner: store,
        }
    }

    pub fn get<T: Transaction>(&self, txn: &T, k: K) -> Result<Option<Value>, StoreError> {
        self.inner.get(txn, Key::new(k)?)
    }

    pub fn put(&mut self, txn: &RwTransaction, k: K, v: &Value) -> Result<(), StoreError> {
        self.inner.put(txn, Key::new(k)?, v)
    }

    pub fn abort(self) {
        self.inner.abort();
    }

    pub fn commit(self) -> Result<(), StoreError> {
        self.inner.commit()
    }
}

#[cfg(test)]
mod tests {
    extern crate tempfile;

    use self::tempfile::Builder;
    use std::fs;

    use super::*;
    use crate::*;

    #[test]
    fn test_integer_keys() {
        let root = Builder::new().prefix("test_integer_keys").tempdir().expect("tempdir");
        fs::create_dir_all(root.path()).expect("dir created");
        let k = Rkv::new(root.path()).expect("new succeeded");
        let s = k.open_or_create_integer("s").expect("open");

        macro_rules! test_integer_keys {
            ($type:ty, $key:expr) => {{
                let mut writer = k.write_int::<$type>().expect("writer");

                writer.put(s, $key, &Value::Str("hello!")).expect("write");
                assert_eq!(writer.get(s, $key).expect("read"), Some(Value::Str("hello!")));
                writer.commit().expect("committed");

                let reader = k.read_int::<$type>().expect("reader");
                assert_eq!(reader.get(s, $key).expect("read"), Some(Value::Str("hello!")));
            }};
        }

        test_integer_keys!(u32, std::u32::MIN);
        test_integer_keys!(u32, std::u32::MAX);
    }
}
