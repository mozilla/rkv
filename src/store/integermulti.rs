// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use lmdb::{
    Database,
    RwTransaction,
    Transaction,
    WriteFlags,
};

use std::marker::PhantomData;

use crate::error::StoreError;

use crate::value::Value;

use crate::store::multi::{
    Iter,
    MultiStore,
};

use crate::store::integer::{
    Key,
    PrimitiveInt,
};

pub struct MultiIntegerStore<K>
where
    K: PrimitiveInt,
{
    inner: MultiStore,
    phantom: PhantomData<K>,
}

impl<K> MultiIntegerStore<K>
where
    K: PrimitiveInt,
{
    pub(crate) fn new(db: Database) -> MultiIntegerStore<K> {
        MultiIntegerStore {
            inner: MultiStore::new(db),
            phantom: PhantomData,
        }
    }

    pub fn get<'env, T: Transaction>(&self, txn: &'env T, k: K) -> Result<Iter<'env>, StoreError> {
        self.inner.get(txn, Key::new(k)?)
    }

    pub fn get_first<'env, T: Transaction>(&self, txn: &'env T, k: K) -> Result<Option<Value<'env>>, StoreError> {
        self.inner.get_first(txn, Key::new(k)?)
    }

    pub fn put(&mut self, txn: &mut RwTransaction, k: K, v: &Value) -> Result<(), StoreError> {
        self.inner.put(txn, Key::new(k)?, v)
    }

    pub fn put_with_flags(
        &mut self,
        txn: &mut RwTransaction,
        k: K,
        v: &Value,
        flags: WriteFlags,
    ) -> Result<(), StoreError> {
        self.inner.put_with_flags(txn, Key::new(k)?, v, flags)
    }

    pub fn delete_all(&mut self, txn: &mut RwTransaction, k: K) -> Result<(), StoreError> {
        self.inner.delete_all(txn, Key::new(k)?)
    }

    pub fn delete(&mut self, txn: &mut RwTransaction, k: K, v: &Value) -> Result<(), StoreError> {
        self.inner.delete(txn, Key::new(k)?, v)
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
        let mut s = k.open_multi_integer("s", StoreOptions::create()).expect("open");

        macro_rules! test_integer_keys {
            ($type:ty, $key:expr) => {{
                let mut writer = k.write().expect("writer");

                s.put(&mut writer, $key, &Value::Str("hello!")).expect("write");
                assert_eq!(s.get_first(&writer, $key).expect("read"), Some(Value::Str("hello!")));
                writer.commit().expect("committed");

                let reader = k.read().expect("reader");
                assert_eq!(s.get_first(&reader, $key).expect("read"), Some(Value::Str("hello!")));
            }};
        }

        test_integer_keys!(u32, std::u32::MIN);
        test_integer_keys!(u32, std::u32::MAX);
    }
}
