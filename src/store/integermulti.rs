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
    Transaction, 
    RwTransaction,
    WriteFlags,
};

use crate::error::{
    StoreError,
};

use crate::value::Value;

use crate::store::multi::{
    MultiStore,
    Iter,
};

use crate::store::integer::{
    PrimitiveInt,
    Key,
};

pub struct MultiIntegerStore<K>
where
    K: PrimitiveInt,
{
    inner: MultiStore,
}

impl<'env, K> MultiIntegerStore<K>
where
    K: PrimitiveInt,
{
    pub(crate) fn new(store: MultiStore) -> MultiIntegerStore<K> {
        MultiIntegerStore {
            inner: store,
        }
    }

    pub fn get<T: Transaction>(&self, txn: &T, k: K) -> Result<Option<Iter>, StoreError> {
        self.inner.get(txn, Key::new(k)?)
    }
    
    pub fn get_first<T: Transaction>(&self, txn: &T, k: K) -> Result<Option<Value>, StoreError> {
        self.inner.get_first(txn, Key::new(k)?)
    }

    pub fn put(&mut self, txn: &RwTransaction, k: K, v: &Value) -> Result<(), StoreError> {
        self.inner.put(txn, Key::new(k)?, v)
    }
    
    pub fn put_with_flags(&mut self, txn: &RwTransaction, k: K, v: &Value, flags: WriteFlags) -> Result<(), StoreError> {
        self.inner.put(txn, Key::new(k)?, v, flags)
    }
    
    pub fn delete_all<K: AsRef<[u8]>>(&mut self, txn: &RwTransaction, k: K) -> Result<(), StoreError> {
        self.inner.del(txn, Key::new(k)?, None)
    }

    pub fn delete<K: AsRef<[u8]>>(&mut self, txn: &RwTransaction, k: K, v: &Value) -> Result<(), StoreError> {
        self.inner.del(txn, Key::new(k), v).map_err(StoreError::LmdbError)
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
