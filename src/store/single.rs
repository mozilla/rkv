// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use lmdb;

use lmdb::{
    Cursor,
    Database,
    Iter as LmdbIter,
    RoCursor,
    RwTransaction,
    Transaction,
};

use lmdb::WriteFlags;

use crate::error::StoreError;

use crate::value::Value;

fn read_transform(val: Result<&[u8], lmdb::Error>) -> Result<Option<Value>, StoreError> {
    match val {
        Ok(bytes) => Value::from_tagged_slice(bytes).map(Some).map_err(StoreError::DataError),
        Err(lmdb::Error::NotFound) => Ok(None),
        Err(e) => Err(StoreError::LmdbError(e)),
    }
}

#[derive(Copy, Clone)]
pub struct SingleStore {
    db: Database,
}

pub struct Iter<'env> {
    iter: LmdbIter<'env>,
    cursor: RoCursor<'env>,
}

impl SingleStore {
    pub(crate) fn new(db: Database) -> SingleStore {
        SingleStore {
            db,
        }
    }

    pub fn get<'env, T: Transaction, K: AsRef<[u8]>>(
        &self,
        txn: &'env T,
        k: K,
    ) -> Result<Option<Value<'env>>, StoreError> {
        let bytes = txn.get(self.db, &k);
        read_transform(bytes)
    }

    // TODO: flags
    pub fn put<K: AsRef<[u8]>>(&mut self, txn: &mut RwTransaction, k: K, v: &Value) -> Result<(), StoreError> {
        // TODO: don't allocate twice.
        let bytes = v.to_bytes()?;
        txn.put(self.db, &k, &bytes, WriteFlags::empty()).map_err(StoreError::LmdbError)
    }

    pub fn delete<K: AsRef<[u8]>>(&mut self, txn: &mut RwTransaction, k: K) -> Result<(), StoreError> {
        txn.del(self.db, &k, None).map_err(StoreError::LmdbError)
    }

    pub fn iter_start<'env, T: Transaction>(&self, txn: &'env T) -> Result<Iter<'env>, StoreError> {
        let mut cursor = txn.open_ro_cursor(self.db).map_err(StoreError::LmdbError)?;

        // We call Cursor.iter() instead of Cursor.iter_start() because
        // the latter panics at "called `Result::unwrap()` on an `Err` value:
        // NotFound" when there are no items in the store, whereas the former
        // returns an iterator that yields no items.
        //
        // And since we create the Cursor and don't change its position, we can
        // be sure that a call to Cursor.iter() will start at the beginning.
        //
        let iter = cursor.iter();

        Ok(Iter {
            iter,
            cursor,
        })
    }

    pub fn iter_from<'env, T: Transaction, K: AsRef<[u8]>>(
        &self,
        txn: &'env T,
        k: K,
    ) -> Result<Iter<'env>, StoreError> {
        let mut cursor = txn.open_ro_cursor(self.db).map_err(StoreError::LmdbError)?;
        let iter = cursor.iter_from(k);
        Ok(Iter {
            iter,
            cursor,
        })
    }
}

impl<'env> Iterator for Iter<'env> {
    type Item = Result<(&'env [u8], Option<Value<'env>>), StoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            None => None,
            Some(Ok((key, bytes))) => match read_transform(Ok(bytes)) {
                Ok(val) => Some(Ok((key, val))),
                Err(err) => Some(Err(err)),
            },
            Some(Err(err)) => Some(Err(StoreError::LmdbError(err))),
        }
    }
}
