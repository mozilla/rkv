// Copyright 2018-2019 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::sync::RwLockReadGuard;

use lmdb::{
    Database,
    RoCursor,
    RoTransaction,
    RwTransaction,
    Transaction,
    WriteFlags,
};

use crate::error::StoreError;
use crate::read_transform;
use crate::value::Value;

pub struct Reader<'env> {
    pub txn: RoTransaction<'env>,
    lock: RwLockReadGuard<'env, ()>,
}

pub struct Writer<'env> {
    pub txn: RwTransaction<'env>,
    lock: RwLockReadGuard<'env, ()>,
}

pub trait Readable {
    fn get<K: AsRef<[u8]>>(&self, db: Database, k: &K) -> Result<Option<Value>, StoreError>;
    fn open_ro_cursor(&self, db: Database) -> Result<RoCursor, StoreError>;
}

impl<'env> Readable for Reader<'env> {
    fn get<K: AsRef<[u8]>>(&self, db: Database, k: &K) -> Result<Option<Value>, StoreError> {
        let bytes = self.txn.get(db, &k);
        read_transform(bytes)
    }

    fn open_ro_cursor(&self, db: Database) -> Result<RoCursor, StoreError> {
        self.txn.open_ro_cursor(db).map_err(StoreError::LmdbError)
    }
}

impl<'env> Reader<'env> {
    pub(crate) fn new(txn: RoTransaction<'env>, lock: RwLockReadGuard<'env, ()>) -> Reader<'env> {
        Reader {
            txn,
            lock,
        }
    }

    pub fn abort(self) {
        self.txn.abort();
    }
}

impl<'env> Readable for Writer<'env> {
    fn get<K: AsRef<[u8]>>(&self, db: Database, k: &K) -> Result<Option<Value>, StoreError> {
        let bytes = self.txn.get(db, &k);
        read_transform(bytes)
    }

    fn open_ro_cursor(&self, db: Database) -> Result<RoCursor, StoreError> {
        self.txn.open_ro_cursor(db).map_err(StoreError::LmdbError)
    }
}

impl<'env> Writer<'env> {
    pub(crate) fn new(txn: RwTransaction<'env>, lock: RwLockReadGuard<'env, ()>) -> Writer<'env> {
        Writer {
            txn,
            lock,
        }
    }

    pub fn commit(self) -> Result<(), StoreError> {
        self.txn.commit().map_err(StoreError::LmdbError)
    }

    pub fn abort(self) {
        self.txn.abort();
    }

    pub(crate) fn put<K: AsRef<[u8]>>(
        &mut self,
        db: Database,
        k: &K,
        v: &Value,
        flags: WriteFlags,
    ) -> Result<(), StoreError> {
        // TODO: don't allocate twice.
        self.txn.put(db, &k, &v.to_bytes()?, flags).map_err(StoreError::LmdbError)
    }

    pub(crate) fn delete<K: AsRef<[u8]>>(&mut self, db: Database, k: &K, v: Option<&[u8]>) -> Result<(), StoreError> {
        self.txn.del(db, &k, v).map_err(StoreError::LmdbError)
    }

    pub(crate) fn clear(&mut self, db: Database) -> Result<(), StoreError> {
        self.txn.clear_db(db).map_err(StoreError::LmdbError)
    }
}
