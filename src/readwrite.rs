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

use std::marker::{
    PhantomData,
};

use lmdb::{
    Database,
    Transaction,
    RoTransaction,
    RwTransaction,
};

use lmdb::{
    WriteFlags,
};

use error::{
    StoreError,
};

use value::{
    Value,
};

use ::Rkv;

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
    phantom: PhantomData<K>,
}

pub struct Reader<'env, K> where K: AsRef<[u8]> {
    tx: RoTransaction<'env>,
    db: Database,
    phantom: PhantomData<K>,
}

impl<'env, K> Writer<'env, K> where K: AsRef<[u8]> {
    pub fn get<'s>(&'s self, k: K) -> Result<Option<Value<'s>>, StoreError> {
        let bytes = self.tx.get(self.db, &k.as_ref());
        read_transform(bytes)
    }

    // TODO: flags
    pub fn put<'s>(&'s mut self, k: K, v: &Value) -> Result<(), StoreError> {
        // TODO: don't allocate twice.
        let bytes = v.to_bytes()?;
        self.tx
            .put(self.db, &k.as_ref(), &bytes, WriteFlags::empty())
            .map_err(StoreError::LmdbError)
    }

    pub fn commit(self) -> Result<(), StoreError> {
        self.tx.commit().map_err(StoreError::LmdbError)
    }

    pub fn abort(self) {
        self.tx.abort();
    }
}

impl<'env, K> Reader<'env, K> where K: AsRef<[u8]> {
    pub fn get<'s>(&'s self, k: K) -> Result<Option<Value<'s>>, StoreError> {
        let bytes = self.tx.get(self.db, &k.as_ref());
        read_transform(bytes)
    }

    pub fn abort(self) {
        self.tx.abort();
    }
}

/// Wrapper around an `lmdb::Database`.
pub struct Store<K> where K: AsRef<[u8]> {
    db: Database,
    phantom: PhantomData<K>,
}

impl<K> Store<K> where K: AsRef<[u8]> {
    pub fn new(db: Database) -> Store<K> {
        Store {
            db: db,
            phantom: PhantomData,
        }
    }

    pub fn read<'env>(&self, env: &'env Rkv) -> Result<Reader<'env, K>, StoreError> {
        let tx = env.read()?;
        Ok(Reader {
            tx: tx,
            db: self.db,
            phantom: PhantomData,
        })
    }

    pub fn write<'env>(&self, env: &'env Rkv) -> Result<Writer<'env, K>, lmdb::Error> {
        let tx = env.write()?;
        Ok(Writer {
            tx: tx,
            db: self.db,
            phantom: PhantomData,
        })
    }

    pub fn get<'env, 'tx>(&self, tx: &'tx RoTransaction<'env>, k: K) -> Result<Option<Value<'tx>>, StoreError> {
        let bytes = tx.get(self.db, &k.as_ref());
        read_transform(bytes)
    }
}
