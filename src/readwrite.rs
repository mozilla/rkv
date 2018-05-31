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
    Cursor,
    Database,
    Iter as LmdbIter,
    RoCursor,
    RoTransaction,
    RwTransaction,
    Transaction,
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

pub struct Iter<'env> {
    iter: LmdbIter<'env>,
    cursor: RoCursor<'env>,
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

    pub fn delete<'s>(&'s mut self, k: K) -> Result<(), StoreError> {
        self.tx
            .del(self.db, &k.as_ref(), None)
            .map_err(StoreError::LmdbError)
    }

    pub fn delete_value<'s>(&'s mut self, _k: K, _v: &Value) -> Result<(), StoreError> {
        // Even better would be to make this a method only on a dupsort store —
        // it would need a little bit of reorganizing of types and traits,
        // but when I see "If the database does not support sorted duplicate
        // data items (MDB_DUPSORT) the data parameter is ignored" in the docs,
        // I see a footgun that we can avoid by using the type system.
        unimplemented!();
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

    pub fn iter_start<'s>(&'s self) -> Result<Iter<'s>, StoreError> {
        let mut cursor = self.tx.open_ro_cursor(self.db).map_err(StoreError::LmdbError)?;

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
            iter: iter,
            cursor: cursor,
        })
    }

    pub fn iter_from<'s>(&'s self, k: K) -> Result<Iter<'s>, StoreError> {
        let mut cursor = self.tx.open_ro_cursor(self.db).map_err(StoreError::LmdbError)?;
        let iter = cursor.iter_from(k);
        Ok(Iter {
            iter: iter,
            cursor: cursor,
        })
    }
}

impl<'env> Iterator for Iter<'env> {
    type Item = (&'env [u8], Result<Option<Value<'env>>, StoreError>);

    fn next(&mut self) -> Option<(&'env [u8], Result<Option<Value<'env>>, StoreError>)> {
        match self.iter.next() {
            None => None,
            Some((key, bytes)) => Some((key, read_transform(Ok(bytes)))),
        }
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

    /// Note: there may be only one write transaction active at any given time,
    /// so this will block if any other writers currently exist for this store.
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
