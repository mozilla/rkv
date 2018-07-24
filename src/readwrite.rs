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

use std::marker::PhantomData;

use lmdb::{
    Cursor,
    Database,
    Iter as LmdbIter,
    RoCursor,
    RoTransaction,
    RwTransaction,
    Transaction,
};

use lmdb::WriteFlags;

use error::StoreError;

use value::Value;

use Rkv;

fn read_transform<'x>(val: Result<&'x [u8], lmdb::Error>) -> Result<Option<Value<'x>>, StoreError> {
    match val {
        Ok(bytes) => Value::from_tagged_slice(bytes).map(Some).map_err(StoreError::DataError),
        Err(lmdb::Error::NotFound) => Ok(None),
        Err(e) => Err(StoreError::LmdbError(e)),
    }
}

pub struct Writer<'env, K>
where
    K: AsRef<[u8]>,
{
    tx: RwTransaction<'env>,
    db: Database,
    phantom: PhantomData<K>,
}

pub struct Reader<'env, K>
where
    K: AsRef<[u8]>,
{
    tx: RoTransaction<'env>,
    db: Database,
    phantom: PhantomData<K>,
}

pub struct Iter<'env> {
    iter: LmdbIter<'env>,
    cursor: RoCursor<'env>,
}

impl<'env, K> Writer<'env, K>
where
    K: AsRef<[u8]>,
{
    fn inner_get<'s>(&'s self, k: K, store: Option<&'s Store<K>>) -> Result<Option<Value<'s>>, StoreError> {
        let db = match store {
            Some(s) => s.db,
            None => self.db,
        };
        let bytes = self.tx.get(db, &k.as_ref());
        read_transform(bytes)
    }

    pub fn get<'s>(&'s self, k: K) -> Result<Option<Value<'s>>, StoreError> {
        self.inner_get(k, None)
    }

    pub fn get_in<'s>(&'s self, k: K, store: &'s Store<K>) -> Result<Option<Value<'s>>, StoreError> {
        self.inner_get(k, Some(store))
    }

    fn inner_put<'s>(&mut self, k: K, v: &Value, store: Option<&'s Store<K>>) -> Result<(), StoreError> {
        // TODO: don't allocate twice.
        let bytes = v.to_bytes()?;
        let db = match store {
            Some(s) => s.db,
            None => self.db,
        };
        self.tx.put(db, &k.as_ref(), &bytes, WriteFlags::empty()).map_err(StoreError::LmdbError)
    }

    // TODO: flags
    pub fn put<'s>(&'s mut self, k: K, v: &Value) -> Result<(), StoreError> {
        self.inner_put(k, v, None)
    }

    // TODO: flags
    pub fn put_in<'s>(&'s mut self, k: K, v: &Value, store: &'s Store<K>) -> Result<(), StoreError> {
        self.inner_put(k, v, Some(store))
    }

    fn inner_delete<'s>(&'s mut self, k: K, store: Option<&'s Store<K>>) -> Result<(), StoreError> {
        let db = match store {
            Some(s) => s.db,
            None => self.db,
        };
        self.tx.del(db, &k.as_ref(), None).map_err(StoreError::LmdbError)
    }

    pub fn delete<'s>(&'s mut self, k: K) -> Result<(), StoreError> {
        self.inner_delete(k, None)
    }

    pub fn delete_in<'s>(&'s mut self, k: K, store: &'s Store<K>) -> Result<(), StoreError> {
        self.inner_delete(k, Some(store))
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

impl<'env, K> Reader<'env, K>
where
    K: AsRef<[u8]>,
{
    fn inner_get<'s>(&'s self, k: K, store: Option<&'s Store<K>>) -> Result<Option<Value<'s>>, StoreError> {
        let db = match store {
            Some(s) => s.db,
            None => self.db,
        };
        let bytes = self.tx.get(db, &k.as_ref());
        read_transform(bytes)
    }

    pub fn get<'s>(&'s self, k: K) -> Result<Option<Value<'s>>, StoreError> {
        self.inner_get(k, None)
    }

    pub fn get_in<'s>(&'s self, k: K, store: &'s Store<K>) -> Result<Option<Value<'s>>, StoreError> {
        self.inner_get(k, Some(store))
    }

    pub fn abort(self) {
        self.tx.abort();
    }

    fn inner_iter_start<'s>(&'s self, store: Option<&Store<K>>) -> Result<Iter<'s>, StoreError> {
        let db = match store {
            Some(s) => s.db,
            None => self.db,
        };
        let mut cursor = self.tx.open_ro_cursor(db).map_err(StoreError::LmdbError)?;

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

    pub fn iter_start<'s>(&'s self) -> Result<Iter<'s>, StoreError> {
        self.inner_iter_start(None)
    }

    pub fn iter_start_in<'s>(&'s self, store: &Store<K>) -> Result<Iter<'s>, StoreError> {
        self.inner_iter_start(Some(store))
    }

    fn inner_iter_from<'s>(&'s self, k: K, store: Option<&Store<K>>) -> Result<Iter<'s>, StoreError> {
        let db = match store {
            Some(s) => s.db,
            None => self.db,
        };
        let mut cursor = self.tx.open_ro_cursor(db).map_err(StoreError::LmdbError)?;
        let iter = cursor.iter_from(k);
        Ok(Iter {
            iter: iter,
            cursor: cursor,
        })
    }

    pub fn iter_from<'s>(&'s self, k: K) -> Result<Iter<'s>, StoreError> {
        self.inner_iter_from(k, None)
    }

    pub fn iter_from_in<'s>(&'s self, k: K, store: &Store<K>) -> Result<Iter<'s>, StoreError> {
        self.inner_iter_from(k, Some(store))
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
pub struct Store<K>
where
    K: AsRef<[u8]>,
{
    db: Database,
    phantom: PhantomData<K>,
}

impl<K> Store<K>
where
    K: AsRef<[u8]>,
{
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
