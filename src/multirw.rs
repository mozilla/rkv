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
use std::rc::Rc;
use lmdb::{
    Cursor,
    Database,
    IterDup as LmdbIter,
    RoCursor,
    RoTransaction,
    RwTransaction,
    Transaction,
};

use lmdb::WriteFlags;

use error::StoreError;

use value::Value;

fn read_transform(val: Result<&[u8], lmdb::Error>) -> Result<Option<Value>, StoreError> {
    match val {
        Ok(bytes) => Value::from_tagged_slice(bytes).map(Some).map_err(StoreError::DataError),
        Err(lmdb::Error::NotFound) => Ok(None),
        Err(e) => Err(StoreError::LmdbError(e)),
    }
}

pub struct MultiWriter<'env, K>
where
    K: AsRef<[u8]>,
{
    tx: RwTransaction<'env>,
    phantom: PhantomData<K>,
}

pub struct MultiReader<'env, K>
where
    K: AsRef<[u8]>,
{
    tx: RoTransaction<'env>,
    phantom: PhantomData<K>,
}

pub struct Iter<'env> {
    iter: LmdbIter<'env>,
    cursor: RoCursor<'env>,
}

pub struct MultiCursor<'env, K> 
where
    K: AsRef<[u8]>
{
    iter: Option<LmdbIter<'env>>,
    cursor: Option<RoCursor<'env>>,
    writer: MultiWriter<'env, K>,
}

impl<'env, K> MultiCursor<'env, K>
where
    K: AsRef<[u8]> 
{

    pub(crate) fn new(
        iter: LmdbIter<'env>,
        cursor: RoCursor<'env>,
        writer: MultiWriter<'env, K>)
    -> MultiCursor<'env, K> {
        MultiCursor {
            iter: Some(iter),
            cursor: Some(cursor),
            writer: writer,
        }
    }

    /// Update the location of this cursor to point to the provided key. 
    /// This cursor will start at the lexographically smallest value that is equal-to or
    /// greater-than the provided key. 
    pub fn get(&mut self, k: K) -> Result<(), StoreError> {
        self.iter = self.cursor.map(|c| c.iter_dup_from(&k));
        Ok(())
    }

    /// Consume this MultiCursor and give the `MultiWriter` back
    /// So that it may perform additional tasks
    pub fn close(self) -> MultiWriter<'env, K> {
        self.writer
    }
}

impl<'env, K> MultiWriter<'env, K>
where
    K: AsRef<[u8]>,
{
    pub(crate) fn new(txn: RwTransaction) -> MultiWriter<K> {
        MultiWriter {
            tx: txn,
            phantom: PhantomData,
        }
    }

    /// Provides a cursor to the lexographically smallest value that is equal-to, or greater-than
    /// the provided key.
    /// This cursor consumes the writer, as it is not safe to attempt to access records while using
    /// put or delete, as the location of the records might change
    pub fn get(self, store: MultiStore, k: K) -> Result<MultiCursor<'env, K>, StoreError> {
        let mut mc = MultiCursor { iter: None, cursor: None, writer: self };
        mc.cursor = Some(mc.writer.tx.open_ro_cursor(store.0).map_err(StoreError::LmdbError)?);
        mc.iter = mc.cursor.map(|c| c.iter_dup_from(&k));
        Ok(mc)
    }


    /// Insert a value at the specified key. 
    /// This put will allow duplicate entries.  If you wish to have duplicate entries
    /// rejected, use the `put_flags` function and specify NO_DUP_DATA
    pub fn put(&mut self, store: MultiStore, k: K, v: &Value) -> Result<(), StoreError> {
        // TODO: don't allocate twice.
        let bytes = v.to_bytes()?;
        self.tx.put(store.0, &k, &bytes, WriteFlags::empty()).map_err(StoreError::LmdbError)
    }
    
    pub fn put_with_flags(&mut self, store: MultiStore, k: K, v: &Value, flags: WriteFlags) -> Result<(), StoreError> {
        // TODO: don't allocate twice.
        let bytes = v.to_bytes()?;
        self.tx.put(store.0, &k, &bytes, flags).map_err(StoreError::LmdbError)
    }

    pub fn delete(&mut self, store: MultiStore, k: K, v: &Value) -> Result<(), StoreError> {
        self.tx.del(store.0, &k, Some(&v.to_bytes()?)).map_err(StoreError::LmdbError)
    }

    pub fn commit(self) -> Result<(), StoreError> {
        self.tx.commit().map_err(StoreError::LmdbError)
    }

    pub fn abort(self) {
        self.tx.abort();
    }
}

impl<'env, K> MultiReader<'env, K>
where
    K: AsRef<[u8]>,
{
    pub(crate) fn new(txn: RoTransaction) -> MultiReader<K> {
        MultiReader {
            tx: txn,
            phantom: PhantomData,
        }
    }

    /// Provides an iterator starting at the lexographically smallest value that is
    /// equal-to, or greater-than the provided key.
    pub fn get(&self, store: MultiStore, k: K) -> Result<Iter, StoreError> {
        let mut cursor = self.tx.open_ro_cursor(store.0).map_err(StoreError::LmdbError)?;
        let iter = cursor.iter_dup_from(&k);
        Ok(Iter {
            iter,
            cursor,
        })
    }

    /// Cancel this read transaction (not particularly useful)
    pub fn abort(self) {
        self.tx.abort();
    }

    /// Provides an iterator starting at the lexographically smallest value in the store
    pub fn iter_start(&self, store: MultiStore) -> Result<Iter, StoreError> {
        let mut cursor = self.tx.open_ro_cursor(store.0).map_err(StoreError::LmdbError)?;

        // We call Cursor.iter() instead of Cursor.iter_start() because
        // the latter panics at "called `Result::unwrap()` on an `Err` value:
        // NotFound" when there are no items in the store, whereas the former
        // returns an iterator that yields no items.
        //
        // And since we create the Cursor and don't change its position, we can
        // be sure that a call to Cursor.iter() will start at the beginning.
        //
        let iter = cursor.iter_dup();

        Ok(Iter {
            iter,
            cursor,
        })
    }
}

impl<'env, K> Iterator for MultiCursor<'env, K>
where
    K: AsRef<[u8]>
{
    type Item = Iter<'env>

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.unwrap().next() {
            None => None,
            Some((iter) => Some(iter)
        }
    }
}

impl<'env> Iterator for Iter<'env> {
    type Item = (&'env [u8], Result<Option<Value<'env>>, StoreError>);

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            None => None,
            Some((key, bytes)) => Some((key, read_transform(Ok(bytes)))),
        }
    }
}

/// New type around an `lmdb::Database`.  At this time, the underlying LMDB
/// handle (within lmdb-rs::Database) is a C integer, so Copy is automatic.
#[derive(Copy, Clone)]
pub struct MultiStore(Database);

impl MultiStore {
    pub(crate) fn new(db: Database) -> MultiStore {
        MultiStore(db)
    }
}
