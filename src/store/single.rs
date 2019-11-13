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

use crate::backend::{
    BackendDatabase,
    BackendFlags,
    BackendIter,
    BackendRoCursor,
    BackendRwTransaction,
};
use crate::error::StoreError;
use crate::helpers::read_transform;
use crate::readwrite::{
    Readable,
    Writer,
};
use crate::value::Value;

type EmptyResult = Result<(), StoreError>;

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub struct SingleStore<D> {
    db: D,
}

pub struct Iter<'env, I, C> {
    iter: I,
    cursor: C,
    phantom: PhantomData<&'env ()>,
}

impl<D> SingleStore<D>
where
    D: BackendDatabase,
{
    pub(crate) fn new(db: D) -> SingleStore<D> {
        SingleStore {
            db,
        }
    }

    pub fn get<'env, R, K>(&self, reader: &'env R, k: K) -> Result<Option<Value<'env>>, StoreError>
    where
        R: Readable<'env, Database = D>,
        K: AsRef<[u8]>,
    {
        reader.get(&self.db, &k)
    }

    // TODO: flags
    pub fn put<T, K>(&self, writer: &mut Writer<T>, k: K, v: &Value) -> EmptyResult
    where
        T: BackendRwTransaction<Database = D>,
        K: AsRef<[u8]>,
    {
        writer.put(&self.db, &k, v, T::Flags::empty())
    }

    #[cfg(not(feature = "db-dup-sort"))]
    pub fn delete<T, K>(&self, writer: &mut Writer<T>, k: K) -> EmptyResult
    where
        T: BackendRwTransaction<Database = D>,
        K: AsRef<[u8]>,
    {
        writer.delete(&self.db, &k)
    }

    #[cfg(feature = "db-dup-sort")]
    pub fn delete<T, K>(&self, writer: &mut Writer<T>, k: K) -> EmptyResult
    where
        T: BackendRwTransaction<Database = D>,
        K: AsRef<[u8]>,
    {
        writer.delete(&self.db, &k, None)
    }

    pub fn iter_start<'env, R, I, C>(&self, reader: &'env R) -> Result<Iter<'env, I, C>, StoreError>
    where
        R: Readable<'env, Database = D, RoCursor = C>,
        I: BackendIter<'env>,
        C: BackendRoCursor<'env, Iter = I>,
    {
        let mut cursor = reader.open_ro_cursor(&self.db)?;

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
            phantom: PhantomData,
        })
    }

    pub fn iter_from<'env, R, I, C, K>(&self, reader: &'env R, k: K) -> Result<Iter<'env, I, C>, StoreError>
    where
        R: Readable<'env, Database = D, RoCursor = C>,
        I: BackendIter<'env>,
        C: BackendRoCursor<'env, Iter = I>,
        K: AsRef<[u8]>,
    {
        let mut cursor = reader.open_ro_cursor(&self.db)?;
        let iter = cursor.iter_from(k);

        Ok(Iter {
            iter,
            cursor,
            phantom: PhantomData,
        })
    }

    pub fn clear<T>(&self, writer: &mut Writer<T>) -> EmptyResult
    where
        D: BackendDatabase,
        T: BackendRwTransaction<Database = D>,
    {
        writer.clear(&self.db)
    }
}

impl<'env, I, C> Iterator for Iter<'env, I, C>
where
    I: BackendIter<'env>,
    C: BackendRoCursor<'env, Iter = I>,
{
    type Item = Result<(&'env [u8], Option<Value<'env>>), StoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            None => None,
            Some(Ok((key, bytes))) => match read_transform(Ok(bytes)) {
                Ok(val) => Some(Ok((key, val))),
                Err(err) => Some(Err(err)),
            },
            Some(Err(err)) => Some(Err(err.into())),
        }
    }
}
