// Copyright 2018-2019 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::mem;
use std::sync::RwLockReadGuard;

use lmdb::{
    Database,
    Error as LmdbError,
    RoCursor,
    RoTransaction,
    RwTransaction,
    Transaction,
    WriteFlags,
};

use crate::env::Rkv;
use crate::error::StoreError;
use crate::read_transform;
use crate::value::{
    OwnedValue,
    Value,
};

pub struct Reader<'env> {
    pub txn: RoTransaction<'env>,
    lock: RwLockReadGuard<'env, ()>,
}

pub struct Writer<'env> {
    pub txn: RwTransaction<'env>,
    lock: RwLockReadGuard<'env, ()>,
}

enum WriteOps {
    Clear,
    Delete,
    Put,
}

pub struct WriterEx<'env> {
    pub txn: Option<RwTransaction<'env>>,
    lock: RwLockReadGuard<'env, ()>,
    rkv: &'env Rkv,
    redo_logs: Vec<(WriteOps, Database, Option<Vec<u8>>, Option<OwnedValue>, Option<WriteFlags>)>,
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

impl<'env> WriterEx<'env> {
    pub(crate) fn new(txn: RwTransaction<'env>, lock: RwLockReadGuard<'env, ()>, rkv: &'env Rkv) -> WriterEx<'env> {
        WriterEx {
            txn: Some(txn),
            lock,
            rkv,
            redo_logs: Default::default(),
        }
    }

    pub fn commit(self) -> Result<(), StoreError> {
        self.txn.unwrap().commit().map_err(StoreError::LmdbError)
    }

    pub fn abort(self) {
        self.txn.unwrap().abort();
    }

    pub(crate) fn put<K: AsRef<[u8]>>(
        &mut self,
        db: Database,
        k: &K,
        v: &Value,
        flags: WriteFlags,
    ) -> Result<(), StoreError> {
        // self.txn is guaranteed to be Some(txn) here.
        let txn = self.txn.as_mut().unwrap();
        let ret = txn.put(db, &k, &v.to_bytes()?, flags);
        match ret {
            Ok(_) => {
                self.redo_logs.push((WriteOps::Put, db, Some(k.as_ref().to_vec()), Some(OwnedValue::from(v)), Some(flags)));
                Ok(())
            },
            Err(LmdbError::MapFull) => {
                match self.resize(db, k, v, flags) {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        // A failed resize will leave self.txn to None.
                        let raw_txn = self.rkv.raw_write()?;
                        self.txn = Some(raw_txn);
                        Err(e)
                    },
                }
            },
            Err(e) => Err(StoreError::from(e)),
        }
    }

    // Resize the mmap and replay the redo logs.
    fn resize(&mut self, db: Database, key: &AsRef<[u8]>, value: &Value, flags: WriteFlags) -> Result<(), StoreError> {
        // Abort the transaction for resizing.
        let mut temp = None;
        mem::swap(&mut self.txn, &mut temp);
        temp.unwrap().abort();

        const ONE_GIGABYTE: usize = 1_073_741_824;
        let info = self.rkv.info()?;
        let size = info.map_size();
        let new_size;

        if info.map_size() > ONE_GIGABYTE {
            new_size = size.checked_add(ONE_GIGABYTE).ok_or(StoreError::ResizeError)?;
        } else {
            new_size = size.checked_mul(2).ok_or(StoreError::ResizeError)?;
        }
        self.rkv.set_map_size(new_size)?;

        // Redo all the succeeded writes for this writer.
        let mut txn = self.rkv.raw_write()?;
        for (ops, db, key, value, flag) in self.redo_logs.iter() {
            match ops {
                WriteOps::Put => {
                    let k = key.as_ref().unwrap();
                    let v = value.as_ref().unwrap();
                    txn.put(*db, k, &Value::from(v).to_bytes()?, flag.unwrap()).map_err(StoreError::LmdbError)?
                },
                WriteOps::Clear => txn.clear_db(*db).map_err(StoreError::LmdbError)?,
                WriteOps::Delete => {
                    let k = key.as_ref().unwrap();
                    match value {
                        None => txn.del(*db, k, None).map_err(StoreError::LmdbError)?,
                        Some(ov) => txn.del(*db, k, Some(&(Value::from(ov)).to_bytes()?)).map_err(StoreError::LmdbError)?,
                    }
                },
            }
        }
        txn.put(db, &key, &value.to_bytes()?, flags).map_err(StoreError::LmdbError)?;
        self.redo_logs.push((WriteOps::Put, db, Some(key.as_ref().to_vec()), Some(OwnedValue::from(value)), Some(flags)));
        mem::swap(&mut self.txn, &mut Some(txn));
        Ok(())
    }

    pub(crate) fn delete<K: AsRef<[u8]>>(&mut self, db: Database, k: &K, v: Option<&[u8]>) -> Result<(), StoreError> {
        self.txn.as_mut().unwrap().del(db, &k, v).map_err(StoreError::LmdbError)?;
        let ov = v.map(|bytes| Value::from_tagged_slice(bytes).unwrap()).as_ref().map(OwnedValue::from);
        self.redo_logs.push((WriteOps::Delete, db, Some(k.as_ref().to_vec()), ov, None));
        Ok(())
    }

    pub(crate) fn clear(&mut self, db: Database) -> Result<(), StoreError> {
        self.txn.as_mut().unwrap().clear_db(db).map_err(StoreError::LmdbError)?;
        self.redo_logs.push((WriteOps::Clear, db, None, None, None));
        Ok(())
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
