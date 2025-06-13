// Copyright 2018-2019 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use crossbeam_channel::Sender;
use elsa::FrozenVec;
use rusqlite::Connection;


use std::{fmt::{self, Debug, Formatter}, mem::ManuallyDrop};
use super::{DatabaseImpl, ErrorImpl, RoCursorImpl, WriteFlagsImpl};
use crate::backend::traits::{
    BackendRoCursorTransaction, BackendRoTransaction, BackendRwCursorTransaction,
    BackendRwTransaction,
};

pub struct RoTransactionImpl<'t> {
    connection: ManuallyDrop<Connection>,
    tx: Sender<rusqlite::Connection>,
    // we need to keep the values around because the get() method returns a reference to them
    // so we store them in FrozenVec because we can append to non-mut FrozenVec
    values: FrozenVec<Vec<u8>>,
    _phantom: std::marker::PhantomData<&'t ()>
}

impl<'t> RoTransactionImpl<'t> {
    pub(crate) fn new(cnx: rusqlite::Connection, tx: Sender<rusqlite::Connection>) -> Result<RoTransactionImpl<'t>, ErrorImpl> {
        // we don't use rusqlite transcations because they make lifetimes awkward beacuse we'd need to hold on to the connection
        // and the transaction.
        cnx.execute_batch("BEGIN DEFERRED").map_err(ErrorImpl::SqliteError)?;
        Ok(RoTransactionImpl {
            connection: ManuallyDrop::new(cnx),
            tx,
            values: FrozenVec::new(),
            _phantom: std::marker::PhantomData
        })
    }
}

impl<'t> Debug for RoTransactionImpl<'t> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RoTransactionImpl")
            .finish()
    }
}

impl<'t> BackendRoTransaction for RoTransactionImpl<'t> {
    type Database = DatabaseImpl;
    type Error = ErrorImpl;

    fn get(&self, db: &Self::Database, key: &[u8]) -> Result<&[u8], Self::Error> {
        let mut stmt = self.connection.prepare_cached(&format!("SELECT value FROM {} WHERE key = ?1", db.name)).map_err(ErrorImpl::SqliteError)?;
        let result: Vec<u8> = stmt.query_row([key], |r| r.get(0)).map_err(ErrorImpl::SqliteError)?;
        Ok(self.values.push_get(result))
    }

    fn abort(self) {
        // noop
    }
}

impl<'t> Drop for RoTransactionImpl<'t> {
    fn drop(&mut self) {
        self.connection.execute_batch("COMMIT").map_err(ErrorImpl::SqliteError).unwrap();
        unsafe {
            self.tx.send(ManuallyDrop::take(&mut self.connection)).unwrap();
        }
    }
} 

impl<'t> BackendRoCursorTransaction<'t> for RoTransactionImpl<'t> {
    type RoCursor = RoCursorImpl<'t>;

    fn open_ro_cursor(&'t self, db: &Self::Database) -> Result<Self::RoCursor, Self::Error> {
        panic!("Not implemented")
    }
}

pub struct RwTransactionImpl<'t> {
    connection: ManuallyDrop<Connection>,
    tx: Sender<rusqlite::Connection>,
    values: FrozenVec<Vec<u8>>,
    finished: bool,
    _phantom: std::marker::PhantomData<&'t ()>
}

impl<'t> RwTransactionImpl<'t> {
    pub(crate) fn new(cnx: rusqlite::Connection, tx: Sender<rusqlite::Connection>) -> Result<RwTransactionImpl<'t>, ErrorImpl> {
        cnx.execute_batch("BEGIN DEFERRED").map_err(ErrorImpl::SqliteError)?;

        Ok(RwTransactionImpl {
            connection: ManuallyDrop::new(cnx),
            tx,
            values: FrozenVec::new(),
            finished: false,
            _phantom: std::marker::PhantomData
        })
    }
}

impl<'t> Debug for RwTransactionImpl<'t> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RwTransactionImpl")
            .finish()
    }
}

impl<'t> BackendRwTransaction for RwTransactionImpl<'t> {
    type Database = DatabaseImpl;
    type Error = ErrorImpl;
    type Flags = WriteFlagsImpl;

    fn get(&self, db: &Self::Database, key: &[u8]) -> Result<&[u8], Self::Error> {
        let mut stmt = self.connection.prepare_cached(&format!("SELECT value FROM {} WHERE key = ?1", db.name)).map_err(ErrorImpl::SqliteError)?;
        dbg!("get part2");
        let result: Vec<u8> = stmt.query_row(rusqlite::params![key], |r| r.get(0)).map_err(ErrorImpl::SqliteError)?;
        Ok(self.values.push_get(result))
    }

    fn put(
        &mut self,
        db: &Self::Database,
        key: &[u8],
        value: &[u8],
        flags: Self::Flags,
    ) -> Result<(), Self::Error> {
        let mut stmt = self.connection.prepare_cached(&format!("INSERT INTO {}(key, value) values (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value", db.name)).map_err(ErrorImpl::SqliteError)?;
        stmt.execute([key, value]).map_err(ErrorImpl::SqliteError)?;
        Ok(())
    }

    #[cfg(not(feature = "db-dup-sort"))]
    fn del(&mut self, db: &Self::Database, key: &[u8]) -> Result<(), Self::Error> {
        let mut stmt = self.connection.prepare_cached(&format!("DELETE FROM {} WHERE key = ?1", db.name)).map_err(ErrorImpl::SqliteError)?;
        stmt.execute([key]).map_err(ErrorImpl::SqliteError)?;
        Ok(())
    }

    #[cfg(feature = "db-dup-sort")]
    fn del(
        &mut self,
        db: &Self::Database,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<(), Self::Error> {
        unimplemented!()
    }

    fn clear_db(&mut self, db: &Self::Database) -> Result<(), Self::Error> {
       unimplemented!("clear_db is not implemented for SQLite")
    }

    fn commit(mut self) -> Result<(), Self::Error> {
        self.connection.execute_batch("COMMIT").map_err(ErrorImpl::SqliteError)?;
        self.finished = true;
        Ok(())
    }

    fn abort(mut self) {
        self.connection.execute_batch("ROLLBACK").expect("Failed to rollback transaction");
        self.finished = true;
    }
}

impl<'t> Drop for RwTransactionImpl<'t> {
    fn drop(&mut self) {
        if !self.finished { self.connection.execute_batch("ROLLBACK").expect("Failed to rollback transaction"); }
        unsafe {
            self.tx.send(ManuallyDrop::take(&mut self.connection)).unwrap();
        }
    }
} 

impl<'t> BackendRwCursorTransaction<'t> for RwTransactionImpl<'t> {
    type RoCursor = RoCursorImpl<'t>;

    fn open_ro_cursor(&'t self, db: &Self::Database) -> Result<Self::RoCursor, Self::Error> {
        unimplemented!("open_ro_cursor is not implemented for SQLite")
    }
}
