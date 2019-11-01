// Copyright 2018-2019 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::HashMap;

use uuid::Uuid;

use super::{
    database::Snapshot,
    DatabaseFlagsImpl,
    DatabaseImpl,
    EnvironmentImpl,
    ErrorImpl,
    RoCursorImpl,
    WriteFlagsImpl,
};
use crate::backend::traits::{
    BackendRoCursorTransaction,
    BackendRoTransaction,
    BackendRwCursorTransaction,
    BackendRwTransaction,
};

#[derive(Debug)]
pub struct RoTransactionImpl<'env> {
    env: &'env EnvironmentImpl,
    snapshots: HashMap<Uuid, Result<Snapshot, ErrorImpl>>,
}

impl<'env> RoTransactionImpl<'env> {
    pub(crate) fn new(env: &'env EnvironmentImpl) -> Result<RoTransactionImpl<'env>, ErrorImpl> {
        let snapshots = env.dbs()?.iter().map(|(_, db)| (*db.id(), db.snapshot())).collect();
        Ok(RoTransactionImpl {
            env,
            snapshots,
        })
    }
}

impl<'env> BackendRoTransaction for RoTransactionImpl<'env> {
    type Error = ErrorImpl;
    type Database = DatabaseImpl;
    type Flags = WriteFlagsImpl;

    fn get(&self, db: &Self::Database, key: &[u8]) -> Result<&[u8], Self::Error> {
        let snapshot = self.snapshots.get(db.id()).ok_or_else(|| ErrorImpl::DbIsForeignError)?;
        let data = snapshot.as_ref().map_err(|_| ErrorImpl::TxnPoisonError)?;
        data.get(key).ok_or_else(|| ErrorImpl::KeyValuePairNotFound)
    }

    fn abort(self) {
        // noop
    }
}

impl<'env> BackendRoCursorTransaction<'env> for RoTransactionImpl<'env> {
    type RoCursor = RoCursorImpl<'env>;

    fn open_ro_cursor(&'env self, db: &Self::Database) -> Result<Self::RoCursor, Self::Error> {
        let snapshot = self.snapshots.get(db.id()).ok_or_else(|| ErrorImpl::DbIsForeignError)?;
        let data = snapshot.as_ref().map_err(|_| ErrorImpl::TxnPoisonError)?;
        Ok(RoCursorImpl(data))
    }
}

#[derive(Debug)]
pub struct RwTransactionImpl<'env> {
    env: &'env EnvironmentImpl,
    snapshots: HashMap<Uuid, Result<Snapshot, ErrorImpl>>,
}

impl<'env> RwTransactionImpl<'env> {
    pub(crate) fn new(env: &'env EnvironmentImpl) -> Result<RwTransactionImpl<'env>, ErrorImpl> {
        let snapshots = env.dbs()?.iter().map(|(_, db)| (*db.id(), db.snapshot())).collect();
        Ok(RwTransactionImpl {
            env,
            snapshots,
        })
    }
}

impl<'env> BackendRwTransaction for RwTransactionImpl<'env> {
    type Error = ErrorImpl;
    type Database = DatabaseImpl;
    type Flags = WriteFlagsImpl;

    fn get(&self, db: &Self::Database, key: &[u8]) -> Result<&[u8], Self::Error> {
        let snapshot = self.snapshots.get(db.id()).ok_or_else(|| ErrorImpl::DbIsForeignError)?;
        let data = snapshot.as_ref().map_err(|_| ErrorImpl::TxnPoisonError)?;
        data.get(key).ok_or_else(|| ErrorImpl::KeyValuePairNotFound)
    }

    fn put(&mut self, db: &Self::Database, key: &[u8], value: &[u8], _flags: Self::Flags) -> Result<(), Self::Error> {
        let snapshot = self.snapshots.get_mut(db.id()).ok_or_else(|| ErrorImpl::DbIsForeignError)?;
        let data = snapshot.as_mut().map_err(|_| ErrorImpl::TxnPoisonError)?;
        if db.flags().contains(DatabaseFlagsImpl::DUP_SORT) {
            data.put_dup(key, value);
        } else {
            data.put_one(key, value);
        }
        Ok(())
    }

    fn del(&mut self, db: &Self::Database, key: &[u8], value: Option<&[u8]>) -> Result<(), Self::Error> {
        let snapshot = self.snapshots.get_mut(db.id()).ok_or_else(|| ErrorImpl::DbIsForeignError)?;
        let data = snapshot.as_mut().map_err(|_| ErrorImpl::TxnPoisonError)?;
        let deleted = match (value, db.flags()) {
            (Some(value), flags) if flags.contains(DatabaseFlagsImpl::DUP_SORT) => data.del_exact(key, value),
            _ => data.del_all(key),
        };
        Ok(deleted.ok_or_else(|| ErrorImpl::KeyValuePairNotFound)?)
    }

    fn clear_db(&mut self, db: &Self::Database) -> Result<(), Self::Error> {
        let snapshot = self.snapshots.get_mut(db.id()).ok_or_else(|| ErrorImpl::DbIsForeignError)?;
        let data = snapshot.as_mut().map_err(|_| ErrorImpl::TxnPoisonError)?;
        data.clear();
        Ok(())
    }

    fn commit(self) -> Result<(), Self::Error> {
        let mut dbs = self.env.dbs_mut()?;

        for (id, snapshot) in self.snapshots {
            match dbs.iter_mut().find(|(_, db)| db.id() == &id) {
                Some((_, db)) => {
                    db.replace(snapshot?)?;
                },
                None => {
                    unreachable!();
                },
            }
        }

        drop(dbs);
        self.env.write_to_disk()
    }

    fn abort(self) {
        // noop
    }
}

impl<'env> BackendRwCursorTransaction<'env> for RwTransactionImpl<'env> {
    type RoCursor = RoCursorImpl<'env>;

    fn open_ro_cursor(&'env self, db: &Self::Database) -> Result<Self::RoCursor, Self::Error> {
        let snapshot = self.snapshots.get(db.id()).ok_or_else(|| ErrorImpl::DbIsForeignError)?;
        let data = snapshot.as_ref().map_err(|_| ErrorImpl::TxnPoisonError)?;
        Ok(RoCursorImpl(data))
    }
}
