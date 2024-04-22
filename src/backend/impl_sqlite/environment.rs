// Copyright 2018-2019 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::{
    fs,
    path::{Path, PathBuf},
};


use crossbeam_channel::{Receiver, Sender};

use super::{
    DatabaseFlagsImpl, DatabaseImpl, EnvironmentFlagsImpl, ErrorImpl, InfoImpl, RoTransactionImpl,
    RwTransactionImpl, StatImpl,
};
use crate::backend::common::RecoveryStrategy;
use crate::backend::traits::{
    BackendEnvironment, BackendEnvironmentBuilder, BackendInfo, BackendIter, BackendRoCursor,
    BackendRoCursorTransaction, BackendStat,
};

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub struct EnvironmentBuilderImpl {
    env_path_type: EnvironmentPathType,
    env_lock_type: EnvironmentLockType,
    env_db_type: EnvironmentDefaultDbType,
    make_dir_if_needed: bool,
}

impl<'b> BackendEnvironmentBuilder<'b> for EnvironmentBuilderImpl {
    type Environment = EnvironmentImpl;
    type Error = ErrorImpl;
    type Flags = EnvironmentFlagsImpl;

    fn new() -> EnvironmentBuilderImpl {
        EnvironmentBuilderImpl {
            env_path_type: EnvironmentPathType::SubDir,
            env_lock_type: EnvironmentLockType::Lockfile,
            env_db_type: EnvironmentDefaultDbType::SingleDatabase,
            make_dir_if_needed: false,
        }
    }

    fn set_flags<T>(&mut self, flags: T) -> &mut Self
    where
        T: Into<Self::Flags>,
    {
        let flags = flags.into();
        self
    }

    fn set_max_readers(&mut self, max_readers: u32) -> &mut Self {
        self
    }

    fn set_max_dbs(&mut self, max_dbs: u32) -> &mut Self {
        if max_dbs > 0 {
            self.env_db_type = EnvironmentDefaultDbType::MultipleNamedDatabases
        }
        self
    }

    fn set_map_size(&mut self, size: usize) -> &mut Self {
        self
    }

    fn set_make_dir_if_needed(&mut self, make_dir_if_needed: bool) -> &mut Self {
        self.make_dir_if_needed = make_dir_if_needed;
        self
    }

    /// **UNIMPLEMENTED.** Will panic at runtime.
    fn set_corruption_recovery_strategy(&mut self, _strategy: RecoveryStrategy) -> &mut Self {
        // Unfortunately, when opening a database, LMDB doesn't handle all the ways it could have
        // been corrupted. Prefer using the `SafeMode` backend if this is important.
        unimplemented!();
    }

    fn open(&self, path: &Path) -> Result<Self::Environment, Self::Error> {
        dbg!(path);
        let flags: rusqlite::OpenFlags = rusqlite::OpenFlags::default();

        let connection = rusqlite::Connection::open_with_flags(path.join("db"), flags).map_err(|e| {
            ErrorImpl::SqliteError(e)
        })?;
        EnvironmentImpl::new(connection)
    }
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum EnvironmentPathType {
    SubDir,
    NoSubDir,
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum EnvironmentLockType {
    Lockfile,
    NoLockfile,
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum EnvironmentDefaultDbType {
    SingleDatabase,
    MultipleNamedDatabases,
}

#[derive(Debug)]
pub struct EnvironmentImpl {
    connections_in: Sender<rusqlite::Connection>,
    connections_out: Receiver<rusqlite::Connection>,
}

impl EnvironmentImpl {
    pub(crate) fn new(
        connection: rusqlite::Connection,
    ) -> Result<EnvironmentImpl, ErrorImpl> {
        dbg!("new environment");
        let (tx, rx) = crossbeam_channel::bounded(1);
        tx.send(connection).unwrap();
        dbg!("sent");

        Ok(EnvironmentImpl {
            connections_in: tx,
            connections_out: rx,
        })
    }
}

impl<'e> BackendEnvironment<'e> for EnvironmentImpl {
    type Database = DatabaseImpl;
    type Error = ErrorImpl;
    type Flags = DatabaseFlagsImpl;
    type Info = InfoImpl;
    type RoTransaction = RoTransactionImpl<'e>;
    type RwTransaction = RwTransactionImpl<'e>;
    type Stat = StatImpl;

    fn get_dbs(&self) -> Result<Vec<Option<String>>, Self::Error> {
        unimplemented!()
    }

    fn open_db(&self, name: Option<&str>) -> Result<Self::Database, Self::Error> {
        // TODO: check if the database exists
        Ok(DatabaseImpl { name: name.unwrap().to_string() })
    }

    fn create_db(
        &self,
        name: Option<&str>,
        flags: Self::Flags,
    ) -> Result<Self::Database, Self::Error> {
        let cxn = self.connections_out.recv().unwrap();
        cxn.execute(&format!("create table if not exists {} (key BLOB PRIMARY KEY, value BLOB NOT NULL)", name.unwrap()), []).map_err(ErrorImpl::SqliteError)?;
        self.connections_in.send(cxn).unwrap();
        Ok(DatabaseImpl { name: name.unwrap().to_string() })
    }

    fn begin_ro_txn(&'e self) -> Result<Self::RoTransaction, Self::Error> {
        let cxn = self.connections_out.recv().unwrap();
        RoTransactionImpl::new(cxn, self.connections_in.clone())
    }

    fn begin_rw_txn(&'e self) -> Result<Self::RwTransaction, Self::Error> {
        let cxn = self.connections_out.recv().unwrap();
        RwTransactionImpl::new(cxn, self.connections_in.clone())
    }

    fn sync(&self, force: bool) -> Result<(), Self::Error> {
        unimplemented!("sync is not implemented for SQLite")
    }

    fn stat(&self) -> Result<Self::Stat, Self::Error> {
        unimplemented!("stat is not implemented for SQLite")
    }

    fn info(&self) -> Result<Self::Info, Self::Error> {
        unimplemented!("info is not implemented for SQLite")
    }

    fn freelist(&self) -> Result<usize, Self::Error> {
        unimplemented!("freelist is not implemented for SQLite")
    }

    fn load_ratio(&self) -> Result<Option<f32>, Self::Error> {
        unimplemented!("load_ratio is not implemented for SQLite")
    }

    fn set_map_size(&self, size: usize) -> Result<(), Self::Error> {
        unimplemented!("set_map_size is not implemented for SQLite")
    }

    fn get_files_on_disk(&self) -> Vec<PathBuf> {
        unimplemented!("get_files_on_disk is not implemented for SQLite")
    }
}
