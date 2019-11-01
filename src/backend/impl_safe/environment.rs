// Copyright 2018-2019 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::borrow::Cow;
use std::collections::HashMap;
use std::fs;
use std::path::{
    Path,
    PathBuf,
};
use std::sync::{
    RwLock,
    RwLockReadGuard,
    RwLockWriteGuard,
};

use log::warn;

use super::{
    DatabaseFlagsImpl,
    DatabaseImpl,
    EnvironmentFlagsImpl,
    ErrorImpl,
    InfoImpl,
    RoTransactionImpl,
    RwTransactionImpl,
    StatImpl,
};
use crate::backend::traits::{
    BackendEnvironment,
    BackendEnvironmentBuilder,
};

const DEFAULT_DB_FILENAME: &str = "data.safe.bin";

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub struct EnvironmentBuilderImpl {
    flags: EnvironmentFlagsImpl,
}

impl<'env> BackendEnvironmentBuilder<'env> for EnvironmentBuilderImpl {
    type Error = ErrorImpl;
    type Environment = EnvironmentImpl;
    type Flags = EnvironmentFlagsImpl;

    fn new() -> EnvironmentBuilderImpl {
        EnvironmentBuilderImpl {
            flags: EnvironmentFlagsImpl::empty(),
        }
    }

    fn set_flags<T>(&mut self, flags: T) -> &mut Self
    where
        T: Into<Self::Flags>,
    {
        self.flags = flags.into();
        self
    }

    fn set_max_readers(&mut self, max_readers: u32) -> &mut Self {
        warn!("Ignoring `set_max_readers({})`", max_readers);
        self
    }

    fn set_max_dbs(&mut self, max_dbs: u32) -> &mut Self {
        warn!("Ignoring `set_max_dbs({})`", max_dbs);
        self
    }

    fn set_map_size(&mut self, size: usize) -> &mut Self {
        warn!("Ignoring `set_map_size({})`", size);
        self
    }

    fn open(&self, path: &Path) -> Result<Self::Environment, Self::Error> {
        let mut env = EnvironmentImpl::new(path, self.flags)?;
        env.read_from_disk()?;
        Ok(env)
    }
}

#[derive(Debug)]
pub struct EnvironmentImpl {
    path: PathBuf,
    dbs: RwLock<HashMap<Option<String>, DatabaseImpl>>,
}

impl EnvironmentImpl {
    pub(crate) fn new(path: &Path, _flags: EnvironmentFlagsImpl) -> Result<EnvironmentImpl, ErrorImpl> {
        Ok(EnvironmentImpl {
            path: path.to_path_buf(),
            dbs: RwLock::new(HashMap::new()),
        })
    }

    pub(crate) fn read_from_disk(&mut self) -> Result<(), ErrorImpl> {
        let mut path = Cow::from(&self.path);
        if fs::metadata(&path)?.is_dir() {
            path.to_mut().push(DEFAULT_DB_FILENAME);
        };
        if fs::metadata(&path).is_err() {
            fs::write(&path, bincode::serialize(&self.dbs)?)?;
        };
        let serialized = fs::read(&path)?;
        self.dbs = bincode::deserialize(&serialized)?;
        Ok(())
    }

    pub(crate) fn write_to_disk(&self) -> Result<(), ErrorImpl> {
        let mut path = Cow::from(&self.path);
        if fs::metadata(&path)?.is_dir() {
            path.to_mut().push(DEFAULT_DB_FILENAME);
        };
        fs::write(&path, bincode::serialize(&self.dbs)?)?;
        Ok(())
    }

    pub(crate) fn dbs(&self) -> Result<RwLockReadGuard<HashMap<Option<String>, DatabaseImpl>>, ErrorImpl> {
        self.dbs.read().map_err(|_| ErrorImpl::DbPoisonError)
    }

    pub(crate) fn dbs_mut(&self) -> Result<RwLockWriteGuard<HashMap<Option<String>, DatabaseImpl>>, ErrorImpl> {
        self.dbs.write().map_err(|_| ErrorImpl::DbPoisonError)
    }
}

impl<'env> BackendEnvironment<'env> for EnvironmentImpl {
    type Error = ErrorImpl;
    type Database = DatabaseImpl;
    type Flags = DatabaseFlagsImpl;
    type Stat = StatImpl;
    type Info = InfoImpl;
    type RoTransaction = RoTransactionImpl<'env>;
    type RwTransaction = RwTransactionImpl<'env>;

    fn open_db(&self, name: Option<&str>) -> Result<Self::Database, Self::Error> {
        // TOOD: don't reallocate `name`.
        let dbs = self.dbs.read().map_err(|_| ErrorImpl::DbPoisonError)?;
        let db = dbs.get(&name.map(String::from)).ok_or(ErrorImpl::DbNotFoundError)?.clone();
        Ok(db)
    }

    fn create_db(&self, name: Option<&str>, flags: Self::Flags) -> Result<Self::Database, Self::Error> {
        // TOOD: don't reallocate `name`.
        let mut dbs = self.dbs.write().map_err(|_| ErrorImpl::DbPoisonError)?;
        let db = dbs.entry(name.map(String::from)).or_insert_with(|| DatabaseImpl::new(Some(flags), None)).clone();
        Ok(db)
    }

    fn begin_ro_txn(&'env self) -> Result<Self::RoTransaction, Self::Error> {
        RoTransactionImpl::new(self)
    }

    fn begin_rw_txn(&'env self) -> Result<Self::RwTransaction, Self::Error> {
        RwTransactionImpl::new(self)
    }

    fn sync(&self, force: bool) -> Result<(), Self::Error> {
        warn!("Ignoring `force={}`", force);
        self.write_to_disk()
    }

    fn stat(&self) -> Result<Self::Stat, Self::Error> {
        Ok(StatImpl)
    }

    fn info(&self) -> Result<Self::Info, Self::Error> {
        Ok(InfoImpl)
    }

    fn freelist(&self) -> Result<usize, Self::Error> {
        unimplemented!()
    }

    fn set_map_size(&self, size: usize) -> Result<(), Self::Error> {
        warn!("Ignoring `set_map_size({})`", size);
        Ok(())
    }
}
