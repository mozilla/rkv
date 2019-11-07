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
use std::sync::Arc;
use std::sync::{
    RwLock,
    RwLockReadGuard,
    RwLockWriteGuard,
};

use id_arena::Arena;
use log::warn;

use super::{
    database::DatabaseImpl,
    DatabaseFlagsImpl,
    DatabaseId,
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

type DatabaseArena = Arena<DatabaseImpl>;
type DatabaseNameMap = HashMap<Option<String>, DatabaseId>;

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
    arena: RwLock<DatabaseArena>,
    dbs: RwLock<DatabaseNameMap>,
    ro_txns: Arc<()>,
    rw_txns: Arc<()>,
}

impl EnvironmentImpl {
    fn serialize(&self) -> Result<Vec<u8>, ErrorImpl> {
        let arena = self.arena.read().map_err(|_| ErrorImpl::DbPoisonError)?;
        let dbs = self.dbs.read().map_err(|_| ErrorImpl::DbPoisonError)?;
        let data: HashMap<_, _> = dbs.iter().map(|(name, id)| (name, &arena[*id])).collect();
        Ok(bincode::serialize(&data)?)
    }

    fn deserialize(bytes: &[u8]) -> Result<(DatabaseArena, DatabaseNameMap), ErrorImpl> {
        let mut arena = DatabaseArena::new();
        let mut dbs = HashMap::new();
        let data: HashMap<_, _> = bincode::deserialize(&bytes)?;
        for (name, db) in data {
            dbs.insert(name, arena.alloc(db));
        }
        Ok((arena, dbs))
    }
}

impl EnvironmentImpl {
    pub(crate) fn new(path: &Path, _flags: EnvironmentFlagsImpl) -> Result<EnvironmentImpl, ErrorImpl> {
        Ok(EnvironmentImpl {
            path: path.to_path_buf(),
            arena: RwLock::new(DatabaseArena::new()),
            dbs: RwLock::new(HashMap::new()),
            ro_txns: Arc::new(()),
            rw_txns: Arc::new(()),
        })
    }

    pub(crate) fn read_from_disk(&mut self) -> Result<(), ErrorImpl> {
        let mut path = Cow::from(&self.path);
        if fs::metadata(&path)?.is_dir() {
            path.to_mut().push(DEFAULT_DB_FILENAME);
        };
        if fs::metadata(&path).is_err() {
            return Ok(());
        };
        let (arena, dbs) = Self::deserialize(&fs::read(&path)?)?;
        self.arena = RwLock::new(arena);
        self.dbs = RwLock::new(dbs);
        Ok(())
    }

    pub(crate) fn write_to_disk(&self) -> Result<(), ErrorImpl> {
        let mut path = Cow::from(&self.path);
        if fs::metadata(&path)?.is_dir() {
            path.to_mut().push(DEFAULT_DB_FILENAME);
        };
        fs::write(&path, self.serialize()?)?;
        Ok(())
    }

    pub(crate) fn dbs(&self) -> Result<RwLockReadGuard<DatabaseArena>, ErrorImpl> {
        self.arena.read().map_err(|_| ErrorImpl::DbPoisonError)
    }

    pub(crate) fn dbs_mut(&self) -> Result<RwLockWriteGuard<DatabaseArena>, ErrorImpl> {
        self.arena.write().map_err(|_| ErrorImpl::DbPoisonError)
    }
}

impl<'env> BackendEnvironment<'env> for EnvironmentImpl {
    type Error = ErrorImpl;
    type Database = DatabaseId;
    type Flags = DatabaseFlagsImpl;
    type Stat = StatImpl;
    type Info = InfoImpl;
    type RoTransaction = RoTransactionImpl<'env>;
    type RwTransaction = RwTransactionImpl<'env>;

    fn open_db(&self, name: Option<&str>) -> Result<Self::Database, Self::Error> {
        if Arc::strong_count(&self.ro_txns) > 1 {
            return Err(ErrorImpl::DbsIllegalOpen);
        }
        // TOOD: don't reallocate `name`.
        let key = name.map(String::from);
        let dbs = self.dbs.read().map_err(|_| ErrorImpl::DbPoisonError)?;
        let id = dbs.get(&key).ok_or(ErrorImpl::DbNotFoundError)?;
        Ok(*id)
    }

    fn create_db(&self, name: Option<&str>, flags: Self::Flags) -> Result<Self::Database, Self::Error> {
        // TOOD: don't reallocate `name`.
        let key = name.map(String::from);
        let mut dbs = self.dbs.write().map_err(|_| ErrorImpl::DbPoisonError)?;
        let mut arena = self.arena.write().map_err(|_| ErrorImpl::DbPoisonError)?;
        let id = dbs.entry(key).or_insert_with(|| arena.alloc(DatabaseImpl::new(Some(flags), None)));
        Ok(*id)
    }

    fn begin_ro_txn(&'env self) -> Result<Self::RoTransaction, Self::Error> {
        RoTransactionImpl::new(self, self.ro_txns.clone())
    }

    fn begin_rw_txn(&'env self) -> Result<Self::RwTransaction, Self::Error> {
        RwTransactionImpl::new(self, self.rw_txns.clone())
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
