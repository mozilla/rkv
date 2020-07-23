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
    path::{
        Path,
        PathBuf,
    },
};

use lmdb::Error as LmdbError;

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
    BackendInfo,
    BackendIter,
    BackendRoCursor,
    BackendRoCursorTransaction,
    BackendStat,
};

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub struct EnvironmentBuilderImpl {
    builder: lmdb::EnvironmentBuilder,
    envtype: EnvironmentType,
    make_dir: bool,
    check_env_exists: bool,
}

impl<'b> BackendEnvironmentBuilder<'b> for EnvironmentBuilderImpl {
    type Environment = EnvironmentImpl;
    type Error = ErrorImpl;
    type Flags = EnvironmentFlagsImpl;

    fn new() -> EnvironmentBuilderImpl {
        EnvironmentBuilderImpl {
            builder: lmdb::Environment::new(),
            envtype: EnvironmentType::SingleDatabase,
            make_dir: false,
            check_env_exists: false,
        }
    }

    fn set_flags<T>(&mut self, flags: T) -> &mut Self
    where
        T: Into<Self::Flags>,
    {
        self.builder.set_flags(flags.into().0);
        self
    }

    fn set_max_readers(&mut self, max_readers: u32) -> &mut Self {
        self.builder.set_max_readers(max_readers);
        self
    }

    fn set_max_dbs(&mut self, max_dbs: u32) -> &mut Self {
        self.builder.set_max_dbs(max_dbs);
        if max_dbs > 0 {
            self.envtype = EnvironmentType::MultipleNamedDatabases
        }
        self
    }

    fn set_map_size(&mut self, size: usize) -> &mut Self {
        self.builder.set_map_size(size);
        self
    }

    fn set_make_dir_if_needed(&mut self, make_dir: bool) -> &mut Self {
        self.make_dir = make_dir;
        self
    }

    fn set_check_if_env_exists(&mut self, check_env_exists: bool) -> &mut Self {
        self.check_env_exists = check_env_exists;
        self
    }

    fn open(&self, path: &Path) -> Result<Self::Environment, Self::Error> {
        if self.check_env_exists && !path.join("data.mdb").exists() {
            return Err(ErrorImpl::EnvironmentDoesNotExistError(path.into()));
        }
        if !path.is_dir() {
            if !self.make_dir {
                return Err(ErrorImpl::DirectoryDoesNotExistError(path.into()));
            }
            fs::create_dir_all(path).map_err(ErrorImpl::IoError)?;
        }
        self.builder
            .open(path)
            .map_err(ErrorImpl::LmdbError)
            .and_then(|lmdbenv| EnvironmentImpl::new(path, lmdbenv, self.envtype))
    }
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum EnvironmentType {
    SingleDatabase,
    MultipleNamedDatabases,
}

#[derive(Debug)]
pub struct EnvironmentImpl {
    path: PathBuf,
    lmdbenv: lmdb::Environment,
    envtype: EnvironmentType,
}

impl EnvironmentImpl {
    pub(crate) fn new(
        path: &Path,
        lmdbenv: lmdb::Environment,
        envtype: EnvironmentType,
    ) -> Result<EnvironmentImpl, ErrorImpl> {
        Ok(EnvironmentImpl {
            path: path.to_path_buf(),
            lmdbenv,
            envtype,
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
        if self.envtype == EnvironmentType::SingleDatabase {
            return Ok(vec![None]);
        }
        let db = self.lmdbenv.open_db(None).map(DatabaseImpl).map_err(ErrorImpl::LmdbError)?;
        let reader = self.begin_ro_txn()?;
        let cursor = reader.open_ro_cursor(&db)?;
        let mut iter = cursor.into_iter();
        let mut store = vec![];
        while let Some(result) = iter.next() {
            let (key, _) = result?;
            let name = String::from_utf8(key.to_owned()).map_err(|_| ErrorImpl::LmdbError(lmdb::Error::Corrupted))?;
            store.push(Some(name));
        }
        Ok(store)
    }

    fn open_db(&self, name: Option<&str>) -> Result<Self::Database, Self::Error> {
        self.lmdbenv.open_db(name).map(DatabaseImpl).map_err(ErrorImpl::LmdbError)
    }

    fn create_db(&self, name: Option<&str>, flags: Self::Flags) -> Result<Self::Database, Self::Error> {
        self.lmdbenv.create_db(name, flags.0).map(DatabaseImpl).map_err(ErrorImpl::LmdbError)
    }

    fn begin_ro_txn(&'e self) -> Result<Self::RoTransaction, Self::Error> {
        self.lmdbenv.begin_ro_txn().map(RoTransactionImpl).map_err(ErrorImpl::LmdbError)
    }

    fn begin_rw_txn(&'e self) -> Result<Self::RwTransaction, Self::Error> {
        self.lmdbenv.begin_rw_txn().map(RwTransactionImpl).map_err(ErrorImpl::LmdbError)
    }

    fn sync(&self, force: bool) -> Result<(), Self::Error> {
        self.lmdbenv.sync(force).map_err(ErrorImpl::LmdbError)
    }

    fn stat(&self) -> Result<Self::Stat, Self::Error> {
        self.lmdbenv.stat().map(StatImpl).map_err(ErrorImpl::LmdbError)
    }

    fn info(&self) -> Result<Self::Info, Self::Error> {
        self.lmdbenv.info().map(InfoImpl).map_err(ErrorImpl::LmdbError)
    }

    fn freelist(&self) -> Result<usize, Self::Error> {
        self.lmdbenv.freelist().map_err(ErrorImpl::LmdbError)
    }

    fn load_ratio(&self) -> Result<Option<f32>, Self::Error> {
        let stat = self.stat()?;
        let info = self.info()?;
        let freelist = self.freelist()?;

        let last_pgno = info.last_pgno() + 1; // pgno is 0 based.
        let total_pgs = info.map_size() / stat.page_size();
        if freelist > last_pgno {
            return Err(ErrorImpl::LmdbError(LmdbError::Corrupted));
        }
        let used_pgs = last_pgno - freelist;
        Ok(Some(used_pgs as f32 / total_pgs as f32))
    }

    fn set_map_size(&self, size: usize) -> Result<(), Self::Error> {
        self.lmdbenv.set_map_size(size).map_err(ErrorImpl::LmdbError)
    }

    fn get_files_on_disk(&self) -> Vec<PathBuf> {
        let mut db_filename = self.path.clone();
        let mut lock_filename = self.path.clone();
        db_filename.push("data.mdb");
        lock_filename.push("lock.mdb");
        return vec![db_filename, lock_filename];
    }
}
