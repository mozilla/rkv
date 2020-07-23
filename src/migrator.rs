// Copyright 2018-2019 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

//! A simple utility for migrating data from one RVK environment to another. Notably, this
//! tool can migrate data from an enviroment created with a different backend than the
//! current RKV consumer (e.g from Lmdb to SafeMode).
//!
//! The utility doesn't support migrating between 32-bit and 64-bit LMDB environments yet,
//! see `arch_migrator` if this is needed. However, this utility is ultimately intended to
//! handle all possible migrations.
//!
//! The destination environment should be empty of data, otherwise an error is returned.
//!
//! The tool currently has these limitations:
//!
//! 1. It doesn't support migration from environments created with
//!    `EnvironmentFlags::NO_SUB_DIR`. To migrate such an environment, create a temporary
//!    directory, copy the environment's data files in the temporary directory, then
//!    migrate the temporary directory as the source environment.
//! 2. It doesn't support migration from databases created with DatabaseFlags::DUP_SORT`
//!    (with or without `DatabaseFlags::DUP_FIXED`) nor with `DatabaseFlags::INTEGER_KEY`.
//!    This effectively means that migration is limited to `SingleStore`s.
//! 3. It doesn't allow for existing data in the destination environment, which means that
//!    it cannot overwrite nor append data.

use crate::{
    backend::{
        LmdbEnvironment,
        SafeModeEnvironment,
    },
    error::MigrateError,
    Rkv,
    StoreOptions,
};

pub use crate::backend::{
    LmdbArchMigrateError,
    LmdbArchMigrateResult,
    LmdbArchMigrator,
};

// FIXME: should parametrize this instead.
macro_rules! fn_migrator {
    ($name:tt, $src:ty, $dst:ty) => {
        /// Migrate all data in all of databases from the source environment to the destination
        /// environment. This includes all key/value pairs in the main database that aren't
        /// metadata about subdatabases and all key/value pairs in all subdatabases.
        ///
        /// Other backend-specific metadata such as map size or maximum databases left intact on
        /// the given environments.
        ///
        /// The destination environment should be empty of data, otherwise an error is returned.
        pub fn $name(src_env: &Rkv<$src>, dst_env: &Rkv<$dst>) -> Result<(), MigrateError> {
            let src_dbs = src_env.get_dbs().unwrap();
            if src_dbs.is_empty() {
                return Err(MigrateError::SourceEmpty);
            }
            let dst_dbs = dst_env.get_dbs().unwrap();
            if !dst_dbs.is_empty() {
                return Err(MigrateError::DestinationNotEmpty);
            }
            for name in src_dbs {
                let src_store = src_env.open_single(name.as_deref(), StoreOptions::default())?;
                let dst_store = dst_env.open_single(name.as_deref(), StoreOptions::create())?;
                let reader = src_env.read()?;
                let mut writer = dst_env.write()?;
                let mut iter = src_store.iter_start(&reader)?;
                while let Some(Ok((key, value))) = iter.next() {
                    dst_store.put(&mut writer, key, &value).expect("wrote");
                }
                writer.commit()?;
            }
            Ok(())
        }
    };
}

pub struct Migrator;

impl Migrator {
    fn_migrator!(migrate_lmdb_to_safe_mode, LmdbEnvironment, SafeModeEnvironment);

    fn_migrator!(migrate_safe_mode_to_lmdb, SafeModeEnvironment, LmdbEnvironment);
}
