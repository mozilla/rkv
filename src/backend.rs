// Copyright 2018-2019 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

mod common;
mod impl_safe;
mod impl_sqlite;
mod traits;

pub use common::*;
pub use traits::*;
pub use impl_safe::{
    DatabaseFlagsImpl as SafeModeDatabaseFlags, DatabaseImpl as SafeModeDatabase,
    EnvironmentBuilderImpl as SafeMode, EnvironmentFlagsImpl as SafeModeEnvironmentFlags,
    EnvironmentImpl as SafeModeEnvironment, ErrorImpl as SafeModeError, InfoImpl as SafeModeInfo,
    IterImpl as SafeModeIter, RoCursorImpl as SafeModeRoCursor,
    RoTransactionImpl as SafeModeRoTransaction, RwCursorImpl as SafeModeRwCursor,
    RwTransactionImpl as SafeModeRwTransaction, StatImpl as SafeModeStat,
    WriteFlagsImpl as SafeModeWriteFlags,
};

pub use impl_sqlite::{
    DatabaseFlagsImpl as SqliteDatabaseFlags, DatabaseImpl as SqliteDatabase,
    EnvironmentBuilderImpl as Sqlite, EnvironmentFlagsImpl as SqliteEnvironmentFlags,
    EnvironmentImpl as SqliteEnvironment, ErrorImpl as SqliteError, InfoImpl as SqliteInfo,
    IterImpl as SqliteIter, RoCursorImpl as SqliteRoCursor,
    RoTransactionImpl as SqliteRoTransaction, RwCursorImpl as SqliteRwCursor,
    RwTransactionImpl as SqliteRwTransaction, StatImpl as SqliteStat,
    WriteFlagsImpl as SqliteWriteFlags,
};

