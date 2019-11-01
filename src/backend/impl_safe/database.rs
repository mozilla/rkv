// Copyright 2018-2019 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::{
    BTreeSet,
    HashMap,
};
use std::sync::{
    Arc,
    RwLock,
};

use serde_derive::{
    Deserialize,
    Serialize,
};
use uuid::Uuid;

use super::{
    DatabaseFlagsImpl,
    ErrorImpl,
};
use crate::backend::traits::BackendDatabase;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseImpl {
    id: Uuid,
    flags: DatabaseFlagsImpl,
    snapshot: Arc<RwLock<Snapshot>>,
}

impl DatabaseImpl {
    pub(crate) fn new(flags: Option<DatabaseFlagsImpl>, snapshot: Option<Snapshot>) -> DatabaseImpl {
        DatabaseImpl {
            id: Uuid::new_v4(),
            flags: flags.unwrap_or_else(DatabaseFlagsImpl::default),
            snapshot: Arc::new(RwLock::new(snapshot.unwrap_or_else(Snapshot::new))),
        }
    }

    pub(crate) fn id(&self) -> &Uuid {
        &self.id
    }

    pub(crate) fn flags(&self) -> &DatabaseFlagsImpl {
        &self.flags
    }

    pub(crate) fn snapshot(&self) -> Result<Snapshot, ErrorImpl> {
        let snapshot = self.snapshot.read().map_err(|_| ErrorImpl::TxnPoisonError)?;
        Ok(snapshot.clone())
    }

    pub(crate) fn replace(&mut self, value: Snapshot) -> Result<Snapshot, ErrorImpl> {
        let mut snapshot = self.snapshot.write().map_err(|_| ErrorImpl::TxnPoisonError)?;
        Ok(std::mem::replace(&mut snapshot, value))
    }
}

impl BackendDatabase for DatabaseImpl {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    map: HashMap<Box<[u8]>, BTreeSet<Box<[u8]>>>,
}

impl Snapshot {
    pub(crate) fn new() -> Snapshot {
        Snapshot {
            map: HashMap::new(),
        }
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.map.get(key).and_then(|v| v.iter().next()).map(|v| v.as_ref())
    }

    pub(crate) fn put_one(&mut self, key: &[u8], value: &[u8]) {
        let values = self.map.entry(Box::from(key)).or_insert_with(BTreeSet::new);
        values.clear();
        values.insert(Box::from(value));
    }

    pub(crate) fn put_dup(&mut self, key: &[u8], value: &[u8]) {
        let values = self.map.entry(Box::from(key)).or_insert_with(BTreeSet::new);
        values.insert(Box::from(value));
    }

    pub(crate) fn del_exact(&mut self, key: &[u8], value: &[u8]) -> Option<()> {
        let values = self.map.entry(Box::from(key)).or_insert_with(BTreeSet::new);
        let was_removed = values.remove(value);
        Some(()).filter(|_| was_removed)
    }

    pub(crate) fn del_all(&mut self, key: &[u8]) -> Option<()> {
        let values = self.map.entry(Box::from(key)).or_insert_with(BTreeSet::new);
        let was_empty = values.is_empty();
        values.clear();
        Some(()).filter(|_| !was_empty)
    }

    pub(crate) fn clear(&mut self) {
        self.map.clear();
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.map.iter().flat_map(|(key, values)| values.iter().map(move |value| (key.as_ref(), value.as_ref())))
    }
}
