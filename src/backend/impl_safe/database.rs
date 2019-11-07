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
    BTreeMap,
    BTreeSet,
};
use std::sync::Arc;

use id_arena::Id;
use serde_derive::{
    Deserialize,
    Serialize,
};

use super::DatabaseFlagsImpl;
use crate::backend::traits::BackendDatabase;

pub type DatabaseId = Id<DatabaseImpl>;

impl BackendDatabase for DatabaseId {}

#[derive(Debug, Serialize, Deserialize)]
pub struct DatabaseImpl {
    snapshot: Snapshot,
}

impl DatabaseImpl {
    pub(crate) fn new(flags: Option<DatabaseFlagsImpl>, snapshot: Option<Snapshot>) -> DatabaseImpl {
        DatabaseImpl {
            snapshot: snapshot.unwrap_or_else(|| Snapshot::new(flags)),
        }
    }

    pub(crate) fn snapshot(&self) -> Snapshot {
        self.snapshot.clone()
    }

    pub(crate) fn replace(&mut self, snapshot: Snapshot) -> Snapshot {
        std::mem::replace(&mut self.snapshot, snapshot)
    }
}

type Key = Box<[u8]>;
type Value = Box<[u8]>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    flags: DatabaseFlagsImpl,
    map: Arc<BTreeMap<Key, BTreeSet<Value>>>,
}

impl Snapshot {
    pub(crate) fn new(flags: Option<DatabaseFlagsImpl>) -> Snapshot {
        Snapshot {
            flags: flags.unwrap_or_else(DatabaseFlagsImpl::default),
            map: Default::default(),
        }
    }

    pub(crate) fn flags(&self) -> &DatabaseFlagsImpl {
        &self.flags
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.map.get(key).and_then(|v| v.iter().next()).map(|v| v.as_ref())
    }

    pub(crate) fn put_one(&mut self, key: &[u8], value: &[u8]) {
        let map = Arc::make_mut(&mut self.map);
        match map.get_mut(key) {
            None => {
                let mut values = BTreeSet::new();
                values.insert(Box::from(value));
                map.insert(Box::from(key), values);
            },
            Some(values) => {
                values.clear();
                values.insert(Box::from(value));
            },
        }
    }

    pub(crate) fn put_dup(&mut self, key: &[u8], value: &[u8]) {
        let map = Arc::make_mut(&mut self.map);
        match map.get_mut(key) {
            None => {
                let mut values = BTreeSet::new();
                values.insert(Box::from(value));
                map.insert(Box::from(key), values);
            },
            Some(values) => {
                values.insert(Box::from(value));
            },
        }
    }

    pub(crate) fn del_exact(&mut self, key: &[u8], value: &[u8]) -> Option<()> {
        let map = Arc::make_mut(&mut self.map);
        match map.get_mut(key) {
            None => None,
            Some(values) => {
                let was_removed = values.remove(value);
                Some(()).filter(|_| was_removed)
            },
        }
    }

    pub(crate) fn del_all(&mut self, key: &[u8]) -> Option<()> {
        let map = Arc::make_mut(&mut self.map);
        match map.get_mut(key) {
            None => None,
            Some(values) => {
                let was_empty = values.is_empty();
                values.clear();
                Some(()).filter(|_| !was_empty)
            },
        }
    }

    pub(crate) fn clear(&mut self) {
        self.map = Default::default();
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.map.iter().flat_map(|(key, values)| values.iter().map(move |value| (key.as_ref(), value.as_ref())))
    }
}
