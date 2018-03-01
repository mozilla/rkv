// Copyright 2018 Mozilla
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
};

use std::collections::btree_map::{
    Entry,
};

use std::path::{
    Path,
    PathBuf,
};

use std::sync::{
    Arc,
    Mutex,
    RwLock,
};

use error::{
    StoreError,
};

use ::Rkv;

/// A process is only permitted to have one open handle to each database. This manager
/// exists to enforce that constraint: don't open databases directly.
pub struct Manager {
    stores: Mutex<BTreeMap<PathBuf, Arc<RwLock<Rkv>>>>,
}

impl Manager {
    fn new() -> Manager {
        Manager {
            stores: Mutex::new(Default::default()),
        }
    }

    /// Return the open store at `path`, returning `None` if it has not already been opened.
    pub fn get<'p, P>(&self, path: P) -> Result<Option<Arc<RwLock<Rkv>>>, ::std::io::Error>
    where P: Into<&'p Path> {
        let canonical = path.into().canonicalize()?;
        Ok(self.stores.lock().unwrap().get(&canonical).cloned())
    }

    /// Return the open store at `path`, or create it by calling `f`.
    pub fn get_or_create<'p, F, P>(&mut self, path: P, f: F) -> Result<Arc<RwLock<Rkv>>, StoreError>
    where F: FnOnce(&Path) -> Result<Rkv, StoreError>,
          P: Into<&'p Path> {
        let canonical = path.into().canonicalize()?;
        let mut map = self.stores.lock().unwrap();
        Ok(match map.entry(canonical) {
            Entry::Occupied(e) => e.get().clone(),
            Entry::Vacant(e) => {
                let k = Arc::new(RwLock::new(f(e.key().as_path())?));
                e.insert(k).clone()
            }
        })
    }
}

#[cfg(test)]
mod tests {
    extern crate tempdir;

    use self::tempdir::TempDir;
    use std::fs;

    use super::*;

    /// Test that the manager will return the same Rkv instance each time for each path.
    #[test]
    fn test_same() {
        let root = TempDir::new("test_same").expect("tempdir");
        fs::create_dir_all(root.path()).expect("dir created");

        let mut manager = Manager::new();

        let p = root.path();
        assert!(manager.get(p).expect("success").is_none());

        let created_arc = manager.get_or_create(p, Rkv::new).expect("created");
        let fetched_arc = manager.get(p).expect("success").expect("existed");
        assert!(Arc::ptr_eq(&created_arc, &fetched_arc));
    }
}
