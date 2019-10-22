// Copyright 2018-2019 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use lmdb::Cursor;

use super::IterImpl;
use crate::backend::traits::BackendRoCursor;

#[derive(Debug)]
pub struct RoCursorImpl<'env>(pub(crate) lmdb::RoCursor<'env>);

impl<'env> BackendRoCursor<'env> for RoCursorImpl<'env> {
    type Iter = IterImpl<'env>;

    fn iter(&mut self) -> Self::Iter {
        IterImpl(self.0.iter())
    }

    fn iter_from<K>(&mut self, key: K) -> Self::Iter
    where
        K: AsRef<[u8]>,
    {
        IterImpl(self.0.iter_from(key))
    }

    fn iter_dup_of<K>(&mut self, key: K) -> Self::Iter
    where
        K: AsRef<[u8]>,
    {
        IterImpl(self.0.iter_dup_of(key))
    }
}

#[derive(Debug)]
pub struct RwCursorImpl<'env>(pub(crate) lmdb::RwCursor<'env>);

impl<'env> BackendRoCursor<'env> for RwCursorImpl<'env> {
    type Iter = IterImpl<'env>;

    fn iter(&mut self) -> Self::Iter {
        IterImpl(self.0.iter())
    }

    fn iter_from<K>(&mut self, key: K) -> Self::Iter
    where
        K: AsRef<[u8]>,
    {
        IterImpl(self.0.iter_from(key))
    }

    fn iter_dup_of<K>(&mut self, key: K) -> Self::Iter
    where
        K: AsRef<[u8]>,
    {
        IterImpl(self.0.iter_dup_of(key))
    }
}
