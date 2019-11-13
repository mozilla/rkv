// Copyright 2018-2019 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::fmt;
use std::io;

use bincode::Error as BincodeError;

use crate::backend::traits::BackendError;
use crate::error::StoreError;

#[derive(Debug)]
pub enum ErrorImpl {
    KeyValuePairNotFound,
    DbPoisonError,
    DbsFull,
    DbsIllegalOpen,
    DbNotFoundError,
    DbIsForeignError,
    IoError(io::Error),
    BincodeError(BincodeError),
}

impl BackendError for ErrorImpl {}

impl fmt::Display for ErrorImpl {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ErrorImpl::KeyValuePairNotFound => write!(fmt, "KeyValuePairNotFound (safe mode)"),
            ErrorImpl::DbPoisonError => write!(fmt, "DbPoisonError (safe mode)"),
            ErrorImpl::DbsFull => write!(fmt, "DbsFull (safe mode)"),
            ErrorImpl::DbsIllegalOpen => write!(fmt, "DbIllegalOpen (safe mode)"),
            ErrorImpl::DbNotFoundError => write!(fmt, "DbNotFoundError (safe mode)"),
            ErrorImpl::DbIsForeignError => write!(fmt, "DbIsForeignError (safe mode)"),
            ErrorImpl::IoError(e) => e.fmt(fmt),
            ErrorImpl::BincodeError(e) => e.fmt(fmt),
        }
    }
}

impl Into<StoreError> for ErrorImpl {
    fn into(self) -> StoreError {
        match self {
            ErrorImpl::KeyValuePairNotFound => StoreError::KeyValuePairNotFound,
            _ => StoreError::SafeModeError(self),
        }
    }
}

impl From<io::Error> for ErrorImpl {
    fn from(e: io::Error) -> ErrorImpl {
        ErrorImpl::IoError(e)
    }
}

impl From<BincodeError> for ErrorImpl {
    fn from(e: BincodeError) -> ErrorImpl {
        ErrorImpl::BincodeError(e)
    }
}
