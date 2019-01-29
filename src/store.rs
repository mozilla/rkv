pub mod integer;
pub mod integermulti;
pub mod multi;
pub mod single;

use crate::{
    error::StoreError,
    value::OwnedValue,
    value::Value,
};
use lmdb::DatabaseFlags;

#[derive(Default, Debug, Copy, Clone)]
pub struct Options {
    pub create: bool,
    pub flags: DatabaseFlags,
}

impl Options {
    pub fn create() -> Options {
        Options {
            create: true,
            flags: DatabaseFlags::empty(),
        }
    }
}

fn read_transform(val: Result<&[u8], lmdb::Error>) -> Result<Option<Value>, StoreError> {
    match val {
        Ok(bytes) => Value::from_tagged_slice(bytes).map(Some).map_err(StoreError::DataError),
        Err(lmdb::Error::NotFound) => Ok(None),
        Err(e) => Err(StoreError::LmdbError(e)),
    }
}

fn read_transform_owned(val: Result<&[u8], lmdb::Error>) -> Result<Option<OwnedValue>, StoreError> {
    match val {
        Ok(bytes) => Value::from_tagged_slice(bytes).map(|v| Some(OwnedValue::from(&v))).map_err(StoreError::DataError),
        Err(lmdb::Error::NotFound) => Ok(None),
        Err(e) => Err(StoreError::LmdbError(e)),
    }
}
