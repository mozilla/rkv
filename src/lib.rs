// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

#[macro_use] extern crate arrayref;
#[macro_use] extern crate failure;
#[macro_use] extern crate lazy_static;

extern crate bincode;
extern crate lmdb;
extern crate ordered_float;
extern crate serde;               // So we can specify trait bounds. Everything else is bincode.
extern crate uuid;

pub use lmdb::{
    DatabaseFlags,
    EnvironmentBuilder,
    EnvironmentFlags,
    WriteFlags,
};

pub mod value;
pub mod error;
mod env;
mod readwrite;
mod integer;
mod manager;

pub use env::{
    Rkv,
};

pub use error::{
    DataError,
    StoreError,
};

pub use integer::{
    IntegerStore,
    PrimitiveInt,
};

pub use manager::{
    Manager
};

pub use readwrite::{
    Reader,
    Writer,
    Store,
};

pub use value::{
    Value,
};
