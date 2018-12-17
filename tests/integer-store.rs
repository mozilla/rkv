// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate bincode;
extern crate rkv;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate tempfile;

use rkv::{
    PrimitiveInt,
    Rkv,
    Value,
};

use self::tempfile::Builder;

use std::fs;

#[test]
fn test_integer_keys() {
    let root = Builder::new().prefix("test_integer_keys").tempdir().expect("tempdir");
    fs::create_dir_all(root.path()).expect("dir created");
    let k = Rkv::new(root.path()).expect("new succeeded");
    let s = k.open_or_create_integer("s").expect("open");

    macro_rules! test_integer_keys {
        ($type:ty, $key:expr) => {{
            let mut writer = k.write_int::<$type>().expect("writer");

            writer.put(s, $key, &Value::Str("hello!")).expect("write");
            assert_eq!(writer.get(s, $key).expect("read"), Some(Value::Str("hello!")));
            writer.commit().expect("committed");

            let reader = k.read_int::<$type>().expect("reader");
            assert_eq!(reader.get(s, $key).expect("read"), Some(Value::Str("hello!")));
        }};
    }

    // The integer module provides u16, u32, and u64 integer key variants
    // of IntegerStore, so we can use them without further ado.
    test_integer_keys!(u16, std::u16::MIN);
    test_integer_keys!(u16, std::u16::MAX);
    test_integer_keys!(u32, std::u32::MIN);
    test_integer_keys!(u32, std::u32::MAX);
    test_integer_keys!(u64, std::u64::MIN);
    test_integer_keys!(u64, std::u64::MAX);

    // If we want to use another integer key variant, we need to implement
    // a newtype, implement PrimitiveInt, and implement or derive Serialize
    // for it.  Here we do so for the i32 type.

    #[derive(Serialize)]
    struct I32(i32);
    impl PrimitiveInt for I32 {}

    test_integer_keys!(I32, I32(std::i32::MIN));
    test_integer_keys!(I32, I32(std::i32::MAX));
}
