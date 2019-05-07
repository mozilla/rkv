// Copyright 2018-2019 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate rkv;

use rkv::{
    Rkv,
    SingleStore,
    StoreOptions,
    Value,
};
use std::{
    env::args,
    fs::{
        create_dir_all,
        File,
    },
    io::Read,
    path::Path,
};

fn main() {
    let mut args = args();
    let mut database = None;
    let mut path = None;

    // The first arg is the name of the program, which we can ignore.
    args.next();

    while let Some(arg) = args.next() {
        if &arg[0..1] == "-" {
            match &arg[1..] {
                "s" => {
                    database = match args.next() {
                        None => panic!("-s must be followed by database arg"),
                        Some(str) => Some(str),
                    };
                },
                str => panic!("arg -{} not recognized", str),
            }
        } else {
            if path.is_some() {
                panic!("must provide only one path to the LMDB environment");
            }
            path = Some(arg);
        }
    }

    if path.is_none() {
        panic!("must provide a path to the LMDB environment");
    }
    let path = path.unwrap();

    create_dir_all(&path).expect("dir created");

    let mut builder = Rkv::environment_builder();
    builder.set_max_dbs(2);
    // Allocate enough map to accommodate the largest random collection.
    builder.set_map_size(33_554_432); // 32MiB
    let rkv = Rkv::from_env(Path::new(&path), builder).expect("Rkv");
    let store: SingleStore =
        rkv.open_single(database.as_ref().map(|x| x.as_str()), StoreOptions::create()).expect("opened");
    let mut writer = rkv.write().expect("writer");

    // Generate random values for the number of keys and key/value lengths.
    // On Linux, "Just use /dev/urandom!" <https://www.2uo.de/myths-about-urandom/>.
    // Elsewhere, it doesn't matter (/dev/random and /dev/urandom are identical).
    let mut random = File::open("/dev/urandom").unwrap();
    let mut nums = [0u8; 5];
    random.read_exact(&mut nums).unwrap();
    let num_keys = nums[0];

    // Generate 0–255 pairs.
    for _ in 0..num_keys {
        // Generate key and value lengths.  The key must be 1–511 bytes long.
        // The value length can be 0 and is essentially unbounded; we generate
        // value lengths of 0–0xffff (65535).
        // NB: the modulus method for generating a random number within a range
        // introduces distribution skew, but we don't need it to be perfect.
        let key_len = ((u16::from(nums[1]) + (u16::from(nums[2]) << 8)) % 511 + 1) as usize;
        let value_len = (u16::from(nums[3]) + (u16::from(nums[4]) << 8)) as usize;

        let mut key: Vec<u8> = vec![0; key_len];
        random.read_exact(&mut key[0..key_len]).unwrap();

        let mut value: Vec<u8> = vec![0; value_len];
        random.read_exact(&mut value[0..value_len]).unwrap();

        store.put(&mut writer, key, &Value::Blob(&value)).expect("wrote");
    }

    writer.commit().expect("committed");
}
