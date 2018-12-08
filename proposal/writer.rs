
// Stores remain a newtype around `lmdb::Database`
// Initialized with the appropriate flags, they leverage
// their intrinsic type to ensure they are exclusive to 
// the appropriate Reader or Writer method

struct Store {
    db: Db,
}

struct IntegerStore<K: PrimitiveInt> {
    db: Db,
}

struct MultiStore {
    db: Db,
}

//
// In addition to being the manifestation of a transaction,
// Reader or Writer contain all Db functionality. 
// Every method takes a *Store parameter in order to ensure
// the constraints of particular database flags aren't violated
//

// transaction container and core db API
struct Reader {
    // ...
}

// transaction container and core db API
struct Writer {
    // ...
}

impl Reader {
    fn get(store: Store, key: &[u8]) -> RkvResult<Option<Value>>;

    fn get_int<K: PrimitiveInt>(store: IntegerStore<K>, key: &K) -> RkvResult<Value>;

    fn get_multi<T: ReadTxn>(txn: &T, key: &[u8]) -> RkvResult<Iter<Value>>;
    fn get_multi_first<T: ReadTxn>(txn: &T, key: &[u8]) -> RkvResult<Value>;
}

impl Writer {
    fn get(store: Store, key: &[u8]) -> RkvResult<Option<Value>>;
    fn put(store: Store, key: &[u8]) -> RkvResult<()>;
    fn del(store: Store, key: &[u8]) -> RkvResult<()>;

    fn get_int<K: PrimitiveInt>(store: IntegerStore<K>, key: &K) -> RkvResult<Value>;
    fn put_int<K: PrimitiveInt>(store: IntegerStore<K>, key: &K, val: Value) -> RkvResult<()>;
    fn del_int<K: PrimitiveInt>(store: IntegerStore<K>, key: &K) -> RkvResult<()>;

    fn get_multi<T: ReadTxn>(store: MultiStore, key: &[u8]) -> RkvResult<Iter<Value>>;
    fn get_multi_first<T: ReadTxn>(store: MultiStore, key: &[u8]) -> RkvResult<Value>;
    fn put_multi(store: MultiStore, key: &[u8], val: Value) -> RkvResult<()>;
    fn put_multi_flags(store: MultiStore, key: &[u8], val: Value, flags: WriteFlags) -> RkvResult<()>;
    fn del_multi(store: MultiStore, key: &[u8]) -> RkvResult<()>;
    fn del_multi_flags(store: MultiStore, key: &[u8], flags: DelFlags) -> RkvResult<()>;
}
