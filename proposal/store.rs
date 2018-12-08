


// Stores remain a newtype around `lmdb::Database`
// Initialized with the appropriate flags, they use their
// methods to enforce the constraints, INTEGER, DUP_SORT, etc

struct Store {
    db: Db,
}

struct IntegerStore {
    db: Db,
}

struct MultiStore {
    db: Db,
}

//
// Readers and Writers are dumb newtype containers over
// Read or Write transactions
//

// Since a write transaction can also read, we create a simple
// trait which will allow get* functions to take either a read
// or write Txn
trait ReadTxn {
    fn txn(&self) -> Lmdb::Transaction;
}


// concrete transaction container
struct Writer {
    // ...
}

// concrete transaction container
struct Reader {
    // ...
}

impl ReadTxn for Reader;
impl ReadTxn for Writer;

impl Store {
    fn get<T: ReadTxn>(txn: &T, key: &[u8]) -> RkvResult<Option<Value>>;
    fn put(key: &[u8], val: Value) -> RkvResult<()>;
    fn del(key: &[u8]) -> RkvResult<()>;
}

impl IntegerStore<K: PrimitiveInt> {
    fn get<T: ReadTxn>(txn: &T, k: &K) -> RkvResult<Option<Value>>;
    fn put(txn: Writer, key: &K, val: &Value) -> RkvResult<()>;
    fn del(txn: Writer, key: &K) -> RkvResult<()>;
}

impl MultiStore {
    fn get<T: ReadTxn>(txn: &T, key: &[u8]) -> RkvResult<Iter<Value>>;
    fn get_first<T: ReadTxn>(txn: &T, key: &[u8]) -> RkvResult<Value>;
    fn put(txn: Writer, key: &[u8], val: Value) -> RkvResult<()>;
    fn put_flags(txn: Writer, key: &[u8], val: Value, flags: WriteFlags) -> RkvResult<()>;
    fn del(txn: Writer, key: &[u8]) -> RkvResult<()>;
    fn del_flags(txn: Writer, key: &[u8], flags: DelFlags) -> RkvResult<()>;
}
