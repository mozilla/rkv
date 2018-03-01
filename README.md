# rkv

<a href="https://crates.io/crates/rkv">
    <img src="https://img.shields.io/crates/v/rkv.svg">
</a>

rkv is a usable Rust wrapper around LMDB.

It aims to achieve the following:

- Avoid LMDB's sharp edges (e.g., obscure error codes for common situations).
- Report errors via `failure`.
- Correctly restrict to one handle per process via a 'manager'.
- Use Rust's type system to make single-typed key stores (including LMDB's own integer-keyed stores) safe and ergonomic.
- Encode and decode values via `bincode`/`serde` and type tags, achieving platform-independent storage and input/output flexibility.
