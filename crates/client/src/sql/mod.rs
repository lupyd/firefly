//! The SQL layer every store is written against.
//!
//! Native builds re-export sqlx unchanged. wasm32 builds run the very same SQL, schema and
//! migrations on SQLite compiled to wasm and persisted in OPFS, through a small subset of the
//! sqlx API (`query`/`bind`/`fetch_*`, rows, transactions, `QueryBuilder`). Store code imports
//! from here instead of `sqlx` and stays identical across targets.

#[cfg(not(target_arch = "wasm32"))]
pub use sqlx::{
    Error, Executor, FromRow, Pool, QueryBuilder, Row, Sqlite, SqlitePool, Transaction, prelude,
    query, query_as, query_scalar, sqlite,
};

#[cfg(target_arch = "wasm32")]
mod wasm;
#[cfg(target_arch = "wasm32")]
pub use wasm::*;
