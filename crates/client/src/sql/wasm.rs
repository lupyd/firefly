//! sqlx-shaped API over SQLite compiled to wasm32, persisted in OPFS.
//!
//! Only the subset of sqlx the stores use is provided, with the same names and call shapes, so
//! store code compiles unchanged on both targets. The sahpool VFS supports a single connection
//! and no WAL, so the pool is one connection behind an async lock: plain pool queries take it
//! per statement, while `acquire`/`begin` hold it until dropped. Waiting longer than
//! [`LOCK_TIMEOUT`] fails like native SQLite's `busy_timeout` rather than hanging. Statements
//! run synchronously (SQLite is in-process), so no JS future is ever held across a query.

use std::{
    borrow::Cow,
    cell::RefCell,
    ffi::{CStr, CString, c_char, c_int},
    fmt::Display,
    future::Future,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use sqlite_wasm_rs as ffi;
use tokio::sync::{Mutex, OwnedMutexGuard};

const LOCK_TIMEOUT: Duration = Duration::from_secs(10);
const OPFS_VFS_NAME: &str = "firefly-opfs";
const OPFS_DIRECTORY: &str = ".firefly-sqlite";
/// Each database needs a slot for itself and one for its rollback journal.
const OPFS_INITIAL_CAPACITY: usize = 8;

/// Database marker, mirroring `sqlx::Sqlite`.
#[derive(Debug, Clone, Copy, Default)]
pub struct Sqlite;

pub mod sqlite {
    pub use super::{SqliteConnection, SqliteQueryResult, SqliteRow};
}

pub mod prelude {
    pub use super::{Executor, FromRow, Row};
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum Error {
    Database { code: i32, message: String },
    RowNotFound,
    ColumnNotFound(String),
    ColumnDecode { index: String, message: String },
    PoolTimedOut,
    Protocol(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Database { code, message } => {
                write!(f, "error returned from database: (code: {code}) {message}")
            }
            Error::RowNotFound => f.write_str("no rows returned by a query that expected to return at least one row"),
            Error::ColumnNotFound(name) => write!(f, "no column found for name: {name}"),
            Error::ColumnDecode { index, message } => {
                write!(f, "error occurred while decoding column {index}: {message}")
            }
            Error::PoolTimedOut => f.write_str("database is locked: timed out waiting for the connection"),
            Error::Protocol(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for Error {}

// ---------------------------------------------------------------------------
// Values
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

/// A value that can be bound to a statement parameter.
pub trait Encode {
    fn encode(self) -> Value;
}

macro_rules! encode_integer {
    ($($ty:ty),*) => {$(
        impl Encode for $ty {
            fn encode(self) -> Value { Value::Integer(self as i64) }
        }
        impl Encode for &$ty {
            fn encode(self) -> Value { Value::Integer(*self as i64) }
        }
    )*};
}
encode_integer!(i8, i16, i32, i64, u8, u16, u32, u64);

impl Encode for bool {
    fn encode(self) -> Value {
        Value::Integer(self as i64)
    }
}

impl Encode for &bool {
    fn encode(self) -> Value {
        Value::Integer(*self as i64)
    }
}

impl Encode for f32 {
    fn encode(self) -> Value {
        Value::Real(self as f64)
    }
}

impl Encode for f64 {
    fn encode(self) -> Value {
        Value::Real(self)
    }
}

impl Encode for String {
    fn encode(self) -> Value {
        Value::Text(self)
    }
}

impl Encode for &String {
    fn encode(self) -> Value {
        Value::Text(self.clone())
    }
}

impl Encode for &str {
    fn encode(self) -> Value {
        Value::Text(self.to_owned())
    }
}

impl Encode for Cow<'_, str> {
    fn encode(self) -> Value {
        Value::Text(self.into_owned())
    }
}

impl Encode for Vec<u8> {
    fn encode(self) -> Value {
        Value::Blob(self)
    }
}

impl Encode for &Vec<u8> {
    fn encode(self) -> Value {
        Value::Blob(self.clone())
    }
}

impl Encode for &[u8] {
    fn encode(self) -> Value {
        Value::Blob(self.to_vec())
    }
}

impl<const N: usize> Encode for [u8; N] {
    fn encode(self) -> Value {
        Value::Blob(self.to_vec())
    }
}

impl<const N: usize> Encode for &[u8; N] {
    fn encode(self) -> Value {
        Value::Blob(self.to_vec())
    }
}

impl Encode for Box<[u8]> {
    fn encode(self) -> Value {
        Value::Blob(self.into_vec())
    }
}

impl Encode for &Box<[u8]> {
    fn encode(self) -> Value {
        Value::Blob(self.to_vec())
    }
}

impl Encode for Cow<'_, [u8]> {
    fn encode(self) -> Value {
        Value::Blob(self.into_owned())
    }
}

impl Encode for bytes::Bytes {
    fn encode(self) -> Value {
        Value::Blob(self.to_vec())
    }
}

impl<T: Encode> Encode for Option<T> {
    fn encode(self) -> Value {
        self.map_or(Value::Null, Encode::encode)
    }
}

impl<'a, T> Encode for &'a Option<T>
where
    &'a T: Encode,
{
    fn encode(self) -> Value {
        self.as_ref().map_or(Value::Null, Encode::encode)
    }
}

/// A value that can be read out of a result column, possibly borrowing from the row.
pub trait Decode<'r>: Sized {
    fn decode(value: &'r Value) -> Result<Self, String>;
}

fn unexpected(value: &Value, expected: &str) -> String {
    let found = match value {
        Value::Null => "NULL",
        Value::Integer(_) => "INTEGER",
        Value::Real(_) => "REAL",
        Value::Text(_) => "TEXT",
        Value::Blob(_) => "BLOB",
    };
    format!("mismatched types; expected {expected}, found {found}")
}

// Integers are truncated with `as`, exactly like sqlx's SQLite decoders.
macro_rules! decode_integer {
    ($($ty:ty),*) => {$(
        impl Decode<'_> for $ty {
            fn decode(value: &Value) -> Result<Self, String> {
                match value {
                    Value::Integer(v) => Ok(*v as $ty),
                    Value::Real(v) => Ok(*v as $ty),
                    other => Err(unexpected(other, "INTEGER")),
                }
            }
        }
    )*};
}
decode_integer!(i8, i16, i32, i64, u8, u16, u32, u64);

impl Decode<'_> for bool {
    fn decode(value: &Value) -> Result<Self, String> {
        match value {
            Value::Integer(v) => Ok(*v != 0),
            Value::Real(v) => Ok(*v != 0.0),
            other => Err(unexpected(other, "BOOLEAN")),
        }
    }
}

impl Decode<'_> for f64 {
    fn decode(value: &Value) -> Result<Self, String> {
        match value {
            Value::Real(v) => Ok(*v),
            Value::Integer(v) => Ok(*v as f64),
            other => Err(unexpected(other, "REAL")),
        }
    }
}

impl Decode<'_> for f32 {
    fn decode(value: &Value) -> Result<Self, String> {
        f64::decode(value).map(|v| v as f32)
    }
}

impl Decode<'_> for String {
    fn decode(value: &Value) -> Result<Self, String> {
        match value {
            Value::Text(v) => Ok(v.clone()),
            Value::Blob(v) => String::from_utf8(v.clone()).map_err(|err| err.to_string()),
            other => Err(unexpected(other, "TEXT")),
        }
    }
}

impl Decode<'_> for Vec<u8> {
    fn decode(value: &Value) -> Result<Self, String> {
        match value {
            Value::Blob(v) => Ok(v.clone()),
            Value::Text(v) => Ok(v.as_bytes().to_vec()),
            other => Err(unexpected(other, "BLOB")),
        }
    }
}

impl<'r> Decode<'r> for &'r [u8] {
    fn decode(value: &'r Value) -> Result<Self, String> {
        match value {
            Value::Blob(v) => Ok(v),
            Value::Text(v) => Ok(v.as_bytes()),
            other => Err(unexpected(other, "BLOB")),
        }
    }
}

impl<'r> Decode<'r> for &'r str {
    fn decode(value: &'r Value) -> Result<Self, String> {
        match value {
            Value::Text(v) => Ok(v),
            Value::Blob(v) => std::str::from_utf8(v).map_err(|err| err.to_string()),
            other => Err(unexpected(other, "TEXT")),
        }
    }
}

impl<'r, T: Decode<'r>> Decode<'r> for Option<T> {
    fn decode(value: &'r Value) -> Result<Self, String> {
        match value {
            Value::Null => Ok(None),
            value => T::decode(value).map(Some),
        }
    }
}

// ---------------------------------------------------------------------------
// Rows
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct SqliteRow {
    columns: Arc<[String]>,
    values: Vec<Value>,
}

pub trait ColumnIndex {
    fn index(&self, row: &SqliteRow) -> Result<usize, Error>;
}

impl ColumnIndex for usize {
    fn index(&self, row: &SqliteRow) -> Result<usize, Error> {
        if *self < row.values.len() {
            Ok(*self)
        } else {
            Err(Error::ColumnNotFound(self.to_string()))
        }
    }
}

impl ColumnIndex for &str {
    fn index(&self, row: &SqliteRow) -> Result<usize, Error> {
        row.columns
            .iter()
            .position(|column| column == self)
            .or_else(|| row.columns.iter().position(|column| column.eq_ignore_ascii_case(self)))
            .ok_or_else(|| Error::ColumnNotFound(self.to_string()))
    }
}

pub trait Row {
    fn try_get<'r, T: Decode<'r>, I: ColumnIndex>(&'r self, index: I) -> Result<T, Error>;

    /// Panics when the column is missing or does not decode, like sqlx.
    fn get<'r, T: Decode<'r>, I: ColumnIndex>(&'r self, index: I) -> T {
        match self.try_get(index) {
            Ok(value) => value,
            Err(err) => panic!("{err}"),
        }
    }

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Row for SqliteRow {
    fn try_get<'r, T: Decode<'r>, I: ColumnIndex>(&'r self, index: I) -> Result<T, Error> {
        let i = index.index(self)?;
        T::decode(&self.values[i]).map_err(|message| Error::ColumnDecode {
            index: self.columns[i].clone(),
            message,
        })
    }

    fn len(&self) -> usize {
        self.values.len()
    }
}

pub trait FromRow: Sized {
    fn from_row(row: &SqliteRow) -> Result<Self, Error>;
}

macro_rules! from_row_tuple {
    ($($ty:ident $idx:tt),+) => {
        impl<$($ty: for<'r> Decode<'r>),+> FromRow for ($($ty,)+) {
            fn from_row(row: &SqliteRow) -> Result<Self, Error> {
                Ok(($(row.try_get::<$ty, usize>($idx)?,)+))
            }
        }
    };
}
from_row_tuple!(A 0);
from_row_tuple!(A 0, B 1);
from_row_tuple!(A 0, B 1, C 2);
from_row_tuple!(A 0, B 1, C 2, D 3);
from_row_tuple!(A 0, B 1, C 2, D 3, E 4);
from_row_tuple!(A 0, B 1, C 2, D 3, E 4, F 5);
from_row_tuple!(A 0, B 1, C 2, D 3, E 4, F 5, G 6);
from_row_tuple!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7);

#[derive(Debug, Default, Clone, Copy)]
pub struct SqliteQueryResult {
    rows_affected: u64,
    last_insert_rowid: i64,
}

impl SqliteQueryResult {
    pub fn rows_affected(&self) -> u64 {
        self.rows_affected
    }

    pub fn last_insert_rowid(&self) -> i64 {
        self.last_insert_rowid
    }
}

// ---------------------------------------------------------------------------
// Raw connection
// ---------------------------------------------------------------------------

struct RawDb(*mut ffi::sqlite3);

// SAFETY: wasm32 runs a single thread; the handle never crosses threads.
unsafe impl Send for RawDb {}
unsafe impl Sync for RawDb {}

impl Drop for RawDb {
    fn drop(&mut self) {
        unsafe { ffi::sqlite3_close(self.0) };
    }
}

struct Statement(*mut ffi::sqlite3_stmt);

impl Drop for Statement {
    fn drop(&mut self) {
        unsafe { ffi::sqlite3_finalize(self.0) };
    }
}

impl RawDb {
    fn error(&self) -> Error {
        unsafe {
            let message = CStr::from_ptr(ffi::sqlite3_errmsg(self.0)).to_string_lossy().into_owned();
            Error::Database { code: ffi::sqlite3_extended_errcode(self.0), message }
        }
    }

    fn check(&self, rc: c_int) -> Result<(), Error> {
        if rc == ffi::SQLITE_OK { Ok(()) } else { Err(self.error()) }
    }

    /// Runs every statement in `sql`, consuming `args` in order across them, and hands each
    /// result row to `on_row`.
    fn run(
        &self,
        sql: &str,
        args: &[Value],
        mut on_row: impl FnMut(SqliteRow),
    ) -> Result<SqliteQueryResult, Error> {
        let sql = CString::new(sql).map_err(|err| Error::Protocol(err.to_string()))?;
        let mut tail: *const c_char = sql.as_ptr();
        let mut next_arg = 0usize;
        let mut result = SqliteQueryResult::default();

        loop {
            if unsafe { *tail } == 0 {
                break;
            }
            let mut raw: *mut ffi::sqlite3_stmt = std::ptr::null_mut();
            let rc = unsafe { ffi::sqlite3_prepare_v2(self.0, tail, -1, &mut raw, &mut tail) };
            self.check(rc)?;
            if raw.is_null() {
                // Whitespace or a comment between statements.
                continue;
            }
            let stmt = Statement(raw);

            let param_count = unsafe { ffi::sqlite3_bind_parameter_count(stmt.0) } as usize;
            let params = args.get(next_arg..next_arg + param_count).ok_or_else(|| {
                Error::Protocol(format!(
                    "statement expects {} more bind parameters, only {} provided",
                    param_count,
                    args.len().saturating_sub(next_arg)
                ))
            })?;
            for (i, value) in params.iter().enumerate() {
                self.check(unsafe { bind(stmt.0, i as c_int + 1, value) })?;
            }
            next_arg += param_count;

            let column_count = unsafe { ffi::sqlite3_column_count(stmt.0) };
            let columns: Arc<[String]> = (0..column_count)
                .map(|i| unsafe {
                    let name = ffi::sqlite3_column_name(stmt.0, i);
                    if name.is_null() {
                        String::new()
                    } else {
                        CStr::from_ptr(name).to_string_lossy().into_owned()
                    }
                })
                .collect();

            loop {
                match unsafe { ffi::sqlite3_step(stmt.0) } {
                    ffi::SQLITE_ROW => {
                        let values = (0..column_count).map(|i| unsafe { column(stmt.0, i) }).collect();
                        on_row(SqliteRow { columns: columns.clone(), values });
                    }
                    ffi::SQLITE_DONE => break,
                    _ => return Err(self.error()),
                }
            }

            result.rows_affected += unsafe { ffi::sqlite3_changes64(self.0) } as u64;
            result.last_insert_rowid = unsafe { ffi::sqlite3_last_insert_rowid(self.0) };
        }

        if next_arg != args.len() {
            return Err(Error::Protocol(format!(
                "{} bind parameters provided but the query only uses {}",
                args.len(),
                next_arg
            )));
        }
        Ok(result)
    }

    fn fetch(&self, sql: &str, args: &[Value]) -> Result<Vec<SqliteRow>, Error> {
        let mut rows = Vec::new();
        self.run(sql, args, |row| rows.push(row))?;
        Ok(rows)
    }
}

unsafe fn bind(stmt: *mut ffi::sqlite3_stmt, index: c_int, value: &Value) -> c_int {
    unsafe {
        match value {
            Value::Null => ffi::sqlite3_bind_null(stmt, index),
            Value::Integer(v) => ffi::sqlite3_bind_int64(stmt, index, *v),
            Value::Real(v) => ffi::sqlite3_bind_double(stmt, index, *v),
            Value::Text(v) => ffi::sqlite3_bind_text(
                stmt,
                index,
                v.as_ptr().cast(),
                v.len() as c_int,
                ffi::SQLITE_TRANSIENT(),
            ),
            // A null data pointer would bind NULL; keep empty blobs as empty blobs.
            Value::Blob(v) if v.is_empty() => ffi::sqlite3_bind_zeroblob(stmt, index, 0),
            Value::Blob(v) => ffi::sqlite3_bind_blob(
                stmt,
                index,
                v.as_ptr().cast(),
                v.len() as c_int,
                ffi::SQLITE_TRANSIENT(),
            ),
        }
    }
}

unsafe fn column(stmt: *mut ffi::sqlite3_stmt, index: c_int) -> Value {
    unsafe {
        match ffi::sqlite3_column_type(stmt, index) {
            ffi::SQLITE_INTEGER => Value::Integer(ffi::sqlite3_column_int64(stmt, index)),
            ffi::SQLITE_FLOAT => Value::Real(ffi::sqlite3_column_double(stmt, index)),
            ffi::SQLITE_TEXT => {
                let ptr = ffi::sqlite3_column_text(stmt, index);
                let len = ffi::sqlite3_column_bytes(stmt, index) as usize;
                if ptr.is_null() || len == 0 {
                    Value::Text(String::new())
                } else {
                    let bytes = std::slice::from_raw_parts(ptr, len);
                    Value::Text(String::from_utf8_lossy(bytes).into_owned())
                }
            }
            ffi::SQLITE_BLOB => {
                let ptr = ffi::sqlite3_column_blob(stmt, index);
                let len = ffi::sqlite3_column_bytes(stmt, index) as usize;
                if ptr.is_null() || len == 0 {
                    Value::Blob(Vec::new())
                } else {
                    Value::Blob(std::slice::from_raw_parts(ptr.cast::<u8>(), len).to_vec())
                }
            }
            _ => Value::Null,
        }
    }
}

// ---------------------------------------------------------------------------
// Pool, connections and transactions
// ---------------------------------------------------------------------------

struct PoolInner {
    db: Arc<RawDb>,
    lock: Arc<Mutex<()>>,
}

pub struct Pool<DB = Sqlite> {
    inner: Arc<PoolInner>,
    _db: PhantomData<DB>,
}

pub type SqlitePool = Pool<Sqlite>;

impl<DB> Clone for Pool<DB> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone(), _db: PhantomData }
    }
}

impl<DB> std::fmt::Debug for Pool<DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Pool")
    }
}

thread_local! {
    static OPFS: RefCell<Option<sqlite_wasm_vfs::sahpool::OpfsSAHPoolUtil>> = const { RefCell::new(None) };
}

static OPFS_INSTALL: tokio::sync::OnceCell<Result<(), String>> = tokio::sync::OnceCell::const_new();

/// Installs the OPFS VFS as SQLite's default once. Needs a secure context and a dedicated worker.
async fn install_opfs() -> Result<(), Error> {
    let installed = OPFS_INSTALL
        .get_or_init(|| {
            firefly_core::utils::SendWrapper(async {
                let cfg = sqlite_wasm_vfs::sahpool::OpfsSAHPoolCfgBuilder::new()
                    .vfs_name(OPFS_VFS_NAME)
                    .directory(OPFS_DIRECTORY)
                    .initial_capacity(OPFS_INITIAL_CAPACITY)
                    .build();
                match sqlite_wasm_vfs::sahpool::install::<sqlite_wasm_rs::WasmOsCallback>(&cfg, true).await {
                    Ok(util) => {
                        OPFS.with(|opfs| *opfs.borrow_mut() = Some(util));
                        Ok(())
                    }
                    Err(err) => Err(format!("failed to install the OPFS SQLite VFS: {err:?}")),
                }
            })
        })
        .await;
    installed.clone().map_err(Error::Protocol)
}

/// Every open database takes two OPFS slots (database + journal).
async fn reserve_opfs_slots() -> Result<(), Error> {
    use sqlite_wasm_rs::vfs::VfsFilesManager;
    let util = OPFS.with(|opfs| opfs.borrow().clone());
    if let Some(util) = util {
        let used = util.len().map_err(|err| Error::Protocol(format!("OPFS pool unavailable: {err:?}")))?;
        firefly_core::utils::SendWrapper(util.ensure_capacity(used + 2))
            .await
            .map_err(|err| Error::Protocol(format!("failed to reserve OPFS capacity: {err:?}")))?;
    }
    Ok(())
}

impl Pool<Sqlite> {
    /// Opens (creating if needed) a database. `:memory:` stays in memory; any other name is
    /// persisted in OPFS.
    pub async fn open(name: &str) -> Result<Self, Error> {
        let path = if name == ":memory:" {
            name.to_string()
        } else {
            install_opfs().await?;
            reserve_opfs_slots().await?;
            if name.starts_with('/') { name.to_string() } else { format!("/{name}") }
        };
        let c_path = CString::new(path).map_err(|err| Error::Protocol(err.to_string()))?;
        let mut handle: *mut ffi::sqlite3 = std::ptr::null_mut();
        let rc = unsafe {
            ffi::sqlite3_open_v2(
                c_path.as_ptr(),
                &mut handle,
                ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
                std::ptr::null(),
            )
        };
        let db = RawDb(handle);
        db.check(rc)?;
        db.run("PRAGMA foreign_keys = ON", &[], |_| {})?;
        Ok(Self {
            inner: Arc::new(PoolInner { db: Arc::new(db), lock: Arc::new(Mutex::new(())) }),
            _db: PhantomData,
        })
    }

    async fn lock(&self) -> Result<OwnedMutexGuard<()>, Error> {
        crate::rt::timeout(LOCK_TIMEOUT, self.inner.lock.clone().lock_owned())
            .await
            .map_err(|_| Error::PoolTimedOut)
    }

    pub async fn acquire(&self) -> Result<PoolConnection, Error> {
        let guard = self.lock().await?;
        Ok(PoolConnection {
            conn: SqliteConnection { db: self.inner.db.clone() },
            _guard: guard,
        })
    }

    pub async fn begin(&self) -> Result<Transaction<'static, Sqlite>, Error> {
        self.begin_with("BEGIN").await
    }

    pub async fn begin_with(&self, statement: &str) -> Result<Transaction<'static, Sqlite>, Error> {
        let conn = self.acquire().await?;
        conn.db.run(statement, &[], |_| {})?;
        Ok(Transaction { conn: TxConn::Owned(conn), open: true, _db: PhantomData })
    }

    /// The connection is shared by every clone of the pool, so closing is a no-op; it is
    /// released when the last clone drops.
    pub async fn close(&self) {}
}

pub struct SqliteConnection {
    db: Arc<RawDb>,
}

impl SqliteConnection {
    /// Runs `callback` inside a transaction, committing on `Ok` and rolling back on `Err`.
    pub fn transaction<'a, F, R, E>(
        &'a mut self,
        callback: F,
    ) -> Pin<Box<dyn Future<Output = Result<R, E>> + Send + 'a>>
    where
        for<'t> F: FnOnce(&'t mut Transaction<'a, Sqlite>) -> Pin<Box<dyn Future<Output = Result<R, E>> + Send + 't>>
            + Send
            + 'a,
        R: Send,
        E: From<Error> + Send,
    {
        Box::pin(async move {
            self.db.run("BEGIN", &[], |_| {})?;
            let mut tx = Transaction { conn: TxConn::Borrowed(self), open: true, _db: PhantomData };
            match callback(&mut tx).await {
                Ok(value) => {
                    tx.commit().await?;
                    Ok(value)
                }
                Err(err) => {
                    tx.rollback().await?;
                    Err(err)
                }
            }
        })
    }
}

pub struct PoolConnection {
    conn: SqliteConnection,
    _guard: OwnedMutexGuard<()>,
}

impl Deref for PoolConnection {
    type Target = SqliteConnection;

    fn deref(&self) -> &SqliteConnection {
        &self.conn
    }
}

impl DerefMut for PoolConnection {
    fn deref_mut(&mut self) -> &mut SqliteConnection {
        &mut self.conn
    }
}

enum TxConn<'c> {
    Owned(PoolConnection),
    Borrowed(&'c mut SqliteConnection),
}

pub struct Transaction<'c, DB = Sqlite> {
    conn: TxConn<'c>,
    open: bool,
    _db: PhantomData<DB>,
}

impl<DB> Transaction<'_, DB> {
    pub async fn commit(mut self) -> Result<(), Error> {
        self.open = false;
        self.db.run("COMMIT", &[], |_| {})?;
        Ok(())
    }

    pub async fn rollback(mut self) -> Result<(), Error> {
        self.open = false;
        self.db.run("ROLLBACK", &[], |_| {})?;
        Ok(())
    }
}

impl<DB> Deref for Transaction<'_, DB> {
    type Target = SqliteConnection;

    fn deref(&self) -> &SqliteConnection {
        match &self.conn {
            TxConn::Owned(conn) => conn,
            TxConn::Borrowed(conn) => conn,
        }
    }
}

impl<DB> DerefMut for Transaction<'_, DB> {
    fn deref_mut(&mut self) -> &mut SqliteConnection {
        match &mut self.conn {
            TxConn::Owned(conn) => conn,
            TxConn::Borrowed(conn) => conn,
        }
    }
}

impl<DB> Drop for Transaction<'_, DB> {
    fn drop(&mut self) {
        // Statements are synchronous here, so the rollback completes before the lock is released.
        if self.open {
            if let Err(err) = self.db.run("ROLLBACK", &[], |_| {}) {
                log::error!("failed to roll back dropped transaction: {err}");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Executors
// ---------------------------------------------------------------------------

/// Where a statement runs: the pool (locked per statement) or a connection already held.
#[doc(hidden)]
pub enum Target<'c> {
    Pool(&'c Pool<Sqlite>),
    Connection(&'c mut SqliteConnection),
}

impl Target<'_> {
    async fn with_db<R>(self, f: impl FnOnce(&RawDb) -> Result<R, Error>) -> Result<R, Error> {
        match self {
            Target::Pool(pool) => {
                let _guard = pool.lock().await?;
                f(&pool.inner.db)
            }
            Target::Connection(conn) => f(&conn.db),
        }
    }
}

/// Something a query can run on, like sqlx's `Executor`.
pub trait Executor<'c>: Send + Sized {
    #[doc(hidden)]
    fn target(self) -> Target<'c>;

    /// Executes raw SQL, which may contain several statements.
    fn execute(self, sql: &str) -> Pin<Box<dyn Future<Output = Result<SqliteQueryResult, Error>> + Send + 'c>>
    where
        Self: 'c,
    {
        let target = self.target();
        let sql = sql.to_owned();
        Box::pin(async move { target.with_db(|db| db.run(&sql, &[], |_| {})).await })
    }
}

impl<'c> Executor<'c> for &'c Pool<Sqlite> {
    fn target(self) -> Target<'c> {
        Target::Pool(self)
    }
}

impl<'c> Executor<'c> for &'c mut SqliteConnection {
    fn target(self) -> Target<'c> {
        Target::Connection(self)
    }
}

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

pub struct Query<'q> {
    sql: Cow<'q, str>,
    args: Vec<Value>,
}

pub fn query(sql: &str) -> Query<'_> {
    Query { sql: Cow::Borrowed(sql), args: Vec::new() }
}

impl<'q> Query<'q> {
    pub fn bind<T: Encode>(mut self, value: T) -> Self {
        self.args.push(value.encode());
        self
    }

    pub async fn execute<'c, E: Executor<'c>>(self, executor: E) -> Result<SqliteQueryResult, Error> {
        executor.target().with_db(|db| db.run(&self.sql, &self.args, |_| {})).await
    }

    pub async fn fetch_all<'c, E: Executor<'c>>(self, executor: E) -> Result<Vec<SqliteRow>, Error> {
        executor.target().with_db(|db| db.fetch(&self.sql, &self.args)).await
    }

    pub async fn fetch_optional<'c, E: Executor<'c>>(self, executor: E) -> Result<Option<SqliteRow>, Error> {
        Ok(self.fetch_all(executor).await?.into_iter().next())
    }

    pub async fn fetch_one<'c, E: Executor<'c>>(self, executor: E) -> Result<SqliteRow, Error> {
        self.fetch_optional(executor).await?.ok_or(Error::RowNotFound)
    }
}

pub struct QueryAs<'q, DB, O> {
    inner: Query<'q>,
    _out: PhantomData<fn() -> (DB, O)>,
}

pub fn query_as<DB, O>(sql: &str) -> QueryAs<'_, DB, O> {
    QueryAs { inner: query(sql), _out: PhantomData }
}

impl<'q, O: FromRow + Send> QueryAs<'q, Sqlite, O> {
    pub fn bind<T: Encode>(self, value: T) -> Self {
        Self { inner: self.inner.bind(value), _out: PhantomData }
    }

    pub async fn fetch_all<'c, E: Executor<'c>>(self, executor: E) -> Result<Vec<O>, Error> {
        self.inner.fetch_all(executor).await?.iter().map(O::from_row).collect()
    }

    pub async fn fetch_optional<'c, E: Executor<'c>>(self, executor: E) -> Result<Option<O>, Error> {
        self.inner.fetch_optional(executor).await?.as_ref().map(O::from_row).transpose()
    }

    pub async fn fetch_one<'c, E: Executor<'c>>(self, executor: E) -> Result<O, Error> {
        O::from_row(&self.inner.fetch_one(executor).await?)
    }
}

pub struct QueryScalar<'q, DB, O> {
    inner: Query<'q>,
    _out: PhantomData<fn() -> (DB, O)>,
}

pub fn query_scalar<DB, O>(sql: &str) -> QueryScalar<'_, DB, O> {
    QueryScalar { inner: query(sql), _out: PhantomData }
}

impl<'q, O: for<'r> Decode<'r> + Send> QueryScalar<'q, Sqlite, O> {
    pub fn bind<T: Encode>(self, value: T) -> Self {
        Self { inner: self.inner.bind(value), _out: PhantomData }
    }

    pub async fn fetch_all<'c, E: Executor<'c>>(self, executor: E) -> Result<Vec<O>, Error> {
        self.inner.fetch_all(executor).await?.iter().map(|row| row.try_get(0usize)).collect()
    }

    pub async fn fetch_optional<'c, E: Executor<'c>>(self, executor: E) -> Result<Option<O>, Error> {
        self.inner.fetch_optional(executor).await?.map(|row| row.try_get(0usize)).transpose()
    }

    pub async fn fetch_one<'c, E: Executor<'c>>(self, executor: E) -> Result<O, Error> {
        self.inner.fetch_one(executor).await?.try_get(0usize)
    }
}

pub struct QueryBuilder<'a, DB = Sqlite> {
    sql: String,
    args: Vec<Value>,
    _db: PhantomData<(&'a (), DB)>,
}

impl<'a> QueryBuilder<'a, Sqlite> {
    pub fn new(init: impl Into<String>) -> Self {
        Self { sql: init.into(), args: Vec::new(), _db: PhantomData }
    }

    pub fn push(&mut self, sql: impl Display) -> &mut Self {
        use std::fmt::Write;
        let _ = write!(self.sql, "{sql}");
        self
    }

    pub fn push_bind<T: Encode>(&mut self, value: T) -> &mut Self {
        self.sql.push('?');
        self.args.push(value.encode());
        self
    }

    pub fn separated<'qb, Sep: Display>(&'qb mut self, separator: Sep) -> Separated<'qb, 'a, Sep> {
        Separated { builder: self, separator, first: true }
    }

    pub fn build(&mut self) -> Query<'_> {
        Query { sql: Cow::Borrowed(&self.sql), args: std::mem::take(&mut self.args) }
    }
}

pub struct Separated<'qb, 'a, Sep> {
    builder: &'qb mut QueryBuilder<'a, Sqlite>,
    separator: Sep,
    first: bool,
}

impl<Sep: Display> Separated<'_, '_, Sep> {
    fn separate(&mut self) {
        if !std::mem::take(&mut self.first) {
            self.builder.push(&self.separator);
        }
    }

    pub fn push(&mut self, sql: impl Display) -> &mut Self {
        self.separate();
        self.builder.push(sql);
        self
    }

    pub fn push_bind<T: Encode>(&mut self, value: T) -> &mut Self {
        self.separate();
        self.builder.push_bind(value);
        self
    }

    pub fn push_unseparated(&mut self, sql: impl Display) -> &mut Self {
        self.builder.push(sql);
        self
    }

    pub fn push_bind_unseparated<T: Encode>(&mut self, value: T) -> &mut Self {
        self.builder.push_bind(value);
        self
    }
}
