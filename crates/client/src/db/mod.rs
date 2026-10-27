#[cfg(not(target_arch = "wasm32"))]
use std::{path::PathBuf, str::FromStr};
#[cfg(not(target_arch = "wasm32"))]
use crate::sql::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};

pub mod address;
pub mod auth;
pub mod conversations;
pub mod favourites;
pub mod ffi_stores;
pub mod group_messages;
pub mod group_stores;
pub mod history_keys;
pub mod keyvalue;
pub mod message_requests;
pub mod messages;
pub mod migrations;
pub mod search;
pub mod stores;

/// Write transactions must start with `begin_with("BEGIN IMMEDIATE")`: a deferred transaction
/// that reads before writing fails at once with `database is locked` (SQLITE_BUSY_SNAPSHOT) when
/// another connection commits in between, without waiting for `busy_timeout`.
#[cfg(not(target_arch = "wasm32"))]
pub async fn setup_pool(url: &str, max_connections: u32) -> Result<crate::sql::SqlitePool, crate::sql::Error> {
    let opts = SqliteConnectOptions::from_str(url)?
        .journal_mode(SqliteJournalMode::Wal)
        .foreign_keys(true)
        .busy_timeout(std::time::Duration::from_secs(10));

    let pool = SqlitePoolOptions::new()
        .max_connections(max_connections)
        .connect_with(opts)
        .await?;
    Ok(pool)
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn setup_pool_from_path(
    pathname: &str,
    max_connections: u32,
) -> anyhow::Result<crate::sql::SqlitePool> {
    let path = PathBuf::from(&pathname);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let url = format!("sqlite://file:{}?mode=rwc", pathname);
    let pool = setup_pool(&url, max_connections).await?;
    migrations::run_migrations(&pool).await?;

    Ok(pool)
}

/// In the browser the database lives in OPFS: the same schema and migrations, one connection
/// and no WAL (see `crate::sql`). `max_connections` is ignored.
#[cfg(target_arch = "wasm32")]
pub async fn setup_pool(url: &str, _max_connections: u32) -> Result<crate::sql::SqlitePool, crate::sql::Error> {
    let name = url
        .trim_start_matches("sqlite://")
        .trim_start_matches("sqlite:")
        .trim_start_matches("file:");
    let name = name.split('?').next().unwrap_or(name);
    crate::sql::SqlitePool::open(name).await
}

#[cfg(target_arch = "wasm32")]
pub async fn setup_pool_from_path(
    pathname: &str,
    _max_connections: u32,
) -> anyhow::Result<crate::sql::SqlitePool> {
    let pool = crate::sql::SqlitePool::open(pathname).await?;
    migrations::run_migrations(&pool).await?;
    Ok(pool)
}
