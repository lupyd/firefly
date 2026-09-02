use std::path::PathBuf;

use std::str::FromStr;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};

pub mod address;
pub mod auth;
pub mod conversations;
pub mod ffi_stores;
pub mod group_messages;
pub mod group_stores;
pub mod keyvalue;
pub mod messages;
pub mod stores;

pub async fn setup_pool(url: &str, max_connections: u32) -> Result<sqlx::SqlitePool, sqlx::Error> {
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

pub async fn setup_pool_from_path(
    pathname: &str,
    max_connections: u32,
) -> anyhow::Result<sqlx::SqlitePool> {
    let path = PathBuf::from(&pathname);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let url = format!("sqlite://file:{}?mode=rwc", pathname);
    let pool = setup_pool(&url, max_connections).await?;

    Ok(pool)
}
