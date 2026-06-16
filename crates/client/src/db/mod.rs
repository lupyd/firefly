use std::path::PathBuf;

use sqlx::{Executor, sqlite::SqlitePoolOptions};

pub mod address;
pub mod auth;
pub mod conversations;
pub mod ffi_stores;
pub mod group_stores;
pub mod keyvalue;
pub mod messages;
pub mod stores;
pub mod group_messages;

pub async fn setup_pool(url: &str, max_connections: u32) -> Result<sqlx::SqlitePool, sqlx::Error> {
    let pool = SqlitePoolOptions::new()
        .max_connections(max_connections)
        .connect(url)
        .await?;
    pool.execute("PRAGMA journal_mode=WAL").await?;
    pool.execute("PRAGMA foreign_keys=ON").await?;
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
