//! Local cache of message-request state, so notifications and calls can be gated without a
//! network round trip (including from background/FCM wake-ups).
//!
//! The server (`/user/accepted`) is the source of truth; this cache is refreshed on every
//! connection and updated optimistically on accept/unaccept and on sending a message.

use crate::sql::SqlitePool;
use crate::sql::prelude::*;

use crate::utils::get_current_timestamp_millis_since_epoch;

#[derive(Clone)]
pub struct MessageRequestsStore {
    pool: SqlitePool,
}

impl MessageRequestsStore {
    pub async fn new(pool: SqlitePool) -> anyhow::Result<Self> {
        let mut db = pool.acquire().await?;
        db.execute(
            r#"
            CREATE TABLE IF NOT EXISTS accepted_users (
                username TEXT NOT NULL PRIMARY KEY,
                updated_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS request_notified (
                username TEXT NOT NULL PRIMARY KEY,
                notified_at INTEGER NOT NULL
            );
            "#,
        )
        .await?;
        Ok(Self { pool })
    }

    pub async fn is_accepted(&self, username: &str) -> anyhow::Result<bool> {
        let row = crate::sql::query("SELECT 1 FROM accepted_users WHERE username = ?")
            .bind(username)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.is_some())
    }

    pub async fn list(&self) -> anyhow::Result<Vec<String>> {
        let rows = crate::sql::query("SELECT username FROM accepted_users ORDER BY username")
            .fetch_all(&self.pool)
            .await?;
        rows.iter()
            .map(|r| r.try_get::<String, _>("username").map_err(Into::into))
            .collect()
    }

    pub async fn set_accepted(&self, username: &str, accepted: bool) -> anyhow::Result<()> {
        if accepted {
            crate::sql::query(
                "INSERT INTO accepted_users (username, updated_at) VALUES (?, ?)
                 ON CONFLICT(username) DO UPDATE SET updated_at = excluded.updated_at",
            )
            .bind(username)
            .bind(get_current_timestamp_millis_since_epoch() as i64)
            .execute(&self.pool)
            .await?;
        } else {
            crate::sql::query("DELETE FROM accepted_users WHERE username = ?")
                .bind(username)
                .execute(&self.pool)
                .await?;
        }
        // Accepting or declining resolves the request either way.
        crate::sql::query("DELETE FROM request_notified WHERE username = ?")
            .bind(username)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Replaces the cache with the server's list.
    pub async fn replace_all(&self, usernames: &[String]) -> anyhow::Result<()> {
        let now = get_current_timestamp_millis_since_epoch() as i64;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        crate::sql::query("DELETE FROM accepted_users")
            .execute(&mut *tx)
            .await?;
        for username in usernames {
            crate::sql::query(
                "INSERT OR IGNORE INTO accepted_users (username, updated_at) VALUES (?, ?)",
            )
            .bind(username)
            .bind(now)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Records that a request notification was shown for `username`. Returns `true` only the
    /// first time, so a request produces a single notification however many messages follow.
    pub async fn mark_request_notified(&self, username: &str) -> anyhow::Result<bool> {
        let result = crate::sql::query(
            "INSERT OR IGNORE INTO request_notified (username, notified_at) VALUES (?, ?)",
        )
        .bind(username)
        .bind(get_current_timestamp_millis_since_epoch() as i64)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Conversations where this account sent at least one visible message, i.e. users it
    /// implicitly accepted before message requests existed.
    pub async fn users_messaged_by_me(&self) -> anyhow::Result<Vec<String>> {
        let has_messages = crate::sql::query(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'user_messages'",
        )
        .fetch_optional(&self.pool)
        .await?;
        if has_messages.is_none() {
            return Ok(vec![]);
        }
        let rows = crate::sql::query(
            "SELECT DISTINCT other FROM user_messages WHERE sent_by_other = 0 AND (message_type & 2) = 0",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|r| r.try_get::<String, _>("other").map_err(Into::into))
            .collect()
    }
}
