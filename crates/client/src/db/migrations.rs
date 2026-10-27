//! # Client SQLite Database Migrations Protocol
//!
//! This module provides a centralized, standard migration engine for the Firefly client SQLite database.
//!
//! ## Core Principles
//! 1. **Zero Data Loss**: Migrations preserve all existing records and schema definitions.
//! 2. **Dependency Ordering**: Columns are verified via `PRAGMA table_info` and added with `ALTER TABLE`
//!    *before* dependent indexes, triggers, or FTS virtual tables are defined.
//! 3. **Fast-Path Exit**: The `_schema_migrations` table records applied versions; routine store
//!    initializations exit immediately (`O(1)`) if the target version is already present.
//! 4. **No Full-Table Scans**: Checks use `SELECT 1 ... LIMIT 1` rather than `SELECT count(*)` to avoid
//!    freezing startup when tables contain tens of thousands of messages.
//! 5. **Concurrency Safety**: Migration execution is serialized in-process via `MIGRATION_LOCK`.

use sqlx::{Executor, Row, SqlitePool};
use std::collections::HashSet;

use crate::db::group_messages::searchable_group_message_text;
use crate::db::messages::searchable_user_message_text;
use crate::utils::get_current_timestamp_millis_since_epoch;

/// Highest migration version defined in `run_migrations`. Bump this whenever a migration is
/// appended, otherwise the fast path will skip the new migration on already-upgraded databases.
pub const LATEST_MIGRATION_VERSION: i64 = 8;

/// Rows per batch for data backfills (keeps memory and lock hold times bounded).
const MIGRATION_BATCH_SIZE: i64 = 500;

static MIGRATION_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub async fn table_exists(pool: &SqlitePool, table: &str) -> anyhow::Result<bool> {
    let row = sqlx::query("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?")
        .bind(table)
        .fetch_optional(pool)
        .await?;
    Ok(row.is_some())
}

pub async fn get_table_columns(pool: &SqlitePool, table: &str) -> anyhow::Result<HashSet<String>> {
    let query = format!("PRAGMA table_info({})", table);
    let rows = sqlx::query(&query).fetch_all(pool).await?;
    let mut cols = HashSet::new();
    for row in rows {
        let name: String = row.try_get("name")?;
        cols.insert(name.to_lowercase());
    }
    Ok(cols)
}

pub async fn ensure_column(
    pool: &SqlitePool,
    table: &str,
    column: &str,
    column_def: &str,
) -> anyhow::Result<()> {
    if !table_exists(pool, table).await? {
        return Ok(());
    }
    let cols = get_table_columns(pool, table).await?;
    if !cols.contains(&column.to_lowercase()) {
        log::info!(
            "Migrating table '{}': adding column '{} {}'",
            table,
            column,
            column_def
        );
        let sql = format!("ALTER TABLE {} ADD COLUMN {} {}", table, column, column_def);
        pool.execute(sql.as_str()).await?;
    }
    pool.execute(r#"
        CREATE TABLE IF NOT EXISTS receipt_journal (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id BLOB NOT NULL,
            epoch INTEGER NOT NULL,
            capability_key TEXT,
            capability_value TEXT,
            state TEXT NOT NULL DEFAULT 'staged',
            created_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS receipt_journal_staged ON receipt_journal (state);
        INSERT OR IGNORE INTO _schema_migrations(version,name,applied_at) VALUES(6,'receipt_journal_v6',0);
    "#).await?;
    Ok(())
}

/// Standard migration runner. Idempotently ensures all tables, columns, indexes,
/// and FTS triggers exist without data loss.
pub async fn run_migrations(pool: &SqlitePool) -> anyhow::Result<()> {
    let _lock = MIGRATION_LOCK.lock().await;
    let mut conn = pool.acquire().await?;

    // 1. Create migration tracking table
    conn.execute(
        r#"
        CREATE TABLE IF NOT EXISTS _schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at INTEGER NOT NULL
        );
        "#,
    )
    .await?;

    // Fast path: return immediately when every migration 1..=LATEST is recorded. This is a
    // primary-key range lookup on the tiny bookkeeping table (at most LATEST rows), never a scan
    // of message/FTS tables. Requiring every version (not just the latest) means a migration
    // left unrecorded — e.g. v1 after a failed FTS rebuild, or one interrupted mid-DDL — is
    // retried on the next startup even if later migrations were recorded.
    let applied_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM _schema_migrations WHERE version BETWEEN 1 AND ?",
    )
    .bind(LATEST_MIGRATION_VERSION)
    .fetch_one(&mut *conn)
    .await?;
    if applied_count == LATEST_MIGRATION_VERSION {
        return Ok(());
    }

    let v1_applied: Option<i64> =
        sqlx::query_scalar("SELECT 1 FROM _schema_migrations WHERE version = 1")
            .fetch_optional(&mut *conn)
            .await?;
    drop(conn);

    if v1_applied.is_none() {
        // 2. Base tables creation if they don't already exist
        pool.execute(
            r#"
            CREATE TABLE IF NOT EXISTS user_messages (
                id INTEGER NOT NULL,
                other TEXT NOT NULL,
                sent_by_other BOOLEAN NOT NULL,
                message BLOB NOT NULL,
                message_type INTEGER NOT NULL DEFAULT 0,
                text TEXT NOT NULL DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS last_seen_user_timestamps (
                other TEXT NOT NULL PRIMARY KEY,
                id INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS group_messages (
                id INTEGER NOT NULL,
                group_id INTEGER NOT NULL,
                by TEXT NOT NULL,
                message BLOB NOT NULL,
                channel_id INTEGER NOT NULL,
                epoch INTEGER NOT NULL DEFAULT 0,
                message_type INTEGER NOT NULL DEFAULT 0,
                text TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (group_id, id)
            );

            CREATE TABLE IF NOT EXISTS favourite_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                message_id INTEGER NOT NULL,
                other TEXT,
                group_id INTEGER,
                channel_id INTEGER,
                by TEXT NOT NULL,
                text TEXT NOT NULL DEFAULT '',
                message BLOB NOT NULL,
                message_type INTEGER NOT NULL DEFAULT 0,
                epoch INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL
            );
            "#,
        )
        .await?;

        // 3. Migrate user_messages columns
        ensure_column(
            pool,
            "user_messages",
            "message_type",
            "INTEGER NOT NULL DEFAULT 0",
        )
        .await?;
        ensure_column(pool, "user_messages", "text", "TEXT NOT NULL DEFAULT ''").await?;

        // 4. Migrate group_messages columns
        ensure_column(
            pool,
            "group_messages",
            "epoch",
            "INTEGER NOT NULL DEFAULT 0",
        )
        .await?;
        ensure_column(
            pool,
            "group_messages",
            "message_type",
            "INTEGER NOT NULL DEFAULT 0",
        )
        .await?;
        ensure_column(pool, "group_messages", "text", "TEXT NOT NULL DEFAULT ''").await?;

        // 5. Migrate favourite_messages columns
        ensure_column(
            pool,
            "favourite_messages",
            "message_type",
            "INTEGER NOT NULL DEFAULT 0",
        )
        .await?;
        ensure_column(
            pool,
            "favourite_messages",
            "epoch",
            "INTEGER NOT NULL DEFAULT 0",
        )
        .await?;
        ensure_column(
            pool,
            "favourite_messages",
            "text",
            "TEXT NOT NULL DEFAULT ''",
        )
        .await?;

        // 6. Backfill user_messages text in batches
        let mut had_unmigrated_users = false;
        loop {
            let batch = sqlx::query("SELECT rowid, message, message_type FROM user_messages WHERE text = '' AND length(message) > 0 LIMIT 500")
            .fetch_all(pool)
            .await?;
            if batch.is_empty() {
                break;
            }
            had_unmigrated_users = true;
            for row in batch {
                let rowid: i64 = row.try_get("rowid")?;
                let msg_bytes: Vec<u8> = row.try_get("message")?;
                let message_type: i64 = row.try_get("message_type")?;
                // Hidden rows get " " (non-empty so the loop terminates) instead of their payload.
                let text = searchable_user_message_text(&msg_bytes, message_type as u32);
                let update_text = if text.is_empty() { " " } else { text.as_str() };
                sqlx::query("UPDATE user_messages SET text = ? WHERE rowid = ?")
                    .bind(update_text)
                    .bind(rowid)
                    .execute(pool)
                    .await?;
            }
        }

        // 7. Backfill group_messages text in batches
        let mut had_unmigrated_groups = false;
        loop {
            let batch = sqlx::query("SELECT rowid, message, message_type FROM group_messages WHERE text = '' AND length(message) > 0 LIMIT 500")
            .fetch_all(pool)
            .await?;
            if batch.is_empty() {
                break;
            }
            had_unmigrated_groups = true;
            for row in batch {
                let rowid: i64 = row.try_get("rowid")?;
                let msg_bytes: Vec<u8> = row.try_get("message")?;
                let message_type: i64 = row.try_get("message_type")?;
                // Hidden rows get " " (non-empty so the loop terminates) instead of their payload.
                let text = searchable_group_message_text(&msg_bytes, message_type as u32);
                let update_text = if text.is_empty() { " " } else { text.as_str() };
                sqlx::query("UPDATE group_messages SET text = ? WHERE rowid = ?")
                    .bind(update_text)
                    .bind(rowid)
                    .execute(pool)
                    .await?;
            }
        }

        // 8. Create indexes and FTS5 triggers safely
        pool.execute(
        r#"
        CREATE INDEX IF NOT EXISTS user_messages_other_idx ON user_messages (other, id);
        CREATE INDEX IF NOT EXISTS user_messages_other_type_idx ON user_messages (other, message_type);

        CREATE VIRTUAL TABLE IF NOT EXISTS user_messages_fts USING fts5(
            text,
            content='user_messages',
            content_rowid='rowid'
        );

        CREATE TRIGGER IF NOT EXISTS user_messages_ai AFTER INSERT ON user_messages BEGIN
          INSERT INTO user_messages_fts(rowid, text) VALUES (new.rowid, new.text);
        END;

        CREATE TRIGGER IF NOT EXISTS user_messages_ad AFTER DELETE ON user_messages BEGIN
          INSERT INTO user_messages_fts(user_messages_fts, rowid, text) VALUES('delete', old.rowid, old.text);
        END;

        CREATE TRIGGER IF NOT EXISTS user_messages_au AFTER UPDATE ON user_messages BEGIN
          INSERT INTO user_messages_fts(user_messages_fts, rowid, text) VALUES('delete', old.rowid, old.text);
          INSERT INTO user_messages_fts(rowid, text) VALUES (new.rowid, new.text);
        END;

        CREATE INDEX IF NOT EXISTS group_messages_type_idx ON group_messages (group_id, message_type);

        CREATE VIRTUAL TABLE IF NOT EXISTS group_messages_fts USING fts5(
            text,
            content='group_messages',
            content_rowid='rowid'
        );

        CREATE TRIGGER IF NOT EXISTS group_messages_ai AFTER INSERT ON group_messages BEGIN
          INSERT INTO group_messages_fts(rowid, text) VALUES (new.rowid, new.text);
        END;

        CREATE TRIGGER IF NOT EXISTS group_messages_ad AFTER DELETE ON group_messages BEGIN
          INSERT INTO group_messages_fts(group_messages_fts, rowid, text) VALUES('delete', old.rowid, old.text);
        END;

        CREATE TRIGGER IF NOT EXISTS group_messages_au AFTER UPDATE ON group_messages BEGIN
          INSERT INTO group_messages_fts(group_messages_fts, rowid, text) VALUES('delete', old.rowid, old.text);
          INSERT INTO group_messages_fts(rowid, text) VALUES (new.rowid, new.text);
        END;

        CREATE UNIQUE INDEX IF NOT EXISTS favourite_messages_user_idx
            ON favourite_messages (source, message_id, other)
            WHERE source = 'user';

        CREATE UNIQUE INDEX IF NOT EXISTS favourite_messages_group_idx
            ON favourite_messages (source, message_id, group_id)
            WHERE source = 'group';

        CREATE INDEX IF NOT EXISTS favourite_messages_created_idx
            ON favourite_messages (created_at DESC);
        "#,
    )
    .await?;

        // 9. Synchronize FTS5 indexes if new data was migrated (O(1) existence checks, never full table scan)
        let fts_user_has_rows =
            sqlx::query_scalar::<_, i64>("SELECT 1 FROM user_messages_fts LIMIT 1")
                .fetch_optional(pool)
                .await?
                .is_some();
        let user_msgs_have_text =
            sqlx::query_scalar::<_, i64>("SELECT 1 FROM user_messages WHERE text != '' LIMIT 1")
                .fetch_optional(pool)
                .await?
                .is_some();
        let mut fts_rebuilds_ok = true;
        if had_unmigrated_users || (!fts_user_has_rows && user_msgs_have_text) {
            if let Err(e) = pool
                .execute("INSERT INTO user_messages_fts(user_messages_fts) VALUES('rebuild')")
                .await
            {
                log::error!("user_messages_fts rebuild failed, v1 will be retried: {e}");
                fts_rebuilds_ok = false;
            }
        }

        let fts_grp_has_rows =
            sqlx::query_scalar::<_, i64>("SELECT 1 FROM group_messages_fts LIMIT 1")
                .fetch_optional(pool)
                .await?
                .is_some();
        let grp_msgs_have_text =
            sqlx::query_scalar::<_, i64>("SELECT 1 FROM group_messages WHERE text != '' LIMIT 1")
                .fetch_optional(pool)
                .await?
                .is_some();
        if had_unmigrated_groups || (!fts_grp_has_rows && grp_msgs_have_text) {
            if let Err(e) = pool
                .execute("INSERT INTO group_messages_fts(group_messages_fts) VALUES('rebuild')")
                .await
            {
                log::error!("group_messages_fts rebuild failed, v1 will be retried: {e}");
                fts_rebuilds_ok = false;
            }
        }

        // Record migration 1 applied only if the FTS indexes are consistent; otherwise leave it
        // unrecorded so the (idempotent) v1 steps run again on the next startup.
        if fts_rebuilds_ok {
            let now = get_current_timestamp_millis_since_epoch() as i64;
            sqlx::query("INSERT OR IGNORE INTO _schema_migrations (version, name, applied_at) VALUES (1, 'standard_v1', ?)")
                .bind(now)
                .execute(pool)
                .await?;
        }
    }

    // 10. Migration v2: Group History Chunks Keys and Disapprovals
    let v2_applied: Option<i64> =
        sqlx::query_scalar("SELECT 1 FROM _schema_migrations WHERE version = 2")
            .fetch_optional(pool)
            .await?;
    if v2_applied.is_none() {
        pool.execute(
            r#"
            CREATE TABLE IF NOT EXISTS group_history_chunk_keys (
                group_id INTEGER NOT NULL,
                start_msg_id INTEGER NOT NULL,
                end_msg_id INTEGER NOT NULL,
                key BLOB NOT NULL,
                nonce BLOB NOT NULL,
                created_at INTEGER NOT NULL,
                PRIMARY KEY (group_id, start_msg_id, end_msg_id)
            );

            CREATE INDEX IF NOT EXISTS group_history_chunk_keys_idx
                ON group_history_chunk_keys (group_id, start_msg_id);

            CREATE TABLE IF NOT EXISTS group_history_disapprovals (
                group_id INTEGER NOT NULL,
                chunk_id INTEGER NOT NULL,
                reason TEXT NOT NULL,
                disapproved_at INTEGER NOT NULL,
                PRIMARY KEY (group_id, chunk_id)
            );
            "#,
        )
        .await?;

        let now = get_current_timestamp_millis_since_epoch() as i64;
        sqlx::query("INSERT OR IGNORE INTO _schema_migrations (version, name, applied_at) VALUES (2, 'history_chunks_v2', ?)")
            .bind(now)
            .execute(pool)
            .await?;
    }

    pool.execute(r#"
        CREATE TABLE IF NOT EXISTS group_history_keys_v3 (
            group_id INTEGER NOT NULL, start_msg_id INTEGER NOT NULL, end_msg_id INTEGER NOT NULL,
            hash BLOB NOT NULL, key BLOB NOT NULL, nonce BLOB NOT NULL,
            PRIMARY KEY (group_id, start_msg_id, end_msg_id, hash)
        );
        CREATE TABLE IF NOT EXISTS group_history_imports (
            group_id INTEGER NOT NULL, message_id INTEGER NOT NULL, chunk_id INTEGER NOT NULL,
            PRIMARY KEY (group_id, message_id)
        );
        CREATE TABLE IF NOT EXISTS group_history_imported_chunks (
            group_id INTEGER NOT NULL, chunk_id INTEGER NOT NULL, hash BLOB NOT NULL,
            PRIMARY KEY (group_id, chunk_id)
        );
        CREATE INDEX IF NOT EXISTS group_messages_channel_history ON group_messages(group_id, channel_id, id);
        INSERT OR IGNORE INTO _schema_migrations(version, name, applied_at) VALUES (3, 'verified_history_v3', 0);
    "#).await?;
    pool.execute(r#"
        CREATE TABLE IF NOT EXISTS group_pin_updates (
            group_id INTEGER NOT NULL, message_id INTEGER NOT NULL, channel_id INTEGER NOT NULL,
            event_id INTEGER NOT NULL, pinned INTEGER NOT NULL,
            PRIMARY KEY(group_id, message_id, channel_id)
        );
        INSERT OR IGNORE INTO _schema_migrations(version,name,applied_at) VALUES(4,'shared_pins_v4',0);
    "#).await?;
    pool.execute(r#"
        CREATE TABLE IF NOT EXISTS group_snapshot_inbox (
            group_id INTEGER NOT NULL,snapshot_id TEXT NOT NULL,
            done INTEGER NOT NULL DEFAULT 0,attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt INTEGER NOT NULL DEFAULT 0,last_error TEXT,
            PRIMARY KEY(group_id,snapshot_id)
        );
        CREATE INDEX IF NOT EXISTS group_snapshot_inbox_due ON group_snapshot_inbox(group_id,done,next_attempt);
        INSERT OR IGNORE INTO _schema_migrations(version,name,applied_at) VALUES(5,'snapshot_inbox_v5',0);
    "#).await?;
    pool.execute(r#"
        CREATE TABLE IF NOT EXISTS receipt_journal (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id BLOB NOT NULL,
            epoch INTEGER NOT NULL,
            capability_key TEXT,
            capability_value TEXT,
            state TEXT NOT NULL DEFAULT 'staged',
            created_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS receipt_journal_staged ON receipt_journal (state);
        INSERT OR IGNORE INTO _schema_migrations(version,name,applied_at) VALUES(6,'receipt_journal_v6',0);
    "#).await?;

    // Migration v7: hidden messages (e.g. group history keys, whose payload is raw key material)
    // must never be stored as searchable text. Blank `text` for existing hidden rows in bounded
    // batches; the AFTER UPDATE triggers remove the old text from the FTS indexes.
    let v7_applied: Option<i64> =
        sqlx::query_scalar("SELECT 1 FROM _schema_migrations WHERE version = 7")
            .fetch_optional(pool)
            .await?;
    if v7_applied.is_none() {
        for table in ["user_messages", "group_messages"] {
            // FTS5 'secure-delete' (SQLite >= 3.42, persisted in the FTS config) makes trigger
            // deletes remove the old tokens from the index b-tree instead of only tombstoning them.
            if let Err(e) = pool
                .execute(
                    format!("INSERT INTO {table}_fts({table}_fts, rank) VALUES('secure-delete', 1)")
                        .as_str(),
                )
                .await
            {
                log::warn!("could not enable FTS5 secure-delete on {table}_fts: {e}");
            }
            let sql = format!(
                "UPDATE {table} SET text = '' WHERE rowid IN \
                 (SELECT rowid FROM {table} WHERE (message_type & ?) != 0 AND text != '' LIMIT ?)"
            );
            loop {
                let updated = sqlx::query(&sql)
                    .bind(crate::db::search::UNSEARCHABLE_MESSAGE_TYPE_MASK as i64)
                    .bind(MIGRATION_BATCH_SIZE)
                    .execute(pool)
                    .await?
                    .rows_affected();
                if updated == 0 {
                    break;
                }
            }
        }
        let now = get_current_timestamp_millis_since_epoch() as i64;
        sqlx::query("INSERT OR IGNORE INTO _schema_migrations (version, name, applied_at) VALUES (7, 'purge_hidden_text_v7', ?)")
            .bind(now)
            .execute(pool)
            .await?;
    }

    // Migration v8: users who departed a group, recorded from MLS-authenticated commits. Only a
    // stale departure (every removed device's credential expired) permits an automatic re-add of
    // a user who is no longer in the roster; the server's re-add requests are untrusted.
    let v8_applied: Option<i64> =
        sqlx::query_scalar("SELECT 1 FROM _schema_migrations WHERE version = 8")
            .fetch_optional(pool)
            .await?;
    if v8_applied.is_none() {
        pool.execute(
            r#"
            CREATE TABLE IF NOT EXISTS group_departed_members (
                group_id INTEGER NOT NULL,
                username TEXT NOT NULL,
                re_addable INTEGER NOT NULL,
                epoch INTEGER NOT NULL,
                PRIMARY KEY (group_id, username)
            );
            "#,
        )
        .await?;
        let now = get_current_timestamp_millis_since_epoch() as i64;
        sqlx::query("INSERT OR IGNORE INTO _schema_migrations (version, name, applied_at) VALUES (8, 'group_departed_members_v8', ?)")
            .bind(now)
            .execute(pool)
            .await?;
    }
    Ok(())
}
