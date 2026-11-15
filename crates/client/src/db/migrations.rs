//! # Client SQLite database migrations
//!
//! Every change to the client's schema is a numbered entry in [`MIGRATIONS`], applied in order
//! and recorded in `_schema_migrations (version, name, applied_at)`.
//!
//! ## Rules for adding a migration
//! 1. **Append only.** Add a new entry with the next version; never change, reorder or reuse an
//!    existing one. Its `name` is recorded and checked.
//! 2. **Backwards compatible.** Older app versions open the same database (a downgrade, or an
//!    older build of the app on another of the user's machines sharing nothing but the file
//!    format). So only add: new tables, new nullable columns or columns with a `DEFAULT`, new
//!    indexes. Never drop, rename or change the meaning of a column or table an older version
//!    reads; leave old data in place and stop using it instead.
//! 3. **Prefer [`Step::Sql`].** It runs in one `BEGIN IMMEDIATE` transaction together with its
//!    `_schema_migrations` record, so a crash or error leaves the database exactly as before and
//!    the migration runs again on the next start. Columns need [`Step::Code`] with [`add_column`]
//!    or [`ensure_column`] (SQLite has no `ADD COLUMN IF NOT EXISTS`); they check first, so a
//!    repeated run can't fail on a column that is already there.
//! 4. **[`Step::Code`] only when SQL can't express it** (backfills over many rows, FTS rebuilds,
//!    reading data to decide). These run outside a transaction in bounded batches, so they must
//!    be idempotent: safe to run again from any point. They may return
//!    [`Outcome::RetryLater`] to stay unrecorded and run again on the next start.
//! 5. **Test it** in `tests` below: `upgrades_from_every_earlier_version` replays every prefix of
//!    the list, so a new migration is checked against databases of every older version.
//!
//! A database recorded at a version newer than this build knows (opened by an older app after a
//! newer one) is fine: the extra migrations were additive, and are left alone.

use crate::sql::{Executor, Row, SqlitePool};
use std::collections::HashSet;

use crate::db::group_messages::searchable_group_message_text;
use crate::db::messages::searchable_user_message_text;
use crate::utils::get_current_timestamp_millis_since_epoch;

/// Highest version in [`MIGRATIONS`] (a test checks they agree).
pub const LATEST_MIGRATION_VERSION: i64 = 9;

/// Rows per batch for data backfills (keeps memory and lock hold times bounded).
const MIGRATION_BATCH_SIZE: i64 = 500;

static MIGRATION_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// One schema change. See the module docs for the rules.
pub struct Migration {
    pub version: i64,
    pub name: &'static str,
    pub step: Step,
}

/// How a migration runs.
pub enum Step {
    /// SQL statements, applied atomically together with the migration's record.
    Sql(&'static str),
    /// Rust code for what SQL can't express; must be idempotent.
    Code(CodeStep),
}

/// The migrations implemented in Rust (dispatched by `run_code`, so their futures stay `Send`).
#[derive(Clone, Copy)]
pub enum CodeStep {
    BaseTablesAndSearch,
    PurgeHiddenText,
    OwnDeviceTrust,
}

/// What a [`Step::Code`] migration reports.
#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    Applied,
    /// Leave it unrecorded: it runs again on the next start (e.g. an FTS rebuild that failed).
    RetryLater,
}

pub const MIGRATIONS: &[Migration] = &[
    Migration { version: 1, name: "standard_v1", step: Step::Code(CodeStep::BaseTablesAndSearch) },
    Migration { version: 2, name: "history_chunks_v2", step: Step::Sql(V2_HISTORY_CHUNKS) },
    Migration { version: 3, name: "verified_history_v3", step: Step::Sql(V3_VERIFIED_HISTORY) },
    Migration { version: 4, name: "shared_pins_v4", step: Step::Sql(V4_SHARED_PINS) },
    Migration { version: 5, name: "snapshot_inbox_v5", step: Step::Sql(V5_SNAPSHOT_INBOX) },
    Migration { version: 6, name: "receipt_journal_v6", step: Step::Sql(V6_RECEIPT_JOURNAL) },
    Migration { version: 7, name: "purge_hidden_text_v7", step: Step::Code(CodeStep::PurgeHiddenText) },
    Migration { version: 8, name: "group_departed_members_v8", step: Step::Sql(V8_GROUP_DEPARTED_MEMBERS) },
    Migration { version: 9, name: "own_device_trust_v9", step: Step::Code(CodeStep::OwnDeviceTrust) },
];

const V2_HISTORY_CHUNKS: &str = r#"
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
"#;

const V3_VERIFIED_HISTORY: &str = r#"
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
"#;

const V4_SHARED_PINS: &str = r#"
CREATE TABLE IF NOT EXISTS group_pin_updates (
    group_id INTEGER NOT NULL, message_id INTEGER NOT NULL, channel_id INTEGER NOT NULL,
    event_id INTEGER NOT NULL, pinned INTEGER NOT NULL,
    PRIMARY KEY(group_id, message_id, channel_id)
);
"#;

const V5_SNAPSHOT_INBOX: &str = r#"
CREATE TABLE IF NOT EXISTS group_snapshot_inbox (
    group_id INTEGER NOT NULL,snapshot_id TEXT NOT NULL,
    done INTEGER NOT NULL DEFAULT 0,attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt INTEGER NOT NULL DEFAULT 0,last_error TEXT,
    PRIMARY KEY(group_id,snapshot_id)
);
CREATE INDEX IF NOT EXISTS group_snapshot_inbox_due ON group_snapshot_inbox(group_id,done,next_attempt);
"#;

const V6_RECEIPT_JOURNAL: &str = r#"
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
"#;

/// Users who departed a group, recorded from MLS-authenticated commits. Only a stale departure
/// (every removed device's credential expired) permits an automatic re-add of a user who is no
/// longer in the roster; the server's re-add requests are untrusted.
const V8_GROUP_DEPARTED_MEMBERS: &str = r#"
CREATE TABLE IF NOT EXISTS group_departed_members (
    group_id INTEGER NOT NULL,
    username TEXT NOT NULL,
    re_addable INTEGER NOT NULL,
    epoch INTEGER NOT NULL,
    PRIMARY KEY (group_id, username)
);
"#;

pub async fn table_exists(pool: &SqlitePool, table: &str) -> anyhow::Result<bool> {
    let row = crate::sql::query("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?")
        .bind(table)
        .fetch_optional(pool)
        .await?;
    Ok(row.is_some())
}

pub async fn get_table_columns(pool: &SqlitePool, table: &str) -> anyhow::Result<HashSet<String>> {
    let query = format!("PRAGMA table_info({})", table);
    let rows = crate::sql::query(&query).fetch_all(pool).await?;
    let mut cols = HashSet::new();
    for row in rows {
        let name: String = row.try_get("name")?;
        cols.insert(name.to_lowercase());
    }
    Ok(cols)
}

/// Adds `column` to `table` unless it is already there (or the table doesn't exist).
/// `column_def` must allow existing rows: nullable, or with a `DEFAULT`.
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
        log::info!("Migrating table '{}': adding column '{} {}'", table, column, column_def);
        let sql = format!("ALTER TABLE {} ADD COLUMN {} {}", table, column, column_def);
        pool.execute(sql.as_str()).await?;
    }
    Ok(())
}

/// Adds `column` to `table` on this connection (e.g. a transaction's) unless it is already there.
/// SQLite has no `ADD COLUMN IF NOT EXISTS`, so a migration that adds a column is a
/// [`Step::Code`] using this (or [`ensure_column`]) rather than plain [`Step::Sql`].
pub async fn add_column(
    conn: &mut crate::sql::SqliteConnection,
    table: &str,
    column: &str,
    column_def: &str,
) -> anyhow::Result<()> {
    let query = format!("PRAGMA table_info({})", table);
    let rows = crate::sql::query(&query).fetch_all(&mut *conn).await?;
    let mut present = false;
    for row in rows {
        let name: String = row.try_get("name")?;
        present |= name.eq_ignore_ascii_case(column);
    }
    if !present {
        let sql = format!("ALTER TABLE {} ADD COLUMN {} {}", table, column, column_def);
        (&mut *conn).execute(sql.as_str()).await?;
    }
    Ok(())
}

/// Applies every migration in [`MIGRATIONS`] that isn't recorded yet, in order.
pub async fn run_migrations(pool: &SqlitePool) -> anyhow::Result<()> {
    run_list(pool, MIGRATIONS).await
}

async fn run_list(pool: &SqlitePool, migrations: &[Migration]) -> anyhow::Result<()> {
    let _lock = MIGRATION_LOCK.lock().await;
    pool.execute(
        r#"
        CREATE TABLE IF NOT EXISTS _schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at INTEGER NOT NULL
        );
        "#,
    )
    .await?;

    let rows = crate::sql::query("SELECT version, name FROM _schema_migrations").fetch_all(pool).await?;
    let mut applied = std::collections::HashMap::new();
    for row in rows {
        let version: i64 = row.try_get("version")?;
        let name: String = row.try_get("name")?;
        applied.insert(version, name);
    }

    // Fast path: everything this build knows is recorded (newer versions recorded by a newer
    // app are additive and left alone).
    if migrations.iter().all(|m| applied.contains_key(&m.version)) {
        if let Some(newest) = applied.keys().max().filter(|v| **v > migrations.last().map_or(0, |m| m.version)) {
            log::info!("database schema is at version {newest}, newer than this build knows; using it as is");
        }
        return Ok(());
    }

    for migration in migrations {
        if let Some(name) = applied.get(&migration.version) {
            if name != migration.name {
                log::warn!(
                    "migration {} is recorded as '{}' but this build calls it '{}'",
                    migration.version, name, migration.name
                );
            }
            continue;
        }
        log::info!("applying database migration {} ({})", migration.version, migration.name);
        let now = get_current_timestamp_millis_since_epoch() as i64;
        match &migration.step {
            Step::Sql(sql) => {
                let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
                (&mut *tx).execute(*sql).await?;
                crate::sql::query("INSERT INTO _schema_migrations (version, name, applied_at) VALUES (?, ?, ?)")
                    .bind(migration.version)
                    .bind(migration.name)
                    .bind(now)
                    .execute(&mut *tx)
                    .await?;
                tx.commit().await?;
            }
            Step::Code(step) => {
                if run_code(pool, *step).await? == Outcome::Applied {
                    crate::sql::query("INSERT OR IGNORE INTO _schema_migrations (version, name, applied_at) VALUES (?, ?, ?)")
                        .bind(migration.version)
                        .bind(migration.name)
                        .bind(now)
                        .execute(pool)
                        .await?;
                } else {
                    log::warn!("migration {} ({}) will be retried on the next start", migration.version, migration.name);
                }
            }
        }
    }
    Ok(())
}

async fn run_code(pool: &SqlitePool, step: CodeStep) -> anyhow::Result<Outcome> {
    match step {
        CodeStep::BaseTablesAndSearch => base_tables_and_search(pool).await,
        CodeStep::PurgeHiddenText => purge_hidden_text(pool).await,
        CodeStep::OwnDeviceTrust => own_device_trust(pool).await,
    }
}

/// v1: the message tables, their columns, searchable text and the FTS5 indexes.
async fn base_tables_and_search(pool: &SqlitePool) -> anyhow::Result<Outcome> {
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
        let batch = crate::sql::query("SELECT rowid, message, message_type FROM user_messages WHERE text = '' AND length(message) > 0 LIMIT 500")
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
            crate::sql::query("UPDATE user_messages SET text = ? WHERE rowid = ?")
                .bind(update_text)
                .bind(rowid)
                .execute(pool)
                .await?;
        }
    }

    // 7. Backfill group_messages text in batches
    let mut had_unmigrated_groups = false;
    loop {
        let batch = crate::sql::query("SELECT rowid, message, message_type FROM group_messages WHERE text = '' AND length(message) > 0 LIMIT 500")
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
            crate::sql::query("UPDATE group_messages SET text = ? WHERE rowid = ?")
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
        crate::sql::query_scalar::<_, i64>("SELECT 1 FROM user_messages_fts LIMIT 1")
            .fetch_optional(pool)
            .await?
            .is_some();
    let user_msgs_have_text =
        crate::sql::query_scalar::<_, i64>("SELECT 1 FROM user_messages WHERE text != '' LIMIT 1")
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
        crate::sql::query_scalar::<_, i64>("SELECT 1 FROM group_messages_fts LIMIT 1")
            .fetch_optional(pool)
            .await?
            .is_some();
    let grp_msgs_have_text =
        crate::sql::query_scalar::<_, i64>("SELECT 1 FROM group_messages WHERE text != '' LIMIT 1")
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

    // Recorded only if the FTS indexes are consistent; otherwise the (idempotent) v1 steps run
    // again on the next startup.
    Ok(if fts_rebuilds_ok { Outcome::Applied } else { Outcome::RetryLater })

}

/// v7: hidden messages (e.g. group history keys, whose payload is raw key material) must never be
/// stored as searchable text. Blank `text` for existing hidden rows in bounded batches; the AFTER
/// UPDATE triggers remove the old text from the FTS indexes.
async fn purge_hidden_text(pool: &SqlitePool) -> anyhow::Result<Outcome> {
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
            let updated = crate::sql::query(&sql)
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
    Ok(Outcome::Applied)
}

/// v9: which of the account's other devices this device trusts with its history (see
/// `own_devices`). The account's devices this device already exchanged messages with (an address
/// and an identity key here) are trusted now, pinned to that key; devices only the server knows
/// of are trusted on the next connect. Additive: older app versions ignore the table.
async fn own_device_trust(pool: &SqlitePool) -> anyhow::Result<Outcome> {
    pool.execute(
        r#"
        CREATE TABLE IF NOT EXISTS own_device_trust (
            address_id INTEGER PRIMARY KEY,
            device_id INTEGER NOT NULL,
            identity_key BLOB,
            state INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );
        "#,
    )
    .await?;
    let now = get_current_timestamp_millis_since_epoch() as i64;
    if table_exists(pool, "identity_keypair").await?
        && table_exists(pool, "addresses").await?
        && table_exists(pool, "identities").await?
        && get_table_columns(pool, "identity_keypair").await?.contains("username")
    {
        crate::sql::query(
            r#"
            INSERT OR IGNORE INTO own_device_trust (address_id, device_id, identity_key, state, updated_at)
            SELECT a.id, a.device_id,
                   (SELECT i.identity_key FROM identities i WHERE i.address = a.username || '.' || a.device_id),
                   1, ?
            FROM addresses a
            WHERE a.username = (SELECT username FROM identity_keypair LIMIT 1)
              AND a.username <> ''
              AND a.id <> (SELECT id FROM identity_keypair LIMIT 1)
            "#,
        )
        .bind(now)
        .execute(pool)
        .await?;
    }
    Ok(Outcome::Applied)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::sql::sqlite::SqlitePoolOptions;

    async fn empty_pool() -> SqlitePool {
        SqlitePoolOptions::new().max_connections(1).connect("sqlite::memory:").await.unwrap()
    }

    async fn recorded(pool: &SqlitePool) -> Vec<(i64, String)> {
        let rows = crate::sql::query("SELECT version, name FROM _schema_migrations ORDER BY version")
            .fetch_all(pool)
            .await
            .unwrap();
        rows.iter().map(|r| (r.get::<i64, _>("version"), r.get::<String, _>("name"))).collect()
    }

    fn expected() -> Vec<(i64, String)> {
        MIGRATIONS.iter().map(|m| (m.version, m.name.to_string())).collect()
    }

    #[test]
    fn versions_are_contiguous_and_latest_matches() {
        for (i, m) in MIGRATIONS.iter().enumerate() {
            assert_eq!(m.version, i as i64 + 1, "migration versions must be 1, 2, 3, ... in order");
        }
        assert_eq!(MIGRATIONS.last().unwrap().version, LATEST_MIGRATION_VERSION);
        let names: HashSet<_> = MIGRATIONS.iter().map(|m| m.name).collect();
        assert_eq!(names.len(), MIGRATIONS.len(), "migration names must be unique");
    }

    #[tokio::test]
    async fn a_new_database_gets_every_migration() {
        let pool = empty_pool().await;
        run_migrations(&pool).await.unwrap();
        assert_eq!(recorded(&pool).await, expected());
        for table in ["user_messages", "group_messages", "group_history_imports", "receipt_journal", "own_device_trust"] {
            assert!(table_exists(&pool, table).await.unwrap(), "{table} missing");
        }
        // Running again is a no-op.
        run_migrations(&pool).await.unwrap();
        assert_eq!(recorded(&pool).await, expected());
    }

    #[tokio::test]
    async fn upgrades_from_every_earlier_version() {
        for applied in 0..MIGRATIONS.len() {
            let pool = empty_pool().await;
            run_list(&pool, &MIGRATIONS[..applied]).await.unwrap();
            // Rows an older version wrote survive the upgrade.
            if applied >= 1 {
                crate::sql::query("INSERT INTO user_messages (id, other, sent_by_other, message) VALUES (1, 'bob', 1, x'00')")
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            run_migrations(&pool).await.unwrap();
            assert_eq!(recorded(&pool).await, expected(), "upgrading from version {applied}");
            if applied >= 1 {
                let n: i64 = crate::sql::query_scalar("SELECT count(*) FROM user_messages").fetch_one(&pool).await.unwrap();
                assert_eq!(n, 1, "data lost upgrading from version {applied}");
            }
        }
    }

    #[tokio::test]
    async fn a_failing_migration_changes_nothing_and_runs_again() {
        let pool = empty_pool().await;
        run_migrations(&pool).await.unwrap();
        const BROKEN: &[Migration] = &[Migration {
            version: 1000,
            name: "broken",
            step: Step::Sql("CREATE TABLE half_done (x INTEGER); INSERT INTO no_such_table VALUES (1);"),
        }];
        assert!(run_list(&pool, BROKEN).await.is_err());
        assert!(!table_exists(&pool, "half_done").await.unwrap(), "a failed migration left a table behind");
        let version: Option<i64> = crate::sql::query_scalar("SELECT version FROM _schema_migrations WHERE version = 1000")
            .fetch_optional(&pool)
            .await
            .unwrap();
        assert!(version.is_none(), "a failed migration was recorded");
    }

    #[tokio::test]
    async fn a_database_from_a_newer_app_is_left_alone() {
        let pool = empty_pool().await;
        run_migrations(&pool).await.unwrap();
        crate::sql::query("INSERT INTO _schema_migrations (version, name, applied_at) VALUES (99, 'from_a_newer_app', 0)")
            .execute(&pool)
            .await
            .unwrap();
        crate::sql::query("CREATE TABLE newer_feature (x INTEGER)").execute(&pool).await.unwrap();
        run_migrations(&pool).await.unwrap();
        assert!(table_exists(&pool, "newer_feature").await.unwrap());
    }

    #[tokio::test]
    async fn add_column_is_safe_to_repeat() {
        let pool = empty_pool().await;
        run_migrations(&pool).await.unwrap();
        let mut conn = pool.acquire().await.unwrap();
        add_column(&mut conn, "user_messages", "extra", "INTEGER NOT NULL DEFAULT 0").await.unwrap();
        add_column(&mut conn, "user_messages", "extra", "INTEGER NOT NULL DEFAULT 0").await.unwrap();
        drop(conn);
        assert!(get_table_columns(&pool, "user_messages").await.unwrap().contains("extra"));
    }
}
