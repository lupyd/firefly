use std::collections::HashMap;
use zeroize::Zeroizing;

use firefly_core::storage_provider::MlsGroupStateStorage;
use firefly_core::storage_provider::MlsKeyPackageStorage;
use firefly_core::storage_provider::MlsPreSharedKeyStorage;
use crate::storage::{GroupDepartureStorage, GroupInfoStorage};
use sqlx::Executor;
use sqlx::SqlitePool;
use sqlx::prelude::*;

/// Number of most recent prior-epoch records kept per group (mls-rs's own default
/// `DEFAULT_EPOCH_RETENTION_LIMIT`). Older epoch secrets are deleted for forward secrecy at rest;
/// application messages from non-current epochs are rejected, so they are never needed.
pub const EPOCH_RETENTION: i64 = 3;

pub struct GroupStateStore {
    pool: SqlitePool,
}

impl GroupStateStore {
    pub async fn new(pool: SqlitePool) -> anyhow::Result<Self> {
        pool.execute(
            r#"
        CREATE TABLE IF NOT EXISTS group_states (
            id BLOB PRIMARY KEY,
            state BLOB NOT NULL
        )"#,
        )
        .await?;

        pool.execute(
            r#"
        CREATE TABLE IF NOT EXISTS group_epoch_states (
            id BLOB NOT NULL,
            epoch INTEGER NOT NULL,
            state BLOB NOT NULL,
            PRIMARY KEY (id, epoch),
            FOREIGN KEY (id) REFERENCES group_states(id) ON DELETE CASCADE
        )"#,
        )
        .await?;

        crate::receipt_coordinator::ReceiptCoordinator::init(&pool).await?;
        Ok(Self { pool })
    }

    pub async fn get_state(&self, id: &[u8]) -> anyhow::Result<Vec<u8>> {
        log::info!("store select: group_state id={}", hex::encode(id));
        let row = sqlx::query("SELECT state FROM group_states WHERE id = ?")
            .bind(id)
            .fetch_one(&self.pool)
            .await?;
        let state = row.try_get(0)?;
        Ok(state)
    }

    pub async fn set_state(
        &self,
        group_id: &[u8],
        state_data: &[u8],
        epoch_inserts: HashMap<u64, Vec<u8>>,
        epoch_updates: HashMap<u64, Vec<u8>>,
    ) -> anyhow::Result<()> {
        log::info!("store insert: group_state id={}", hex::encode(group_id));
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;

        // NOTE: with `foreign_keys = ON` (see `setup_pool`) REPLACE deletes the old row and the
        // ON DELETE CASCADE drops every stored prior epoch, so only the epochs inserted below
        // survive this write. The explicit EPOCH_RETENTION prune further down bounds retention
        // even where foreign keys are off.
        sqlx::query("INSERT OR REPLACE INTO group_states (id, state) VALUES (?, ?)")
            .bind(group_id)
            .bind(state_data)
            .execute(&mut *tx)
            .await?;

        for (epoch_id, state) in epoch_inserts {
            log::info!(
                "store insert: group_epoch_state id={} epoch={}",
                hex::encode(group_id),
                epoch_id
            );
            sqlx::query(
                "INSERT OR REPLACE INTO group_epoch_states (id, epoch, state) VALUES (?, ?, ?)",
            )
            .bind(group_id)
            .bind(epoch_id as i64)
            .bind(state)
            .execute(&mut *tx)
            .await?;
        }
        for (epoch_id, state) in epoch_updates {
            log::info!(
                "store update: group_epoch_state id={} epoch={}",
                hex::encode(group_id),
                epoch_id
            );
            sqlx::query(
                "INSERT OR REPLACE INTO group_epoch_states (id, epoch, state) VALUES (?, ?, ?)",
            )
            .bind(group_id)
            .bind(epoch_id as i64)
            .bind(state)
            .execute(&mut *tx)
            .await?;
        }

        // Keep only the EPOCH_RETENTION newest prior epochs. The newest record is always kept, so
        // `max_epoch_id` (which mls-rs uses to validate the next inserted epoch id) is unchanged.
        let pruned = sqlx::query(
            "DELETE FROM group_epoch_states WHERE id = ?1 AND epoch <= (SELECT MAX(epoch) FROM group_epoch_states WHERE id = ?1) - ?2",
        )
        .bind(group_id)
        .bind(EPOCH_RETENTION)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if pruned > 0 {
            log::info!(
                "store delete: pruned {} old group_epoch_states id={}",
                pruned,
                hex::encode(group_id)
            );
        }

        tx.commit().await?;

        Ok(())
    }

    /// Deletes every piece of local state for one group (MLS state and prior-epoch secrets,
    /// history keys, messages + FTS entries, favourites, pins, snapshot/history bookkeeping and
    /// group-scoped key-value entries) in a single transaction.
    ///
    /// `group_id` is the MLS group identifier (`group_states.id` / `group_infos.group_state_id`).
    /// The numeric Firefly group id is resolved through `group_infos`; if that row is already
    /// gone use [`Self::purge_group_local_state_with_id`] instead so message tables are purged too.
    pub async fn purge_group_local_state(&self, group_id: &[u8]) -> anyhow::Result<()> {
        self.purge_group_local_state_inner(None, group_id).await
    }

    /// Like [`Self::purge_group_local_state`], but with the numeric Firefly group id supplied by
    /// the caller (still also purges any other numeric id mapped to `identifier` in `group_infos`).
    pub async fn purge_group_local_state_with_id(
        &self,
        numeric_group_id: u64,
        identifier: &[u8],
    ) -> anyhow::Result<()> {
        self.purge_group_local_state_inner(Some(numeric_group_id), identifier)
            .await
    }

    async fn purge_group_local_state_inner(
        &self,
        numeric_group_id: Option<u64>,
        identifier: &[u8],
    ) -> anyhow::Result<()> {
        log::info!(
            "store delete: purge group local state id={} numeric={:?}",
            hex::encode(identifier),
            numeric_group_id
        );
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;

        let existing: std::collections::HashSet<String> =
            sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type = 'table'")
                .fetch_all(&mut *tx)
                .await?
                .into_iter()
                .collect();
        let has = |t: &str| existing.contains(t);

        let mut numeric_ids: Vec<i64> = numeric_group_id.map(|g| g as i64).into_iter().collect();
        if has("group_infos") {
            let mapped: Vec<i64> =
                sqlx::query_scalar("SELECT id FROM group_infos WHERE group_state_id = ?")
                    .bind(identifier)
                    .fetch_all(&mut *tx)
                    .await?;
            for id in mapped {
                if !numeric_ids.contains(&id) {
                    numeric_ids.push(id);
                }
            }
        }

        // Tables keyed by the MLS group identifier.
        for (table, column) in [
            ("group_epoch_states", "id"),
            ("group_states", "id"),
            ("receipt_journal", "group_id"),
            ("group_infos", "group_state_id"),
        ] {
            if has(table) {
                sqlx::query(&format!("DELETE FROM {table} WHERE {column} = ?"))
                    .bind(identifier)
                    .execute(&mut *tx)
                    .await?;
            }
        }

        for gid in numeric_ids {
            // Deleting group_messages fires the FTS delete trigger for each row.
            for table in [
                "group_messages",
                "group_history_keys_v3",
                "group_history_chunk_keys",
                "group_history_disapprovals",
                "group_history_imports",
                "group_history_imported_chunks",
                "group_pin_updates",
                "group_snapshot_inbox",
                "group_departed_members",
                "group_infos",
            ] {
                if has(table) {
                    let column = if table == "group_infos" { "id" } else { "group_id" };
                    sqlx::query(&format!("DELETE FROM {table} WHERE {column} = ?"))
                        .bind(gid)
                        .execute(&mut *tx)
                        .await?;
                }
            }
            if has("favourite_messages") {
                sqlx::query("DELETE FROM favourite_messages WHERE source = 'group' AND group_id = ?")
                    .bind(gid)
                    .execute(&mut *tx)
                    .await?;
            }
            if has("key_value_store") {
                for key in [
                    format!("snapshot-join-pending:{gid}"),
                    format!("snapshot-join-completed:{gid}"),
                    format!("history_export_cursor:{gid}"),
                    format!("history_verify_cursor:{gid}"),
                ] {
                    sqlx::query("DELETE FROM key_value_store WHERE key = ?")
                        .bind(key)
                        .execute(&mut *tx)
                        .await?;
                }
                // Prefix match without LIKE so `_` / `%` need no escaping.
                for prefix in [
                    format!("snapshot-secret:{gid}:"),
                    format!("snapshot-outbox:{gid}:"),
                    format!("snapshot-candidate:{gid}:"),
                    format!("snapshot-archive:{gid}:"),
                ] {
                    sqlx::query("DELETE FROM key_value_store WHERE substr(key, 1, length(?1)) = ?1")
                        .bind(prefix)
                        .execute(&mut *tx)
                        .await?;
                }
            }
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn get_epoch_state(&self, id: &[u8], epoch_id: u64) -> anyhow::Result<Vec<u8>> {
        log::info!(
            "store select: group_epoch_state id={} epoch={}",
            hex::encode(id),
            epoch_id
        );
        let row = sqlx::query("SELECT state FROM group_epoch_states WHERE id = ? AND epoch = ?")
            .bind(id)
            .bind(epoch_id as i64)
            .fetch_one(&self.pool)
            .await?;

        Ok(row.try_get(0)?)
    }

    pub async fn get_max_epoch_id(&self, group_id: &[u8]) -> anyhow::Result<Option<u64>> {
        log::info!("store select: group_max_epoch id={}", hex::encode(group_id));
        let row = sqlx::query("SELECT MAX(epoch) FROM group_epoch_states WHERE id = ?")
            .bind(group_id)
            .fetch_one(&self.pool)
            .await?;

        let id: Option<i64> = row.try_get(0)?;

        Ok(id.map(|val| val as u64))
    }
}

#[async_trait::async_trait]
impl MlsGroupStateStorage for GroupStateStore {
    async fn state(&self, group_id: Vec<u8>) -> Option<Zeroizing<Vec<u8>>> {
        self.get_state(&group_id).await.ok().map(Zeroizing::new)
    }
    async fn epoch(&self, group_id: Vec<u8>, epoch_id: u64) -> Option<Zeroizing<Vec<u8>>> {
        self.get_epoch_state(&group_id, epoch_id)
            .await
            .ok()
            .map(Zeroizing::new)
    }
    async fn write(
        &self,
        group_id: Vec<u8>,
        state_data: Zeroizing<Vec<u8>>,
        epoch_inserts: HashMap<u64, Zeroizing<Vec<u8>>>,
        epoch_updates: HashMap<u64, Zeroizing<Vec<u8>>>,
    ) -> bool {
        self.set_state(
            &group_id,
            &state_data,
            epoch_inserts
                .into_iter()
                .map(|(k, v)| (k, v.to_vec()))
                .collect(),
            epoch_updates
                .into_iter()
                .map(|(k, v)| (k, v.to_vec()))
                .collect(),
        )
        .await
        .is_ok()
    }
    async fn max_epoch_id(&self, group_id: Vec<u8>) -> Option<u64> {
        self.get_max_epoch_id(&group_id).await.ok().flatten()
    }
}

pub struct GroupPskStore {}

#[async_trait::async_trait]
impl MlsPreSharedKeyStorage for GroupPskStore {
    async fn get(&self, _id: Vec<u8>) -> Option<Vec<u8>> {
        None
    }
}

pub struct SelfGroupKeyPackageStore {
    pool: SqlitePool,
}

impl SelfGroupKeyPackageStore {
    pub async fn new(pool: SqlitePool) -> anyhow::Result<Self> {
        pool.execute(
            r#"
            CREATE TABLE IF NOT EXISTS self_group_key_packages(
                id INTEGER PRIMARY KEY NOT NULL,
                key_package BLOB NOT NULL
            )
            "#,
        )
        .await?;

        Ok(Self { pool })
    }

    pub async fn set(&self, id: i32, key_package_data: &[u8]) -> anyhow::Result<()> {
        log::info!("store insert: self_group_key_package id={}", id);
        sqlx::query(
            r#"
                INSERT OR REPLACE INTO self_group_key_packages (id, key_package)
                VALUES (?, ?)
                "#,
        )
        .bind(id)
        .bind(key_package_data)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn get(&self, id: i32) -> anyhow::Result<Vec<u8>> {
        log::info!("store select: self_group_key_package id={}", id);
        let row = sqlx::query(
            r#"
                SELECT key_package FROM self_group_key_packages
                WHERE id = ?
                "#,
        )
        .bind(id)
        .fetch_one(&self.pool)
        .await?;

        let key_package: Vec<u8> = row.try_get(0)?;

        Ok(key_package)
    }

    pub async fn delete(&self, id: i32) -> anyhow::Result<()> {
        log::info!("store delete: self_group_key_package id={}", id);
        sqlx::query(
            r#"
                DELETE FROM self_group_key_packages
                WHERE id = ?
                "#,
        )
        .bind(id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn delete_many(&self, ids: &[i32]) -> anyhow::Result<()> {
        for chunk in ids.chunks(50) {
            let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let query = format!(
                "DELETE FROM self_group_key_packages WHERE id IN ({})",
                placeholders
            );
            let mut q = sqlx::query(&query);
            for id in chunk {
                q = q.bind(id);
            }
            q.execute(&self.pool).await?;
        }

        Ok(())
    }
}

pub struct GroupKeyPackageStore {
    pool: SqlitePool,
}

impl GroupKeyPackageStore {
    pub async fn new(pool: SqlitePool) -> anyhow::Result<Self> {
        pool.execute(
            r#"
            CREATE TABLE IF NOT EXISTS group_key_packages(
                id BLOB PRIMARY KEY NOT NULL,
                key_package BLOB NOT NULL
            )
            "#,
        )
        .await?;

        Ok(Self { pool })
    }

    pub async fn contains(&self, id: &[u8]) -> bool {
        sqlx::query(
            r#"
                SELECT 1 FROM group_key_packages
                WHERE id = ?
                "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .ok()
        .flatten()
        .is_some()
    }
}

#[async_trait::async_trait]
impl MlsKeyPackageStorage for GroupKeyPackageStore {
    async fn insert(&self, id: Vec<u8>, key_package_data: Vec<u8>) -> bool {
        log::info!("store insert: group_key_package id={}", hex::encode(&id));
        sqlx::query(
            r#"
                INSERT INTO group_key_packages (id, key_package)
                VALUES (?, ?)
                "#,
        )
        .bind(id)
        .bind(key_package_data)
        .execute(&self.pool)
        .await
        .is_ok()
    }

    async fn delete(&self, id: Vec<u8>) -> bool {
        log::info!("store delete: group_key_package id={}", hex::encode(&id));
        sqlx::query(
            r#"
                DELETE FROM group_key_packages
                WHERE id = ?
                "#,
        )
        .bind(id)
        .execute(&self.pool)
        .await
        .is_ok()
    }

    async fn get(&self, id: Vec<u8>) -> Option<Vec<u8>> {
        log::info!("store select: group_key_package id={}", hex::encode(&id));
        let row = sqlx::query(
            r#"
                SELECT key_package FROM group_key_packages
                WHERE id = ?
                "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .ok()??;

        let key_package: Vec<u8> = row.try_get(0).ok()?;

        Some(key_package)
    }
}

pub use crate::storage::GroupInfo;

/// SQLite [`GroupDepartureStorage`] over `group_departed_members` (migration v8).
#[derive(Clone)]
pub struct GroupDepartureStore {
    pool: SqlitePool,
}

impl GroupDepartureStore {
    pub async fn new(pool: SqlitePool) -> anyhow::Result<Self> {
        crate::db::migrations::run_migrations(&pool).await?;
        Ok(Self { pool })
    }
}

#[async_trait::async_trait]
impl GroupDepartureStorage for GroupDepartureStore {
    async fn record(&self, group_id: u64, departure: &firefly_core::Departure) -> anyhow::Result<()> {
        log::info!(
            "store upsert: group_departed_members group={} user={} re_addable={} epoch={}",
            group_id, departure.username, departure.re_addable, departure.epoch
        );
        sqlx::query(
            r#"
            INSERT INTO group_departed_members (group_id, username, re_addable, epoch)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (group_id, username) DO UPDATE SET
                re_addable = excluded.re_addable,
                epoch = excluded.epoch
            WHERE excluded.epoch >= group_departed_members.epoch
            "#,
        )
        .bind(group_id as i64)
        .bind(&departure.username)
        .bind(departure.re_addable as i64)
        .bind(departure.epoch as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn lookup(&self, group_id: u64, username: &str) -> anyhow::Result<Option<bool>> {
        let row: Option<i64> = sqlx::query_scalar(
            "SELECT re_addable FROM group_departed_members WHERE group_id = ? AND username = ?",
        )
        .bind(group_id as i64)
        .bind(username)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|v| v == 1))
    }
}

#[derive(Clone)]
pub struct GroupInfoStore {
    pool: SqlitePool,
}

impl GroupInfoStore {
    pub async fn new(pool: SqlitePool) -> anyhow::Result<Self> {
        pool.execute(
            r#"
            CREATE TABLE IF NOT EXISTS group_infos(
                id INTEGER PRIMARY KEY NOT NULL,
                group_state_id BLOB NOT NULL,
                name TEXT NOT NULL,
                description TEXT NOT NULL
            )
            "#,
        )
        .await?;

        Ok(Self { pool })
    }

    pub async fn get_all(&self) -> anyhow::Result<Vec<GroupInfo>> {
        log::info!("store select: group_infos all");
        let rows = sqlx::query(
            r#"
                SELECT id, group_state_id, name, description FROM group_infos
                "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let mut infos = Vec::new();
        for row in rows {
            let id: i64 = row.try_get(0)?;
            let identifier: Vec<u8> = row.try_get(1)?;
            let name: String = row.try_get(2)?;
            let description: String = row.try_get(3)?;

            let group = GroupInfo {
                id: id as u64,
                name,
                description,
                identifier,
            };

            infos.push(group);
        }

        Ok(infos)
    }

    pub async fn get(&self, id: u64) -> anyhow::Result<GroupInfo> {
        log::info!("store select: group_info id={}", id);
        let row = sqlx::query(
            r#"
                SELECT id, group_state_id, name, description FROM group_infos WHERE id = ?
                "#,
        )
        .bind(id as i64)
        .fetch_one(&self.pool)
        .await?;

        let id: i64 = row.try_get(0)?;
        let identifier: Vec<u8> = row.try_get(1)?;
        let name: String = row.try_get(2)?;
        let description: String = row.try_get(3)?;

        let group = GroupInfo {
            id: id as u64,
            name,
            description,
            identifier,
        };

        Ok(group)
    }

    pub async fn set(
        &self,
        id: u64,
        name: String,
        description: String,
        group_state_id: Vec<u8>,
    ) -> anyhow::Result<()> {
        log::info!("store insert: group_info id={} name={}", id, name);
        sqlx::query(
            r#"
                INSERT OR REPLACE INTO group_infos (id, name, description, group_state_id) VALUES (?, ?, ?, ?)
                "#,
        )
        .bind(id as i64)
        .bind(name)
        .bind(description)
        .bind(group_state_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn delete(&self, id: u64) -> anyhow::Result<()> {
        log::info!("store delete: group_info id={}", id);
        sqlx::query("DELETE FROM group_infos WHERE id = ?")
            .bind(id as i64)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl GroupInfoStorage for GroupInfoStore {
    async fn get_all(&self) -> anyhow::Result<Vec<GroupInfo>> {
        self.get_all().await
    }
    async fn get(&self, id: u64) -> anyhow::Result<GroupInfo> {
        self.get(id).await
    }
    async fn set(
        &self,
        id: u64,
        name: String,
        description: String,
        group_state_id: Vec<u8>,
    ) -> anyhow::Result<()> {
        self.set(id, name, description, group_state_id).await
    }
    async fn delete(&self, id: u64) -> anyhow::Result<()> {
        self.delete(id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::SqlitePool;
    use std::collections::HashMap;

    async fn setup_test_db() -> SqlitePool {
        SqlitePool::connect(":memory:").await.unwrap()
    }

    #[tokio::test]
    async fn test_group_state_store() {
        let pool = setup_test_db().await;
        let store = GroupStateStore::new(pool).await.unwrap();

        let group_id = vec![1, 2, 3];
        let state_data = vec![4, 5, 6];
        let mut epoch_inserts = HashMap::new();
        epoch_inserts.insert(1, vec![7, 8, 9]);
        let epoch_updates = HashMap::new();

        store
            .set_state(&group_id, &state_data, epoch_inserts, epoch_updates)
            .await
            .unwrap();

        let retrieved_state = store.get_state(&group_id).await.unwrap();
        assert_eq!(retrieved_state, state_data);

        let epoch_state = store.get_epoch_state(&group_id, 1).await.unwrap();
        assert_eq!(epoch_state, vec![7, 8, 9]);

        let max_epoch = store.get_max_epoch_id(&group_id).await.unwrap().unwrap();
        assert_eq!(max_epoch, 1);
    }

    #[tokio::test]
    async fn test_group_key_package_store() {
        let pool = setup_test_db().await;
        let store = GroupKeyPackageStore::new(pool).await.unwrap();

        let id = vec![1, 2, 3];
        let key_package = vec![4, 5, 6];

        assert!(store.insert(id.clone(), key_package.clone()).await);

        let retrieved = store.get(id.clone()).await.unwrap();
        assert_eq!(retrieved, key_package);

        assert!(store.delete(id.clone()).await);
        assert!(store.get(id).await.is_none());
    }

    #[tokio::test]
    async fn test_group_info_store() {
        let pool = setup_test_db().await;
        let store = GroupInfoStore::new(pool).await.unwrap();

        let id = 1;
        let name = "Test Group".to_string();
        let description = "Test Description".to_string();
        let group_state_id = vec![1, 2, 3];

        store
            .set(
                id,
                name.clone(),
                description.clone(),
                group_state_id.clone(),
            )
            .await
            .unwrap();

        let groups = store.get_all().await.unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].name, name);
        assert_eq!(groups[0].description, description);
    }

    #[tokio::test]
    async fn test_group_info_delete() {
        let pool = setup_test_db().await;
        let store = GroupInfoStore::new(pool).await.unwrap();

        let id = 1;
        store
            .set(
                id,
                "Test Group".to_string(),
                "Test Description".to_string(),
                vec![1, 2, 3],
            )
            .await
            .unwrap();

        assert_eq!(store.get_all().await.unwrap().len(), 1);

        store.delete(id).await.unwrap();

        assert_eq!(store.get_all().await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_group_psk_store() {
        let store = GroupPskStore {};
        let result = store.get(vec![1, 2, 3]).await;
        assert!(result.is_none());
    }
}
