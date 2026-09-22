use sqlx::{SqlitePool, prelude::*};

pub const KEY_LAST_RECEIVED_MESSAGE_ID: &str = "last_received_message_id";

pub const KEY_LAST_RECEIVED_GROUP_MESSAGE_ID: &str = "last_received_group_message_id";

pub const KEY_FCM_TOKEN: &str = "fcm_token";

#[derive(Clone)]
pub struct KeyValueStore {
    pool: SqlitePool,
}

impl KeyValueStore {
    pub async fn new(pool: SqlitePool) -> anyhow::Result<Self> {
        crate::db::migrations::run_migrations(&pool).await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS key_value_store (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            "#,
        )
        .execute(&pool)
        .await?;

        crate::receipt_coordinator::ReceiptCoordinator::recover(&pool).await?;
        Ok(Self { pool })
    }

    pub async fn get(&self, key: &str) -> anyhow::Result<String> {
        let row = sqlx::query(
            r#"
            SELECT value FROM key_value_store WHERE key = ?
            "#,
        )
        .bind(key)
        .fetch_one(&self.pool)
        .await?;
        Ok(row.try_get(0)?)
    }

    pub async fn set(&self, key: &str, value: &str) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT OR REPLACE INTO key_value_store (key, value) VALUES (?, ?)
            "#,
        )
        .bind(key)
        .bind(value)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

impl KeyValueStore {
    pub async fn update_last_received_message_id(
        &self,
        last_received_message_id: u64,
    ) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            r#"
            SELECT value FROM key_value_store WHERE key = ?
            "#,
        )
        .bind(KEY_LAST_RECEIVED_MESSAGE_ID)
        .fetch_optional(&mut *tx)
        .await?;

        if let Some(row) = row {
            let value: &str = row.try_get(0)?;
            let value = value.parse::<u64>()?;
            if value < last_received_message_id {
                sqlx::query(
                    r#"
                    UPDATE key_value_store SET value = ? WHERE key = ?
                    "#,
                )
                .bind(last_received_message_id.to_string())
                .bind(KEY_LAST_RECEIVED_MESSAGE_ID)
                .execute(&mut *tx)
                .await?;
            }
        } else {
            sqlx::query(
                r#"
                INSERT INTO key_value_store (key, value) VALUES (?, ?)
                "#,
            )
            .bind(KEY_LAST_RECEIVED_MESSAGE_ID)
            .bind(last_received_message_id.to_string())
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::SqlitePool;

    async fn setup_test_db() -> SqlitePool {
        SqlitePool::connect(":memory:").await.unwrap()
    }

    #[tokio::test]
    async fn test_new_creates_table() {
        let pool = setup_test_db().await;
        let _store = KeyValueStore::new(pool).await.unwrap();
    }

    #[tokio::test]
    async fn test_set_and_get() {
        let pool = setup_test_db().await;
        let store = KeyValueStore::new(pool).await.unwrap();

        store.set("test_key", "test_value").await.unwrap();
        let value = store.get("test_key").await.unwrap();
        assert_eq!(value, "test_value");
    }

    #[tokio::test]
    async fn test_get_nonexistent_key() {
        let pool = setup_test_db().await;
        let store = KeyValueStore::new(pool).await.unwrap();

        let result = store.get("nonexistent").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_set_overwrites_existing() {
        let pool = setup_test_db().await;
        let store = KeyValueStore::new(pool).await.unwrap();

        store.set("key", "value1").await.unwrap();
        store.set("key", "value2").await.unwrap();
        let value = store.get("key").await.unwrap();
        assert_eq!(value, "value2");
    }

    #[tokio::test]
    async fn test_update_last_received_message_id_new() {
        let pool = setup_test_db().await;
        let store = KeyValueStore::new(pool).await.unwrap();

        store.update_last_received_message_id(100).await.unwrap();
        let value = store.get(KEY_LAST_RECEIVED_MESSAGE_ID).await.unwrap();
        assert_eq!(value, "100");
    }

    #[tokio::test]
    async fn test_update_last_received_message_id_higher() {
        let pool = setup_test_db().await;
        let store = KeyValueStore::new(pool).await.unwrap();

        store.update_last_received_message_id(100).await.unwrap();
        store.update_last_received_message_id(200).await.unwrap();
        let value = store.get(KEY_LAST_RECEIVED_MESSAGE_ID).await.unwrap();
        assert_eq!(value, "200");
    }

    #[tokio::test]
    async fn test_update_last_received_message_id_lower() {
        let pool = setup_test_db().await;
        let store = KeyValueStore::new(pool).await.unwrap();

        store.update_last_received_message_id(200).await.unwrap();
        store.update_last_received_message_id(100).await.unwrap();
        let value = store.get(KEY_LAST_RECEIVED_MESSAGE_ID).await.unwrap();
        assert_eq!(value, "200");
    }
}

impl KeyValueStore {
    pub async fn enqueue_snapshot_tail(&self, group: u64, id: &str) -> anyhow::Result<()> {
        anyhow::ensure!(
            group > 0
                && group <= i64::MAX as u64
                && id.len() == 32
                && id
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "Invalid snapshot inbox identity"
        );
        sqlx::query("INSERT INTO group_snapshot_inbox(group_id,snapshot_id) VALUES(?,?) ON CONFLICT DO NOTHING")
            .bind(group as i64).bind(id).execute(&self.pool).await?;
        Ok(())
    }
    pub async fn has_pending_snapshot_tails(&self, group: u64) -> anyhow::Result<bool> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM group_snapshot_inbox WHERE group_id=? AND done=0 LIMIT 1",
        )
        .bind(group as i64)
        .fetch_optional(&self.pool)
        .await?
        .is_some())
    }
    /// Per-row acknowledgements cannot erase a concurrently received capability.
    /// One bad item is deferred, not allowed to stop later items in the batch.
    pub async fn drain_snapshot_tails<F, Fut>(
        &self,
        group: u64,
        now_ms: i64,
        mut process: F,
    ) -> anyhow::Result<usize>
    where
        F: FnMut(String) -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<()>>,
    {
        let ids:Vec<String>=sqlx::query_scalar("SELECT snapshot_id FROM group_snapshot_inbox WHERE group_id=? AND done=0 AND next_attempt<=? ORDER BY next_attempt,snapshot_id LIMIT 8")
            .bind(group as i64).bind(now_ms).fetch_all(&self.pool).await?;
        let mut completed = 0;
        for id in ids {
            match process(id.clone()).await {
                Ok(()) => {
                    sqlx::query("UPDATE group_snapshot_inbox SET done=1,last_error=NULL WHERE group_id=? AND snapshot_id=?")
                        .bind(group as i64).bind(&id).execute(&self.pool).await?;
                    completed += 1;
                }
                Err(_) => {
                    // No keys, plaintext or raw transport errors in the persisted log.
                    sqlx::query("UPDATE group_snapshot_inbox SET attempts=attempts+1,next_attempt=?,last_error='Snapshot retrieval or validation failed' WHERE group_id=? AND snapshot_id=? AND done=0")
                        .bind(now_ms.saturating_add(30_000)).bind(group as i64).bind(&id).execute(&self.pool).await?;
                }
            }
        }
        Ok(completed)
    }
}

#[cfg(test)]
mod snapshot_inbox_tests {
    use super::*;
    #[tokio::test]
    async fn failed_head_does_not_block_good_tail_or_erase_concurrent_arrival() -> anyhow::Result<()>
    {
        let pool = crate::db::setup_pool("sqlite::memory:", 1).await?;
        let store = KeyValueStore::new(pool.clone()).await?;
        let bad = "a".repeat(32);
        let good = "b".repeat(32);
        let late = "c".repeat(32);
        store.enqueue_snapshot_tail(42, &bad).await?;
        store.enqueue_snapshot_tail(42, &good).await?;
        let completed = store
            .drain_snapshot_tails(42, 100, |id| {
                let store = store.clone();
                let bad = bad.clone();
                let late = late.clone();
                async move {
                    store.enqueue_snapshot_tail(42, &late).await?;
                    anyhow::ensure!(id != bad, "unavailable");
                    Ok(())
                }
            })
            .await?;
        assert_eq!(completed, 1);
        assert!(store.has_pending_snapshot_tails(42).await?);
        let next = store
            .drain_snapshot_tails(42, 101, |id| {
                let late = late.clone();
                async move {
                    assert_eq!(id, late);
                    Ok(())
                }
            })
            .await?;
        assert_eq!(
            next, 1,
            "Concurrent enqueue must survive old batch completion"
        );
        assert_eq!(
            store
                .drain_snapshot_tails(42, 29999, |_| async { panic!("backoff was bypassed") })
                .await?,
            0
        );
        drop(store);
        let store = KeyValueStore::new(pool).await?;
        assert_eq!(
            store
                .drain_snapshot_tails(42, 30100, |_| async { Ok(()) })
                .await?,
            1
        );
        store.enqueue_snapshot_tail(42, &good).await?;
        assert!(
            !store.has_pending_snapshot_tails(42).await?,
            "Completed replay must not requeue"
        );
        Ok(())
    }
    #[tokio::test]
    async fn batches_are_bounded_and_account_groups_are_isolated() -> anyhow::Result<()> {
        let store = KeyValueStore::new(crate::db::setup_pool("sqlite::memory:", 1).await?).await?;
        for id in 0..12 {
            store
                .enqueue_snapshot_tail(42, &format!("{id:032x}"))
                .await?;
        }
        store.enqueue_snapshot_tail(43, &"f".repeat(32)).await?;
        assert_eq!(
            store
                .drain_snapshot_tails(42, 1, |_| async { Ok(()) })
                .await?,
            8
        );
        assert!(store.has_pending_snapshot_tails(42).await?);
        assert_eq!(
            store
                .drain_snapshot_tails(42, 2, |_| async { Ok(()) })
                .await?,
            4
        );
        assert!(!store.has_pending_snapshot_tails(42).await?);
        assert!(store.has_pending_snapshot_tails(43).await?);
        assert!(store.enqueue_snapshot_tail(42, "../escape").await.is_err());
        Ok(())
    }
}

#[cfg(test)]
mod snapshot_inbox_recovery_tests {
    use super::*;

    #[tokio::test]
    async fn snapshot_cancel_after_effect_before_ack_replays_after_disk_reopen()
    -> anyhow::Result<()> {
        struct Scratch(std::path::PathBuf);
        impl Drop for Scratch {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let dir = Scratch(std::env::temp_dir().join(format!(
            "firefly-inbox-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        )));
        std::fs::create_dir_all(&dir.0)?;
        let path = dir.0.join("account.sqlite");
        let path = path
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("temp path UTF8"))?;
        let pool = crate::db::setup_pool_from_path(path, 1).await?;
        let store = KeyValueStore::new(pool.clone()).await?;
        let id = "a".repeat(32);
        store.enqueue_snapshot_tail(42, &id).await?;
        let (tx, rx) = tokio::sync::oneshot::channel();
        let worker_store = store.clone();
        let worker = tokio::spawn(async move {
            let mut tx = Some(tx);
            worker_store
                .drain_snapshot_tails(42, 0, |_| {
                    let store = worker_store.clone();
                    let tx = tx.take();
                    async move {
                        // Simulate the idempotent import committing before inbox acknowledgement.
                        store.set("test-imported", "original bytes").await?;
                        if let Some(tx) = tx {
                            let _ = tx.send(());
                        }
                        std::future::pending::<anyhow::Result<()>>().await
                    }
                })
                .await
        });
        rx.await?;
        worker.abort();
        assert!(worker.await.unwrap_err().is_cancelled());
        drop(store);
        pool.close().await;
        drop(pool);
        let pool = crate::db::setup_pool_from_path(path, 1).await?;
        let store = KeyValueStore::new(pool.clone()).await?;
        assert!(store.has_pending_snapshot_tails(42).await?);
        assert_eq!(
            store
                .drain_snapshot_tails(42, 1, |_| {
                    let store = store.clone();
                    async move {
                        assert_eq!(store.get("test-imported").await?, "original bytes");
                        store.set("test-imported", "original bytes").await
                    }
                })
                .await?,
            1
        );
        assert!(!store.has_pending_snapshot_tails(42).await?);
        drop(store);
        pool.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn snapshot_v5_upgrade_preserves_v4_records_and_restarts_partial_ddl()
    -> anyhow::Result<()> {
        let pool = crate::db::setup_pool("sqlite::memory:", 1).await?;
        crate::db::migrations::run_migrations(&pool).await?;
        sqlx::query("INSERT INTO group_messages(id,group_id,by,message,channel_id,epoch,message_type,text) VALUES(10,42,'alice',X'0102',1,5,0,'original')").execute(&pool).await?;
        // Simulate a v4 database whose first v5 DDL statement committed before restart.
        sqlx::query("DELETE FROM _schema_migrations WHERE version=5")
            .execute(&pool)
            .await?;
        sqlx::query("DROP INDEX group_snapshot_inbox_due")
            .execute(&pool)
            .await?;
        crate::db::migrations::run_migrations(&pool).await?;
        crate::db::migrations::run_migrations(&pool).await?;
        let original: (Vec<u8>, String, i64) = sqlx::query_as(
            "SELECT message,by,epoch FROM group_messages WHERE group_id=42 AND id=10",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(original, (vec![1, 2], "alice".into(), 5));
        let marker: i64 =
            sqlx::query_scalar("SELECT version FROM _schema_migrations WHERE version=5")
                .fetch_one(&pool)
                .await?;
        assert_eq!(marker, 5);
        let store = KeyValueStore::new(pool).await?;
        store.enqueue_snapshot_tail(42, &"a".repeat(32)).await?;
        assert_eq!(
            store
                .drain_snapshot_tails(42, 0, |_| async { Ok(()) })
                .await?,
            1
        );
        Ok(())
    }
}
