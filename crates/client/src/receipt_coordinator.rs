use crate::sql::{Row, SqlitePool};

pub struct ReceiptCoordinator;

impl ReceiptCoordinator {
    pub async fn init(pool: &SqlitePool) -> anyhow::Result<()> {
        crate::sql::query(
            r#"
            CREATE TABLE IF NOT EXISTS key_value_store (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
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
            "#,
        )
        .execute(pool)
        .await?;
        Self::recover(pool).await?;
        Ok(())
    }

    pub async fn stage_receipt(
        pool: &SqlitePool,
        group_id: &[u8],
        epoch: u64,
        key: &str,
        value: &str,
    ) -> anyhow::Result<i64> {
        Self::init(pool).await?;
        let now = crate::utils::get_current_timestamp_millis_since_epoch() as i64;
        let res = crate::sql::query(
            "INSERT INTO receipt_journal (group_id, epoch, capability_key, capability_value, state, created_at) VALUES (?, ?, ?, ?, 'staged', ?)"
        )
        .bind(group_id)
        .bind(epoch as i64)
        .bind(key)
        .bind(value)
        .bind(now)
        .execute(pool)
        .await?;
        Ok(res.last_insert_rowid())
    }

    pub async fn commit_receipt(
        pool: &SqlitePool,
        journal_id: i64,
        key: &str,
        value: &str,
    ) -> anyhow::Result<()> {
        let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
        crate::sql::query("INSERT OR REPLACE INTO key_value_store (key, value) VALUES (?, ?)")
            .bind(key)
            .bind(value)
            .execute(&mut *tx)
            .await?;
        crate::sql::query("UPDATE receipt_journal SET state = 'committed' WHERE id = ?")
            .bind(journal_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn recover(pool: &SqlitePool) -> anyhow::Result<usize> {
        let table_exists: Option<i64> = crate::sql::query_scalar(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='receipt_journal'"
        )
        .fetch_optional(pool)
        .await?;
        if table_exists.is_none() {
            return Ok(0);
        }

        let rows = crate::sql::query(
            "SELECT id, capability_key, capability_value FROM receipt_journal WHERE state = 'staged'"
        )
        .fetch_all(pool)
        .await?;

        let count = rows.len();
        for row in rows {
            let id: i64 = row.try_get("id")?;
            let key: Option<String> = row.try_get("capability_key").ok();
            let val: Option<String> = row.try_get("capability_value").ok();
            if let (Some(k), Some(v)) = (key, val) {
                let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
                crate::sql::query("INSERT OR REPLACE INTO key_value_store (key, value) VALUES (?, ?)")
                    .bind(&k)
                    .bind(&v)
                    .execute(&mut *tx)
                    .await?;
                crate::sql::query("UPDATE receipt_journal SET state = 'committed' WHERE id = ?")
                    .bind(id)
                    .execute(&mut *tx)
                    .await?;
                tx.commit().await?;
            }
        }
        Ok(count)
    }
}
