use sqlx::{Row, SqlitePool};
use crate::utils::get_current_timestamp_millis_since_epoch;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct GroupHistoryChunkKeyRecord {
    pub group_id: u64,
    pub start_msg_id: u64,
    pub end_msg_id: u64,
    pub key: Vec<u8>,
    pub nonce: Vec<u8>,
    pub created_at: i64,
}

#[derive(Clone)]
pub struct HistoryKeysStore {
    pool: SqlitePool,
}

impl HistoryKeysStore {
    pub async fn new(pool: SqlitePool) -> anyhow::Result<Self> {
        crate::db::migrations::run_migrations(&pool).await?;
        Ok(Self { pool })
    }


    pub async fn save_verified_key(&self, group: u64, start: u64, end: u64, hash: &[u8], key: &[u8], nonce: &[u8]) -> anyhow::Result<()> {
        anyhow::ensure!(group > 0 && start > 0 && end >= start && end <= i64::MAX as u64 && hash.len() == 32 && key.len() == 32 && nonce.len() == 12, "Invalid history key");
        sqlx::query("INSERT OR IGNORE INTO group_history_keys_v3(group_id,start_msg_id,end_msg_id,hash,key,nonce) VALUES(?,?,?,?,?,?)")
            .bind(group as i64).bind(start as i64).bind(end as i64).bind(hash).bind(key).bind(nonce).execute(&self.pool).await?;
        Ok(())
    }
    pub async fn verified_material(&self, group: u64, start: u64, end: u64, hash: &[u8]) -> anyhow::Result<Option<(Vec<u8>,Vec<u8>)>> {
        let row=sqlx::query("SELECT key,nonce FROM group_history_keys_v3 WHERE group_id=? AND start_msg_id=? AND end_msg_id=? AND hash=?")
            .bind(group as i64).bind(start as i64).bind(end as i64).bind(hash).fetch_optional(&self.pool).await?;
        row.map(|r| Ok((r.try_get(0)?,r.try_get(1)?))).transpose()
    }
    pub async fn verified_key(&self, group: u64, start: u64, end: u64, hash: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(sqlx::query_scalar("SELECT key FROM group_history_keys_v3 WHERE group_id=? AND start_msg_id=? AND end_msg_id=? AND hash=?")
            .bind(group as i64).bind(start as i64).bind(end as i64).bind(hash).fetch_optional(&self.pool).await?)
    }
    pub async fn is_imported(&self, group: u64, chunk: u64, hash: &[u8]) -> anyhow::Result<bool> {
        Ok(sqlx::query_scalar::<_,i64>("SELECT 1 FROM group_history_imported_chunks WHERE group_id=? AND chunk_id=? AND hash=?")
            .bind(group as i64).bind(chunk as i64).bind(hash).fetch_optional(&self.pool).await?.is_some())
    }
    pub async fn save_chunk_key(
        &self,
        group_id: u64,
        start_msg_id: u64,
        end_msg_id: u64,
        key: &[u8],
        nonce: &[u8],
    ) -> anyhow::Result<()> {
        let now = get_current_timestamp_millis_since_epoch() as i64;
        sqlx::query(
            r#"
            INSERT INTO group_history_chunk_keys (group_id, start_msg_id, end_msg_id, key, nonce, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (group_id, start_msg_id, end_msg_id) DO UPDATE SET
                key = excluded.key,
                nonce = excluded.nonce
            "#,
        )
        .bind(group_id as i64)
        .bind(start_msg_id as i64)
        .bind(end_msg_id as i64)
        .bind(key)
        .bind(nonce)
        .bind(now)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn get_chunk_key(
        &self,
        group_id: u64,
        start_msg_id: u64,
        end_msg_id: u64,
    ) -> anyhow::Result<Option<(Vec<u8>, Vec<u8>)>> {
        let row = sqlx::query(
            r#"
            SELECT key, nonce FROM group_history_chunk_keys
            WHERE group_id = ? AND start_msg_id = ? AND end_msg_id = ?
            "#,
        )
        .bind(group_id as i64)
        .bind(start_msg_id as i64)
        .bind(end_msg_id as i64)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| {
            let key: Vec<u8> = r.get("key");
            let nonce: Vec<u8> = r.get("nonce");
            (key, nonce)
        }))
    }

    pub async fn get_all_keys_for_group(
        &self,
        group_id: u64,
    ) -> anyhow::Result<Vec<GroupHistoryChunkKeyRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT group_id, start_msg_id, end_msg_id, key, nonce, created_at
            FROM group_history_chunk_keys
            WHERE group_id = ?
            ORDER BY start_msg_id ASC
            "#,
        )
        .bind(group_id as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut results = Vec::with_capacity(rows.len());
        for row in rows {
            let gid: i64 = row.get("group_id");
            let start: i64 = row.get("start_msg_id");
            let end: i64 = row.get("end_msg_id");
            let key: Vec<u8> = row.get("key");
            let nonce: Vec<u8> = row.get("nonce");
            let created: i64 = row.get("created_at");

            results.push(GroupHistoryChunkKeyRecord {
                group_id: gid as u64,
                start_msg_id: start as u64,
                end_msg_id: end as u64,
                key,
                nonce,
                created_at: created,
            });
        }

        Ok(results)
    }

    pub async fn record_disapproval(
        &self,
        group_id: u64,
        chunk_id: u64,
        reason: &str,
    ) -> anyhow::Result<()> {
        let now = get_current_timestamp_millis_since_epoch() as i64;
        sqlx::query(
            r#"
            INSERT INTO group_history_disapprovals (group_id, chunk_id, reason, disapproved_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (group_id, chunk_id) DO UPDATE SET
                reason = excluded.reason,
                disapproved_at = excluded.disapproved_at
            "#,
        )
        .bind(group_id as i64)
        .bind(chunk_id as i64)
        .bind(reason)
        .bind(now)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn is_chunk_disapproved(
        &self,
        group_id: u64,
        chunk_id: u64,
    ) -> anyhow::Result<bool> {
        let row = sqlx::query(
            "SELECT 1 FROM group_history_disapprovals WHERE group_id = ? AND chunk_id = ? LIMIT 1"
        )
        .bind(group_id as i64)
        .bind(chunk_id as i64)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::setup_pool;

    #[tokio::test]
    async fn test_history_keys_store_crud() -> anyhow::Result<()> {
        let pool = setup_pool("sqlite::memory:", 1).await?;
        let store = HistoryKeysStore::new(pool).await?;

        // 1. Initially no key
        let key_opt = store.get_chunk_key(100, 1, 50).await?;
        assert!(key_opt.is_none());

        // 2. Save key
        let key_bytes = vec![1u8; 32];
        let nonce_bytes = vec![2u8; 12];
        store.save_chunk_key(100, 1, 50, &key_bytes, &nonce_bytes).await?;

        // 3. Retrieve key
        let retrieved = store.get_chunk_key(100, 1, 50).await?;
        assert!(retrieved.is_some());
        let (k, n) = retrieved.unwrap();
        assert_eq!(k, key_bytes);
        assert_eq!(n, nonce_bytes);

        // 4. Save second key in group 100
        store.save_chunk_key(100, 51, 100, &[3u8; 32], &[4u8; 12]).await?;
        let all_keys = store.get_all_keys_for_group(100).await?;
        assert_eq!(all_keys.len(), 2);
        assert_eq!(all_keys[0].start_msg_id, 1);
        assert_eq!(all_keys[1].start_msg_id, 51);

        // 5. Test disapprovals
        assert!(!store.is_chunk_disapproved(100, 999).await?);
        store.record_disapproval(100, 999, "hash mismatch").await?;
        assert!(store.is_chunk_disapproved(100, 999).await?);

        Ok(())
    }
}
