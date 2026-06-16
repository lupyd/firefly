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
