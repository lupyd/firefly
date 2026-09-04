use sqlx::{SqlitePool, prelude::*};

use crate::db::setup_pool_from_path;

pub struct UserMessage {
    pub id: u64,
    pub other: String,
    pub message: Vec<u8>,
    pub sent_by_other: bool,
}

pub struct LastMessageAndUnreadCount {
    pub count: u32,
    pub message: UserMessage,
}

pub struct MessagesStore {
    pool: SqlitePool,
}

impl MessagesStore {
    pub async fn new(pool: SqlitePool) -> anyhow::Result<Self> {
        let sql = r#"
CREATE TABLE IF NOT EXISTS user_messages (
    id INTEGER NOT NULL,
    other TEXT NOT NULL,
    sent_by_other BOOLEAN NOT NULL,
    message BLOB NOT NULL
);

CREATE INDEX IF NOT EXISTS user_messages_other_idx ON user_messages (other, id);

CREATE TABLE IF NOT EXISTS last_seen_user_timestamps (
    other TEXT NOT NULL PRIMARY KEY,
    id INTEGER NOT NULL
);
"#;

        let mut conn = pool.acquire().await?;
        for stmt in sql.split(';') {
            conn.execute(stmt).await?;
        }

        Ok(Self { pool })
    }
}

impl MessagesStore {
    pub async fn from_path(path: String) -> anyhow::Result<Self> {
        let pool = setup_pool_from_path(&path, 5).await?;

        Self::new(pool).await
    }

    pub async fn get_last_messages_of(
        &self,
        other: &str,
        before: i64,
        limit: i64,
    ) -> anyhow::Result<Vec<UserMessage>> {
        let rows = sqlx::query(
            "SELECT other, message, sent_by_other, id FROM user_messages WHERE other = ? AND id < ? ORDER BY id DESC LIMIT ?",
        )
        .bind(other)
        .bind(before)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        let mut messages = Vec::<UserMessage>::with_capacity(rows.len());

        for row in rows {
            messages.push(UserMessage {
                id: row.try_get("id")?,
                other: row.try_get("other")?,
                message: row.try_get("message")?,
                sent_by_other: row.try_get("sent_by_other")?,
            });
        }

        Ok(messages)
    }

    pub async fn get_last_message_from_all_conversations(
        &self,
    ) -> anyhow::Result<Vec<LastMessageAndUnreadCount>> {
        let q = r#"WITH stats AS (
                    SELECT
                        um.other,
                        MAX(um.id) AS last_id,
                        SUM(CASE WHEN um.id > COALESCE(ls.id, -1)
                            THEN 1 ELSE 0 END) AS unread_count
                    FROM user_messages AS um
                    LEFT JOIN last_seen_user_timestamps AS ls
                        ON ls.other = um.other
                    GROUP BY um.other
                )
                SELECT
                    s.other,
                    s.unread_count,
                    m.id,
                    m.sent_by_other,
                    m.message
                FROM stats AS s
                JOIN user_messages AS m
                    ON m.other = s.other
                    AND m.id = s.last_id;"#;

        let rows = sqlx::query(q).fetch_all(&self.pool).await?;
        let mut messages = Vec::<LastMessageAndUnreadCount>::with_capacity(rows.len());

        for row in rows {
            let message = UserMessage {
                id: row.try_get("id")?,
                other: row.try_get("other")?,
                message: row.try_get("message")?,
                sent_by_other: row.try_get("sent_by_other")?,
            };

            let count: i64 = row.try_get("unread_count")?;
            messages.push(LastMessageAndUnreadCount {
                count: count as u32,
                message,
            });
        }

        Ok(messages)
    }

    pub async fn insert_user_message(&self, row: UserMessage) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO user_messages (id, other, message, sent_by_other) VALUES (?, ?, ?, ?)",
        )
        .bind(row.id as i64)
        .bind(row.other)
        .bind(row.message)
        .bind(row.sent_by_other)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn has_user_message(&self, id: u64) -> anyhow::Result<bool> {
        let row = sqlx::query("SELECT 1 FROM user_messages WHERE id = ? LIMIT 1")
            .bind(id as i64)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.is_some())
    }

    pub async fn mark_as_read_until(&self, other: &str, id: i64) -> anyhow::Result<()> {
        let q = "INSERT OR REPLACE INTO last_seen_user_timestamps (other, id) VALUES (?, ?)";
        sqlx::query(q)
            .bind(other)
            .bind(id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn delete_user_message(&self, other: &str, id: u64) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM user_messages WHERE other = ? AND id = ?")
            .bind(other)
            .bind(id as i64)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn delete_user_conversation(&self, other: &str) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM user_messages WHERE other = ?")
            .bind(other)
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM last_seen_user_timestamps WHERE other = ?")
            .bind(other)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn update_user_message_text(
        &self,
        other: &str,
        id: u64,
        message: Vec<u8>,
    ) -> anyhow::Result<()> {
        sqlx::query("UPDATE user_messages SET message = ? WHERE other = ? AND id = ?")
            .bind(message)
            .bind(other)
            .bind(id as i64)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::db::setup_pool;

    use super::*;

    // const DB_URI: &str = "sqlite:file:/tmp/user_messages.db?mode=rwc";
    const DB_URI: &str = ":memory:";

    #[tokio::test]
    async fn test_new() {
        let pool = setup_pool(DB_URI, 1).await.unwrap();
        let store = MessagesStore::new(pool).await;
        assert!(store.is_ok());
    }

    #[tokio::test]
    async fn test_insert_and_get_messages() {
        let pool = setup_pool(DB_URI, 1).await.unwrap();
        let store = MessagesStore::new(pool).await.unwrap();

        let msg = UserMessage {
            id: 0,
            other: "alice".to_string(),
            message: vec![1, 2, 3],
            sent_by_other: false,
        };

        store.insert_user_message(msg).await.unwrap();

        let messages = store
            .get_last_messages_of("alice", i64::MAX, 10)
            .await
            .unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].other, "alice");
        assert_eq!(messages[0].message, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_get_last_message_from_all_conversations() {
        let pool = setup_pool(DB_URI, 1).await.unwrap();
        let store = MessagesStore::new(pool).await.unwrap();

        store
            .insert_user_message(UserMessage {
                id: 0,
                other: "alice".to_string(),
                message: vec![1],
                sent_by_other: true,
            })
            .await
            .unwrap();

        store
            .insert_user_message(UserMessage {
                id: 0,
                other: "bob".to_string(),
                message: vec![2],
                sent_by_other: false,
            })
            .await
            .unwrap();

        let results = store
            .get_last_message_from_all_conversations()
            .await
            .unwrap();
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn test_mark_as_read() {
        let pool = setup_pool(DB_URI, 1).await.unwrap();
        let store = MessagesStore::new(pool).await.unwrap();

        store
            .insert_user_message(UserMessage {
                id: 0,
                other: "alice".to_string(),
                message: vec![1],
                sent_by_other: true,
            })
            .await
            .unwrap();

        let result = store.mark_as_read_until("alice", 1).await;
        assert!(result.is_ok());
    }
}
