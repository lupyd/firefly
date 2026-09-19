use sqlx::{SqlitePool, prelude::*};

use crate::db::setup_pool_from_path;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct UserMessage {
    pub id: u64,
    pub other: String,
    pub message: Vec<u8>,
    pub sent_by_other: bool,
    #[serde(default)]
    pub message_type: u32,
}

pub struct LastMessageAndUnreadCount {
    pub count: u32,
    pub message: UserMessage,
}

#[derive(Clone)]
pub struct MessagesStore {
    pool: SqlitePool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct UserMessageSearchResult {
    pub message: UserMessage,
    pub text: String,
    pub snippet: String,
    pub score: f64,
}

pub fn extract_user_message_text(message: &[u8]) -> String {
    if let Ok(inner) = firefly_protos::deserialize_proto::<firefly_protos::firefly::UserMessageInner>(message) {
        match inner.message {
            firefly_protos::firefly::mod_UserMessageInner::OneOfmessage::messagePayload(p) => {
                return p.text.to_string();
            }
            firefly_protos::firefly::mod_UserMessageInner::OneOfmessage::plainText(b) => {
                return String::from_utf8_lossy(&b).to_string();
            }
            _ => {}
        }
    }
    String::from_utf8_lossy(message).to_string()
}

impl MessagesStore {
    pub async fn new(pool: SqlitePool) -> anyhow::Result<Self> {
        crate::db::migrations::run_migrations(&pool).await?;
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
            "SELECT other, message, sent_by_other, id, message_type FROM user_messages WHERE other = ? AND id < ? ORDER BY id DESC LIMIT ?",
        )
        .bind(other)
        .bind(before)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        let mut messages = Vec::<UserMessage>::with_capacity(rows.len());

        for row in rows {
            let message_type: i64 = row.try_get("message_type").unwrap_or(0);
            messages.push(UserMessage {
                id: row.try_get("id")?,
                other: row.try_get("other")?,
                message: row.try_get("message")?,
                sent_by_other: row.try_get("sent_by_other")?,
                message_type: message_type as u32,
            });
        }

        Ok(messages)
    }

    pub async fn get_pinned_messages_of(&self, other: &str) -> anyhow::Result<Vec<UserMessage>> {
        let rows = sqlx::query(
            "SELECT other, message, sent_by_other, id, message_type FROM user_messages WHERE other = ? AND (message_type & 1) != 0 ORDER BY id ASC",
        )
        .bind(other)
        .fetch_all(&self.pool)
        .await?;
        let mut messages = Vec::<UserMessage>::with_capacity(rows.len());

        for row in rows {
            let message_type: i64 = row.try_get("message_type").unwrap_or(0);
            messages.push(UserMessage {
                id: row.try_get("id")?,
                other: row.try_get("other")?,
                message: row.try_get("message")?,
                sent_by_other: row.try_get("sent_by_other")?,
                message_type: message_type as u32,
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
                    m.message,
                    m.message_type
                FROM stats AS s
                JOIN user_messages AS m
                    ON m.other = s.other
                    AND m.id = s.last_id;"#;

        let rows = sqlx::query(q).fetch_all(&self.pool).await?;
        let mut messages = Vec::<LastMessageAndUnreadCount>::with_capacity(rows.len());

        for row in rows {
            let message_type: i64 = row.try_get("message_type").unwrap_or(0);
            let message = UserMessage {
                id: row.try_get("id")?,
                other: row.try_get("other")?,
                message: row.try_get("message")?,
                sent_by_other: row.try_get("sent_by_other")?,
                message_type: message_type as u32,
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
        let text = extract_user_message_text(&row.message);
        sqlx::query(
            "INSERT INTO user_messages (id, other, message, sent_by_other, message_type, text) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(row.id as i64)
        .bind(row.other)
        .bind(row.message)
        .bind(row.sent_by_other)
        .bind(row.message_type as i64)
        .bind(text)
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
        let text = extract_user_message_text(&message);
        sqlx::query("UPDATE user_messages SET message = ?, text = ? WHERE other = ? AND id = ?")
            .bind(message)
            .bind(text)
            .bind(other)
            .bind(id as i64)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn search(
        &self,
        query: &str,
        other: Option<&str>,
        limit: u32,
        offset: u32,
    ) -> anyhow::Result<Vec<UserMessageSearchResult>> {
        let fts_query = crate::db::search::sanitize_fts5_query(query);
        if fts_query.is_empty() {
            return Ok(Vec::new());
        }

        let (sql, has_other) = if other.is_some() {
            (
                r#"
                SELECT
                    um.id, um.other, um.sent_by_other, um.message, um.message_type, um.text,
                    snippet(user_messages_fts, 0, '<b>', '</b>', '...', 10) AS match_snippet,
                    fts.rank AS rank_score
                FROM user_messages_fts fts
                JOIN user_messages um ON um.rowid = fts.rowid
                WHERE user_messages_fts MATCH ? AND um.other = ?
                ORDER BY rank_score ASC
                LIMIT ? OFFSET ?
                "#,
                true,
            )
        } else {
            (
                r#"
                SELECT
                    um.id, um.other, um.sent_by_other, um.message, um.message_type, um.text,
                    snippet(user_messages_fts, 0, '<b>', '</b>', '...', 10) AS match_snippet,
                    fts.rank AS rank_score
                FROM user_messages_fts fts
                JOIN user_messages um ON um.rowid = fts.rowid
                WHERE user_messages_fts MATCH ?
                ORDER BY rank_score ASC
                LIMIT ? OFFSET ?
                "#,
                false,
            )
        };

        let mut q = sqlx::query(sql).bind(&fts_query);
        if has_other {
            q = q.bind(other.unwrap());
        }
        q = q.bind(limit as i64).bind(offset as i64);

        let rows = q.fetch_all(&self.pool).await?;
        let mut results = Vec::with_capacity(rows.len());
        for row in rows {
            let message_type: i64 = row.try_get("message_type").unwrap_or(0);
            let message = UserMessage {
                id: row.try_get("id")?,
                other: row.try_get("other")?,
                message: row.try_get("message")?,
                sent_by_other: row.try_get("sent_by_other")?,
                message_type: message_type as u32,
            };
            results.push(UserMessageSearchResult {
                message,
                text: row.try_get("text")?,
                snippet: row.try_get("match_snippet")?,
                score: row.try_get("rank_score").unwrap_or(0.0),
            });
        }
        Ok(results)
    }

    pub async fn update_message_type(
        &self,
        other: &str,
        id: u64,
        message_type: u32,
    ) -> anyhow::Result<()> {
        sqlx::query("UPDATE user_messages SET message_type = ? WHERE other = ? AND id = ?")
            .bind(message_type as i64)
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
            message_type: 0,
        };

        store.insert_user_message(msg).await.unwrap();

        let messages = store
            .get_last_messages_of("alice", i64::MAX, 10)
            .await
            .unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].other, "alice");
        assert_eq!(messages[0].message, vec![1, 2, 3]);
        assert_eq!(messages[0].message_type, 0);
    }

    #[tokio::test]
    async fn test_pinned_messages() {
        let pool = setup_pool(DB_URI, 1).await.unwrap();
        let store = MessagesStore::new(pool).await.unwrap();

        store
            .insert_user_message(UserMessage {
                id: 1,
                other: "alice".to_string(),
                message: vec![1],
                sent_by_other: true,
                message_type: 0,
            })
            .await
            .unwrap();

        store
            .insert_user_message(UserMessage {
                id: 2,
                other: "alice".to_string(),
                message: vec![2],
                sent_by_other: false,
                message_type: 1, // Pinned bitflag
            })
            .await
            .unwrap();

        let pinned = store.get_pinned_messages_of("alice").await.unwrap();
        assert_eq!(pinned.len(), 1);
        assert_eq!(pinned[0].id, 2);
        assert_eq!(pinned[0].message_type, 1);

        // Update message 1 to pinned
        store.update_message_type("alice", 1, 1).await.unwrap();
        let pinned_updated = store.get_pinned_messages_of("alice").await.unwrap();
        assert_eq!(pinned_updated.len(), 2);
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
                message_type: 0,
            })
            .await
            .unwrap();

        store
            .insert_user_message(UserMessage {
                id: 0,
                other: "bob".to_string(),
                message: vec![2],
                sent_by_other: false,
                message_type: 0,
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
                message_type: 0,
            })
            .await
            .unwrap();

        let result = store.mark_as_read_until("alice", 1).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_search_user_messages() {
        let pool = setup_pool(DB_URI, 1).await.unwrap();
        let store = MessagesStore::new(pool).await.unwrap();

        store
            .insert_user_message(UserMessage {
                id: 1,
                other: "alice".to_string(),
                message: b"Hello Alice, are you joining the sprint planning meeting today?".to_vec(),
                sent_by_other: true,
                message_type: 0,
            })
            .await
            .unwrap();

        store
            .insert_user_message(UserMessage {
                id: 2,
                other: "bob".to_string(),
                message: b"Hey Bob, here is the sprint review doc.".to_vec(),
                sent_by_other: false,
                message_type: 0,
            })
            .await
            .unwrap();

        store
            .insert_user_message(UserMessage {
                id: 3,
                other: "alice".to_string(),
                message: b"Thanks for the update, see you in the review!".to_vec(),
                sent_by_other: true,
                message_type: 0,
            })
            .await
            .unwrap();

        // Search across all users
        let res_all = store.search("sprint", None, 10, 0).await.unwrap();
        assert_eq!(res_all.len(), 2);

        // Search scoped to alice
        let res_alice = store.search("sprint", Some("alice"), 10, 0).await.unwrap();
        assert_eq!(res_alice.len(), 1);
        assert_eq!(res_alice[0].message.id, 1);
        assert!(res_alice[0].snippet.contains("<b>sprint</b>"));

        // Search scoped to bob
        let res_bob = store.search("sprint", Some("bob"), 10, 0).await.unwrap();
        assert_eq!(res_bob.len(), 1);
        assert_eq!(res_bob[0].message.id, 2);

        // Search with prefix
        let res_prefix = store.search("plan", None, 10, 0).await.unwrap();
        assert_eq!(res_prefix.len(), 1);
        assert_eq!(res_prefix[0].message.id, 1);

        // Search with special chars / punctuation (sanitized safely)
        let res_punct = store.search("meeting? today!", None, 10, 0).await.unwrap();
        assert_eq!(res_punct.len(), 1);
        assert_eq!(res_punct[0].message.id, 1);

        // Limit & offset pagination
        let page1 = store.search("sprint", None, 1, 0).await.unwrap();
        assert_eq!(page1.len(), 1);
        let page2 = store.search("sprint", None, 1, 1).await.unwrap();
        assert_eq!(page2.len(), 1);
        assert_ne!(page1[0].message.id, page2[0].message.id);
    }
}
