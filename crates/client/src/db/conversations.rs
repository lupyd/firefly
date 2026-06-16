
use sqlx::SqlitePool;
use sqlx::prelude::*;

#[derive(Default)]
pub struct ConversationSettings {
    inner: u64,
}

impl ConversationSettings {
    pub fn new(settings: u64) -> Self {
        Self { inner: settings }
    }
}

#[derive(Clone)]
pub struct ConversationStore {
    pool: SqlitePool,
}

impl ConversationStore {
    pub async fn new(pool: SqlitePool) -> anyhow::Result<Self> {
        let mut db = pool.acquire().await?;

        db.execute(
            r#"
        CREATE TABLE IF NOT EXISTS conversations (
            username TEXT NOT NULL PRIMARY KEY,
            settings INTEGER NOT NULL
        )
        "#,
        )
        .await?;

        Ok(Self { pool })
    }

    pub async fn get_conversation(
        &self,
        username: &str,
    ) -> anyhow::Result<Option<ConversationSettings>> {
        let row = sqlx::query(
            r#"
            SELECT settings
            FROM conversations
            WHERE username = ?
            "#,
        )
        .bind(username)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = row {
            let settings = row.try_get(0)?;
            return Ok(Some(ConversationSettings::new(settings)));
        } else {
            return Ok(None);
        }
    }

    pub async fn set_conversation(
        &self,
        username: &str,
        settings: ConversationSettings,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT OR REPLACE INTO conversations (username, settings)
            VALUES (?, ?)
            "#,
        )
        .bind(username)
        .bind(settings.inner as u32)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
