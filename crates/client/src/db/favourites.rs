use std::collections::HashMap;
use std::sync::Arc;
use sqlx::{Executor, Row, SqlitePool};

use crate::db::group_messages::{extract_group_message_text, GroupMessage};
use crate::db::group_stores::GroupInfoStore;
use crate::db::messages::{extract_user_message_text, UserMessage};
use crate::group::FfiMlsClient;
use crate::storage::{FavouriteMessage, FavouriteMessageStorage, FavouriteSource};
use crate::utils::get_current_timestamp_millis_since_epoch;

struct MessageReadAccess {
    client: Arc<tokio::sync::OnceCell<Arc<FfiMlsClient>>>,
    groups: GroupInfoStore,
}

#[derive(Clone)]
pub struct FavouriteMessagesStore {
    pool: SqlitePool,
    read_access: Option<Arc<MessageReadAccess>>,
}

pub type FavoriteMessagesStore = FavouriteMessagesStore;

impl FavouriteMessagesStore {
    pub async fn new(pool: SqlitePool) -> anyhow::Result<Self> {
        crate::db::migrations::run_migrations(&pool).await?;

        Ok(Self {
            pool,
            read_access: None,
        })
    }

    pub(crate) fn with_read_access(
        &self,
        client: Arc<tokio::sync::OnceCell<Arc<FfiMlsClient>>>,
        groups: GroupInfoStore,
    ) -> Self {
        Self {
            pool: self.pool.clone(),
            read_access: Some(Arc::new(MessageReadAccess { client, groups })),
        }
    }

    pub async fn can_see_channel(&self, group_id: u64, channel_id: u32) -> anyhow::Result<bool> {
        let Some(access) = &self.read_access else {
            return Ok(true);
        };
        let client = access
            .client
            .get()
            .ok_or_else(|| anyhow::anyhow!("MLS client not initialized"))?;
        let info = access.groups.get(group_id).await?;
        let group = client.load_group(group_id, info.identifier).await?;
        group.can_see_message(channel_id).await
    }

    async fn filter_visible_favourites(
        &self,
        items: Vec<FavouriteMessage>,
    ) -> anyhow::Result<Vec<FavouriteMessage>> {
        if self.read_access.is_none() {
            return Ok(items);
        }
        let mut visible = Vec::with_capacity(items.len());
        let mut channel_perm_cache: HashMap<(u64, u32), bool> = HashMap::new();
        for item in items {
            if item.source == FavouriteSource::Group {
                if let (Some(gid), Some(cid)) = (item.group_id, item.channel_id) {
                    let can_see = match channel_perm_cache.get(&(gid, cid)) {
                        Some(&allowed) => allowed,
                        None => {
                            let allowed = self.can_see_channel(gid, cid).await.unwrap_or(false);
                            channel_perm_cache.insert((gid, cid), allowed);
                            allowed
                        }
                    };
                    if can_see {
                        visible.push(item);
                    }
                } else {
                    visible.push(item);
                }
            } else {
                visible.push(item);
            }
        }
        Ok(visible)
    }

    pub async fn add_user_message(
        &self,
        message: &UserMessage,
        custom_text: Option<String>,
    ) -> anyhow::Result<u64> {
        let text = custom_text.unwrap_or_else(|| extract_user_message_text(&message.message));
        let created_at = get_current_timestamp_millis_since_epoch();
        let fav = FavouriteMessage {
            id: 0,
            source: FavouriteSource::User,
            message_id: message.id,
            other: Some(message.other.clone()),
            group_id: None,
            channel_id: None,
            by: message.other.clone(),
            text,
            message: message.message.clone(),
            message_type: message.message_type,
            epoch: None,
            created_at,
        };
        self.add(fav).await
    }

    pub async fn add_group_message(
        &self,
        message: &GroupMessage,
        custom_text: Option<String>,
    ) -> anyhow::Result<u64> {
        let text = custom_text.unwrap_or_else(|| extract_group_message_text(&message.message));
        let created_at = get_current_timestamp_millis_since_epoch();
        let fav = FavouriteMessage {
            id: 0,
            source: FavouriteSource::Group,
            message_id: message.id,
            other: None,
            group_id: Some(message.group_id),
            channel_id: Some(message.channel_id),
            by: message.by.clone(),
            text,
            message: message.message.clone(),
            message_type: message.message_type,
            epoch: Some(message.epoch),
            created_at,
        };
        self.add(fav).await
    }

    fn row_to_favourite(row: &sqlx::sqlite::SqliteRow) -> anyhow::Result<FavouriteMessage> {
        let source_str: String = row.try_get("source")?;
        let source = if source_str == "user" {
            FavouriteSource::User
        } else {
            FavouriteSource::Group
        };
        let group_id: Option<i64> = row.try_get("group_id").ok();
        let channel_id: Option<i64> = row.try_get("channel_id").ok();
        let other: Option<String> = row.try_get("other").ok();
        let epoch: Option<i64> = row.try_get("epoch").ok();
        let created_at: i64 = row.try_get("created_at")?;

        Ok(FavouriteMessage {
            id: row.try_get::<i64, _>("id")? as u64,
            source,
            message_id: row.try_get::<i64, _>("message_id")? as u64,
            other,
            group_id: group_id.map(|g| g as u64),
            channel_id: channel_id.map(|c| c as u32),
            by: row.try_get("by")?,
            text: row.try_get("text")?,
            message: row.try_get("message")?,
            message_type: row.try_get::<i64, _>("message_type")? as u32,
            epoch: epoch.map(|e| e as u32),
            created_at: created_at as u64,
        })
    }
}

#[async_trait::async_trait]
impl FavouriteMessageStorage for FavouriteMessagesStore {
    async fn add(&self, favourite: FavouriteMessage) -> anyhow::Result<u64> {
        let source_str = match favourite.source {
            FavouriteSource::User => "user",
            FavouriteSource::Group => "group",
        };

        // Insert or return existing ID if already favourited
        let sql = r#"
        INSERT INTO favourite_messages (
            source, message_id, other, group_id, channel_id, by, text, message, message_type, epoch, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT DO UPDATE SET
            text = excluded.text,
            message = excluded.message
        RETURNING id
        "#;

        let row = sqlx::query(sql)
            .bind(source_str)
            .bind(favourite.message_id as i64)
            .bind(favourite.other)
            .bind(favourite.group_id.map(|g| g as i64))
            .bind(favourite.channel_id.map(|c| c as i64))
            .bind(favourite.by)
            .bind(favourite.text)
            .bind(favourite.message)
            .bind(favourite.message_type as i64)
            .bind(favourite.epoch.map(|e| e as i64).unwrap_or(0))
            .bind(favourite.created_at as i64)
            .fetch_one(&self.pool)
            .await?;

        let id: i64 = row.try_get("id")?;
        Ok(id as u64)
    }

    async fn remove_user_favourite(&self, other: &str, message_id: u64) -> anyhow::Result<bool> {
        let res = sqlx::query(
            "DELETE FROM favourite_messages WHERE source = 'user' AND other = ? AND message_id = ?",
        )
        .bind(other)
        .bind(message_id as i64)
        .execute(&self.pool)
        .await?;

        Ok(res.rows_affected() > 0)
    }

    async fn remove_group_favourite(&self, group_id: u64, message_id: u64) -> anyhow::Result<bool> {
        let res = sqlx::query(
            "DELETE FROM favourite_messages WHERE source = 'group' AND group_id = ? AND message_id = ?",
        )
        .bind(group_id as i64)
        .bind(message_id as i64)
        .execute(&self.pool)
        .await?;

        Ok(res.rows_affected() > 0)
    }

    async fn remove_by_id(&self, favourite_id: u64) -> anyhow::Result<bool> {
        let res = sqlx::query("DELETE FROM favourite_messages WHERE id = ?")
            .bind(favourite_id as i64)
            .execute(&self.pool)
            .await?;

        Ok(res.rows_affected() > 0)
    }

    async fn is_user_favourite(&self, other: &str, message_id: u64) -> anyhow::Result<bool> {
        let row = sqlx::query(
            "SELECT 1 FROM favourite_messages WHERE source = 'user' AND other = ? AND message_id = ? LIMIT 1",
        )
        .bind(other)
        .bind(message_id as i64)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.is_some())
    }

    async fn is_group_favourite(&self, group_id: u64, message_id: u64) -> anyhow::Result<bool> {
        let row = sqlx::query(
            "SELECT 1 FROM favourite_messages WHERE source = 'group' AND group_id = ? AND message_id = ? LIMIT 1",
        )
        .bind(group_id as i64)
        .bind(message_id as i64)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.is_some())
    }

    async fn get_by_id(&self, favourite_id: u64) -> anyhow::Result<Option<FavouriteMessage>> {
        let row = sqlx::query("SELECT * FROM favourite_messages WHERE id = ?")
            .bind(favourite_id as i64)
            .fetch_optional(&self.pool)
            .await?;

        if let Some(row) = row {
            let fav = Self::row_to_favourite(&row)?;
            let filtered = self.filter_visible_favourites(vec![fav]).await?;
            Ok(filtered.into_iter().next())
        } else {
            Ok(None)
        }
    }

    async fn get_all(&self, limit: u32, offset: u32) -> anyhow::Result<Vec<FavouriteMessage>> {
        let rows = sqlx::query(
            "SELECT * FROM favourite_messages ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?",
        )
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut items = Vec::with_capacity(rows.len());
        for row in &rows {
            items.push(Self::row_to_favourite(row)?);
        }

        self.filter_visible_favourites(items).await
    }

    async fn get_user_favourites(
        &self,
        other: Option<&str>,
        limit: u32,
        offset: u32,
    ) -> anyhow::Result<Vec<FavouriteMessage>> {
        let (sql, has_other) = if other.is_some() {
            (
                "SELECT * FROM favourite_messages WHERE source = 'user' AND other = ? ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?",
                true,
            )
        } else {
            (
                "SELECT * FROM favourite_messages WHERE source = 'user' ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?",
                false,
            )
        };

        let mut q = sqlx::query(sql);
        if has_other {
            q = q.bind(other.unwrap());
        }
        q = q.bind(limit as i64).bind(offset as i64);

        let rows = q.fetch_all(&self.pool).await?;
        let mut items = Vec::with_capacity(rows.len());
        for row in &rows {
            items.push(Self::row_to_favourite(row)?);
        }

        Ok(items)
    }

    async fn get_group_favourites(
        &self,
        group_id: Option<u64>,
        channel_id: Option<u32>,
        limit: u32,
        offset: u32,
    ) -> anyhow::Result<Vec<FavouriteMessage>> {
        let mut conditions = vec!["source = 'group'".to_string()];
        if group_id.is_some() {
            conditions.push("group_id = ?".to_string());
        }
        if channel_id.is_some() {
            conditions.push("channel_id = ?".to_string());
        }

        let where_clause = conditions.join(" AND ");
        let sql = format!(
            "SELECT * FROM favourite_messages WHERE {where_clause} ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?"
        );

        let mut q = sqlx::query(&sql);
        if let Some(gid) = group_id {
            q = q.bind(gid as i64);
        }
        if let Some(cid) = channel_id {
            q = q.bind(cid as i64);
        }
        q = q.bind(limit as i64).bind(offset as i64);

        let rows = q.fetch_all(&self.pool).await?;
        let mut items = Vec::with_capacity(rows.len());
        for row in &rows {
            items.push(Self::row_to_favourite(row)?);
        }

        self.filter_visible_favourites(items).await
    }

    async fn clear_all(&self) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM favourite_messages")
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::SqlitePoolOptions;

    async fn setup_test_db() -> SqlitePool {
        SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_favourite_store_user_messages() {
        let pool = setup_test_db().await;
        let store = FavouriteMessagesStore::new(pool).await.unwrap();

        let user_msg = UserMessage {
            id: 42,
            other: "alice".to_string(),
            message: b"Important message from Alice".to_vec(),
            sent_by_other: true,
            message_type: 0,
        };

        // Initially not favourite
        assert!(!store.is_user_favourite("alice", 42).await.unwrap());

        // Add to favourites
        let fav_id = store.add_user_message(&user_msg, None).await.unwrap();
        assert!(fav_id > 0);

        // Now is favourite
        assert!(store.is_user_favourite("alice", 42).await.unwrap());
        // Other message or other user is not
        assert!(!store.is_user_favourite("alice", 43).await.unwrap());
        assert!(!store.is_user_favourite("bob", 42).await.unwrap());

        // Retrieve by id
        let item = store.get_by_id(fav_id).await.unwrap().expect("item found");
        assert_eq!(item.source, FavouriteSource::User);
        assert_eq!(item.message_id, 42);
        assert_eq!(item.other.as_deref(), Some("alice"));
        assert_eq!(item.text, "Important message from Alice");

        // Retrieve user favourites
        let user_favs = store.get_user_favourites(Some("alice"), 10, 0).await.unwrap();
        assert_eq!(user_favs.len(), 1);

        // Remove favourite
        let removed = store.remove_user_favourite("alice", 42).await.unwrap();
        assert!(removed);
        assert!(!store.is_user_favourite("alice", 42).await.unwrap());
    }

    #[tokio::test]
    async fn test_favourite_store_group_messages() {
        let pool = setup_test_db().await;
        let store = FavouriteMessagesStore::new(pool).await.unwrap();

        let group_msg = GroupMessage {
            id: 100,
            group_id: 500,
            by: "bob".to_string(),
            message: b"Sprint planning notes".to_vec(),
            channel_id: 2,
            epoch: 1,
            message_type: 0,
        };

        // Add to favourites
        let fav_id = store.add_group_message(&group_msg, None).await.unwrap();
        assert!(fav_id > 0);

        // Check is favourite
        assert!(store.is_group_favourite(500, 100).await.unwrap());
        assert!(!store.is_group_favourite(500, 101).await.unwrap());
        assert!(!store.is_group_favourite(501, 100).await.unwrap());

        // Query by group and channel
        let grp_favs = store.get_group_favourites(Some(500), Some(2), 10, 0).await.unwrap();
        assert_eq!(grp_favs.len(), 1);
        assert_eq!(grp_favs[0].group_id, Some(500));
        assert_eq!(grp_favs[0].channel_id, Some(2));
        assert_eq!(grp_favs[0].text, "Sprint planning notes");

        // Query by wrong channel
        let wrong_chan = store.get_group_favourites(Some(500), Some(3), 10, 0).await.unwrap();
        assert_eq!(wrong_chan.len(), 0);

        // Remove by id
        let removed = store.remove_by_id(fav_id).await.unwrap();
        assert!(removed);
        assert!(!store.is_group_favourite(500, 100).await.unwrap());
    }
}
