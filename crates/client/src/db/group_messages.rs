use sqlx::SqlitePool;
use sqlx::prelude::*;
use std::sync::Arc;

struct MessageReadAccess {
    client: Arc<tokio::sync::OnceCell<Arc<crate::group::FfiMlsClient>>>,
    groups: super::group_stores::GroupInfoStore,
}

#[derive(Clone, Debug, sqlx::FromRow, serde::Serialize, serde::Deserialize)]
pub struct GroupMessage {
    pub id: u64,
    pub group_id: u64,
    pub by: String,
    pub message: Vec<u8>,
    pub channel_id: u32,
    pub epoch: u32,
    #[serde(default)]
    pub message_type: u32,
}

#[derive(Clone)]
pub struct GroupMessagesStore {
    pool: SqlitePool,
    read_access: Option<Arc<MessageReadAccess>>,
    // Clones used by concurrent receive/re-add/upload tasks share this boundary.
    pin_writes: Arc<tokio::sync::Mutex<()>>,
}

impl GroupMessagesStore {
    pub async fn new(pool: SqlitePool) -> anyhow::Result<Self> {
        pool.execute(
            r#"
        CREATE TABLE IF NOT EXISTS group_messages (
            id INTEGER NOT NULL,
            group_id INTEGER NOT NULL,
            by TEXT NOT NULL,
            message BLOB NOT NULL,
            channel_id INTEGER NOT NULL,
            epoch INTEGER NOT NULL DEFAULT 0,
            message_type INTEGER NOT NULL DEFAULT 0,

            PRIMARY KEY (group_id, id)
        );

        CREATE INDEX IF NOT EXISTS group_messages_type_idx ON group_messages (group_id, message_type);
        "#,
        )
        .await?;

        // Migration for existing tables without message_type column
        let _ = pool
            .execute(
                "ALTER TABLE group_messages ADD COLUMN message_type INTEGER NOT NULL DEFAULT 0",
            )
            .await;

        Ok(Self {
            pool,
            read_access: None,
            pin_writes: Default::default(),
        })
    }

    /// Client-facing view. Internal synchronization retains a raw store so
    /// denying plaintext access cannot hide protocol cursors or stop commits.
    pub(crate) fn with_read_access(
        &self,
        client: Arc<tokio::sync::OnceCell<Arc<crate::group::FfiMlsClient>>>,
        groups: super::group_stores::GroupInfoStore,
    ) -> Self {
        Self {
            pool: self.pool.clone(),
            read_access: Some(Arc::new(MessageReadAccess { client, groups })),
            pin_writes: self.pin_writes.clone(),
        }
    }

    async fn visible_messages(
        &self,
        messages: Vec<GroupMessage>,
    ) -> anyhow::Result<Vec<GroupMessage>> {
        let Some(access) = &self.read_access else {
            return Ok(messages);
        };
        let client = access
            .client
            .get()
            .ok_or_else(|| anyhow::anyhow!("MLS client not initialized"))?;
        let mut visible = Vec::new();
        for message in messages {
            let info = access.groups.get(message.group_id).await?;
            let group = client.load_group(message.group_id, info.identifier).await?;
            if group.can_see_message(message.channel_id).await? {
                visible.push(message);
            }
        }
        Ok(visible)
    }

    pub async fn update_cursor(&self, id: u64, group_id: u64, epoch: u32) -> anyhow::Result<()> {
        self.add(id, group_id, 0, epoch, "", "".as_bytes(), 0).await
    }

    pub async fn add(
        &self,
        id: u64,
        group_id: u64,
        channel_id: u32,
        epoch: u32,
        by: &str,
        message: &[u8],
        message_type: u32,
    ) -> anyhow::Result<()> {
        let _pin_write = self.pin_writes.lock().await;
        let pinned = message_type & firefly_protos::MESSAGE_TYPE_PINNED != 0;
        let mut duplicates = Vec::new();
        let mut newest = id;
        if pinned {
            for existing in self.pinned_messages_unfiltered_for_group(group_id).await?.into_iter()
                .filter(|m| m.channel_id == channel_id
                    && crate::storage::is_same_pinned_group_message(&m.message, message)) {
                newest = newest.max(existing.id);
                duplicates.push(existing);
            }
        }
        log::info!(
            "store insert: group_message id={} group_id={}, channel_id={} by={} message_type={}",
            id,
            group_id,
            channel_id,
            by,
            message_type
        );
        sqlx::query(
            r#"
        INSERT INTO group_messages (id, group_id, by, message, channel_id, epoch, message_type)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (group_id, id) DO UPDATE SET
            by = CASE WHEN excluded.by != '' THEN excluded.by ELSE group_messages.by END,
            message = CASE WHEN length(excluded.message) > 0 THEN excluded.message ELSE group_messages.message END,
            channel_id = CASE WHEN excluded.channel_id != 0 THEN excluded.channel_id ELSE group_messages.channel_id END,
            epoch = CASE WHEN excluded.epoch != 0 THEN excluded.epoch ELSE group_messages.epoch END,
            message_type = CASE WHEN excluded.message_type != 0 THEN excluded.message_type ELSE group_messages.message_type END
        "#,
        )
        .bind(id as i64)
        .bind(group_id as i64)
        .bind(by)
        .bind(message)
        .bind(channel_id)
        .bind(epoch)
        .bind(message_type as i64)
        .execute(&self.pool)
        .await?;

        // All insertion paths, including background re-adds and replayed/older
        // ciphertext, converge on the same highest-id pinned copy.
        for existing in duplicates.into_iter().filter(|m| m.id != newest) {
            self.update_message_type_unlocked(group_id, existing.id,
                existing.message_type & !firefly_protos::MESSAGE_TYPE_PINNED).await?;
        }
        if pinned && id != newest {
            self.update_message_type_unlocked(group_id, id,
                message_type & !firefly_protos::MESSAGE_TYPE_PINNED).await?;
        }

        Ok(())
    }

    pub async fn get(
        &self,
        group_id: u64,
        start_before: u64,
        limit: u32,
    ) -> anyhow::Result<Vec<GroupMessage>> {
        let rows = sqlx::query(
            r#"
        SELECT id, by, message, channel_id, group_id, epoch, message_type
        FROM group_messages
        WHERE group_id = ? AND id < ?
        ORDER BY id DESC LIMIT ?
        "#,
        )
        .bind(group_id as i64)
        .bind(start_before as i64)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;

        self.visible_messages(
            rows.iter()
                .map(GroupMessage::from_row)
                .collect::<Result<Vec<_>, _>>()?,
        )
        .await
    }

    pub async fn get_pinned_messages(&self, group_id: u64) -> anyhow::Result<Vec<GroupMessage>> {
        let _pin_write = self.pin_writes.lock().await;
        self.visible_messages(self.pinned_messages_unfiltered_for_group(group_id).await?).await
    }

    async fn pinned_messages_unfiltered_for_group(&self, group_id: u64) -> anyhow::Result<Vec<GroupMessage>> {
        let rows = sqlx::query(
            r#"
        SELECT id, by, message, channel_id, group_id, epoch, message_type
        FROM group_messages
        WHERE group_id = ? AND (message_type & 1) != 0
        ORDER BY id ASC
        "#,
        )
        .bind(group_id as i64)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.iter()
                .map(GroupMessage::from_row)
                .collect::<Result<Vec<_>, _>>()?)
    }

    pub async fn update_message_type(
        &self,
        group_id: u64,
        id: u64,
        message_type: u32,
    ) -> anyhow::Result<()> {
        let _pin_write = self.pin_writes.lock().await;
        self.update_message_type_unlocked(group_id, id, message_type).await
    }

    async fn update_message_type_unlocked(&self, group_id: u64, id: u64, message_type: u32) -> anyhow::Result<()> {
        sqlx::query("UPDATE group_messages SET message_type = ? WHERE group_id = ? AND id = ?")
            .bind(message_type as i64)
            .bind(group_id as i64)
            .bind(id as i64)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn get_all_last_messages(&self) -> anyhow::Result<Vec<GroupMessage>> {
        let rows = sqlx::query(
            r#"
SELECT gm.group_id, gm.id, gm.by, gm.message, gm.channel_id, gm.epoch, gm.message_type
FROM group_messages gm
JOIN (
    SELECT group_id, MAX(id) AS max_id
    FROM group_messages
    GROUP BY group_id
) last
ON gm.group_id = last.group_id
AND gm.id = last.max_id;

        "#,
        )
        .fetch_all(&self.pool)
        .await?;
        self.visible_messages(
            rows.iter()
                .map(GroupMessage::from_row)
                .collect::<Result<Vec<_>, _>>()?,
        )
        .await
    }

    pub async fn get_last_message_of_group(&self, group_id: u64) -> anyhow::Result<GroupMessage> {
        let row = sqlx::query("SELECT group_id, id, by, message, channel_id, epoch, message_type FROM group_messages WHERE group_id = ? ORDER BY id DESC LIMIT 1").bind(group_id as i64).fetch_one(&self.pool).await?;
        self.visible_messages(vec![GroupMessage::from_row(&row)?])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("SeeMessage permission required"))
    }

    pub async fn delete_by_group_id(&self, group_id: u64) -> anyhow::Result<()> {
        log::info!("store delete_by_group_id: group_id={}", group_id);
        sqlx::query("DELETE FROM group_messages WHERE group_id = ?")
            .bind(group_id as i64)
            .execute(&self.pool)
            .await?;

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
        let _store = GroupMessagesStore::new(pool).await.unwrap();
    }

    #[tokio::test]
    async fn test_add_message() {
        let pool = setup_test_db().await;
        let store = GroupMessagesStore::new(pool).await.unwrap();

        let result = store.add(1, 100, 1, 1, "user1", &[1, 2, 3], 0).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_pinned_messages() {
        let pool = setup_test_db().await;
        let store = GroupMessagesStore::new(pool).await.unwrap();

        store.add(1, 100, 1, 1, "user1", &[1], 0).await.unwrap();
        store.add(2, 100, 1, 1, "user2", &[2], 1).await.unwrap();

        let pinned = store.get_pinned_messages(100).await.unwrap();
        assert_eq!(pinned.len(), 1);
        assert_eq!(pinned[0].id, 2);
        assert_eq!(pinned[0].message_type, 1);

        store.update_message_type(100, 1, 1).await.unwrap();
        let pinned_updated = store.get_pinned_messages(100).await.unwrap();
        assert_eq!(pinned_updated.len(), 2);
    }

    #[tokio::test]
    async fn test_get_empty_group() {
        let pool = setup_test_db().await;
        let store = GroupMessagesStore::new(pool).await.unwrap();

        let messages = store.get(999, 100, 10).await.unwrap();
        assert_eq!(messages.len(), 0);
    }

    #[tokio::test]
    async fn test_get_messages_ordering() {
        let pool = setup_test_db().await;
        let store = GroupMessagesStore::new(pool).await.unwrap();

        store.add(1, 100, 1, 1, "user1", &[1], 0).await.unwrap();
        store.add(3, 100, 1, 1, "user2", &[3], 0).await.unwrap();
        store.add(2, 100, 1, 1, "user3", &[2], 0).await.unwrap();

        let messages = store.get(100, 10, 10).await.unwrap();
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].id, 3);
        assert_eq!(messages[1].id, 2);
        assert_eq!(messages[2].id, 1);
    }

    #[tokio::test]
    async fn test_get_messages_limit() {
        let pool = setup_test_db().await;
        let store = GroupMessagesStore::new(pool).await.unwrap();

        for i in 1..=5 {
            store
                .add(i, 100, 1, 1, "user", &[i as u8], 0)
                .await
                .unwrap();
        }

        let messages = store.get(100, 10, 2).await.unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].id, 5);
        assert_eq!(messages[1].id, 4);
    }

    #[tokio::test]
    async fn test_get_messages_start_before() {
        let pool = setup_test_db().await;
        let store = GroupMessagesStore::new(pool).await.unwrap();

        store.add(1, 100, 1, 1, "user1", &[1], 0).await.unwrap();
        store.add(2, 100, 1, 1, "user2", &[2], 0).await.unwrap();
        store.add(3, 100, 1, 1, "user3", &[3], 0).await.unwrap();

        let messages = store.get(100, 3, 10).await.unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].id, 2);
        assert_eq!(messages[1].id, 1);
    }

    #[tokio::test]
    async fn test_get_messages_different_groups() {
        let pool = setup_test_db().await;
        let store = GroupMessagesStore::new(pool).await.unwrap();

        store.add(1, 100, 1, 1, "user1", &[1], 0).await.unwrap();
        store.add(2, 200, 1, 1, "user2", &[2], 0).await.unwrap();

        let messages_100 = store.get(100, 10, 10).await.unwrap();
        let messages_200 = store.get(200, 10, 10).await.unwrap();

        assert_eq!(messages_100.len(), 1);
        assert_eq!(messages_200.len(), 1);
        assert_eq!(messages_100[0].group_id, 100);
        assert_eq!(messages_200[0].group_id, 200);
    }

    #[tokio::test]
    async fn test_get_all_last_messages_empty() {
        let pool = setup_test_db().await;
        let store = GroupMessagesStore::new(pool).await.unwrap();

        let messages = store.get_all_last_messages().await.unwrap();
        assert_eq!(messages.len(), 0);
    }

    #[tokio::test]
    async fn test_get_all_last_messages_single_group() {
        let pool = setup_test_db().await;
        let store = GroupMessagesStore::new(pool).await.unwrap();

        store.add(1, 100, 1, 1, "user1", &[1], 0).await.unwrap();
        store.add(2, 100, 1, 1, "user2", &[2], 0).await.unwrap();

        let messages = store.get_all_last_messages().await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].id, 2);
        assert_eq!(messages[0].group_id, 100);
    }

    #[tokio::test]
    async fn test_get_all_last_messages_multiple_groups() {
        let pool = setup_test_db().await;
        let store = GroupMessagesStore::new(pool).await.unwrap();

        store.add(1, 100, 1, 1, "user1", &[1], 0).await.unwrap();
        store.add(3, 100, 1, 1, "user2", &[3], 0).await.unwrap();
        store.add(2, 200, 1, 1, "user3", &[2], 0).await.unwrap();
        store.add(4, 300, 1, 1, "user4", &[4], 0).await.unwrap();

        let messages = store.get_all_last_messages().await.unwrap();
        assert_eq!(messages.len(), 3);

        let group_100_msg = messages.iter().find(|m| m.group_id == 100).unwrap();
        let group_200_msg = messages.iter().find(|m| m.group_id == 200).unwrap();
        let group_300_msg = messages.iter().find(|m| m.group_id == 300).unwrap();

        assert_eq!(group_100_msg.id, 3);
        assert_eq!(group_200_msg.id, 2);
        assert_eq!(group_300_msg.id, 4);
    }

    #[tokio::test]
    async fn test_message_data_integrity() {
        let pool = setup_test_db().await;
        let store = GroupMessagesStore::new(pool).await.unwrap();

        let message_data = vec![0xFF, 0x00, 0xAB, 0xCD];
        let username = "test_user";

        store
            .add(1, 100, 1, 1, username, &message_data, 0)
            .await
            .unwrap();

        let messages = store.get(100, 10, 1).await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].by, username);
        assert_eq!(messages[0].message, message_data);
    }

    #[tokio::test]
    async fn test_message_data_integrity2() {
        let pool = setup_test_db().await;
        let store = GroupMessagesStore::new(pool).await.unwrap();

        store.add(1, 100, 1, 1, "user1", &[1], 0).await.unwrap();
        store.add(3, 100, 1, 1, "user2", &[3], 0).await.unwrap();
        store.add(2, 100, 1, 1, "user3", &[2], 0).await.unwrap();

        let last_message = store.get_last_message_of_group(100).await.unwrap();
        assert_eq!(last_message.id, 3);
        assert_eq!(last_message.group_id, 100);
        assert_eq!(last_message.by, "user2");
    }

    #[tokio::test]
    async fn test_delete_by_group_id() {
        let pool = setup_test_db().await;
        let store = GroupMessagesStore::new(pool).await.unwrap();

        store.add(1, 100, 1, 1, "user1", &[1], 0).await.unwrap();
        store.add(2, 100, 1, 1, "user2", &[2], 0).await.unwrap();
        store.add(3, 200, 1, 1, "user3", &[3], 0).await.unwrap();

        assert_eq!(store.get(100, 10, 10).await.unwrap().len(), 2);
        assert_eq!(store.get(200, 10, 10).await.unwrap().len(), 1);

        store.delete_by_group_id(100).await.unwrap();

        assert_eq!(store.get(100, 10, 10).await.unwrap().len(), 0);
        assert_eq!(store.get(200, 10, 10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_update_cursor_advances_last_message() {
        let pool = setup_test_db().await;
        let store = GroupMessagesStore::new(pool).await.unwrap();

        store.add(1, 100, 1, 1, "user1", &[1], 0).await.unwrap();
        assert_eq!(store.get_last_message_of_group(100).await.unwrap().id, 1);

        // Advance cursor via commit/readd id without full message body
        store.update_cursor(10, 100, 2).await.unwrap();
        assert_eq!(store.get_last_message_of_group(100).await.unwrap().id, 10);
    }
}
