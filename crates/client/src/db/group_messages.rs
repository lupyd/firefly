use sqlx::SqlitePool;
use sqlx::prelude::*;

#[derive(Clone, Debug, sqlx::FromRow, serde::Serialize, serde::Deserialize)]
pub struct GroupMessage {
    pub id: u64,
    pub group_id: u64,
    pub by: String,
    pub message: Vec<u8>,
    pub channel_id: u32,
    pub epoch: u32,
}

#[derive(Clone)]
pub struct GroupMessagesStore {
    pool: SqlitePool,
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

            PRIMARY KEY (group_id, id)
        )
        "#,
        )
        .await?;

        Ok(Self { pool })
    }

    pub async fn update_cursor(&self, id: u64, group_id: u64, epoch: u32) -> anyhow::Result<()> {
        self.add(id, group_id, 0, epoch, "", "".as_bytes()).await
    }

    pub async fn add(
        &self,
        id: u64,
        group_id: u64,
        channel_id: u32,
        epoch: u32,
        by: &str,
        message: &[u8],
    ) -> anyhow::Result<()> {
        log::info!(
            "store insert: group_message id={} group_id={}, channel_id={} by={}",
            id,
            group_id,
            channel_id,
            by
        );
        sqlx::query(
            r#"
        INSERT INTO group_messages (id, group_id, by, message, channel_id, epoch)
        VALUES (?, ?, ?, ?, ?, ?)
        "#,
        )
        .bind(id as i64)
        .bind(group_id as i64)
        .bind(by)
        .bind(message)
        .bind(channel_id)
        .bind(epoch)
        .execute(&self.pool)
        .await?;

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
        SELECT id, by, message, channel_id, group_id, epoch
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

        Ok(rows
            .iter()
            .map(GroupMessage::from_row)
            .collect::<Result<Vec<_>, _>>()?)
    }

    pub async fn get_all_last_messages(&self) -> anyhow::Result<Vec<GroupMessage>> {
        let rows = sqlx::query(
            r#"
SELECT gm.group_id, gm.id, gm.by, gm.message, gm.channel_id, gm.epoch
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
        Ok(rows
            .iter()
            .map(GroupMessage::from_row)
            .collect::<Result<Vec<_>, _>>()?)
    }

    pub async fn get_last_message_of_group(&self, group_id: u64) -> anyhow::Result<GroupMessage> {
        let row = sqlx::query("SELECT group_id, id, by, message, channel_id, epoch FROM group_messages WHERE group_id = ? ORDER BY id DESC LIMIT 1").bind(group_id as i64).fetch_one(&self.pool).await?;
        Ok(GroupMessage::from_row(&row)?)
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

        let result = store.add(1, 100, 1, 1, "user1", &[1, 2, 3]).await;
        assert!(result.is_ok());
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

        store.add(1, 100, 1, 1, "user1", &[1]).await.unwrap();
        store.add(3, 100, 1, 1, "user2", &[3]).await.unwrap();
        store.add(2, 100, 1, 1, "user3", &[2]).await.unwrap();

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
            store.add(i, 100, 1, 1, "user", &[i as u8]).await.unwrap();
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

        store.add(1, 100, 1, 1, "user1", &[1]).await.unwrap();
        store.add(2, 100, 1, 1, "user2", &[2]).await.unwrap();
        store.add(3, 100, 1, 1, "user3", &[3]).await.unwrap();

        let messages = store.get(100, 3, 10).await.unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].id, 2);
        assert_eq!(messages[1].id, 1);
    }

    #[tokio::test]
    async fn test_get_messages_different_groups() {
        let pool = setup_test_db().await;
        let store = GroupMessagesStore::new(pool).await.unwrap();

        store.add(1, 100, 1, 1, "user1", &[1]).await.unwrap();
        store.add(2, 200, 1, 1, "user2", &[2]).await.unwrap();

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

        store.add(1, 100, 1, 1, "user1", &[1]).await.unwrap();
        store.add(2, 100, 1, 1, "user2", &[2]).await.unwrap();

        let messages = store.get_all_last_messages().await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].id, 2);
        assert_eq!(messages[0].group_id, 100);
    }

    #[tokio::test]
    async fn test_get_all_last_messages_multiple_groups() {
        let pool = setup_test_db().await;
        let store = GroupMessagesStore::new(pool).await.unwrap();

        store.add(1, 100, 1, 1, "user1", &[1]).await.unwrap();
        store.add(3, 100, 1, 1, "user2", &[3]).await.unwrap();
        store.add(2, 200, 1, 1, "user3", &[2]).await.unwrap();
        store.add(4, 300, 1, 1, "user4", &[4]).await.unwrap();

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
            .add(1, 100, 1, 1, username, &message_data)
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

        store.add(1, 100, 1, 1, "user1", &[1]).await.unwrap();
        store.add(3, 100, 1, 1, "user2", &[3]).await.unwrap();
        store.add(2, 100, 1, 1, "user3", &[2]).await.unwrap();

        let last_message = store.get_last_message_of_group(100).await.unwrap();
        assert_eq!(last_message.id, 3);
        assert_eq!(last_message.group_id, 100);
        assert_eq!(last_message.by, "user2");
    }

    #[tokio::test]
    async fn test_delete_by_group_id() {
        let pool = setup_test_db().await;
        let store = GroupMessagesStore::new(pool).await.unwrap();

        store.add(1, 100, 1, 1, "user1", &[1]).await.unwrap();
        store.add(2, 100, 1, 1, "user2", &[2]).await.unwrap();
        store.add(3, 200, 1, 1, "user3", &[3]).await.unwrap();

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

        store.add(1, 100, 1, 1, "user1", &[1]).await.unwrap();
        assert_eq!(store.get_last_message_of_group(100).await.unwrap().id, 1);

        // Advance cursor via commit/readd id without full message body
        store.update_cursor(10, 100, 2).await.unwrap();
        assert_eq!(store.get_last_message_of_group(100).await.unwrap().id, 10);
    }
}
