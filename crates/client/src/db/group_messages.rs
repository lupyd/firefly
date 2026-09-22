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

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct GroupMessageSearchResult {
    pub message: GroupMessage,
    pub text: String,
    pub snippet: String,
    pub score: f64,
}

pub fn extract_group_message_text(message: &[u8]) -> String {
    if let Ok(inner) = firefly_protos::deserialize_proto::<firefly_protos::firefly::GroupMessageInner>(message) {
        if let firefly_protos::firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(p) = inner.message {
            return p.text.to_string();
        }
    }
    String::from_utf8_lossy(message).to_string()
}

impl GroupMessagesStore {
    pub async fn new(pool: SqlitePool) -> anyhow::Result<Self> {
        crate::db::migrations::run_migrations(&pool).await?;

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

    pub async fn visible_messages(
        &self,
        messages: Vec<GroupMessage>,
    ) -> anyhow::Result<Vec<GroupMessage>> {
        let messages: Vec<GroupMessage> = messages
            .into_iter()
            .filter(|m| m.message_type & firefly_protos::MESSAGE_TYPE_HIDDEN == 0)
            .collect();

        let Some(access) = &self.read_access else {
            return Ok(messages);
        };
        let client = access
            .client
            .get()
            .ok_or_else(|| anyhow::anyhow!("MLS client not initialized"))?;
        let mut visible = Vec::new();
        let mut channel_perm_cache: std::collections::HashMap<(u64, u32), bool> =
            std::collections::HashMap::new();
        for message in messages {
            let can_see = match channel_perm_cache.get(&(message.group_id, message.channel_id)) {
                Some(&allowed) => allowed,
                None => {
                    let allowed = match access.groups.get(message.group_id).await {
                        Ok(info) => match client.load_group(message.group_id, info.identifier).await {
                            Ok(group) => group.can_see_message(message.channel_id).await.unwrap_or(false),
                            Err(_) => false,
                        },
                        Err(_) => false,
                    };
                    channel_perm_cache.insert((message.group_id, message.channel_id), allowed);
                    allowed
                }
            };
            if can_see {
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
        if let Ok(inner) = firefly_protos::deserialize_proto::<firefly_protos::firefly::GroupMessageInner>(message) {
            if let firefly_protos::firefly::mod_GroupMessageInner::OneOfmessage::pinUpdate(update) = inner.message {
                anyhow::ensure!(message_type == firefly_protos::MESSAGE_TYPE_HIDDEN && update.message_id > 0 && update.message_id < id && inner.channelId == channel_id, "Invalid shared pin event");
                let mut tx = self.pool.begin().await?;
                sqlx::query("INSERT INTO group_pin_updates(group_id,message_id,channel_id,event_id,pinned) VALUES(?,?,?,?,?) ON CONFLICT(group_id,message_id,channel_id) DO UPDATE SET event_id=excluded.event_id,pinned=excluded.pinned WHERE excluded.event_id>group_pin_updates.event_id")
                    .bind(group_id as i64).bind(update.message_id as i64).bind(channel_id as i64).bind(id as i64).bind(update.pinned).execute(&mut *tx).await?;
                Self::apply_pin_state(&mut tx, group_id, update.message_id).await?;
                tx.commit().await?;
            }
        }
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
        let text = extract_group_message_text(message);
        sqlx::query(
            r#"
        INSERT INTO group_messages (id, group_id, by, message, channel_id, epoch, message_type, text)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (group_id, id) DO UPDATE SET
            by = CASE WHEN excluded.by != '' THEN excluded.by ELSE group_messages.by END,
            message = CASE WHEN length(excluded.message) > 0 THEN excluded.message ELSE group_messages.message END,
            channel_id = CASE WHEN excluded.channel_id != 0 THEN excluded.channel_id ELSE group_messages.channel_id END,
            epoch = CASE WHEN excluded.epoch != 0 THEN excluded.epoch ELSE group_messages.epoch END,
            message_type = CASE WHEN excluded.message_type != 0 THEN excluded.message_type ELSE group_messages.message_type END,
            text = CASE WHEN excluded.text != '' THEN excluded.text ELSE group_messages.text END
        "#,
        )
        .bind(id as i64)
        .bind(group_id as i64)
        .bind(by)
        .bind(message)
        .bind(channel_id)
        .bind(epoch)
        .bind(message_type as i64)
        .bind(text)
        .execute(&self.pool)
        .await?;

        {
            let mut tx = self.pool.begin().await?;
            Self::apply_pin_state(&mut tx, group_id, id).await?;
            tx.commit().await?;
        }

        // A subsequently authenticated live receipt is independent evidence.
        sqlx::query("DELETE FROM group_history_imports WHERE group_id=? AND message_id=?").bind(group_id as i64).bind(id as i64).execute(&self.pool).await?;

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


    async fn apply_pin_state(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>, group: u64, message: u64) -> anyhow::Result<()> {
        sqlx::query("UPDATE group_messages SET message_type=(message_type & ~1) | (SELECT pinned FROM group_pin_updates p WHERE p.group_id=group_messages.group_id AND p.message_id=group_messages.id AND p.channel_id=group_messages.channel_id) WHERE group_id=? AND id=? AND (message_type & 2)=0 AND EXISTS(SELECT 1 FROM group_pin_updates p WHERE p.group_id=group_messages.group_id AND p.message_id=group_messages.id AND p.channel_id=group_messages.channel_id)")
            .bind(group as i64).bind(message as i64).execute(&mut **tx).await?;
        Ok(())
    }

    pub async fn get_message(&self, group: u64, id: u64) -> anyhow::Result<Option<GroupMessage>> {
        let row=sqlx::query("SELECT id,by,message,channel_id,group_id,epoch,message_type FROM group_messages WHERE group_id=? AND id=?")
            .bind(group as i64).bind(id as i64).fetch_optional(&self.pool).await?;
        let Some(row)=row else {return Ok(None)};
        Ok(self.visible_messages(vec![GroupMessage::from_row(&row)?]).await?.pop())
    }

    pub async fn get_channel_page(&self, group_id: u64, channel_id: u32, before: u64, limit: u32) -> anyhow::Result<Vec<GroupMessage>> {
        anyhow::ensure!(limit > 0 && limit <= 101, "Invalid page size");
        if !self.can_see_channel(group_id, channel_id).await? { return Ok(vec![]); }
        let rows = sqlx::query("SELECT id,by,message,channel_id,group_id,epoch,message_type FROM group_messages WHERE group_id=? AND channel_id=? AND id<? AND (message_type & ?) = 0 ORDER BY id DESC LIMIT ?")
            .bind(group_id as i64).bind(channel_id as i64).bind(before as i64).bind(firefly_protos::MESSAGE_TYPE_HIDDEN as i64).bind(limit as i64).fetch_all(&self.pool).await?;
        self.visible_messages(rows.iter().map(GroupMessage::from_row).collect::<Result<Vec<_>,_>>()?).await
    }
    pub async fn authenticated_history_range(&self, group: u64, start: u64, end: u64, limit: u32) -> anyhow::Result<Vec<GroupMessage>> {
        let rows = sqlx::query("SELECT m.id,m.by,m.message,m.channel_id,m.group_id,m.epoch,m.message_type FROM group_messages m WHERE m.group_id=? AND m.id>=? AND m.id<=? AND m.by!='' AND (m.message_type & ?) = 0 AND NOT EXISTS (SELECT 1 FROM group_history_imports h WHERE h.group_id=m.group_id AND h.message_id=m.id) ORDER BY m.id LIMIT ?")
            .bind(group as i64).bind(start as i64).bind(end as i64).bind(firefly_protos::MESSAGE_TYPE_HIDDEN as i64).bind(limit as i64).fetch_all(&self.pool).await?;
        rows.iter().map(GroupMessage::from_row).collect::<Result<Vec<_>,_>>().map_err(Into::into)
    }
    /// Only independently received messages may substantiate a peer vote.
    pub async fn authenticated_history_ids(&self, group: u64, ids: &[u64]) -> anyhow::Result<Vec<GroupMessage>> {
        anyhow::ensure!(!ids.is_empty() && ids.len() <= crate::history::DEFAULT_CHUNK_SIZE, "Invalid history evidence count");
        let mut query = sqlx::QueryBuilder::<sqlx::Sqlite>::new("SELECT m.id,m.by,m.message,m.channel_id,m.group_id,m.epoch,m.message_type FROM group_messages m WHERE m.group_id=");
        query.push_bind(group as i64).push(" AND m.by!='' AND NOT EXISTS (SELECT 1 FROM group_history_imports h WHERE h.group_id=m.group_id AND h.message_id=m.id) AND m.id IN (");
        let mut values = query.separated(",");
        for id in ids { values.push_bind(*id as i64); }
        values.push_unseparated(") ORDER BY m.id");
        let rows = query.build().fetch_all(&self.pool).await?;
        rows.iter().map(GroupMessage::from_row).collect::<Result<Vec<_>,_>>().map_err(Into::into)
    }
    pub async fn import_verified_history(&self, group: u64, chunk: u64, hash: &[u8], records: &[GroupMessage]) -> anyhow::Result<usize> {
        let _lock = self.pin_writes.lock().await;
        let mut tx = self.pool.begin().await?;
        let mut imported = 0;
        for r in records {
            anyhow::ensure!(r.group_id == group, "Foreign history record");
            let changed = sqlx::query("INSERT INTO group_messages(id,group_id,by,message,channel_id,epoch,message_type,text) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(group_id,id) DO UPDATE SET by=excluded.by,message=excluded.message,channel_id=excluded.channel_id,epoch=excluded.epoch,message_type=excluded.message_type,text=excluded.text WHERE EXISTS(SELECT 1 FROM group_history_imports h WHERE h.group_id=group_messages.group_id AND h.message_id=group_messages.id AND h.chunk_id<>0) AND ?<>0")
                .bind(r.id as i64).bind(group as i64).bind(&r.by).bind(&r.message).bind(r.channel_id as i64).bind(r.epoch as i64).bind(r.message_type as i64).bind(extract_group_message_text(&r.message)).bind(chunk as i64).execute(&mut *tx).await?.rows_affected();
            if changed > 0 {
                sqlx::query("INSERT INTO group_history_imports(group_id,message_id,chunk_id) VALUES(?,?,?) ON CONFLICT(group_id,message_id) DO UPDATE SET chunk_id=excluded.chunk_id").bind(group as i64).bind(r.id as i64).bind(chunk as i64).execute(&mut *tx).await?;
                imported += 1;
            }
        }
        for record in records { Self::apply_pin_state(&mut tx, group, record.id).await?; }
        sqlx::query("INSERT OR IGNORE INTO group_history_imported_chunks(group_id,chunk_id,hash) VALUES(?,?,?)").bind(group as i64).bind(chunk as i64).bind(hash).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(imported)
    }
    pub async fn purge_imported_chunk(&self, group: u64, chunk: u64) -> anyhow::Result<usize> {
        let _lock = self.pin_writes.lock().await;
        let mut tx = self.pool.begin().await?;
        let deleted = sqlx::query(
            "DELETE FROM group_messages WHERE group_id = ? AND id IN (SELECT message_id FROM group_history_imports WHERE group_id = ? AND chunk_id = ?)"
        )
        .bind(group as i64)
        .bind(group as i64)
        .bind(chunk as i64)
        .execute(&mut *tx)
        .await?
        .rows_affected() as usize;

        sqlx::query("DELETE FROM group_history_imports WHERE group_id = ? AND chunk_id = ?")
            .bind(group as i64)
            .bind(chunk as i64)
            .execute(&mut *tx)
            .await?;

        sqlx::query("DELETE FROM group_history_imported_chunks WHERE group_id = ? AND chunk_id = ?")
            .bind(group as i64)
            .bind(chunk as i64)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(deleted)
    }
    pub async fn get_range(
        &self,
        group_id: u64,
        start_id: u64,
        end_id: u64,
    ) -> anyhow::Result<Vec<GroupMessage>> {
        let rows = sqlx::query(
            r#"
            SELECT id, by, message, channel_id, group_id, epoch, message_type
            FROM group_messages
            WHERE group_id = ? AND id >= ? AND id <= ?
            ORDER BY id ASC
            "#,
        )
        .bind(group_id as i64)
        .bind(start_id as i64)
        .bind(end_id as i64)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(GroupMessage::from_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }


    /// Apply a complete validated snapshot. Missing references are cached separately
    /// from authenticated receipts and can never become independent vote evidence.
    /// Adder-trusted recent snapshots may be reshared, but never used as votes.
    pub async fn snapshot_history_range(&self,group:u64,start:u64,end:u64)->anyhow::Result<Vec<GroupMessage>> {
        let rows=sqlx::query_as::<_,GroupMessage>("SELECT id,group_id,by,message,channel_id,epoch,message_type FROM group_messages WHERE group_id=? AND id>=? AND id<=? AND (message_type & 2)=0 AND by<>'' ORDER BY id LIMIT 1000")
            .bind(group as i64).bind(start as i64).bind(end as i64).fetch_all(&self.pool).await?;
        self.visible_messages(rows).await
    }

    pub async fn mark_snapshot_join_pending(&self,group_id:u64)->anyhow::Result<()> {
        super::keyvalue::KeyValueStore::new(self.pool.clone()).await?.set(
            &format!("snapshot-join-pending:{group_id}"),&format!("{:016x}",rand::random::<u64>())
        ).await
    }

    pub async fn apply_pin_snapshot(&self, group_id:u64, records:&[GroupMessage], hash:&[u8]) -> anyhow::Result<()> {
        anyhow::ensure!(records.len()<=50,"Too many pins");
        if let (Some(first),Some(last))=(records.first(),records.last()) {
            crate::history::validate_chunk_records(records,group_id,first.id,last.id,records.len() as u32)?;
        }
        self.import_verified_history(group_id,0,hash,records).await?;
        let mut tx=self.pool.begin().await?;
        sqlx::query("UPDATE group_messages SET message_type=message_type & ~1 WHERE group_id=? AND (message_type & 1)<>0").bind(group_id as i64).execute(&mut *tx).await?;
        // Snapshot state supersedes the unreleased per-message pin protocol.
        // Preserve it when the authenticated original later replaces an import.
        sqlx::query("UPDATE group_pin_updates SET pinned=0,event_id=9223372036854775807 WHERE group_id=?").bind(group_id as i64).execute(&mut *tx).await?;
        for record in records {
            sqlx::query("INSERT INTO group_pin_updates(group_id,message_id,channel_id,event_id,pinned) VALUES(?,?,?,9223372036854775807,1) ON CONFLICT(group_id,message_id,channel_id) DO UPDATE SET pinned=1,event_id=excluded.event_id").bind(group_id as i64).bind(record.id as i64).bind(record.channel_id as i64).execute(&mut *tx).await?;
            sqlx::query("UPDATE group_messages SET message_type=message_type | 1 WHERE group_id=? AND id=?").bind(group_id as i64).bind(record.id as i64).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn get_pinned_messages(&self, group_id: u64) -> anyhow::Result<Vec<GroupMessage>> {
        let _pin_write = self.pin_writes.lock().await;
        self.visible_messages(self.pinned_messages_unfiltered_for_group(group_id).await?).await
    }

    pub async fn search(
        &self,
        query: &str,
        group_id: Option<u64>,
        channel_id: Option<u32>,
        limit: u32,
        offset: u32,
    ) -> anyhow::Result<Vec<GroupMessageSearchResult>> {
        let fts_query = crate::db::search::sanitize_fts5_query(query);
        if fts_query.is_empty() {
            return Ok(Vec::new());
        }

        let mut conditions = vec!["group_messages_fts MATCH ?".to_string()];
        if group_id.is_some() {
            conditions.push("gm.group_id = ?".to_string());
        }
        if channel_id.is_some() {
            conditions.push("gm.channel_id = ?".to_string());
        }

        let where_clause = conditions.join(" AND ");
        let sql = format!(
            r#"
        SELECT
            gm.id, gm.group_id, gm.by, gm.message, gm.channel_id, gm.epoch, gm.message_type, gm.text,
            snippet(group_messages_fts, 0, '<b>', '</b>', '...', 10) AS match_snippet,
            fts.rank AS rank_score
        FROM group_messages_fts fts
        JOIN group_messages gm ON gm.rowid = fts.rowid
        WHERE {where_clause}
        ORDER BY rank_score ASC
        LIMIT ? OFFSET ?
        "#
        );

        let mut q = sqlx::query(&sql).bind(&fts_query);
        if let Some(gid) = group_id {
            q = q.bind(gid as i64);
        }
        if let Some(cid) = channel_id {
            q = q.bind(cid as i64);
        }
        q = q.bind(limit as i64).bind(offset as i64);

        let rows = q.fetch_all(&self.pool).await?;
        let mut raw_results = Vec::with_capacity(rows.len());
        for row in &rows {
            let message = GroupMessage::from_row(row)?;
            let text: String = row.try_get("text")?;
            let snippet: String = row.try_get("match_snippet")?;
            let score: f64 = row.try_get("rank_score").unwrap_or(0.0);
            raw_results.push((message, text, snippet, score));
        }

        if let Some(access) = &self.read_access {
            let client = access
                .client
                .get()
                .ok_or_else(|| anyhow::anyhow!("MLS client not initialized"))?;
            let mut filtered = Vec::new();
            let mut channel_perm_cache: std::collections::HashMap<(u64, u32), bool> =
                std::collections::HashMap::new();
            for (message, text, snippet, score) in raw_results {
                let can_see = match channel_perm_cache.get(&(message.group_id, message.channel_id)) {
                    Some(&allowed) => allowed,
                    None => {
                        let allowed = match access.groups.get(message.group_id).await {
                            Ok(info) => match client.load_group(message.group_id, info.identifier).await {
                                Ok(group) => group.can_see_message(message.channel_id).await.unwrap_or(false),
                                Err(_) => false,
                            },
                            Err(_) => false,
                        };
                        channel_perm_cache.insert((message.group_id, message.channel_id), allowed);
                        allowed
                    }
                };
                if can_see {
                    filtered.push(GroupMessageSearchResult {
                        message,
                        text,
                        snippet,
                        score,
                    });
                }
            }
            Ok(filtered)
        } else {
            Ok(raw_results
                .into_iter()
                .map(|(message, text, snippet, score)| GroupMessageSearchResult {
                    message,
                    text,
                    snippet,
                    score,
                })
                .collect())
        }
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

    #[tokio::test]
    async fn test_search_group_and_channel_messages() {
        let pool = setup_test_db().await;
        let store = GroupMessagesStore::new(pool).await.unwrap();

        // Message 1: group 100, channel 1
        store
            .add(
                1,
                100,
                1,
                1,
                "alice",
                b"Welcome everyone to the general channel of group 100!",
                0,
            )
            .await
            .unwrap();

        // Message 2: group 100, channel 2
        store
            .add(
                2,
                100,
                2,
                1,
                "bob",
                b"Design discussion: new architecture proposal for database.",
                0,
            )
            .await
            .unwrap();

        // Message 3: group 200, channel 1
        store
            .add(
                3,
                200,
                1,
                1,
                "charlie",
                b"General channel in group 200: database migration is done.",
                0,
            )
            .await
            .unwrap();

        // Search across all groups and channels
        let res_all = store.search("database", None, None, 10, 0).await.unwrap();
        assert_eq!(res_all.len(), 2);

        // Search scoped to group 100 (all channels)
        let res_grp100 = store.search("database", Some(100), None, 10, 0).await.unwrap();
        assert_eq!(res_grp100.len(), 1);
        assert_eq!(res_grp100[0].message.id, 2);
        assert_eq!(res_grp100[0].message.group_id, 100);
        assert!(res_grp100[0].snippet.contains("<b>database</b>"));

        // Search scoped to channel 1 within group 100
        let res_grp100_ch1 = store.search("general", Some(100), Some(1), 10, 0).await.unwrap();
        assert_eq!(res_grp100_ch1.len(), 1);
        assert_eq!(res_grp100_ch1[0].message.id, 1);

        // Search scoped to channel 2 within group 100 (should not match channel 1)
        let res_grp100_ch2 = store.search("general", Some(100), Some(2), 10, 0).await.unwrap();
        assert_eq!(res_grp100_ch2.len(), 0);

        // Search with triggers: delete group 200 and ensure search results update
        store.delete_by_group_id(200).await.unwrap();
        let res_after_del = store.search("database", None, None, 10, 0).await.unwrap();
        assert_eq!(res_after_del.len(), 1);
        assert_eq!(res_after_del[0].message.group_id, 100);
    }
}
