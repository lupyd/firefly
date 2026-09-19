use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchSource {
    User,
    Group,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SearchScope {
    /// Search across both user messages and group/channel messages.
    All,
    /// Search specifically in 1:1 user messages (optionally for a specific user).
    User { other: Option<String> },
    /// Search in group messages (optionally for a specific group and/or channel).
    Group {
        group_id: Option<u64>,
        channel_id: Option<u32>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SearchResultItem {
    pub source: SearchSource,
    pub message_id: u64,
    pub group_id: Option<u64>,
    pub channel_id: Option<u32>,
    pub other: Option<String>,
    pub by: String,
    pub text: String,
    pub snippet: String,
    pub message_type: u32,
    pub epoch: Option<u32>,
    pub score: f64,
}

/// Safely sanitizes an arbitrary user search string for SQLite FTS5 MATCH syntax.
/// Converts user tokens into quoted prefix expressions so special punctuation
/// (such as punctuation, colons, brackets, or operators) cannot trigger syntax errors.
pub fn sanitize_fts5_query(input: &str) -> String {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    let mut terms = Vec::new();
    // Split into words, strip dangerous trailing wildcard characters for sanitization,
    // then wrap each token in double quotes with a prefix wildcard.
    for word in trimmed.split_whitespace() {
        let clean = word.trim_matches(|c: char| c == '*' || c == '"' || c == '\'');
        if !clean.is_empty() {
            let escaped = clean.replace('"', "\"\"");
            terms.push(format!("\"{escaped}\"*"));
        }
    }

    if terms.is_empty() {
        // If all characters were trimmed (e.g. only quotes/asterisks), fallback to quoted literal
        format!("\"{}\"*", trimmed.replace('"', "\"\""))
    } else {
        terms.join(" ")
    }
}

use super::group_messages::GroupMessagesStore;
use std::collections::HashMap;

pub struct SearchEngine {
    pool: SqlitePool,
    group_messages_store: Option<GroupMessagesStore>,
}

impl SearchEngine {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            group_messages_store: None,
        }
    }

    pub fn with_group_messages_store(
        pool: SqlitePool,
        group_messages_store: GroupMessagesStore,
    ) -> Self {
        Self {
            pool,
            group_messages_store: Some(group_messages_store),
        }
    }

    pub async fn search_all(
        &self,
        query: &str,
        limit: u32,
        offset: u32,
    ) -> anyhow::Result<Vec<SearchResultItem>> {
        self.search(query, &SearchScope::All, limit, offset).await
    }

    pub async fn search_user(
        &self,
        query: &str,
        other: Option<&str>,
        limit: u32,
        offset: u32,
    ) -> anyhow::Result<Vec<SearchResultItem>> {
        self.search(
            query,
            &SearchScope::User {
                other: other.map(|s| s.to_string()),
            },
            limit,
            offset,
        )
        .await
    }

    pub async fn search_group(
        &self,
        query: &str,
        group_id: Option<u64>,
        channel_id: Option<u32>,
        limit: u32,
        offset: u32,
    ) -> anyhow::Result<Vec<SearchResultItem>> {
        self.search(
            query,
            &SearchScope::Group {
                group_id,
                channel_id,
            },
            limit,
            offset,
        )
        .await
    }

    async fn filter_visible_group_results(
        &self,
        items: Vec<SearchResultItem>,
    ) -> anyhow::Result<Vec<SearchResultItem>> {
        let Some(ref store) = self.group_messages_store else {
            return Ok(items);
        };
        let mut visible = Vec::with_capacity(items.len());
        let mut perm_cache: HashMap<(u64, u32), bool> = HashMap::new();
        for item in items {
            if item.source == SearchSource::Group {
                if let (Some(gid), Some(cid)) = (item.group_id, item.channel_id) {
                    let can_see = match perm_cache.get(&(gid, cid)) {
                        Some(&allowed) => allowed,
                        None => {
                            let allowed = store.can_see_channel(gid, cid).await.unwrap_or(false);
                            perm_cache.insert((gid, cid), allowed);
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

    /// Unified search across user messages and/or group messages.
    pub async fn search(
        &self,
        query: &str,
        scope: &SearchScope,
        limit: u32,
        offset: u32,
    ) -> anyhow::Result<Vec<SearchResultItem>> {
        let fts_query = sanitize_fts5_query(query);
        if fts_query.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();

        match scope {
            SearchScope::User { other } => {
                let items = self
                    .search_user_messages(&fts_query, other.as_deref(), limit, offset)
                    .await?;
                results.extend(items);
            }
            SearchScope::Group {
                group_id,
                channel_id,
            } => {
                let items = self
                    .search_group_messages(&fts_query, *group_id, *channel_id, limit, offset)
                    .await?;
                let filtered = self.filter_visible_group_results(items).await?;
                results.extend(filtered);
            }
            SearchScope::All => {
                // Unified query across both tables
                let sql = r#"
                    SELECT
                        'user' AS source,
                        um.id AS message_id,
                        0 AS group_id,
                        0 AS channel_id,
                        um.other AS other,
                        um.other AS by_user,
                        um.text AS text_content,
                        snippet(user_messages_fts, 0, '<b>', '</b>', '...', 10) AS match_snippet,
                        um.message_type AS msg_type,
                        0 AS msg_epoch,
                        fts.rank AS rank_score
                    FROM user_messages_fts fts
                    JOIN user_messages um ON um.rowid = fts.rowid
                    WHERE user_messages_fts MATCH ?

                    UNION ALL

                    SELECT
                        'group' AS source,
                        gm.id AS message_id,
                        gm.group_id AS group_id,
                        gm.channel_id AS channel_id,
                        NULL AS other,
                        gm.by AS by_user,
                        gm.text AS text_content,
                        snippet(group_messages_fts, 0, '<b>', '</b>', '...', 10) AS match_snippet,
                        gm.message_type AS msg_type,
                        gm.epoch AS msg_epoch,
                        fts.rank AS rank_score
                    FROM group_messages_fts fts
                    JOIN group_messages gm ON gm.rowid = fts.rowid
                    WHERE group_messages_fts MATCH ?

                    ORDER BY rank_score ASC
                    LIMIT ? OFFSET ?
                "#;

                let rows = sqlx::query(sql)
                    .bind(&fts_query)
                    .bind(&fts_query)
                    .bind(limit as i64)
                    .bind(offset as i64)
                    .fetch_all(&self.pool)
                    .await?;

                for row in rows {
                    let source_str: String = row.try_get("source")?;
                    let source = if source_str == "user" {
                        SearchSource::User
                    } else {
                        SearchSource::Group
                    };
                    let group_id: Option<i64> = row.try_get("group_id").ok();
                    let channel_id: Option<i64> = row.try_get("channel_id").ok();
                    let other: Option<String> = row.try_get("other").ok();
                    let epoch: Option<i64> = row.try_get("msg_epoch").ok();
                    let score: f64 = row.try_get("rank_score").unwrap_or(0.0);

                    results.push(SearchResultItem {
                        source,
                        message_id: row.try_get::<i64, _>("message_id")? as u64,
                        group_id: group_id.filter(|&g| g != 0).map(|g| g as u64),
                        channel_id: channel_id.map(|c| c as u32),
                        other,
                        by: row.try_get("by_user")?,
                        text: row.try_get("text_content")?,
                        snippet: row.try_get("match_snippet")?,
                        message_type: row.try_get::<i64, _>("msg_type")? as u32,
                        epoch: epoch.map(|e| e as u32),
                        score,
                    });
                }

                results = self.filter_visible_group_results(results).await?;
            }
        }

        Ok(results)
    }

    async fn search_user_messages(
        &self,
        fts_query: &str,
        other: Option<&str>,
        limit: u32,
        offset: u32,
    ) -> anyhow::Result<Vec<SearchResultItem>> {
        let (sql, bind_other) = if other.is_some() {
            (
                r#"
                SELECT
                    um.id AS message_id,
                    um.other AS other,
                    um.text AS text_content,
                    snippet(user_messages_fts, 0, '<b>', '</b>', '...', 10) AS match_snippet,
                    um.message_type AS msg_type,
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
                    um.id AS message_id,
                    um.other AS other,
                    um.text AS text_content,
                    snippet(user_messages_fts, 0, '<b>', '</b>', '...', 10) AS match_snippet,
                    um.message_type AS msg_type,
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

        let mut query = sqlx::query(sql).bind(fts_query);
        if bind_other {
            query = query.bind(other.unwrap());
        }
        query = query.bind(limit as i64).bind(offset as i64);

        let rows = query.fetch_all(&self.pool).await?;
        let mut results = Vec::with_capacity(rows.len());

        for row in rows {
            let other_val: String = row.try_get("other")?;
            let score: f64 = row.try_get("rank_score").unwrap_or(0.0);
            results.push(SearchResultItem {
                source: SearchSource::User,
                message_id: row.try_get::<i64, _>("message_id")? as u64,
                group_id: None,
                channel_id: None,
                other: Some(other_val.clone()),
                by: other_val,
                text: row.try_get("text_content")?,
                snippet: row.try_get("match_snippet")?,
                message_type: row.try_get::<i64, _>("msg_type")? as u32,
                epoch: None,
                score,
            });
        }

        Ok(results)
    }

    async fn search_group_messages(
        &self,
        fts_query: &str,
        group_id: Option<u64>,
        channel_id: Option<u32>,
        limit: u32,
        offset: u32,
    ) -> anyhow::Result<Vec<SearchResultItem>> {
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
                gm.id AS message_id,
                gm.group_id AS group_id,
                gm.channel_id AS channel_id,
                gm.by AS by_user,
                gm.text AS text_content,
                snippet(group_messages_fts, 0, '<b>', '</b>', '...', 10) AS match_snippet,
                gm.message_type AS msg_type,
                gm.epoch AS msg_epoch,
                fts.rank AS rank_score
            FROM group_messages_fts fts
            JOIN group_messages gm ON gm.rowid = fts.rowid
            WHERE {where_clause}
            ORDER BY rank_score ASC
            LIMIT ? OFFSET ?
            "#
        );

        let mut query = sqlx::query(&sql).bind(fts_query);
        if let Some(gid) = group_id {
            query = query.bind(gid as i64);
        }
        if let Some(cid) = channel_id {
            query = query.bind(cid as i64);
        }
        query = query.bind(limit as i64).bind(offset as i64);

        let rows = query.fetch_all(&self.pool).await?;
        let mut results = Vec::with_capacity(rows.len());

        for row in rows {
            let score: f64 = row.try_get("rank_score").unwrap_or(0.0);
            results.push(SearchResultItem {
                source: SearchSource::Group,
                message_id: row.try_get::<i64, _>("message_id")? as u64,
                group_id: Some(row.try_get::<i64, _>("group_id")? as u64),
                channel_id: Some(row.try_get::<i64, _>("channel_id")? as u32),
                other: None,
                by: row.try_get("by_user")?,
                text: row.try_get("text_content")?,
                snippet: row.try_get("match_snippet")?,
                message_type: row.try_get::<i64, _>("msg_type")? as u32,
                epoch: Some(row.try_get::<i64, _>("msg_epoch")? as u32),
                score,
            });
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::messages::{MessagesStore, UserMessage};
    use crate::db::group_messages::GroupMessagesStore;
    use sqlx::sqlite::SqlitePoolOptions;

    async fn setup_test_db() -> SqlitePool {
        SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap()
    }

    #[test]
    fn test_sanitize_fts5_query_edge_cases() {
        // Empty and whitespace
        assert_eq!(sanitize_fts5_query(""), "");
        assert_eq!(sanitize_fts5_query("   "), "");

        // Simple terms
        assert_eq!(sanitize_fts5_query("hello"), "\"hello\"*");
        assert_eq!(sanitize_fts5_query("hello world"), "\"hello\"* \"world\"*");

        // Quotes and asterisks stripped and escaped
        assert_eq!(sanitize_fts5_query("\"hello\"*"), "\"hello\"*");
        assert_eq!(sanitize_fts5_query("foo* bar?"), "\"foo\"* \"bar?\"*");

        // Quotes inside terms escaped
        assert_eq!(sanitize_fts5_query("john's \"quote\""), "\"john's\"* \"quote\"*");

        // Special characters and operators that would otherwise cause FTS5 syntax errors
        assert_eq!(sanitize_fts5_query("NEAR/2 OR NOT"), "\"NEAR/2\"* \"OR\"* \"NOT\"*");
        assert_eq!(sanitize_fts5_query("foo:bar"), "\"foo:bar\"*");
        assert_eq!(sanitize_fts5_query("test(123)"), "\"test(123)\"*");
    }

    #[tokio::test]
    async fn test_search_engine_all_and_scoped() {
        let pool = setup_test_db().await;
        let user_store = MessagesStore::new(pool.clone()).await.unwrap();
        let group_store = GroupMessagesStore::new(pool.clone()).await.unwrap();
        let search_engine = SearchEngine::with_group_messages_store(pool.clone(), group_store.clone());

        // 1. Insert 1:1 user messages
        user_store
            .insert_user_message(UserMessage {
                id: 10,
                other: "alice".to_string(),
                message: b"Alice: Critical security bug found in database layer.".to_vec(),
                sent_by_other: true,
                message_type: 0,
            })
            .await
            .unwrap();

        user_store
            .insert_user_message(UserMessage {
                id: 11,
                other: "bob".to_string(),
                message: b"Bob: Database performance benchmarks are looking good.".to_vec(),
                sent_by_other: false,
                message_type: 0,
            })
            .await
            .unwrap();

        // 2. Insert group & channel messages
        group_store
            .add(
                20,
                100, // group_id
                1,   // channel_id
                1,
                "charlie",
                b"Charlie: Security audit kickoff for the database backend.",
                0,
            )
            .await
            .unwrap();

        group_store
            .add(
                21,
                100, // group_id
                2,   // channel_id
                1,
                "dave",
                b"Dave: Deploying security patch to production.",
                0,
            )
            .await
            .unwrap();

        // Test 1: Unified search across whole app for "database"
        let res_all_db = search_engine.search_all("database", 10, 0).await.unwrap();
        // Should match 2 user messages and 1 group message
        assert_eq!(res_all_db.len(), 3);
        let user_count = res_all_db.iter().filter(|r| r.source == SearchSource::User).count();
        let group_count = res_all_db.iter().filter(|r| r.source == SearchSource::Group).count();
        assert_eq!(user_count, 2);
        assert_eq!(group_count, 1);
        for item in &res_all_db {
            assert!(item.snippet.contains("<b>database</b>") || item.snippet.contains("<b>Database</b>"));
        }

        // Test 2: Unified search for "security"
        let res_all_sec = search_engine.search_all("security", 10, 0).await.unwrap();
        // Should match 1 user message (id 10) and 2 group messages (id 20, 21)
        assert_eq!(res_all_sec.len(), 3);

        // Test 3: Scoped to User ("alice")
        let res_alice = search_engine.search_user("security", Some("alice"), 10, 0).await.unwrap();
        assert_eq!(res_alice.len(), 1);
        assert_eq!(res_alice[0].source, SearchSource::User);
        assert_eq!(res_alice[0].message_id, 10);
        assert_eq!(res_alice[0].other.as_deref(), Some("alice"));

        // Test 4: Scoped to User ("bob") - no security mention
        let res_bob = search_engine.search_user("security", Some("bob"), 10, 0).await.unwrap();
        assert_eq!(res_bob.len(), 0);

        // Test 5: Scoped to Group (group 100, all channels)
        let res_grp100 = search_engine.search_group("security", Some(100), None, 10, 0).await.unwrap();
        assert_eq!(res_grp100.len(), 2);

        // Test 6: Scoped to Channel (group 100, channel 2)
        let res_ch2 = search_engine.search_group("security", Some(100), Some(2), 10, 0).await.unwrap();
        assert_eq!(res_ch2.len(), 1);
        assert_eq!(res_ch2[0].message_id, 21);
        assert_eq!(res_ch2[0].channel_id, Some(2));
    }
}

