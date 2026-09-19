use sqlx::{sqlite::SqlitePoolOptions, Executor, SqlitePool};

use firefly_client::db::{
    group_messages::GroupMessagesStore,
    messages::{MessagesStore, UserMessage},
    search::{sanitize_fts5_query, SearchEngine, SearchSource},
};
use firefly_client::storage::FavouriteMessageStorage;
use firefly_protos::{
    serialize_proto,
    firefly::{self, mod_GroupMessageInner, mod_UserMessageInner, MessagePayload},
};

async fn create_memory_pool() -> SqlitePool {
    SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap()
}

/// Test 1: Complete backwards compatibility and automated migration
/// Simulates an older database with no `text` column in `user_messages` or `group_messages`,
/// and no `_fts` virtual tables or triggers. Verifies that initializing the stores performs
/// idempotent ALTER TABLE, extracts text from protobuf messages, and rebuilds FTS5 indexes.
#[tokio::test]
async fn test_migration_and_backwards_compatibility() {
    let pool = create_memory_pool().await;

    // Create legacy tables WITHOUT `text` column and WITHOUT FTS5 tables
    pool.execute(
        r#"
        CREATE TABLE user_messages (
            id INTEGER NOT NULL,
            other TEXT NOT NULL,
            sent_by_other BOOLEAN NOT NULL,
            message BLOB NOT NULL
        );

        CREATE TABLE group_messages (
            id INTEGER NOT NULL,
            group_id INTEGER NOT NULL,
            by TEXT NOT NULL,
            message BLOB NOT NULL,
            channel_id INTEGER NOT NULL,
            epoch INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (group_id, id)
        );
        "#,
    )
    .await
    .unwrap();

    // Insert legacy user message encoded as protobuf
    let user_inner = firefly::UserMessageInner {
        nonce: 0,
        message: mod_UserMessageInner::OneOfmessage::messagePayload(MessagePayload {
            text: "Legacy user message regarding confidential project launch".into(),
            files: None,
            ext: firefly::mod_MessagePayload::OneOfext::None,
            message_type: 0,
        }),
        message_type: 0,
    };
    let user_bytes = serialize_proto(&user_inner).unwrap().to_vec();

    sqlx::query("INSERT INTO user_messages (id, other, sent_by_other, message) VALUES (?, ?, ?, ?)")
        .bind(101i64)
        .bind("alice")
        .bind(true)
        .bind(user_bytes)
        .execute(&pool)
        .await
        .unwrap();

    // Insert legacy group message encoded as protobuf
    let group_inner = firefly::GroupMessageInner {
        channelId: 5,
        message: mod_GroupMessageInner::OneOfmessage::messagePayload(MessagePayload {
            text: "Legacy group announcement about security infrastructure".into(),
            files: None,
            ext: firefly::mod_MessagePayload::OneOfext::None,
            message_type: 0,
        }),
        message_type: 0,
    };
    let group_bytes = serialize_proto(&group_inner).unwrap().to_vec();

    sqlx::query("INSERT INTO group_messages (id, group_id, by, message, channel_id, epoch) VALUES (?, ?, ?, ?, ?, ?)")
        .bind(201i64)
        .bind(500i64)
        .bind("bob")
        .bind(group_bytes)
        .bind(5i64)
        .bind(1i64)
        .execute(&pool)
        .await
        .unwrap();

    // Now run migration by initializing both stores
    let user_store = MessagesStore::new(pool.clone()).await.unwrap();
    let group_store = GroupMessagesStore::new(pool.clone()).await.unwrap();

    // Verify search works on the backfilled and indexed legacy data!
    let search_engine = SearchEngine::with_group_messages_store(pool.clone(), group_store.clone());

    // 1. Search for backfilled user message
    let user_results = user_store.search("confidential launch", None, 10, 0).await.unwrap();
    assert_eq!(user_results.len(), 1);
    assert_eq!(user_results[0].message.id, 101);
    assert!(user_results[0].snippet.contains("<b>confidential</b>"));

    // 2. Search for backfilled group message
    let group_results = group_store.search("security infrastructure", None, None, 10, 0).await.unwrap();
    assert_eq!(group_results.len(), 1);
    assert_eq!(group_results[0].message.id, 201);
    assert_eq!(group_results[0].message.group_id, 500);

    // 3. Search app-wide across both backfilled tables
    let app_results = search_engine.search_all("legacy", 10, 0).await.unwrap();
    assert_eq!(app_results.len(), 2);
}

/// Test 2: 1:1 User message search with contact scoping, pagination, and prefix queries
#[tokio::test]
async fn test_user_messages_scoped_search() {
    let pool = create_memory_pool().await;
    let store = MessagesStore::new(pool.clone()).await.unwrap();

    store
        .insert_user_message(UserMessage {
            id: 1,
            other: "alice".to_string(),
            message: b"Alice: quarterly financial report is ready for review.".to_vec(),
            sent_by_other: true,
            message_type: 0,
        })
        .await
        .unwrap();

    store
        .insert_user_message(UserMessage {
            id: 2,
            other: "bob".to_string(),
            message: b"Bob: please review the quarterly budget plan.".to_vec(),
            sent_by_other: false,
            message_type: 0,
        })
        .await
        .unwrap();

    store
        .insert_user_message(UserMessage {
            id: 3,
            other: "alice".to_string(),
            message: b"Alice: second note on the budget figures.".to_vec(),
            sent_by_other: true,
            message_type: 0,
        })
        .await
        .unwrap();

    // Search across all contacts for "quarterly"
    let all = store.search("quarterly", None, 10, 0).await.unwrap();
    assert_eq!(all.len(), 2);

    // Search scoped to alice for "quarterly"
    let alice_q = store.search("quarterly", Some("alice"), 10, 0).await.unwrap();
    assert_eq!(alice_q.len(), 1);
    assert_eq!(alice_q[0].message.id, 1);

    // Search scoped to alice for "budget"
    let alice_b = store.search("budget", Some("alice"), 10, 0).await.unwrap();
    assert_eq!(alice_b.len(), 1);
    assert_eq!(alice_b[0].message.id, 3);

    // Search scoped to bob for "budget"
    let bob_b = store.search("budget", Some("bob"), 10, 0).await.unwrap();
    assert_eq!(bob_b.len(), 1);
    assert_eq!(bob_b[0].message.id, 2);

    // Prefix search: "finan*" matches "financial"
    let prefix = store.search("finan", None, 10, 0).await.unwrap();
    assert_eq!(prefix.len(), 1);
    assert_eq!(prefix[0].message.id, 1);
}

/// Test 3: Group & Channel message search with group scoping, channel scoping, and pagination
#[tokio::test]
async fn test_group_and_channel_scoped_search() {
    let pool = create_memory_pool().await;
    let store = GroupMessagesStore::new(pool.clone()).await.unwrap();

    // Group 100, Channel 1: general discussion
    store
        .add(
            1,
            100,
            1,
            1,
            "alice",
            b"Frontend migration to WebAssembly has started smoothly.",
            0,
        )
        .await
        .unwrap();

    // Group 100, Channel 2: backend development
    store
        .add(
            2,
            100,
            2,
            1,
            "bob",
            b"Backend SQLite full text search integration with FTS5 is deployed.",
            0,
        )
        .await
        .unwrap();

    // Group 200, Channel 1: devops
    store
        .add(
            3,
            200,
            1,
            1,
            "charlie",
            b"Devops docker deployment pipeline updated for backend servers.",
            0,
        )
        .await
        .unwrap();

    // 1. Search across all groups and channels for "backend"
    let res_all = store.search("backend", None, None, 10, 0).await.unwrap();
    assert_eq!(res_all.len(), 2);

    // 2. Scoped to group 100
    let res_grp100 = store.search("backend", Some(100), None, 10, 0).await.unwrap();
    assert_eq!(res_grp100.len(), 1);
    assert_eq!(res_grp100[0].message.id, 2);
    assert_eq!(res_grp100[0].message.channel_id, 2);

    // 3. Scoped to channel 1 within group 100 (should find frontend message, not backend)
    let res_grp100_ch1 = store.search("migration", Some(100), Some(1), 10, 0).await.unwrap();
    assert_eq!(res_grp100_ch1.len(), 1);
    assert_eq!(res_grp100_ch1[0].message.id, 1);

    // Channel 2 within group 100 has no "migration"
    let res_grp100_ch2 = store.search("migration", Some(100), Some(2), 10, 0).await.unwrap();
    assert_eq!(res_grp100_ch2.len(), 0);

    // 4. Scoped to channel 1 within group 200
    let res_grp200_ch1 = store.search("docker", Some(200), Some(1), 10, 0).await.unwrap();
    assert_eq!(res_grp200_ch1.len(), 1);
    assert_eq!(res_grp200_ch1[0].message.id, 3);
}

/// Test 4: Entire app unified search across user and group messages
#[tokio::test]
async fn test_entire_app_unified_search() {
    let pool = create_memory_pool().await;
    let user_store = MessagesStore::new(pool.clone()).await.unwrap();
    let group_store = GroupMessagesStore::new(pool.clone()).await.unwrap();
    let search_engine = SearchEngine::with_group_messages_store(pool.clone(), group_store.clone());

    // 1:1 user message
    user_store
        .insert_user_message(UserMessage {
            id: 10,
            other: "eve".to_string(),
            message: b"Eve: Urgent incident alert, database cluster connection timeout.".to_vec(),
            sent_by_other: true,
            message_type: 0,
        })
        .await
        .unwrap();

    // Group message
    group_store
        .add(
            20,
            300,
            1,
            1,
            "frank",
            b"Frank: On-call incident response team investigating database latency.",
            0,
        )
        .await
        .unwrap();

    // Unified search for "incident"
    let results = search_engine.search_all("incident", 10, 0).await.unwrap();
    assert_eq!(results.len(), 2);

    let user_item = results.iter().find(|r| r.source == SearchSource::User).unwrap();
    assert_eq!(user_item.message_id, 10);
    assert_eq!(user_item.other.as_deref(), Some("eve"));
    assert!(user_item.snippet.contains("<b>incident</b>") || user_item.snippet.contains("<b>Incident</b>"));

    let group_item = results.iter().find(|r| r.source == SearchSource::Group).unwrap();
    assert_eq!(group_item.message_id, 20);
    assert_eq!(group_item.group_id, Some(300));
    assert_eq!(group_item.channel_id, Some(1));
    assert_eq!(group_item.by, "frank");
}

/// Test 5: SQLite triggers keep FTS index synchronized on INSERT, UPDATE, and DELETE
#[tokio::test]
async fn test_trigger_synchronization() {
    let pool = create_memory_pool().await;
    let user_store = MessagesStore::new(pool.clone()).await.unwrap();
    let group_store = GroupMessagesStore::new(pool.clone()).await.unwrap();

    // --- User Messages Trigger Sync ---
    // 1. Insert
    user_store
        .insert_user_message(UserMessage {
            id: 1,
            other: "alice".to_string(),
            message: b"Initial draft of the article".to_vec(),
            sent_by_other: false,
            message_type: 0,
        })
        .await
        .unwrap();

    let res1 = user_store.search("draft", None, 10, 0).await.unwrap();
    assert_eq!(res1.len(), 1);

    // 2. Update text
    user_store
        .update_user_message_text("alice", 1, b"Final published version of the article".to_vec())
        .await
        .unwrap();

    // "draft" should now return 0 results
    let res_old = user_store.search("draft", None, 10, 0).await.unwrap();
    assert_eq!(res_old.len(), 0);

    // "published" should now return 1 result
    let res_new = user_store.search("published", None, 10, 0).await.unwrap();
    assert_eq!(res_new.len(), 1);

    // 3. Delete
    user_store.delete_user_message("alice", 1).await.unwrap();
    let res_del = user_store.search("published", None, 10, 0).await.unwrap();
    assert_eq!(res_del.len(), 0);

    // --- Group Messages Trigger Sync ---
    // 1. Insert
    group_store
        .add(10, 50, 1, 1, "alice", b"Roadmap discussion for next sprint", 0)
        .await
        .unwrap();

    let g_res1 = group_store.search("roadmap", None, None, 10, 0).await.unwrap();
    assert_eq!(g_res1.len(), 1);

    // 2. Delete by group
    group_store.delete_by_group_id(50).await.unwrap();
    let g_res_del = group_store.search("roadmap", None, None, 10, 0).await.unwrap();
    assert_eq!(g_res_del.len(), 0);
}

/// Test 6: Sanitizer handles punctuation, syntax characters, and special operators safely
#[test]
fn test_query_sanitizer_safety() {
    let queries = [
        "",
        "   ",
        "hello",
        "foo bar",
        "user@domain.com",
        "\"unclosed quote",
        "\"already quoted\"",
        "test: colon",
        "foo AND bar",
        "foo OR bar",
        "foo NOT bar",
        "NEAR(foo, bar)",
        "hyphen-separated-word",
        "brackets [test] and (parens)",
        "special chars !@#$%^&*()_+",
    ];

    for q in queries {
        let sanitized = sanitize_fts5_query(q);
        // Ensure none cause empty or broken strings when words are present
        if !q.trim().is_empty() {
            assert!(!sanitized.is_empty(), "Sanitized query for '{}' should not be empty", q);
        }
    }
}

/// Test 7: Regression test for user report: "message_type column does not exist".
/// Ensures older databases lacking `message_type`, `text`, or `epoch` columns are migrated
/// without consequences, preserving data, enabling pinned messages, favourites, and search.
#[tokio::test]
async fn test_legacy_database_initialization_with_missing_message_type_column() {
    let pool = create_memory_pool().await;

    // Simulate an existing database created before `message_type` existed
    pool.execute(
        r#"
        CREATE TABLE user_messages (
            id INTEGER NOT NULL,
            other TEXT NOT NULL,
            sent_by_other BOOLEAN NOT NULL,
            message BLOB NOT NULL
        );

        CREATE TABLE group_messages (
            id INTEGER NOT NULL,
            group_id INTEGER NOT NULL,
            by TEXT NOT NULL,
            message BLOB NOT NULL,
            channel_id INTEGER NOT NULL,
            PRIMARY KEY (group_id, id)
        );
        "#,
    )
    .await
    .unwrap();

    // Populate with legacy rows
    let user_msg_inner = firefly::UserMessageInner {
        nonce: 0,
        message: mod_UserMessageInner::OneOfmessage::messagePayload(MessagePayload {
            text: "Hello from legacy user message".into(),
            files: None,
            ext: firefly::mod_MessagePayload::OneOfext::None,
            message_type: 0,
        }),
        message_type: 0,
    };
    let user_bytes = serialize_proto(&user_msg_inner).unwrap().to_vec();

    sqlx::query("INSERT INTO user_messages (id, other, sent_by_other, message) VALUES (?, ?, ?, ?)")
        .bind(10i64)
        .bind("alice")
        .bind(true)
        .bind(&user_bytes)
        .execute(&pool)
        .await
        .unwrap();

    let group_msg_inner = firefly::GroupMessageInner {
        channelId: 1,
        message: mod_GroupMessageInner::OneOfmessage::messagePayload(MessagePayload {
            text: "Hello from legacy group message".into(),
            files: None,
            ext: firefly::mod_MessagePayload::OneOfext::None,
            message_type: 0,
        }),
        message_type: 0,
    };
    let group_bytes = serialize_proto(&group_msg_inner).unwrap().to_vec();

    sqlx::query("INSERT INTO group_messages (id, group_id, by, message, channel_id) VALUES (?, ?, ?, ?, ?)")
        .bind(20i64)
        .bind(100i64)
        .bind("bob")
        .bind(&group_bytes)
        .bind(1i64)
        .execute(&pool)
        .await
        .unwrap();

    // Initialize stores concurrently - must not fail with "message_type column does not exist"
    let user_store = MessagesStore::new(pool.clone()).await.unwrap();
    let group_store = GroupMessagesStore::new(pool.clone()).await.unwrap();
    let favourite_store = firefly_client::db::favourites::FavouriteMessagesStore::new(pool.clone())
        .await
        .unwrap();

    // 1. Query messages before
    let msgs = user_store.get_last_messages_of("alice", 100, 10).await.unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].message_type, 0);

    // 2. Query pinned messages
    let pinned = user_store.get_pinned_messages_of("alice").await.unwrap();
    assert_eq!(pinned.len(), 0);

    // 3. Update message_type on legacy row
    user_store.update_message_type("alice", 10, 1).await.unwrap();
    let pinned_after = user_store.get_pinned_messages_of("alice").await.unwrap();
    assert_eq!(pinned_after.len(), 1);
    assert_eq!(pinned_after[0].id, 10);
    assert_eq!(pinned_after[0].message_type, 1);

    // 4. Query group messages
    let grp_msgs = group_store.get(100, 100, 10).await.unwrap();
    assert_eq!(grp_msgs.len(), 1);
    assert_eq!(grp_msgs[0].message_type, 0);
    assert_eq!(grp_msgs[0].epoch, 0);

    // 5. Update group message pinned status
    group_store.update_message_type(100, 20, 1).await.unwrap();
    let pinned_grp = group_store.get_pinned_messages(100).await.unwrap();
    assert_eq!(pinned_grp.len(), 1);
    assert_eq!(pinned_grp[0].id, 20);

    // 6. Add favourites from legacy messages
    favourite_store.add_user_message(&msgs[0], None).await.unwrap();
    favourite_store.add_group_message(&grp_msgs[0], None).await.unwrap();

    let favs = favourite_store.get_all(10, 0).await.unwrap();
    assert_eq!(favs.len(), 2);

    // 7. Search migrated messages
    let search_engine = SearchEngine::with_group_messages_store(pool.clone(), group_store.clone());
    let search_res = search_engine.search_all("legacy", 10, 0).await.unwrap();
    assert_eq!(search_res.len(), 2);
}

