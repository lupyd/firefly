//! Regression tests for storage-layer security fixes (hidden-message search leaks, migration
//! fast path, identity change detection, epoch secret retention, per-group purge).

use std::collections::HashMap;

use libsignal_protocol::{IdentityChange, IdentityKeyPair, ProtocolAddress};
use sqlx::{Row, SqlitePool};

use crate::db::group_messages::GroupMessagesStore;
use crate::db::group_stores::{GroupInfoStore, GroupStateStore, EPOCH_RETENTION};
use crate::db::keyvalue::KeyValueStore;
use crate::db::messages::{MessagesStore, UserMessage};
use crate::db::migrations::{run_migrations, LATEST_MIGRATION_VERSION};
use crate::db::search::{SearchEngine, SearchScope};
use crate::db::setup_pool;
use crate::db::stores::IdentityDb;

const HIDDEN_KEYS: u32 =
    firefly_protos::MESSAGE_TYPE_HIDDEN | firefly_protos::MESSAGE_TYPE_HISTORY_KEYS;

async fn memory_pool() -> SqlitePool {
    setup_pool("sqlite::memory:", 1).await.unwrap()
}

async fn fts_hits(pool: &SqlitePool, table: &str, term: &str) -> i64 {
    sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {table}_fts WHERE {table}_fts MATCH ?"
    ))
    .bind(term)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn count(pool: &SqlitePool, sql: &str) -> i64 {
    sqlx::query_scalar(sql).fetch_one(pool).await.unwrap()
}

#[tokio::test]
async fn secure_delete_pragma_is_enabled() {
    let pool = memory_pool().await;
    assert_eq!(count(&pool, "PRAGMA secure_delete").await, 1);
}

#[tokio::test]
async fn hidden_messages_are_not_stored_as_text_or_searchable() {
    let pool = memory_pool().await;
    let groups = GroupMessagesStore::new(pool.clone()).await.unwrap();
    let users = MessagesStore::new(pool.clone()).await.unwrap();

    groups.add(1, 7, 1, 1, "alice", b"visible secretkey talk", 0).await.unwrap();
    groups.add(2, 7, 1, 1, "alice", b"secretkey deadbeefcafebabe", HIDDEN_KEYS).await.unwrap();
    users
        .insert_user_message(UserMessage {
            id: 1,
            other: "bob".into(),
            message: b"visible secretkey dm".to_vec(),
            sent_by_other: true,
            message_type: 0,
        })
        .await
        .unwrap();
    users
        .insert_user_message(UserMessage {
            id: 2,
            other: "bob".into(),
            message: b"secretkey hidden dm".to_vec(),
            sent_by_other: true,
            message_type: firefly_protos::MESSAGE_TYPE_HIDDEN,
        })
        .await
        .unwrap();

    // (b) hidden payloads never reach the text column / FTS index.
    assert_eq!(count(&pool, "SELECT count(*) FROM group_messages WHERE id = 2 AND text = ''").await, 1);
    assert_eq!(count(&pool, "SELECT count(*) FROM user_messages WHERE id = 2 AND text = ''").await, 1);
    assert_eq!(fts_hits(&pool, "group_messages", "deadbeefcafebabe").await, 0);
    assert_eq!(fts_hits(&pool, "user_messages", "hidden").await, 0);

    // Re-delivery of the hidden message via upsert must not resurrect text either.
    groups.add(2, 7, 1, 1, "alice", b"secretkey deadbeefcafebabe", HIDDEN_KEYS).await.unwrap();
    assert_eq!(fts_hits(&pool, "group_messages", "deadbeefcafebabe").await, 0);

    // (a) every search path excludes hidden rows, even if text slipped into the index.
    sqlx::query("UPDATE group_messages SET text = 'secretkey leaked' WHERE id = 2").execute(&pool).await.unwrap();
    sqlx::query("UPDATE user_messages SET text = 'secretkey leaked' WHERE id = 2").execute(&pool).await.unwrap();

    let g = groups.search("secretkey", None, None, 10, 0).await.unwrap();
    assert_eq!(g.iter().map(|r| r.message.id).collect::<Vec<_>>(), vec![1]);
    let g = groups.search("secretkey", Some(7), Some(1), 10, 0).await.unwrap();
    assert_eq!(g.len(), 1);
    let u = users.search("secretkey", None, 10, 0).await.unwrap();
    assert_eq!(u.iter().map(|r| r.message.id).collect::<Vec<_>>(), vec![1]);
    let u = users.search("secretkey", Some("bob"), 10, 0).await.unwrap();
    assert_eq!(u.len(), 1);

    let engine = SearchEngine::new(pool.clone());
    for scope in [
        SearchScope::All,
        SearchScope::User { other: None },
        SearchScope::User { other: Some("bob".into()) },
        SearchScope::Group { group_id: None, channel_id: None },
        SearchScope::Group { group_id: Some(7), channel_id: Some(1) },
    ] {
        let res = engine.search("leaked", &scope, 10, 0).await.unwrap();
        assert!(res.is_empty(), "hidden row leaked through search");
        let res = engine.search("secretkey", &scope, 10, 0).await.unwrap();
        assert!(res.iter().all(|r| r.message_type & HIDDEN_KEYS == 0));
        assert!(!res.is_empty());
    }

    // Becoming hidden later blanks the text (and FTS entry) too.
    users.update_message_type("bob", 1, firefly_protos::MESSAGE_TYPE_HIDDEN).await.unwrap();
    assert_eq!(fts_hits(&pool, "user_messages", "dm").await, 0);
}

#[tokio::test]
async fn migration_fast_path_runs_new_migrations_and_v7_purges_hidden_text() {
    let pool = memory_pool().await;
    run_migrations(&pool).await.unwrap();
    let max: i64 = count(&pool, "SELECT max(version) FROM _schema_migrations").await;
    assert_eq!(max, LATEST_MIGRATION_VERSION);

    // Simulate a database upgraded before v7: hidden key material already indexed.
    sqlx::query("DELETE FROM _schema_migrations WHERE version = 7").execute(&pool).await.unwrap();
    for id in 0..1100i64 {
        sqlx::query("INSERT INTO group_messages(id,group_id,by,message,channel_id,epoch,message_type,text) VALUES(?,9,'a',X'00',1,1,6,'historykey deadbeef')")
            .bind(id).execute(&pool).await.unwrap();
    }
    sqlx::query("INSERT INTO group_messages(id,group_id,by,message,channel_id,epoch,message_type,text) VALUES(5000,9,'a',X'00',1,1,0,'normal deadbeef')")
        .execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO user_messages(id,other,sent_by_other,message,message_type,text) VALUES(1,'bob',1,X'00',2,'historykey')")
        .execute(&pool).await.unwrap();
    assert_eq!(fts_hits(&pool, "group_messages", "historykey").await, 1100);

    // v5 (and v6) are recorded, which used to short-circuit everything after them.
    run_migrations(&pool).await.unwrap();

    assert_eq!(count(&pool, "SELECT count(*) FROM _schema_migrations WHERE version = 7").await, 1);
    assert_eq!(count(&pool, "SELECT count(*) FROM group_messages WHERE (message_type & 6) != 0 AND text != ''").await, 0);
    assert_eq!(fts_hits(&pool, "group_messages", "historykey").await, 0);
    assert_eq!(fts_hits(&pool, "user_messages", "historykey").await, 0);
    assert_eq!(fts_hits(&pool, "group_messages", "deadbeef").await, 1);

    // Fast path: a fully migrated DB is a no-op.
    run_migrations(&pool).await.unwrap();
}

#[tokio::test]
async fn save_identity_reports_change_only_when_key_differs() {
    let pool = memory_pool().await;
    let mut db = IdentityDb::new(pool).await.unwrap();
    let addr = ProtocolAddress::new("peer".into(), 1u8.try_into().unwrap());
    let k1 = *IdentityKeyPair::generate(&mut crate::utils::rng()).identity_key();
    let k2 = *IdentityKeyPair::generate(&mut crate::utils::rng()).identity_key();

    assert!(matches!(db.save_identity(&addr, &k1).await.unwrap(), IdentityChange::NewOrUnchanged));
    assert!(matches!(db.save_identity(&addr, &k1).await.unwrap(), IdentityChange::NewOrUnchanged));
    assert!(matches!(db.save_identity(&addr, &k2).await.unwrap(), IdentityChange::ReplacedExisting));

    let mut mem = crate::storage::MemoryIdentityDb::new().await.unwrap();
    assert!(matches!(mem.save_identity(&addr, &k1).await.unwrap(), IdentityChange::NewOrUnchanged));
    assert!(matches!(mem.save_identity(&addr, &k1).await.unwrap(), IdentityChange::NewOrUnchanged));
    assert!(matches!(mem.save_identity(&addr, &k2).await.unwrap(), IdentityChange::ReplacedExisting));
}

#[tokio::test]
async fn old_epoch_states_are_pruned_without_foreign_key_cascade() {
    let pool = memory_pool().await;
    sqlx::query("PRAGMA foreign_keys = OFF").execute(&pool).await.unwrap();
    let store = GroupStateStore::new(pool.clone()).await.unwrap();
    for e in 1..=10u64 {
        store.set_state(b"g", b"state", HashMap::from([(e, vec![e as u8])]), HashMap::from([(1, vec![1])])).await.unwrap();
    }
    let epochs: Vec<i64> = sqlx::query_scalar("SELECT epoch FROM group_epoch_states WHERE id = ? ORDER BY epoch")
        .bind(&b"g"[..]).fetch_all(&pool).await.unwrap();
    assert_eq!(epochs, vec![8, 9, 10]);
    assert_eq!(store.get_max_epoch_id(b"g").await.unwrap(), Some(10));
}

#[tokio::test]
async fn old_epoch_states_are_pruned() {
    let pool = memory_pool().await;
    let store = GroupStateStore::new(pool.clone()).await.unwrap();
    let gid = b"group-a".to_vec();

    // A single write carrying several epochs.
    let inserts: HashMap<u64, Vec<u8>> = (1..=5).map(|e| (e, vec![e as u8])).collect();
    store.set_state(&gid, b"state", inserts, HashMap::new()).await.unwrap();
    let epochs: Vec<i64> = sqlx::query_scalar("SELECT epoch FROM group_epoch_states WHERE id = ? ORDER BY epoch")
        .bind(&gid).fetch_all(&pool).await.unwrap();
    assert_eq!(epochs, vec![3, 4, 5]);

    // Incremental writes, plus an update to an already pruned epoch.
    for e in 6..=10u64 {
        store.set_state(&gid, b"state", HashMap::from([(e, vec![e as u8])]), HashMap::from([(1, vec![1])])).await.unwrap();
    }
    let epochs: Vec<i64> = sqlx::query_scalar("SELECT epoch FROM group_epoch_states WHERE id = ? ORDER BY epoch")
        .bind(&gid).fetch_all(&pool).await.unwrap();
    // With foreign keys on, `INSERT OR REPLACE INTO group_states` cascades and drops older
    // epochs on every write; either way never more than EPOCH_RETENTION survive.
    assert!(epochs.len() as i64 <= EPOCH_RETENTION, "{epochs:?}");
    assert_eq!(epochs.last(), Some(&10));
    assert_eq!(store.get_max_epoch_id(&gid).await.unwrap(), Some(10));
    assert_eq!(store.get_epoch_state(&gid, 10).await.unwrap(), vec![10]);
    assert!(store.get_epoch_state(&gid, 7).await.is_err());

    // Other groups are untouched.
    store.set_state(b"group-b", b"s", HashMap::from([(1, vec![1])]), HashMap::new()).await.unwrap();
    assert_eq!(store.get_max_epoch_id(b"group-b").await.unwrap(), Some(1));
}

async fn seed_group(pool: &SqlitePool, groups: &GroupMessagesStore, kv: &KeyValueStore, store: &GroupStateStore, gid: i64, ident: &[u8]) {
    store.set_state(ident, b"state", HashMap::from([(1, vec![1])]), HashMap::new()).await.unwrap();
    sqlx::query("INSERT INTO group_infos(id,group_state_id,name,description) VALUES(?,?,'n','d')")
        .bind(gid).bind(ident).execute(pool).await.unwrap();
    groups.add(1, gid as u64, 1, 1, "alice", format!("purgeword{gid} hello").as_bytes(), 0).await.unwrap();
    for sql in [
        "INSERT INTO group_history_keys_v3(group_id,start_msg_id,end_msg_id,hash,key,nonce) VALUES(?,1,2,X'01',X'02',X'03')",
        "INSERT INTO group_history_chunk_keys(group_id,start_msg_id,end_msg_id,key,nonce,created_at) VALUES(?,1,2,X'02',X'03',0)",
        "INSERT INTO group_history_disapprovals(group_id,chunk_id,reason,disapproved_at) VALUES(?,1,'r',0)",
        "INSERT INTO group_history_imports(group_id,message_id,chunk_id) VALUES(?,1,1)",
        "INSERT INTO group_history_imported_chunks(group_id,chunk_id,hash) VALUES(?,1,X'01')",
        "INSERT INTO group_pin_updates(group_id,message_id,channel_id,event_id,pinned) VALUES(?,1,1,1,1)",
        "INSERT INTO group_snapshot_inbox(group_id,snapshot_id) VALUES(?,'s')",
        "INSERT INTO favourite_messages(source,message_id,group_id,channel_id,by,message,created_at) VALUES('group',1,?,1,'a',X'00',0)",
    ] {
        sqlx::query(sql).bind(gid).execute(pool).await.unwrap();
    }
    sqlx::query("INSERT INTO receipt_journal(group_id,epoch,state,created_at) VALUES(?,1,'staged',0)")
        .bind(ident).execute(pool).await.unwrap();
    for key in [
        format!("snapshot-join-pending:{gid}"),
        format!("history_export_cursor:{gid}"),
        format!("snapshot-secret:{gid}:1:abc"),
        format!("snapshot-candidate:{gid}:2:abc"),
    ] {
        kv.set(&key, "v").await.unwrap();
    }
}

#[tokio::test]
async fn purge_group_local_state_removes_everything_for_one_group() {
    let pool = memory_pool().await;
    let groups = GroupMessagesStore::new(pool.clone()).await.unwrap();
    let kv = KeyValueStore::new(pool.clone()).await.unwrap();
    let store = GroupStateStore::new(pool.clone()).await.unwrap();
    GroupInfoStore::new(pool.clone()).await.unwrap();

    seed_group(&pool, &groups, &kv, &store, 42, b"ident-42").await;
    seed_group(&pool, &groups, &kv, &store, 4, b"ident-4").await;
    // Prefix lookalike that must survive purging group 4.
    kv.set("snapshot-secret:42:9:x", "keep").await.unwrap();

    store.purge_group_local_state(b"ident-4").await.unwrap();

    let tables_by_num = [
        "group_messages", "group_history_keys_v3", "group_history_chunk_keys",
        "group_history_disapprovals", "group_history_imports", "group_history_imported_chunks",
        "group_pin_updates", "group_snapshot_inbox", "favourite_messages",
    ];
    for t in tables_by_num {
        let gone = count(&pool, &format!("SELECT count(*) FROM {t} WHERE group_id = 4")).await;
        let kept = count(&pool, &format!("SELECT count(*) FROM {t} WHERE group_id = 42")).await;
        assert_eq!((gone, kept > 0), (0, true), "table {t}");
    }
    for (t, c) in [("group_states", "id"), ("group_epoch_states", "id"), ("receipt_journal", "group_id"), ("group_infos", "group_state_id")] {
        let gone: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {t} WHERE {c} = ?")).bind(&b"ident-4"[..]).fetch_one(&pool).await.unwrap();
        let kept: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {t} WHERE {c} = ?")).bind(&b"ident-42"[..]).fetch_one(&pool).await.unwrap();
        assert_eq!((gone, kept > 0), (0, true), "table {t}");
    }
    let keys: Vec<String> = sqlx::query("SELECT key FROM key_value_store ORDER BY key").fetch_all(&pool).await.unwrap()
        .iter().map(|r| r.get::<String, _>(0)).collect();
    assert!(keys.iter().all(|k| !k.contains(":4:") && !k.ends_with(":4")), "{keys:?}");
    assert!(keys.contains(&"snapshot-secret:42:9:x".to_string()));
    assert!(keys.contains(&"snapshot-join-pending:42".to_string()));

    // FTS entries for the purged group are gone.
    assert_eq!(fts_hits(&pool, "group_messages", "purgeword4").await, 0);
    assert_eq!(fts_hits(&pool, "group_messages", "purgeword42").await, 1);

    // With an explicit numeric id the purge works even after group_infos is gone.
    sqlx::query("DELETE FROM group_infos WHERE id = 42").execute(&pool).await.unwrap();
    store.purge_group_local_state_with_id(42, b"ident-42").await.unwrap();
    assert_eq!(count(&pool, "SELECT count(*) FROM group_messages").await, 0);
    assert_eq!(count(&pool, "SELECT count(*) FROM group_states").await, 0);
}

#[tokio::test]
async fn conversation_settings_keep_high_bits() {
    let pool = memory_pool().await;
    let store = crate::db::conversations::ConversationStore::new(pool).await.unwrap();
    let value = (1u64 << 63) | (1u64 << 40) | 5;
    store
        .set_conversation("bob", crate::db::conversations::ConversationSettings::new(value))
        .await
        .unwrap();
    let got = store.get_conversation("bob").await.unwrap().unwrap();
    assert_eq!(got.bits(), value);
}

#[tokio::test]
async fn departure_store_records_latest_departure_and_is_purged() {
    use crate::db::group_stores::GroupDepartureStore;
    use crate::storage::GroupDepartureStorage;
    use firefly_core::Departure;

    let pool = memory_pool().await;
    let store = GroupDepartureStore::new(pool.clone()).await.unwrap();
    assert_eq!(
        count(&pool, "SELECT count(*) FROM _schema_migrations WHERE version = 8").await,
        1
    );
    let dep = |u: &str, re_addable, epoch| Departure { username: u.into(), re_addable, epoch };

    // Fail closed without a record.
    assert_eq!(store.lookup(1, "bob").await.unwrap(), None);
    assert!(!store.is_re_addable(1, "bob").await.unwrap());

    store.record(1, &dep("bob", true, 5)).await.unwrap();
    assert!(store.is_re_addable(1, "bob").await.unwrap());
    // Scoped per group.
    assert!(!store.is_re_addable(2, "bob").await.unwrap());

    // A later kick overrides the stale departure...
    store.record(1, &dep("bob", false, 8)).await.unwrap();
    assert_eq!(store.lookup(1, "bob").await.unwrap(), Some(false));
    // ...and an out-of-order earlier record cannot resurrect it.
    store.record(1, &dep("bob", true, 6)).await.unwrap();
    assert!(!store.is_re_addable(1, "bob").await.unwrap());
    // A later stale departure (after a legit re-add) is re-addable again.
    store.record(1, &dep("bob", true, 12)).await.unwrap();
    assert!(store.is_re_addable(1, "bob").await.unwrap());

    store.record(2, &dep("carol", true, 1)).await.unwrap();
    let states = GroupStateStore::new(pool.clone()).await.unwrap();
    GroupInfoStore::new(pool.clone()).await.unwrap();
    sqlx::query("INSERT INTO group_infos(id,group_state_id,name,description) VALUES(1,X'01','n','d')")
        .execute(&pool).await.unwrap();
    states.purge_group_local_state(&[1u8]).await.unwrap();
    assert_eq!(store.lookup(1, "bob").await.unwrap(), None);
    assert!(store.is_re_addable(2, "carol").await.unwrap());
}

#[tokio::test]
async fn migration_v8_applies_on_v7_database() {
    let pool = memory_pool().await;
    run_migrations(&pool).await.unwrap();
    sqlx::query("DROP TABLE group_departed_members").execute(&pool).await.unwrap();
    sqlx::query("DELETE FROM _schema_migrations WHERE version = 8").execute(&pool).await.unwrap();
    run_migrations(&pool).await.unwrap();
    assert_eq!(
        count(&pool, "SELECT count(*) FROM sqlite_master WHERE name = 'group_departed_members'").await,
        1
    );
    run_migrations(&pool).await.unwrap();
}

#[tokio::test]
async fn key_value_departure_store_matches_sqlite_semantics() {
    use crate::storage::{GroupDepartureStorage, KeyValueGroupDepartureStore, MemoryKeyValueStore};
    use firefly_core::Departure;
    use std::sync::Arc;

    let store = KeyValueGroupDepartureStore::new(Arc::new(MemoryKeyValueStore::new()));
    let dep = |u: &str, re_addable, epoch| Departure { username: u.into(), re_addable, epoch };
    assert_eq!(store.lookup(1, "bob").await.unwrap(), None);
    store.record(1, &dep("bob", true, 5)).await.unwrap();
    assert!(store.is_re_addable(1, "bob").await.unwrap());
    store.record(1, &dep("bob", false, 8)).await.unwrap();
    store.record(1, &dep("bob", true, 6)).await.unwrap();
    assert_eq!(store.lookup(1, "bob").await.unwrap(), Some(false));
}
