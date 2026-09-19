use async_trait::async_trait;
use firefly_client::callbacks::{FireflyWsClientCallback, GroupHistorySignal};
use firefly_client::db::{
    group_messages::{GroupMessage, GroupMessagesStore},
    history_keys::HistoryKeysStore,
    messages::UserMessage,
    setup_pool,
};
use firefly_client::history::{
    compute_unencrypted_hash, decrypt_and_unpack_chunk, pack_messages_into_chunk,
};
use firefly_protos::firefly;
use quick_protobuf::{deserialize_from_slice, serialize_into_vec};
use tokio::sync::mpsc;

#[allow(dead_code)]
struct TestCallbacks {
    name: String,
    token: String,
    message_tx: mpsc::Sender<UserMessage>,
    group_message_tx: mpsc::Sender<GroupMessage>,
    history_signal_tx: mpsc::Sender<GroupHistorySignal>,
}

#[async_trait]
impl FireflyWsClientCallback for TestCallbacks {
    fn name(&self) -> &str {
        &self.name
    }

    async fn get_access_token(&self) -> Option<String> {
        Some(self.token.clone())
    }

    async fn on_message(&self, message: UserMessage) {
        let _ = self.message_tx.send(message).await;
    }

    async fn on_group_message(&self, group_message: GroupMessage) {
        let _ = self.group_message_tx.send(group_message).await;
    }

    async fn on_group_history_signal(&self, signal: GroupHistorySignal) {
        let _ = self.history_signal_tx.send(signal).await;
    }
}

#[tokio::test]
async fn test_history_chunk_pack_unpack_integrity() -> anyhow::Result<()> {
    // Test end-to-end packing, AES-256-GCM encryption, SHA-256 hash verification, and unpacking
    let messages = vec![
        GroupMessage {
            id: 1,
            group_id: 100,
            by: "alice".into(),
            message: b"first history message".to_vec(),
            channel_id: 1,
            epoch: 1,
            message_type: 0,
        },
        GroupMessage {
            id: 2,
            group_id: 100,
            by: "bob".into(),
            message: b"second history message".to_vec(),
            channel_id: 1,
            epoch: 1,
            message_type: 0,
        },
        GroupMessage {
            id: 3,
            group_id: 100,
            by: "charlie".into(),
            message: b"third history message".to_vec(),
            channel_id: 1,
            epoch: 2,
            message_type: 0,
        },
    ];

    let packed = pack_messages_into_chunk(&messages)?;
    assert_eq!(packed.start_msg_id, 1);
    assert_eq!(packed.end_msg_id, 3);
    assert_eq!(packed.msg_count, 3);

    // Verify local SHA-256 hash matches
    let computed_hash = compute_unencrypted_hash(&messages)?;
    assert_eq!(packed.unencrypted_hash, computed_hash);

    // Store keys in HistoryKeysStore
    let pool = setup_pool("sqlite::memory:", 1).await?;
    let keys_store = HistoryKeysStore::new(pool.clone()).await?;
    keys_store
        .save_chunk_key(
            100,
            packed.start_msg_id,
            packed.end_msg_id,
            &packed.key,
            &packed.nonce,
        )
        .await?;

    let (retrieved_key, retrieved_nonce) = keys_store
        .get_chunk_key(100, packed.start_msg_id, packed.end_msg_id)
        .await?
        .expect("key must exist");
    assert_eq!(retrieved_key, packed.key);
    assert_eq!(retrieved_nonce, packed.nonce);

    // Decrypt and unpack using retrieved key
    let unpacked = decrypt_and_unpack_chunk(&packed.blob, &retrieved_key, &packed.unencrypted_hash)?;
    assert_eq!(unpacked.len(), 3);
    assert_eq!(unpacked[0].id, 1);
    assert_eq!(unpacked[0].message, b"first history message".to_vec());
    assert_eq!(unpacked[1].id, 2);
    assert_eq!(unpacked[1].message, b"second history message".to_vec());
    assert_eq!(unpacked[2].id, 3);
    assert_eq!(unpacked[2].message, b"third history message".to_vec());

    // Test tamper resistance: tampered hash must fail and record disapproval
    let tampered_hash = vec![0u8; 32];
    let tamper_result = decrypt_and_unpack_chunk(&packed.blob, &retrieved_key, &tampered_hash);
    assert!(tamper_result.is_err());

    // Record disapproval
    keys_store.record_disapproval(100, 555, "tampered hash").await?;
    assert!(keys_store.is_chunk_disapproved(100, 555).await?);
    assert!(!keys_store.is_chunk_disapproved(100, 556).await?);

    Ok(())
}

#[tokio::test]
async fn test_history_keys_proto_hidden_sharing() -> anyhow::Result<()> {
    // Verify protobuf serialization & deserialization of GroupHistoryKeysPayload
    let chunk_key = firefly::GroupHistoryChunkKey {
        group_id: 100,
        start_msg_id: 1,
        end_msg_id: 50,
        key: vec![42u8; 32].into(),
        nonce: vec![24u8; 12].into(),
    };

    let payload = firefly::GroupHistoryKeysPayload {
        keys: vec![chunk_key],
    };

    let serialized = serialize_into_vec(&payload)?;
    let hex_encoded = hex::encode(&serialized);

    let decoded_bytes = hex::decode(&hex_encoded)?;
    let deserialized: firefly::GroupHistoryKeysPayload = deserialize_from_slice(&decoded_bytes)?;
    assert_eq!(deserialized.keys.len(), 1);
    assert_eq!(deserialized.keys[0].group_id, 100);
    assert_eq!(deserialized.keys[0].start_msg_id, 1);
    assert_eq!(deserialized.keys[0].end_msg_id, 50);
    assert_eq!(deserialized.keys[0].key.as_ref(), &[42u8; 32]);
    assert_eq!(deserialized.keys[0].nonce.as_ref(), &[24u8; 12]);

    // Verify bitmask flags for hidden history keys
    let message_type = firefly_protos::MESSAGE_TYPE_HIDDEN | firefly_protos::MESSAGE_TYPE_HISTORY_KEYS;
    assert_ne!(message_type & firefly_protos::MESSAGE_TYPE_HIDDEN, 0);
    assert_ne!(message_type & firefly_protos::MESSAGE_TYPE_HISTORY_KEYS, 0);

    // Verify messages store filters out hidden messages in visible_messages
    let pool = setup_pool("sqlite::memory:", 1).await?;
    let gms = GroupMessagesStore::new(pool).await?;

    // Normal message
    gms.add(1, 100, 0, 1, "alice", b"visible msg", 0).await?;
    // Hidden history keys message
    gms.add(2, 100, 0, 1, "alice", b"hidden keys msg", message_type).await?;
    // Another normal message
    gms.add(3, 100, 0, 1, "bob", b"another visible", 0).await?;

    let all = gms.get_range(100, 1, 3).await?;
    let visible = gms.visible_messages(all).await?;
    assert_eq!(visible.len(), 2);
    assert_eq!(visible[0].id, 1);
    assert_eq!(visible[1].id, 3);

    // Verify get_range includes all messages in range
    let range = gms.get_range(100, 1, 3).await?;
    assert_eq!(range.len(), 3);

    Ok(())
}

#[tokio::test]
async fn test_backwards_compatibility_and_database_upgrade() -> anyhow::Result<()> {

    // Simulate an older device running with legacy v1 SQLite schema
    let pool = setup_pool("sqlite::memory:", 1).await?;

    // Create legacy v1 tables manually (before v2 existed)
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS _schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at INTEGER NOT NULL
        );
        INSERT INTO _schema_migrations (version, name, applied_at) VALUES (1, 'standard_v1', 1700000000000);

        CREATE TABLE IF NOT EXISTS user_messages (
            id INTEGER NOT NULL,
            other TEXT NOT NULL,
            sent_by_other BOOLEAN NOT NULL,
            message BLOB NOT NULL,
            message_type INTEGER NOT NULL DEFAULT 0,
            text TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS group_messages (
            id INTEGER NOT NULL,
            group_id INTEGER NOT NULL,
            by TEXT NOT NULL,
            message BLOB NOT NULL,
            channel_id INTEGER NOT NULL,
            epoch INTEGER NOT NULL DEFAULT 0,
            message_type INTEGER NOT NULL DEFAULT 0,
            text TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (group_id, id)
        );
        "#,
    )
    .execute(&pool)
    .await?;

    // Insert pre-existing messages on the older device
    sqlx::query("INSERT INTO user_messages (id, other, sent_by_other, message, message_type, text) VALUES (1, 'bob', 0, X'1234', 0, 'hello from older device')")
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO group_messages (id, group_id, by, message, channel_id, epoch, message_type, text) VALUES (10, 42, 'alice', X'5678', 1, 1, 0, 'group message from older device')")
        .execute(&pool)
        .await?;

    // Run migrations as happens when the app upgrades
    firefly_client::db::migrations::run_migrations(&pool).await?;

    // 1. Verify pre-existing data is 100% preserved
    let user_msg_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM user_messages WHERE other = 'bob'")
        .fetch_one(&pool)
        .await?;
    assert_eq!(user_msg_count, 1);

    let grp_msg: (i64, String) = sqlx::query_as("SELECT id, text FROM group_messages WHERE group_id = 42 AND id = 10")
        .fetch_one(&pool)
        .await?;
    assert_eq!(grp_msg.0, 10);
    assert_eq!(grp_msg.1, "group message from older device");

    // 2. Verify _schema_migrations now contains version 2
    let has_v2: Option<i64> = sqlx::query_scalar("SELECT 1 FROM _schema_migrations WHERE version = 2")
        .fetch_optional(&pool)
        .await?;
    assert!(has_v2.is_some(), "Migration v2 must be recorded");

    // 3. Verify new v2 stores can be initialized and used on the upgraded database
    let keys_store = HistoryKeysStore::new(pool.clone()).await?;
    keys_store
        .save_chunk_key(42, 1, 10, &[7u8; 32], &[8u8; 12])
        .await?;
    let key = keys_store.get_chunk_key(42, 1, 10).await?.expect("key must be present");
    assert_eq!(key.0, vec![7u8; 32]);
    assert_eq!(key.1, vec![8u8; 12]);

    // 4. Verify fast-path exit: subsequent migration runs return immediately without errors
    firefly_client::db::migrations::run_migrations(&pool).await?;

    Ok(())
}

