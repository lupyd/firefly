use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;
use tokio::sync::mpsc;
use sqlx::SqlitePool;

use firefly_client::callbacks::FireflyWsClientCallback;
use firefly_client::db::{
    group_messages::{GroupMessage, GroupMessagesStore},
    messages::{MessagesStore, UserMessage},
};
use firefly_client::websocket::FireflyWsClient;
use firefly_protos::{
    deserialize_proto, serialize_proto,
    firefly::{self, mod_GroupMessageInner, mod_UserMessageInner, MessagePayload},
    MESSAGE_TYPE_NORMAL, MESSAGE_TYPE_PINNED,
};

#[tokio::test]
async fn test_pinned_bitflags_and_proto_serialization() {
    // 1. Bitflag constants
    assert_eq!(MESSAGE_TYPE_NORMAL, 0);
    assert_eq!(MESSAGE_TYPE_PINNED, 1);
    assert_eq!(MESSAGE_TYPE_PINNED & (1 << 0), 1);

    // Multiple flags combination
    let custom_flag: u32 = 1 << 1;
    let combined = MESSAGE_TYPE_PINNED | custom_flag;
    assert_ne!(combined & MESSAGE_TYPE_PINNED, 0);
    assert_ne!(combined & custom_flag, 0);
    assert_eq!(combined & (1 << 2), 0);

    // 2. GroupMessageInner serialization with message_type
    let group_inner = firefly::GroupMessageInner {
        channelId: 42,
        message: mod_GroupMessageInner::OneOfmessage::messagePayload(MessagePayload {
            text: "Important announcement!".into(),
            files: None,
            ext: firefly::mod_MessagePayload::OneOfext::None,
            message_type: MESSAGE_TYPE_PINNED,
        }),
        message_type: MESSAGE_TYPE_PINNED,
    };

    let serialized = serialize_proto(&group_inner).expect("serialize group_inner");
    let deserialized: firefly::GroupMessageInner =
        deserialize_proto(&serialized).expect("deserialize group_inner");

    assert_eq!(deserialized.channelId, 42);
    assert_eq!(deserialized.message_type, MESSAGE_TYPE_PINNED);
    if let mod_GroupMessageInner::OneOfmessage::messagePayload(payload) = deserialized.message {
        assert_eq!(payload.text, "Important announcement!");
        assert_eq!(payload.message_type, MESSAGE_TYPE_PINNED);
    } else {
        panic!("expected messagePayload");
    }

    // 3. UserMessageInner serialization with message_type
    let user_inner = firefly::UserMessageInner {
        message: mod_UserMessageInner::OneOfmessage::messagePayload(MessagePayload {
            text: "Pinned 1:1 note".into(),
            files: None,
            ext: firefly::mod_MessagePayload::OneOfext::None,
            message_type: MESSAGE_TYPE_PINNED,
        }),
        nonce: 12345,
        message_type: MESSAGE_TYPE_PINNED,
    };

    let serialized_user = serialize_proto(&user_inner).expect("serialize user_inner");
    let deserialized_user: firefly::UserMessageInner =
        deserialize_proto(&serialized_user).expect("deserialize user_inner");

    assert_eq!(deserialized_user.message_type, MESSAGE_TYPE_PINNED);
    assert_eq!(deserialized_user.nonce, 12345);
    if let mod_UserMessageInner::OneOfmessage::messagePayload(payload) = deserialized_user.message {
        assert_eq!(payload.text, "Pinned 1:1 note");
        assert_eq!(payload.message_type, MESSAGE_TYPE_PINNED);
    } else {
        panic!("expected messagePayload");
    }

    // 4. Backwards compatibility: empty proto payload defaults to 0
    let empty_bytes = serialize_proto(&firefly::GroupMessageInner {
        channelId: 1,
        message: mod_GroupMessageInner::OneOfmessage::None,
        message_type: 0,
    })
    .expect("serialize empty");
    let parsed: firefly::GroupMessageInner = deserialize_proto(&empty_bytes).unwrap();
    assert_eq!(parsed.message_type, 0);
}

#[tokio::test]
async fn test_db_group_pinned_messages_crud_and_re_encryption_dedup() {
    let pool = SqlitePool::connect(":memory:").await.unwrap();
    let store = GroupMessagesStore::new(pool).await.unwrap();

    let group_id = 1001u64;

    // Initially no pinned messages
    let pinned = store.get_pinned_messages(group_id).await.unwrap();
    assert!(pinned.is_empty());

    // Add normal message (message_type = 0)
    let normal_inner = firefly::GroupMessageInner {
        channelId: 0,
        message: mod_GroupMessageInner::OneOfmessage::messagePayload(MessagePayload {
            text: "Normal chat".into(),
            files: None,
            ext: firefly::mod_MessagePayload::OneOfext::None,
            message_type: 0,
        }),
        message_type: 0,
    };
    let normal_bytes = serialize_proto(&normal_inner).unwrap();
    store
        .add(1, group_id, 0, 1, "alice", &normal_bytes, 0)
        .await
        .unwrap();

    // Add pinned message 1 (message_type = MESSAGE_TYPE_PINNED)
    let pinned_inner1 = firefly::GroupMessageInner {
        channelId: 0,
        message: mod_GroupMessageInner::OneOfmessage::messagePayload(MessagePayload {
            text: "Group Rules: Be kind".into(),
            files: None,
            ext: firefly::mod_MessagePayload::OneOfext::None,
            message_type: MESSAGE_TYPE_PINNED,
        }),
        message_type: MESSAGE_TYPE_PINNED,
    };
    let pinned_bytes1 = serialize_proto(&pinned_inner1).unwrap();
    store
        .add(2, group_id, 0, 1, "alice", &pinned_bytes1, MESSAGE_TYPE_PINNED)
        .await
        .unwrap();

    // Add pinned message 2
    let pinned_inner2 = firefly::GroupMessageInner {
        channelId: 0,
        message: mod_GroupMessageInner::OneOfmessage::messagePayload(MessagePayload {
            text: "Meeting at 3PM UTC".into(),
            files: None,
            ext: firefly::mod_MessagePayload::OneOfext::None,
            message_type: MESSAGE_TYPE_PINNED,
        }),
        message_type: MESSAGE_TYPE_PINNED,
    };
    let pinned_bytes2 = serialize_proto(&pinned_inner2).unwrap();
    store
        .add(3, group_id, 0, 1, "bob", &pinned_bytes2, MESSAGE_TYPE_PINNED)
        .await
        .unwrap();

    // Query pinned messages: should return exactly the 2 pinned messages in ASC order
    let pinned = store.get_pinned_messages(group_id).await.unwrap();
    assert_eq!(pinned.len(), 2);
    assert_eq!(pinned[0].id, 2);
    assert_eq!(pinned[1].id, 3);
    assert_ne!(pinned[0].message_type & MESSAGE_TYPE_PINNED, 0);
    assert_ne!(pinned[1].message_type & MESSAGE_TYPE_PINNED, 0);

    // Simulate Re-encryption when a new member is added:
    // Committer reads pinned messages, re-encrypts and re-uploads them under new epoch 2
    // Committer unpins the old message (id 2) so it doesn't duplicate
    store
        .update_message_type(group_id, 2, 0)
        .await
        .unwrap();
    // And adds the re-encrypted message with new id (e.g. id 10) in epoch 2
    store
        .add(10, group_id, 0, 2, "alice", &pinned_bytes1, MESSAGE_TYPE_PINNED)
        .await
        .unwrap();

    // Verify pinned list now has id 3 and id 10, NOT id 2!
    let updated_pinned = store.get_pinned_messages(group_id).await.unwrap();
    assert_eq!(updated_pinned.len(), 2);
    assert_eq!(updated_pinned[0].id, 3);
    assert_eq!(updated_pinned[1].id, 10);

    // Verify unpinning id 3
    store.update_message_type(group_id, 3, 0).await.unwrap();
    let final_pinned = store.get_pinned_messages(group_id).await.unwrap();
    assert_eq!(final_pinned.len(), 1);
    assert_eq!(final_pinned[0].id, 10);
}

#[tokio::test]
async fn test_db_user_pinned_messages_crud() {
    let pool = SqlitePool::connect(":memory:").await.unwrap();
    let store = MessagesStore::new(pool).await.unwrap();

    let other = "bob";

    // Add unpinned user message
    store
        .insert_user_message(UserMessage {
            id: 100,
            other: other.to_string(),
            message: b"hey".to_vec(),
            sent_by_other: false,
            message_type: MESSAGE_TYPE_NORMAL,
        })
        .await
        .unwrap();

    // Add pinned user message
    store
        .insert_user_message(UserMessage {
            id: 101,
            other: other.to_string(),
            message: b"WiFi password is 1234".to_vec(),
            sent_by_other: false,
            message_type: MESSAGE_TYPE_PINNED,
        })
        .await
        .unwrap();

    // Add another pinned user message with bitflag combined
    let multi_flag = MESSAGE_TYPE_PINNED | (1 << 3);
    store
        .insert_user_message(UserMessage {
            id: 102,
            other: other.to_string(),
            message: b"Door code is 5678".to_vec(),
            sent_by_other: true,
            message_type: multi_flag,
        })
        .await
        .unwrap();

    let pinned = store.get_pinned_messages_of(other).await.unwrap();
    assert_eq!(pinned.len(), 2);
    assert_eq!(pinned[0].id, 101);
    assert_eq!(pinned[0].message_type, MESSAGE_TYPE_PINNED);
    assert_eq!(pinned[1].id, 102);
    assert_eq!(pinned[1].message_type, multi_flag);

    // Unpin message 101
    store.update_message_type(other, 101, 0).await.unwrap();
    let remaining = store.get_pinned_messages_of(other).await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].id, 102);

    // Re-pin message 101
    store.update_message_type(other, 101, MESSAGE_TYPE_PINNED).await.unwrap();
    let re_pinned = store.get_pinned_messages_of(other).await.unwrap();
    assert_eq!(re_pinned.len(), 2);
}

struct TestCallbacks {
    name: String,
    token: String,
    message_tx: mpsc::Sender<UserMessage>,
    group_message_tx: mpsc::Sender<GroupMessage>,
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
}

async fn setup_server() -> Option<(String, String)> {
    let _ = std::fs::create_dir_all("/tmp/firefly");
    dotenv::from_filename(".env.test").ok();
    dotenv::dotenv().ok();
    if let Ok(base_url) = std::env::var("FIREFLY_BASE_URL") {
        let ws_url = std::env::var("FIREFLY_WS_URL").unwrap_or_else(|_| {
            base_url
                .replace("http://", "ws://")
                .replace("https://", "wss://")
        });
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(1000))
            .build()
            .ok()?;
        if client.get(format!("{}/jwks.json", base_url)).send().await.is_err() {
            println!("Skipping live server test: FIREFLY_BASE_URL ({}) is unreachable.", base_url);
            return None;
        }
        firefly_client::init_logger("/tmp/firefly/test_pinned.log".to_string());
        Some((base_url, ws_url))
    } else {
        None
    }
}

async fn wait_for_init(client: &FireflyWsClient) -> anyhow::Result<()> {
    for _ in 0..60 {
        if client.is_initialized() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(anyhow::anyhow!("Client timeout waiting for initialization"))
}

#[tokio::test]
async fn test_integration_pinned_messages_re_encryption_on_member_add() {
    let (base_url, ws_url) = match setup_server().await {
        Some(urls) => urls,
        None => {
            println!("Skipping live server test: FIREFLY_BASE_URL is not set.");
            return;
        }
    };

    let test_run_id = rand::random::<u32>();
    let test_dir = format!("/tmp/firefly/pinned_{}", test_run_id);
    let _ = std::fs::create_dir_all(&test_dir);

    let alice_name = format!("alice_pin_{}", test_run_id);
    let bob_name = format!("bob_pin_{}", test_run_id);

    let (alice_msg_tx, _alice_msg_rx) = mpsc::channel(100);
    let (alice_gmsg_tx, _alice_gmsg_rx) = mpsc::channel(100);
    let alice_callbacks = TestCallbacks {
        name: alice_name.clone(),
        token: alice_name.clone(),
        message_tx: alice_msg_tx,
        group_message_tx: alice_gmsg_tx,
    };

    let alice_client = FireflyWsClient::create(
        base_url.clone(),
        ws_url.clone(),
        1000,
        Box::new(alice_callbacks),
        format!("{}/alice.db", test_dir),
        5000,
    )
    .await
    .expect("Alice client create");
    let alice_client = Arc::new(alice_client);

    let alice_init = alice_client.clone();
    tokio::spawn(async move {
        let _ = alice_init.initialize_with_retrying().await;
    });
    wait_for_init(&alice_client).await.expect("Alice wait_for_init");

    // Alice creates a group
    let group = alice_client
        .create_group("Pinned Test Group".into(), "Desc".into(), 0)
        .await
        .expect("Alice create group");

    // Alice sends a PINNED group message
    let inner_pinned = firefly::GroupMessageInner {
        channelId: 0,
        message: mod_GroupMessageInner::OneOfmessage::messagePayload(MessagePayload {
            text: "Group Mission Statement".into(),
            files: None,
            ext: firefly::mod_MessagePayload::OneOfext::None,
            message_type: MESSAGE_TYPE_PINNED,
        }),
        message_type: MESSAGE_TYPE_PINNED,
    };
    let payload = serialize_proto(&inner_pinned).unwrap();

    let uploaded_id = alice_client
        .encrypt_and_send_group_pinned(group.id, payload.to_vec())
        .await
        .expect("Alice send pinned message");

    assert!(uploaded_id > 0);

    // Verify Alice has 1 pinned message locally
    let alice_pinned = alice_client
        .group_message_store()
        .get_pinned_messages(group.id)
        .await
        .expect("Alice get_pinned_messages");
    assert_eq!(alice_pinned.len(), 1);
    assert_ne!(alice_pinned[0].message_type & MESSAGE_TYPE_PINNED, 0);

    // Setup Bob
    let (bob_msg_tx, _bob_msg_rx) = mpsc::channel(100);
    let (bob_gmsg_tx, mut bob_gmsg_rx) = mpsc::channel(100);
    let bob_callbacks = TestCallbacks {
        name: bob_name.clone(),
        token: bob_name.clone(),
        message_tx: bob_msg_tx,
        group_message_tx: bob_gmsg_tx,
    };

    let bob_client = FireflyWsClient::create(
        base_url.clone(),
        ws_url.clone(),
        1000,
        Box::new(bob_callbacks),
        format!("{}/bob.db", test_dir),
        5000,
    )
    .await
    .expect("Bob client create");
    let bob_client = Arc::new(bob_client);

    let bob_init = bob_client.clone();
    tokio::spawn(async move {
        let _ = bob_init.initialize_with_retrying().await;
    });
    wait_for_init(&bob_client).await.expect("Bob wait_for_init");

    bob_client.check_setup().await.expect("Bob check_setup");

    // Alice adds Bob to the group -> triggers re_encrypt_and_send_pinned_messages!
    alice_client
        .add_group_member(group.id, bob_name.clone(), 0)
        .await
        .expect("Alice add Bob");

    // Bob syncs to join group
    bob_client.check_setup().await.expect("Bob check_setup after added");

    // Bob should receive the re-encrypted pinned message in the new epoch!
    let received = tokio::time::timeout(Duration::from_secs(15), bob_gmsg_rx.recv())
        .await
        .expect("Timeout waiting for re-encrypted pinned message on Bob")
        .expect("Channel closed");

    assert_eq!(received.group_id, group.id);
    assert_ne!(received.message_type & MESSAGE_TYPE_PINNED, 0);

    let bob_pinned = bob_client
        .group_message_store()
        .get_pinned_messages(group.id)
        .await
        .expect("Bob get_pinned_messages");
    assert_eq!(bob_pinned.len(), 1);
    assert_ne!(bob_pinned[0].message_type & MESSAGE_TYPE_PINNED, 0);

    let _ = std::fs::remove_dir_all(test_dir);
}
