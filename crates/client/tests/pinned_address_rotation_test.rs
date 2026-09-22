use async_trait::async_trait;
use firefly_client::callbacks::{FireflyWsClientCallback, GroupHistorySignal};
use firefly_client::db::{
    group_messages::GroupMessage,
    messages::{MessagesStore, UserMessage},
};
use firefly_client::websocket::FireflyWsClient;
use firefly_protos::{
    deserialize_proto, serialize_proto,
    firefly::{self, mod_GroupMessageInner, MessagePayload},
    MESSAGE_TYPE_PINNED,
};
use sqlx::SqlitePool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

fn extract_user_message_text(msg_bytes: &[u8]) -> String {
    if let Ok(inner) = deserialize_proto::<firefly::UserMessageInner>(msg_bytes) {
        if let firefly::mod_UserMessageInner::OneOfmessage::messagePayload(p) = inner.message {
            return p.text.to_string();
        }
    }
    String::from_utf8_lossy(msg_bytes).to_string()
}

fn extract_group_message_text(msg_bytes: &[u8]) -> String {
    if let Ok(inner) = deserialize_proto::<firefly::GroupMessageInner>(msg_bytes) {
        if let firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(p) = inner.message {
            return p.text.to_string();
        }
    }
    String::from_utf8_lossy(msg_bytes).to_string()
}

struct TestCallbacks {
    name: String,
    token: String,
    message_tx: mpsc::Sender<UserMessage>,
    group_message_tx: mpsc::Sender<GroupMessage>,
    messages_store: MessagesStore,
    client: Arc<tokio::sync::RwLock<Option<Arc<FireflyWsClient>>>>,
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
        let _ = self.messages_store.insert_user_message(message.clone()).await;
        if message.sent_by_other {
            let _ = self.message_tx.send(message).await;
        }
    }

    async fn on_group_message(&self, group_message: GroupMessage) {
        let _ = self.group_message_tx.send(group_message).await;
    }

    async fn on_group_history_signal(&self, signal: GroupHistorySignal) {
        if let Some(client) = self.client.read().await.as_ref() {
            let _ = client.handle_group_history_signal(signal).await;
        }
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
        firefly_client::init_logger("/tmp/firefly/test_pinned_rotation.log".to_string());
        Some((base_url, ws_url))
    } else {
        println!("Skipping integration test: FIREFLY_BASE_URL is not set.");
        None
    }
}

fn cleanup_dir(dir: &str) {
    if let Err(e) = std::fs::remove_dir_all(dir) {
        if e.kind() != std::io::ErrorKind::NotFound {
            log::warn!("Failed to cleanup test dir {}: {:?}", dir, e);
        }
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

/// Test 1: 1:1 chat pinned messages persist across address rotation (5 devices -> 6th device rotates device 1 out)
#[tokio::test]
async fn test_pinned_messages_persist_across_1to1_address_rotation() {
    let (base_url, ws_url) = match setup_server().await {
        Some(urls) => urls,
        None => return,
    };

    let test_run_id = rand::random::<u32>();
    let test_dir = format!("/tmp/firefly/pinned_rot_1to1_{}", test_run_id);
    let _ = std::fs::create_dir_all(&test_dir);

    let alice_name = format!("alice_prot_{}", test_run_id);
    let bob_name = format!("bob_prot_{}", test_run_id);

    let mut alice_clients = Vec::new();
    let mut alice_receivers = Vec::new();
    let mut alice_stores = Vec::new();

    // 1. Create 5 Alice devices (max allowed before rotation)
    println!("Creating 5 Alice devices...");
    for i in 1..=5 {
        let (msg_tx, msg_rx) = mpsc::channel(100);
        let (gmsg_tx, _gmsg_rx) = mpsc::channel(100);

        let mem_pool = SqlitePool::connect(":memory:").await.unwrap();
        let store = MessagesStore::new(mem_pool).await.unwrap();
        alice_stores.push(store.clone());

        let client_holder = Arc::new(tokio::sync::RwLock::new(None));
        let callbacks = TestCallbacks {
            name: alice_name.clone(),
            token: alice_name.clone(),
            message_tx: msg_tx,
            group_message_tx: gmsg_tx,
            messages_store: store,
            client: client_holder.clone(),
        };

        let db = format!("{}/alice_{}.db", test_dir, i);

        let client = FireflyWsClient::create(
            base_url.clone(),
            ws_url.clone(),
            1000,
            Box::new(callbacks),
            db.clone(),
            5000,
        )
        .await
        .expect("Failed to create Alice client");
        let client = Arc::new(client);

        let client_init = client.clone();
        tokio::spawn(async move {
            let _ = client_init.initialize_with_retrying().await;
        });

        wait_for_init(&client)
            .await
            .expect(&format!("Alice client {} failed to initialize", i));

        alice_clients.push(client);
        alice_receivers.push(msg_rx);

        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // 2. Create Bob
    println!("Creating Bob device...");
    let (bob_msg_tx, _bob_msg_rx) = mpsc::channel(100);
    let (bob_gmsg_tx, _bob_gmsg_rx) = mpsc::channel(100);
    let bob_mem_pool = SqlitePool::connect(":memory:").await.unwrap();
    let bob_store = MessagesStore::new(bob_mem_pool).await.unwrap();
    let bob_holder = Arc::new(tokio::sync::RwLock::new(None));
    let bob_callbacks = TestCallbacks {
        name: bob_name.clone(),
        token: bob_name.clone(),
        message_tx: bob_msg_tx,
        group_message_tx: bob_gmsg_tx,
        messages_store: bob_store.clone(),
        client: bob_holder.clone(),
    };
    let bob_db = format!("{}/bob.db", test_dir);
    let bob_client = FireflyWsClient::create(
        base_url.clone(),
        ws_url.clone(),
        1000,
        Box::new(bob_callbacks),
        bob_db.clone(),
        5000,
    )
    .await
    .expect("Failed to create Bob client");
    let bob_client = Arc::new(bob_client);
    *bob_holder.write().await = Some(bob_client.clone());
    let bob_init = bob_client.clone();
    tokio::spawn(async move {
        let _ = bob_init.initialize_with_retrying().await;
    });

    wait_for_init(&bob_client)
        .await
        .expect("Bob client failed to initialize");

    tokio::time::sleep(Duration::from_secs(2)).await;

    // 3. Bob sends a PINNED 1:1 message to Alice
    println!("Bob sending PINNED message 1 to Alice...");
    let sent_msg1 = bob_client
        .encrypt_and_send_pinned(alice_name.clone(), b"Secret note #1".to_vec())
        .await
        .expect("Bob failed to send pinned message");
    bob_store.insert_user_message(sent_msg1).await.unwrap();

    // 4. Verify all 5 Alice devices receive the pinned message
    for i in 0..5 {
        println!("Checking pinned message on Alice device {}...", i + 1);
        let msg = tokio::time::timeout(Duration::from_secs(10), alice_receivers[i].recv())
            .await
            .expect(&format!("Timeout waiting for pinned message on device {}", i + 1))
            .unwrap();
        assert_eq!(extract_user_message_text(&msg.message), "Secret note #1");
        assert_ne!(
            msg.message_type & MESSAGE_TYPE_PINNED,
            0,
            "Device {} must receive message with MESSAGE_TYPE_PINNED flag",
            i + 1
        );

        // Check local DB on this device
        let pinned_list = alice_stores[i]
            .get_pinned_messages_of(&bob_name)
            .await
            .expect("get_pinned_messages_of");
        assert_eq!(pinned_list.len(), 1, "Device {} should have 1 pinned message", i + 1);
        assert_eq!(extract_user_message_text(&pinned_list[0].message), "Secret note #1");
    }

    // 5. Create 6th Alice device (triggers address rotation: Device 1 rotated out)
    println!("Creating 6th Alice device (triggering address rotation of device 1)...");
    let (msg_tx6, mut msg_rx6) = mpsc::channel(100);
    let (gmsg_tx6, _gmsg_rx6) = mpsc::channel(100);
    let mem_pool6 = SqlitePool::connect(":memory:").await.unwrap();
    let store6 = MessagesStore::new(mem_pool6).await.unwrap();
    let client6_holder = Arc::new(tokio::sync::RwLock::new(None));
    let callbacks6 = TestCallbacks {
        name: alice_name.clone(),
        token: alice_name.clone(),
        message_tx: msg_tx6,
        group_message_tx: gmsg_tx6,
        messages_store: store6.clone(),
        client: client6_holder.clone(),
    };

    let db6 = format!("{}/alice_6.db", test_dir);
    let client6 = FireflyWsClient::create(
        base_url.clone(),
        ws_url.clone(),
        1000,
        Box::new(callbacks6),
        db6.clone(),
        5000,
    )
    .await
    .expect("Failed to create Alice client 6");
    let client6 = Arc::new(client6);
    *client6_holder.write().await = Some(client6.clone());

    let client_init6 = client6.clone();
    tokio::spawn(async move {
        let _ = client_init6.initialize_with_retrying().await;
    });

    wait_for_init(&client6)
        .await
        .expect("Alice device 6 failed to initialize");

    tokio::time::sleep(Duration::from_secs(2)).await;

    // 6. Bob sends a SECOND PINNED message to Alice
    println!("Bob sending PINNED message 2 to Alice (post-rotation)...");
    let sent_msg2 = bob_client
        .encrypt_and_send_pinned(alice_name.clone(), b"Secret note #2".to_vec())
        .await
        .expect("Bob failed to send second pinned message");
    bob_store.insert_user_message(sent_msg2).await.unwrap();

    // Check device 1 (the rotated-out device): must NOT receive message 2
    println!("Verifying device 1 (rotated out) does NOT receive the message...");
    let res1 = tokio::time::timeout(Duration::from_secs(5), alice_receivers[0].recv()).await;
    assert!(
        res1.is_err(),
        "Device 1 should NOT have received the new message as it was rotated out"
    );

    // Check that device 1 STILL preserves the original pinned message in its local DB!
    let dev1_pinned = alice_stores[0]
        .get_pinned_messages_of(&bob_name)
        .await
        .expect("device 1 get_pinned_messages_of");
        assert_eq!(
        dev1_pinned.len(),
        1,
        "Device 1 must still preserve its existing pinned message despite address rotation"
    );
    assert_eq!(extract_user_message_text(&dev1_pinned[0].message), "Secret note #1");

    // Check active devices 2..5: must receive message 2 and now have BOTH pinned messages in DB
    for i in 1..5 {
        println!("Checking message 2 on active device {}...", i + 1);
        let msg = tokio::time::timeout(Duration::from_secs(10), alice_receivers[i].recv())
            .await
            .expect(&format!("Timeout waiting for message 2 on device {}", i + 1))
            .unwrap();
        assert_eq!(extract_user_message_text(&msg.message), "Secret note #2");
        assert_ne!(msg.message_type & MESSAGE_TYPE_PINNED, 0);

        let pinned_list = alice_stores[i]
            .get_pinned_messages_of(&bob_name)
            .await
            .expect("get_pinned_messages_of");
        assert_eq!(
            pinned_list.len(),
            2,
            "Device {} should now have 2 pinned messages in DB",
            i + 1
        );
        assert_eq!(extract_user_message_text(&pinned_list[0].message), "Secret note #1");
        assert_eq!(extract_user_message_text(&pinned_list[1].message), "Secret note #2");
    }

    // Check device 6 (the new rotated-in device): receives message 2 with PINNED flag
    println!("Checking message 2 on device 6...");
    let msg6 = tokio::time::timeout(Duration::from_secs(10), msg_rx6.recv())
        .await
        .expect("Timeout waiting for message 2 on device 6")
        .unwrap();
    assert_eq!(extract_user_message_text(&msg6.message), "Secret note #2");
    assert_ne!(msg6.message_type & MESSAGE_TYPE_PINNED, 0);

    let dev6_pinned = store6
        .get_pinned_messages_of(&bob_name)
        .await
        .expect("device 6 get_pinned_messages_of");
    assert_eq!(dev6_pinned.len(), 1);
    assert_eq!(extract_user_message_text(&dev6_pinned[0].message), "Secret note #2");

    // Check Bob's store: Bob has both pinned messages stored locally
    let bob_pinned = bob_store
        .get_pinned_messages_of(&alice_name)
        .await
        .expect("bob get_pinned_messages_of");
    assert_eq!(bob_pinned.len(), 2, "Bob should have both pinned messages saved");
    assert_eq!(extract_user_message_text(&bob_pinned[0].message), "Secret note #1");
    assert_eq!(extract_user_message_text(&bob_pinned[1].message), "Secret note #2");

    println!("1:1 pinned messages address rotation test PASSED!");

    // Cleanup
    for c in alice_clients {
        c.dispose().await;
    }
    client6.dispose().await;
    bob_client.dispose().await;
    cleanup_dir(&test_dir);
}

/// Test 2: MLS Group pinned messages stay accessible across member addition and address rotation/re-add
#[tokio::test]
async fn test_pinned_messages_persist_across_group_readd_and_rotation() {
    let (base_url, ws_url) = match setup_server().await {
        Some(urls) => urls,
        None => return,
    };

    let test_run_id = rand::random::<u32>();
    let test_dir = format!("/tmp/firefly/pinned_rot_grp_{}", test_run_id);
    let _ = std::fs::create_dir_all(&test_dir);

    let alice_name = format!("alice_grot_{}", test_run_id);
    let bob_name = format!("bob_grot_{}", test_run_id);

    // 1. Setup Alice
    let (alice_msg_tx, _alice_msg_rx) = mpsc::channel(100);
    let (alice_gmsg_tx, mut alice_gmsg_rx) = mpsc::channel(100);
    let alice_pool = SqlitePool::connect(":memory:").await.unwrap();
    let alice_store = MessagesStore::new(alice_pool).await.unwrap();
    let alice_holder = Arc::new(tokio::sync::RwLock::new(None));
    let alice_callbacks = TestCallbacks {
        name: alice_name.clone(),
        token: alice_name.clone(),
        message_tx: alice_msg_tx,
        group_message_tx: alice_gmsg_tx,
        messages_store: alice_store,
        client: alice_holder.clone(),
    };
    let alice_db = format!("{}/alice.db", test_dir);
    let alice_client = FireflyWsClient::create(
        base_url.clone(),
        ws_url.clone(),
        1000,
        Box::new(alice_callbacks),
        alice_db.clone(),
        5000,
    )
    .await
    .expect("Alice client create");
    let alice_client = Arc::new(alice_client);
    *alice_holder.write().await = Some(alice_client.clone());
    let alice_init = alice_client.clone();
    tokio::spawn(async move {
        let _ = alice_init.initialize_with_retrying().await;
    });
    wait_for_init(&alice_client).await.expect("Alice wait_for_init");

    // 2. Setup Bob Device 1
    let (bob1_msg_tx, _bob1_msg_rx) = mpsc::channel(100);
    let (bob1_gmsg_tx, mut bob1_gmsg_rx) = mpsc::channel(100);
    let bob1_pool = SqlitePool::connect(":memory:").await.unwrap();
    let bob1_store = MessagesStore::new(bob1_pool).await.unwrap();
    let bob1_holder = Arc::new(tokio::sync::RwLock::new(None));
    let bob1_callbacks = TestCallbacks {
        name: bob_name.clone(),
        token: bob_name.clone(),
        message_tx: bob1_msg_tx,
        group_message_tx: bob1_gmsg_tx,
        messages_store: bob1_store,
        client: bob1_holder.clone(),
    };
    let bob1_db = format!("{}/bob1.db", test_dir);
    let bob1_client = FireflyWsClient::create(
        base_url.clone(),
        ws_url.clone(),
        1000,
        Box::new(bob1_callbacks),
        bob1_db.clone(),
        5000,
    )
    .await
    .expect("Bob1 client create");
    let bob1_client = Arc::new(bob1_client);
    *bob1_holder.write().await = Some(bob1_client.clone());
    let bob1_init = bob1_client.clone();
    tokio::spawn(async move {
        let _ = bob1_init.initialize_with_retrying().await;
    });
    wait_for_init(&bob1_client).await.expect("Bob1 wait_for_init");

    // 3. Alice creates MLS group and adds Bob1
    println!("Alice creating MLS group...");
    let group = alice_client
        .create_group("Pinned Rotation Group".into(), "Desc".into(), 0)
        .await
        .expect("Alice create group");
    let group_id = group.id;

    // This scenario lets Bob pin after rotating; explicitly grant that right.
    alice_client.update_group_roles(group_id, vec![firefly_client::group::UpdateRoleProposalFfi {
        name: "default".into(), role_id: 0,
        permissions: firefly_core::config::DEFAULT_GROUP_PERMISSIONS
            | firefly_core::config::UserPermission::PinMessage as u32,
        delete: false, color: 0,
    }]).await.expect("Grant PinMessage for rotation scenario");

    alice_client
        .add_group_member(group_id, bob_name.clone(), 0)
        .await
        .expect("Alice add Bob1");

    bob1_client.check_setup().await.expect("Bob1 check_setup");
    tokio::time::sleep(Duration::from_secs(1)).await;

    // 4. Alice sends a PINNED group message
    println!("Alice sending PINNED group message...");
    let inner_pinned = firefly::GroupMessageInner {
        channelId: 0,
        message: mod_GroupMessageInner::OneOfmessage::messagePayload(MessagePayload {
            text: "Important Group Guidelines".into(),
            files: None,
            ext: firefly::mod_MessagePayload::OneOfext::None,
            message_type: MESSAGE_TYPE_PINNED,
        }),
        message_type: MESSAGE_TYPE_PINNED,
    };
    let payload = serialize_proto(&inner_pinned).unwrap();
    let uploaded_id = alice_client
        .encrypt_and_send_group_pinned(group_id, payload.to_vec())
        .await
        .expect("Alice send pinned message");
    assert!(uploaded_id > 0);

    // Bob 1 receives the group message
    let bob1_msg = tokio::time::timeout(Duration::from_secs(10), bob1_gmsg_rx.recv())
        .await
        .expect("Timeout waiting for pinned message on Bob1")
        .expect("Channel closed");
    assert_eq!(bob1_msg.group_id, group_id);

    // Verify Bob 1 and Alice both have 1 pinned message in their local DBs
    let mut b1_pinned = Vec::new();
    for _ in 0..50 {
        let _ = bob1_client.resume_snapshots(group_id).await;
        b1_pinned = bob1_client
            .group_message_store()
            .get_pinned_messages(group_id)
            .await
            .expect("Bob1 get_pinned_messages");
        if !b1_pinned.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(b1_pinned.len(), 1);
    assert_ne!(b1_pinned[0].message_type & MESSAGE_TYPE_PINNED, 0);

    let a_pinned = alice_client
        .group_message_store()
        .get_pinned_messages(group_id)
        .await
        .expect("Alice get_pinned_messages");
    assert_eq!(a_pinned.len(), 1);

    // 5. Setup Bob Device 2 (simulating a newly rotated/added device for Bob)
    println!("Setting up Bob Device 2...");
    let (bob2_msg_tx, _bob2_msg_rx) = mpsc::channel(100);
    let (bob2_gmsg_tx, mut bob2_gmsg_rx) = mpsc::channel(100);
    let bob2_pool = SqlitePool::connect(":memory:").await.unwrap();
    let bob2_store = MessagesStore::new(bob2_pool).await.unwrap();
    let bob2_holder = Arc::new(tokio::sync::RwLock::new(None));
    let bob2_callbacks = TestCallbacks {
        name: bob_name.clone(),
        token: bob_name.clone(),
        message_tx: bob2_msg_tx,
        group_message_tx: bob2_gmsg_tx,
        messages_store: bob2_store,
        client: bob2_holder.clone(),
    };
    let bob2_db = format!("{}/bob2.db", test_dir);
    let bob2_client = FireflyWsClient::create(
        base_url.clone(),
        ws_url.clone(),
        1000,
        Box::new(bob2_callbacks),
        bob2_db.clone(),
        5000,
    )
    .await
    .expect("Bob2 client create");
    let bob2_client = Arc::new(bob2_client);
    *bob2_holder.write().await = Some(bob2_client.clone());
    let bob2_init = bob2_client.clone();
    tokio::spawn(async move {
        let _ = bob2_init.initialize_with_retrying().await;
    });
    wait_for_init(&bob2_client).await.expect("Bob2 wait_for_init");

    // 6. Bob Device 2 requests re-add to the group
    println!("Bob Device 2 requesting re-add to group...");
    bob2_client
        .request_re_add(vec![group_id])
        .await
        .expect("Bob2 request_re_add");

    // Alice automatically handles the reAdd request:
    // committing Bob2 to the MLS tree AND re-encrypting all pinned messages!
    println!("Waiting for Alice to process reAdd and re-encrypt pinned messages...");
    tokio::time::sleep(Duration::from_secs(8)).await;

    // Bob 2 syncs
    bob2_client.check_setup().await.expect("Bob2 check_setup");
    let _ = bob2_client.resume_snapshots(group_id).await;

    // 7. Bob Device 2 must receive the re-encrypted pinned message!
    println!("Checking if Bob Device 2 receives the re-encrypted pinned message...");
    let bob2_received_pinned = tokio::time::timeout(Duration::from_secs(15), bob2_gmsg_rx.recv())
        .await
        .expect("Timeout waiting for re-encrypted pinned message on Bob Device 2")
        .expect("Channel closed");

    assert_eq!(bob2_received_pinned.group_id, group_id);
    assert_ne!(
        bob2_received_pinned.message_type & MESSAGE_TYPE_PINNED,
        0,
        "Bob2 must receive message marked with MESSAGE_TYPE_PINNED"
    );

    // Verify Bob Device 2 now has the pinned message stored in its local database
    let bob2_pinned = bob2_client
        .group_message_store()
        .get_pinned_messages(group_id)
        .await
        .expect("Bob2 get_pinned_messages");
    assert_eq!(bob2_pinned.len(), 1, "Bob2 must have exactly 1 pinned message stored");
    assert_ne!(bob2_pinned[0].message_type & MESSAGE_TYPE_PINNED, 0);

    // 8. Bob Device 2 sends a new message to the group to confirm group membership is healthy
    println!("Bob Device 2 sending group message to verify healthy MLS state...");
    let test_msg = firefly::GroupMessageInner {
        channelId: 0,
        message: mod_GroupMessageInner::OneOfmessage::messagePayload(MessagePayload {
            text: "Hello from Bob Device 2!".into(),
            files: None,
            ext: firefly::mod_MessagePayload::OneOfext::None,
            message_type: 0,
        }),
        message_type: 0,
    };
    bob2_client
        .upload_group_message(group_id, test_msg, 0)
        .await
        .expect("Bob2 send group message");

    // 9. Bob Device 2 pins a new message
    println!("Bob Device 2 pinning a second message...");
    let bob2_pinned_inner = firefly::GroupMessageInner {
        channelId: 0,
        message: mod_GroupMessageInner::OneOfmessage::messagePayload(MessagePayload {
            text: "Bob2 Pinned Resource Link".into(),
            files: None,
            ext: firefly::mod_MessagePayload::OneOfext::None,
            message_type: MESSAGE_TYPE_PINNED,
        }),
        message_type: MESSAGE_TYPE_PINNED,
    };
    let b2_payload = serialize_proto(&bob2_pinned_inner).unwrap();
    bob2_client
        .encrypt_and_send_group_pinned(group_id, b2_payload.to_vec())
        .await
        .expect("Bob2 send pinned message");

    // Verify Alice receives the newly pinned message from Bob2
    let mut alice_new_pinned = None;
    while let Ok(Some(msg)) = tokio::time::timeout(Duration::from_secs(10), alice_gmsg_rx.recv()).await {
        if (msg.message_type & MESSAGE_TYPE_PINNED) != 0 && extract_group_message_text(&msg.message).contains("Bob2 Pinned") {
            alice_new_pinned = Some(msg);
            break;
        }
    }
    assert!(alice_new_pinned.is_some(), "Alice must receive Bob2's pinned message");

    // Verify Bob1 (the rotated-out device) still has its initial pinned message preserved in local DB!
    let b1_all_pinned = bob1_client
        .group_message_store()
        .get_pinned_messages(group_id)
        .await
        .expect("Bob1 get_pinned_messages");
    assert!(
        b1_all_pinned
            .iter()
            .any(|m| extract_group_message_text(&m.message).contains("Important Group Guidelines")),
        "Bob1 must still preserve its initial pinned message despite being rotated out"
    );

    // Verify Alice has both pinned messages stored
    let alice_all_pinned = alice_client
        .group_message_store()
        .get_pinned_messages(group_id)
        .await
        .expect("Alice get_pinned_messages");
    assert_eq!(alice_all_pinned.len(), 2, "Alice must have 2 pinned messages");

    // Verify Bob2 has both pinned messages stored (re-encrypted #1 + newly pinned #2)
    let bob2_all_pinned = bob2_client
        .group_message_store()
        .get_pinned_messages(group_id)
        .await
        .expect("Bob2 get_pinned_messages");
    assert_eq!(bob2_all_pinned.len(), 2, "Bob2 must have 2 pinned messages");

    println!("Group pinned messages persist across re-add and rotation test PASSED!");

    // Cleanup
    alice_client.dispose().await;
    bob1_client.dispose().await;
    bob2_client.dispose().await;
    cleanup_dir(&test_dir);
}
