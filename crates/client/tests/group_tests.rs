use async_trait::async_trait;
use firefly_client::callbacks::FireflyWsClientCallback;
use firefly_client::db::{group_messages::GroupMessage, messages::UserMessage};
use firefly_client::websocket::FireflyWsClient;
use firefly_protos::{deserialize_proto, firefly};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

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
        firefly_client::init_logger("/tmp/firefly/test.log".to_string());
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

#[tokio::test]
async fn test_client_group_flow() {
    let (base_url, ws_url) = match setup_server().await {
        Some(urls) => urls,
        None => return,
    };

    let test_run_id = rand::random::<u32>();
    let test_dir = format!("/tmp/firefly/flow_{}", test_run_id);
    let _ = std::fs::create_dir_all(&test_dir);

    let alice_name = format!("alice_grp_{}", test_run_id);
    let bob_name = format!("bob_grp_{}", test_run_id);

    // Alice Setup
    let (alice_msg_tx, _alice_msg_rx) = mpsc::channel(100);
    let (alice_gmsg_tx, mut alice_gmsg_rx) = mpsc::channel(100);
    let alice_callbacks = TestCallbacks {
        name: alice_name.clone(),
        token: alice_name.clone(),
        message_tx: alice_msg_tx,
        group_message_tx: alice_gmsg_tx,
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
    .expect("Failed to create Alice client");
    let alice_client = Arc::new(alice_client);

    // Initialize Alice
    let alice_init = alice_client.clone();
    tokio::spawn(async move {
        let _ = alice_init.initialize_with_retrying().await;
    });

    // Wait for Alice to connect and register
    println!("Waiting for Alice to initialize...");
    wait_for_init(&alice_client)
        .await
        .expect("Alice failed to initialize");
    println!("Alice initialized!");

    // Bob Setup
    let (bob_msg_tx, _bob_msg_rx) = mpsc::channel(100);
    let (bob_gmsg_tx, _bob_gmsg_rx) = mpsc::channel(100);
    let bob_callbacks = TestCallbacks {
        name: bob_name.clone(),
        token: bob_name.clone(),
        message_tx: bob_msg_tx,
        group_message_tx: bob_gmsg_tx,
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

    let bob_init = bob_client.clone();
    tokio::spawn(async move {
        let _ = bob_init.initialize_with_retrying().await;
    });

    // Wait for Bob to connect and register
    println!("Waiting for Bob to initialize...");
    wait_for_init(&bob_client)
        .await
        .expect("Bob failed to initialize");
    println!("Bob initialized!");
    // Give some time for background tasks (like key package uploads) to finish
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Alice creates group
    println!("Alice creating group...");
    let group_info = alice_client
        .create_group("Test Group".into(), "Description".into(), 0)
        .await
        .expect("Alice failed to create group");
    let group_id = group_info.id;
    println!("Group created with ID: {}", group_id);

    // Alice adds Bob
    println!("Alice adding Bob...");
    alice_client
        .add_group_member(group_id, bob_name.clone(), 0)
        .await
        .expect("Alice failed to add Bob");

    // Bob should auto-join. We might need to wait for Bob to sync and join.
    // Since initialize_with_retrying is running in background, it should pick it up.
    // However, it might pick it up on next reconnect or periodic check.
    // Let's force a sync for Bob if possible, or just wait.
    println!("Bob joining group...");
    bob_client
        .check_setup()
        .await
        .expect("Bob failed to join group");
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Bob sends a message
    println!("Bob sending group message...");
    let message_text = "Hello from Bob!".to_string();
    let group_msg = firefly::GroupMessageInner {
        channelId: 0,
        message: firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(
            firefly::MessagePayload {
                text: message_text.clone().into(),
                ..Default::default()
            },
        ),
    };
    bob_client
        .upload_group_message(group_id, group_msg, 0)
        .await
        .expect("Bob failed to send message");

    // Alice should receive the message
    println!("Alice waiting for message...");
    let received = tokio::time::timeout(Duration::from_secs(30), alice_gmsg_rx.recv())
        .await
        .expect("Timeout waiting for group message")
        .expect("Channel closed");

    assert_eq!(received.group_id, group_id);
    let decoded_inner = deserialize_proto::<firefly::GroupMessageInner>(&received.message)
        .expect("Failed to decode group message inner");
    if let firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(payload) =
        decoded_inner.message
    {
        assert_eq!(payload.text.as_ref(), message_text);
    } else {
        panic!(
            "Received unexpected message type: {:?}",
            decoded_inner.message
        );
    }

    println!("Test passed!");

    // Cleanup
    let _ = alice_client.dispose().await;
    let _ = bob_client.dispose().await;
    cleanup_dir(&test_dir);
}

#[tokio::test]
async fn test_key_package_exhaustion_and_restart_cleanup_with_real_client() {
    let (base_url, ws_url) = match setup_server().await {
        Some(urls) => urls,
        None => return,
    };

    let test_run_id = rand::random::<u32>();
    let test_dir = format!("/tmp/firefly/clean_{}", test_run_id);
    let _ = std::fs::create_dir_all(&test_dir);

    let alice_name = format!("alice_cl_{}", test_run_id);
    let bob_name = format!("bob_cl_{}", test_run_id);

    // 1. Setup Alice with real SQLite db
    let (alice_msg_tx, _alice_msg_rx) = mpsc::channel(100);
    let (alice_gmsg_tx, mut alice_gmsg_rx) = mpsc::channel(100);
    let alice_callbacks = TestCallbacks {
        name: alice_name.clone(),
        token: alice_name.clone(),
        message_tx: alice_msg_tx,
        group_message_tx: alice_gmsg_tx,
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
    .expect("Failed to create Alice client");
    let alice_client = Arc::new(alice_client);
    let alice_init = alice_client.clone();
    tokio::spawn(async move {
        let _ = alice_init.initialize_with_retrying().await;
    });

    wait_for_init(&alice_client)
        .await
        .expect("Alice failed to initialize");
    println!("Alice initialized with initial 32 key packages.");

    // 2. Setup Bob
    let (bob_msg_tx, _bob_msg_rx) = mpsc::channel(100);
    let (bob_gmsg_tx, _bob_gmsg_rx) = mpsc::channel(100);
    let bob_callbacks = TestCallbacks {
        name: bob_name.clone(),
        token: bob_name.clone(),
        message_tx: bob_msg_tx,
        group_message_tx: bob_gmsg_tx,
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
    let bob_init = bob_client.clone();
    tokio::spawn(async move {
        let _ = bob_init.initialize_with_retrying().await;
    });

    wait_for_init(&bob_client)
        .await
        .expect("Bob failed to initialize");
    println!("Bob initialized.");

    // Wait for initial key packages upload
    tokio::time::sleep(Duration::from_secs(2)).await;

    // 3. Bob creates 3 groups and adds Alice to each of them
    // Alice genuinely joins each group via real SQLite MLS stores
    let num_groups = 3;
    let mut group_ids = Vec::new();

    for i in 1..=num_groups {
        println!("Bob creating group {}...", i);
        let group_info = bob_client
            .create_group(format!("Group {}", i), "Test description".into(), 0)
            .await
            .expect("Bob failed to create group");
        let gid = group_info.id;
        group_ids.push(gid);

        println!("Bob adding Alice to group {}...", i);
        bob_client
            .add_group_member(gid, alice_name.clone(), 0)
            .await
            .expect("Bob failed to add Alice");

        // Alice receives group invite and joins
        alice_client
            .check_setup()
            .await
            .expect("Alice failed to check setup and join");

        // Bob sends a message in the group to verify Alice joined and is part of MLS group
        let msg = firefly::GroupMessageInner {
            channelId: 0,
            message: firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(
                firefly::MessagePayload {
                    text: format!("Hello in group {}", i).into(),
                    ..Default::default()
                },
            ),
        };
        bob_client
            .upload_group_message(gid, msg, 0)
            .await
            .expect("Bob failed to upload group message");

        let received = tokio::time::timeout(Duration::from_secs(10), alice_gmsg_rx.recv())
            .await
            .expect("Alice timed out waiting for group message")
            .expect("Channel closed");
        assert_eq!(received.group_id, gid);
        println!("Alice successfully received message in group {}!", i);
    }

    // 4. Restart Alice's client with the SAME SQLite database
    println!("Disposing Alice to simulate client restart...");
    alice_client.dispose().await;

    println!("Restarting Alice client with existing SQLite DB...");
    let (alice_msg_tx2, _alice_msg_rx2) = mpsc::channel(100);
    let (alice_gmsg_tx2, mut alice_gmsg_rx2) = mpsc::channel(100);
    let alice_callbacks2 = TestCallbacks {
        name: alice_name.clone(),
        token: alice_name.clone(),
        message_tx: alice_msg_tx2,
        group_message_tx: alice_gmsg_tx2,
    };

    let alice_client2 = FireflyWsClient::create(
        base_url.clone(),
        ws_url.clone(),
        1000,
        Box::new(alice_callbacks2),
        alice_db.clone(),
        5000,
    )
    .await
    .expect("Failed to recreate Alice client");
    let alice_client2 = Arc::new(alice_client2);
    let alice_init2 = alice_client2.clone();
    tokio::spawn(async move {
        let _ = alice_init2.initialize_with_retrying().await;
    });

    wait_for_init(&alice_client2)
        .await
        .expect("Alice client 2 failed to initialize");
    println!("Alice client 2 initialized and completed check_key_packages reconciliation!");

    // Give time for startup keypackage sync
    tokio::time::sleep(Duration::from_secs(2)).await;

    // 5. Bob creates another group and adds Alice -> Alice must successfully join!
    println!("Bob creating new group after Alice restarted...");
    let group_after = bob_client
        .create_group("Post-restart Group".into(), "Desc".into(), 0)
        .await
        .expect("Bob failed to create post-restart group");
    let post_gid = group_after.id;

    println!("Bob adding Alice to post-restart group...");
    bob_client
        .add_group_member(post_gid, alice_name.clone(), 0)
        .await
        .expect("Bob failed to add Alice to post-restart group");

    alice_client2
        .check_setup()
        .await
        .expect("Alice 2 failed to join post-restart group");

    bob_client
        .upload_group_message(
            post_gid,
            firefly::GroupMessageInner {
                channelId: 0,
                message: firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(
                    firefly::MessagePayload {
                        text: "Welcome back Alice!".into(),
                        ..Default::default()
                    },
                ),
            },
            0,
        )
        .await
        .expect("Bob failed to send post-restart message");

    let received2 = tokio::time::timeout(Duration::from_secs(10), alice_gmsg_rx2.recv())
        .await
        .expect("Alice 2 timed out waiting for post-restart message")
        .expect("Channel closed");
    assert_eq!(received2.group_id, post_gid);
    println!("Alice successfully joined and received message in post-restart group!");

    // Cleanup
    alice_client2.dispose().await;
    bob_client.dispose().await;
    cleanup_dir(&test_dir);
}

#[tokio::test]
async fn test_late_joiner_does_not_receive_prior_messages_and_reconnection_sync() {
    let (base_url, ws_url) = match setup_server().await {
        Some(urls) => urls,
        None => return,
    };

    let test_run_id = rand::random::<u32>();
    let test_dir = format!("/tmp/firefly/late_{}", test_run_id);
    let _ = std::fs::create_dir_all(&test_dir);
    let alice_name = format!("alice_late_{}", test_run_id);
    let bob_name = format!("bob_late_{}", test_run_id);
    let charlie_name = format!("charlie_late_{}", test_run_id);

    // 1. Setup Alice (group creator)
    let (alice_msg_tx, _alice_msg_rx) = mpsc::channel(100);
    let (alice_gmsg_tx, mut alice_gmsg_rx) = mpsc::channel(100);
    let alice_callbacks = TestCallbacks {
        name: alice_name.clone(),
        token: alice_name.clone(),
        message_tx: alice_msg_tx,
        group_message_tx: alice_gmsg_tx,
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
    .unwrap();
    let alice_client = Arc::new(alice_client);
    let a_init = alice_client.clone();
    tokio::spawn(async move {
        let _ = a_init.initialize_with_retrying().await;
    });
    wait_for_init(&alice_client).await.unwrap();

    // 2. Setup Bob (existing member from start)
    let (bob_msg_tx, _bob_msg_rx) = mpsc::channel(100);
    let (bob_gmsg_tx, mut bob_gmsg_rx) = mpsc::channel(100);
    let bob_callbacks = TestCallbacks {
        name: bob_name.clone(),
        token: bob_name.clone(),
        message_tx: bob_msg_tx,
        group_message_tx: bob_gmsg_tx,
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
    .unwrap();
    let bob_client = Arc::new(bob_client);
    let b_init = bob_client.clone();
    tokio::spawn(async move {
        let _ = b_init.initialize_with_retrying().await;
    });
    wait_for_init(&bob_client).await.unwrap();

    // 3. Setup Charlie (late joiner)
    let (charlie_msg_tx, _charlie_msg_rx) = mpsc::channel(100);
    let (charlie_gmsg_tx, mut charlie_gmsg_rx) = mpsc::channel(100);
    let charlie_callbacks = TestCallbacks {
        name: charlie_name.clone(),
        token: charlie_name.clone(),
        message_tx: charlie_msg_tx,
        group_message_tx: charlie_gmsg_tx,
    };
    let charlie_db = format!("{}/charlie.db", test_dir);

    let charlie_client = FireflyWsClient::create(
        base_url.clone(),
        ws_url.clone(),
        1000,
        Box::new(charlie_callbacks),
        charlie_db.clone(),
        5000,
    )
    .await
    .unwrap();
    let charlie_client = Arc::new(charlie_client);
    let c_init = charlie_client.clone();
    tokio::spawn(async move {
        let _ = c_init.initialize_with_retrying().await;
    });
    wait_for_init(&charlie_client).await.unwrap();

    tokio::time::sleep(Duration::from_secs(2)).await;

    // 4. Alice creates group and adds Bob
    println!("Alice creating group with Bob...");
    let group = alice_client
        .create_group("Secret Chat".into(), "History Test".into(), 0)
        .await
        .unwrap();
    let gid = group.id;

    alice_client
        .add_group_member(gid, bob_name.clone(), 0)
        .await
        .unwrap();
    bob_client.check_setup().await.unwrap();

    // 5. Alice and Bob chat before Charlie ever joins (Sent in epoch 1)
    println!("Alice and Bob chatting prior to Charlie joining...");
    alice_client
        .upload_group_message(
            gid,
            firefly::GroupMessageInner {
                channelId: 0,
                message: firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(
                    firefly::MessagePayload {
                        text: "Secret prior message 1".into(),
                        ..Default::default()
                    },
                ),
            },
            0,
        )
        .await
        .unwrap();

    let bob_recvd = tokio::time::timeout(Duration::from_secs(10), bob_gmsg_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(bob_recvd.group_id, gid);

    bob_client
        .upload_group_message(
            gid,
            firefly::GroupMessageInner {
                channelId: 0,
                message: firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(
                    firefly::MessagePayload {
                        text: "Secret prior message 2".into(),
                        ..Default::default()
                    },
                ),
            },
            0,
        )
        .await
        .unwrap();

    let alice_recvd = tokio::time::timeout(Duration::from_secs(10), alice_gmsg_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(alice_recvd.group_id, gid);

    // 6. Now Alice adds Charlie (New Epoch)
    println!("Alice adding Charlie now...");
    alice_client
        .add_group_member(gid, charlie_name.clone(), 0)
        .await
        .unwrap();

    // Charlie joins
    charlie_client.check_setup().await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Verify Charlie did NOT receive messages sent before his join commit
    println!("Verifying Charlie received NO prior history messages...");
    let unexpected = tokio::time::timeout(Duration::from_secs(2), charlie_gmsg_rx.recv()).await;
    assert!(
        unexpected.is_err(),
        "Charlie should NOT receive messages sent before he was added to the group!"
    );

    // 7. Test Reconnection resilience:
    // Charlie disconnects and reconnects (simulating phone sleep or network drop)
    println!("Charlie disconnecting and reconnecting (simulating network drop/sleep)...");
    charlie_client.dispose().await;

    // Alice sends a message while Charlie is offline
    println!("Alice sending message while Charlie is offline...");
    alice_client
        .upload_group_message(
            gid,
            firefly::GroupMessageInner {
                channelId: 0,
                message: firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(
                    firefly::MessagePayload {
                        text: "Message while Charlie offline".into(),
                        ..Default::default()
                    },
                ),
            },
            0,
        )
        .await
        .unwrap();

    // Charlie comes back online with his database
    let (c_msg_tx2, _c_msg_rx2) = mpsc::channel(100);
    let (c_gmsg_tx2, mut c_gmsg_rx2) = mpsc::channel(100);
    let c_callbacks2 = TestCallbacks {
        name: charlie_name.clone(),
        token: charlie_name.clone(),
        message_tx: c_msg_tx2,
        group_message_tx: c_gmsg_tx2,
    };

    let charlie_reconnected = FireflyWsClient::create(
        base_url.clone(),
        ws_url.clone(),
        1000,
        Box::new(c_callbacks2),
        charlie_db.clone(),
        5000,
    )
    .await
    .unwrap();
    let charlie_reconnected = Arc::new(charlie_reconnected);
    let c_re_init = charlie_reconnected.clone();
    tokio::spawn(async move {
        let _ = c_re_init.initialize_with_retrying().await;
    });
    wait_for_init(&charlie_reconnected).await.unwrap();

    // Charlie checks setup / syncs missed message
    charlie_reconnected.check_setup().await.unwrap();

    // Charlie should receive the missed message that was sent AFTER he joined
    println!("Checking Charlie receives the message sent while he was offline...");
    let recvd_offline = tokio::time::timeout(Duration::from_secs(10), c_gmsg_rx2.recv())
        .await
        .expect("Charlie should receive message sent while offline")
        .expect("channel open");
    assert_eq!(recvd_offline.group_id, gid);

    let decoded = deserialize_proto::<firefly::GroupMessageInner>(&recvd_offline.message).unwrap();
    if let firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(payload) = decoded.message {
        assert_eq!(payload.text.as_ref(), "Message while Charlie offline");
    } else {
        panic!("unexpected message payload");
    }
    println!("Reconnection sync verified seamlessly!");

    // Cleanup
    alice_client.dispose().await;
    bob_client.dispose().await;
    charlie_reconnected.dispose().await;
    cleanup_dir(&test_dir);
}
