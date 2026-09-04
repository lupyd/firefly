use async_trait::async_trait;
use firefly_client::callbacks::FireflyWsClientCallback;
use firefly_client::db::{group_messages::GroupMessage, messages::UserMessage};
use firefly_client::websocket::FireflyWsClient;
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
        if message.sent_by_other {
            let _ = self.message_tx.send(message).await;
        }
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
        firefly_client::init_logger("/tmp/firefly/test_rotation.log".to_string());
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
async fn test_address_rotation() {
    let (base_url, ws_url) = match setup_server().await {
        Some(urls) => urls,
        None => return,
    };

    let test_run_id = rand::random::<u32>();
    let test_dir = format!("/tmp/firefly/rot_{}", test_run_id);
    let _ = std::fs::create_dir_all(&test_dir);

    let alice_name = format!("alice_rot_{}", test_run_id);
    let bob_name = format!("bob_rot_{}", test_run_id);

    let mut alice_clients = Vec::new();
    let mut alice_receivers = Vec::new();

    // 1. Create 5 Alice devices
    println!("Creating 5 Alice devices...");
    for i in 1..=5 {
        let (msg_tx, msg_rx) = mpsc::channel(100);
        let (gmsg_tx, _gmsg_rx) = mpsc::channel(100);
        let callbacks = TestCallbacks {
            name: alice_name.clone(),
            token: alice_name.clone(),
            message_tx: msg_tx,
            group_message_tx: gmsg_tx,
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

        // Small sleep to ensure different activity timestamps for rotation sorting
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // 2. Create Bob
    println!("Creating Bob device...");
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
        .expect("Bob client failed to initialize");

    tokio::time::sleep(Duration::from_secs(2)).await;

    // 3. Bob sends message to Alice
    println!("Bob sending first message to Alice...");
    bob_client
        .encrypt_and_send(alice_name.clone(), b"hello 1".to_vec())
        .await
        .expect("Bob failed to send message");

    // 4. Verify all 5 receive it
    for i in 0..5 {
        println!("Checking message for device {}...", i + 1);
        let msg = tokio::time::timeout(Duration::from_secs(10), alice_receivers[i].recv())
            .await
            .expect(&format!("Timeout waiting for message on device {}", i + 1))
            .unwrap();
        assert_eq!(msg.message, b"hello 1");
    }
    println!("All 5 devices received the first message.");
    // 6. Create 6th device
    println!("Creating 6th Alice device (should trigger rotation)...");
    let (msg_tx6, mut msg_rx6) = mpsc::channel(100);
    let (gmsg_tx6, _gmsg_rx6) = mpsc::channel(100);
    let callbacks6 = TestCallbacks {
        name: alice_name.clone(),
        token: alice_name.clone(),
        message_tx: msg_tx6,
        group_message_tx: gmsg_tx6,
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

    let client_init6 = client6.clone();
    tokio::spawn(async move {
        let _ = client_init6.initialize_with_retrying().await;
    });

    wait_for_init(&client6)
        .await
        .expect("Alice device 6 failed to initialize");

    tokio::time::sleep(Duration::from_secs(2)).await;

    // 7. Bob sends second message
    println!("Bob sending second message to Alice...");
    bob_client
        .encrypt_and_send(alice_name.clone(), b"hello 2".to_vec())
        .await
        .expect("Bob failed to send second message");

    // Check device 1 (the oldest one)
    println!("Checking that device 1 does NOT receive the message...");
    let res1 = tokio::time::timeout(Duration::from_secs(5), alice_receivers[0].recv()).await;
    if let Ok(Some(ref msg)) = res1 {
        panic!("Device 1 should NOT have received the message, but received: {:?}", String::from_utf8_lossy(&msg.message));
    }
    assert!(
        res1.is_err(),
        "Device 1 should NOT have received the message as it should be dropped"
    );

    // Check others (2-5)
    for i in 1..5 {
        println!("Checking message for device {}...", i + 1);
        let msg = tokio::time::timeout(Duration::from_secs(10), alice_receivers[i].recv())
            .await
            .expect(&format!("Timeout waiting for message on device {}", i + 1))
            .unwrap();
        assert_eq!(msg.message, b"hello 2");
    }
    // Check device 6
    println!("Checking message for device 6...");
    let msg6 = tokio::time::timeout(Duration::from_secs(10), msg_rx6.recv())
        .await
        .expect("Timeout waiting for message on device 6")
        .unwrap();
    assert_eq!(msg6.message, b"hello 2");

    println!("Address rotation test passed!");

    // Cleanup
    for c in alice_clients {
        c.dispose().await;
    }
    client6.dispose().await;
    bob_client.dispose().await;

    cleanup_dir(&test_dir);
}

#[tokio::test]
async fn test_bidirectional_multi_device_rotation_and_messaging() {
    let (base_url, ws_url) = match setup_server().await {
        Some(urls) => urls,
        None => return,
    };

    let test_run_id = rand::random::<u32>();
    let test_dir = format!("/tmp/firefly/bidi_{}", test_run_id);
    let _ = std::fs::create_dir_all(&test_dir);

    let alice_name = format!("alice_bidi_{}", test_run_id);
    let bob_name = format!("bob_bidi_{}", test_run_id);

    // 1. Create 3 initial Alice devices and 3 initial Bob devices
    let mut alice_clients = Vec::new();
    let mut alice_receivers = Vec::new();
    for i in 1..=3 {
        let (msg_tx, msg_rx) = mpsc::channel(100);
        let (gmsg_tx, _gmsg_rx) = mpsc::channel(100);
        let callbacks = TestCallbacks {
            name: alice_name.clone(),
            token: alice_name.clone(),
            message_tx: msg_tx,
            group_message_tx: gmsg_tx,
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

    tokio::time::sleep(Duration::from_secs(1)).await;

    let mut bob_clients = Vec::new();
    let mut bob_receivers = Vec::new();
    for i in 1..=3 {
        let (msg_tx, msg_rx) = mpsc::channel(100);
        let (gmsg_tx, _gmsg_rx) = mpsc::channel(100);
        let callbacks = TestCallbacks {
            name: bob_name.clone(),
            token: bob_name.clone(),
            message_tx: msg_tx,
            group_message_tx: gmsg_tx,
        };

        let db = format!("{}/bob_{}.db", test_dir, i);

        let client = FireflyWsClient::create(
            base_url.clone(),
            ws_url.clone(),
            1000,
            Box::new(callbacks),
            db.clone(),
            5000,
        )
        .await
        .expect("Failed to create Bob client");
        let client = Arc::new(client);

        let client_init = client.clone();
        tokio::spawn(async move {
            let _ = client_init.initialize_with_retrying().await;
        });

        wait_for_init(&client)
            .await
            .expect(&format!("Bob client {} failed to initialize", i));

        bob_clients.push(client);
        bob_receivers.push(msg_rx);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    tokio::time::sleep(Duration::from_secs(2)).await;

    // 2. Alice1 sends message to Bob (All 3 Bob devices receive)
    println!("Alice 1 sending message to Bob...");
    alice_clients[0]
        .encrypt_and_send(bob_name.clone(), b"alice -> bob 1".to_vec())
        .await
        .expect("Alice 1 failed to send");

    for i in 0..3 {
        let msg = tokio::time::timeout(Duration::from_secs(10), bob_receivers[i].recv())
            .await
            .expect(&format!("Timeout waiting for message on Bob device {}", i + 1))
            .unwrap();
        assert_eq!(msg.message, b"alice -> bob 1");
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    // 3. Bob1 replies to Alice (All 3 Alice devices receive)
    println!("Bob 1 sending reply to Alice...");
    bob_clients[0]
        .encrypt_and_send(alice_name.clone(), b"bob -> alice 1".to_vec())
        .await
        .expect("Bob 1 failed to send");

    for i in 0..3 {
        let msg = tokio::time::timeout(Duration::from_secs(10), alice_receivers[i].recv())
            .await
            .expect(&format!("Timeout waiting for message on Alice device {}", i + 1))
            .unwrap();
        assert_eq!(msg.message, b"bob -> alice 1");
    }

    // 4. Add Bob devices 4, 5, 6 (Device 1 should be rotated out of active 5)
    println!("Adding Bob devices 4, 5, 6 (triggering rotation of Bob 1)...");
    for i in 4..=6 {
        let (msg_tx, msg_rx) = mpsc::channel(100);
        let (gmsg_tx, _gmsg_rx) = mpsc::channel(100);
        let callbacks = TestCallbacks {
            name: bob_name.clone(),
            token: bob_name.clone(),
            message_tx: msg_tx,
            group_message_tx: gmsg_tx,
        };

        let db = format!("{}/bob_{}.db", test_dir, i);

        let client = FireflyWsClient::create(
            base_url.clone(),
            ws_url.clone(),
            1000,
            Box::new(callbacks),
            db.clone(),
            5000,
        )
        .await
        .expect("Failed to create Bob client");
        let client = Arc::new(client);

        let client_init = client.clone();
        tokio::spawn(async move {
            let _ = client_init.initialize_with_retrying().await;
        });

        wait_for_init(&client)
            .await
            .expect(&format!("Bob client {} failed to initialize", i));

        bob_clients.push(client);
        bob_receivers.push(msg_rx);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    tokio::time::sleep(Duration::from_secs(2)).await;

    // 5. Alice 2 sends message to Bob -> Server will notice Bob1 is gone and return new active Bob devices
    println!("Alice 2 sending message to Bob after Bob rotation...");
    alice_clients[1]
        .encrypt_and_send(bob_name.clone(), b"alice -> bob 2".to_vec())
        .await
        .expect("Alice 2 failed to send");

    // Bob 1 (index 0) must NOT receive (dropped)
    let res_dropped = tokio::time::timeout(Duration::from_secs(4), bob_receivers[0].recv()).await;
    assert!(res_dropped.is_err(), "Bob 1 should have been rotated out and receive nothing");

    // Active Bob devices (indices 1..6 -> Bob 2, 3, 4, 5, 6) must receive
    for i in 1..6 {
        let msg = tokio::time::timeout(Duration::from_secs(10), bob_receivers[i].recv())
            .await
            .expect(&format!("Timeout waiting for message on active Bob device {}", i + 1))
            .unwrap();
        assert_eq!(msg.message, b"alice -> bob 2");
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    // 6. Bob 6 replies to Alice -> All active Alice devices receive
    println!("Bob 6 sending reply to Alice...");
    bob_clients[5]
        .encrypt_and_send(alice_name.clone(), b"bob 6 -> alice".to_vec())
        .await
        .expect("Bob 6 failed to send");

    for i in 0..3 {
        let msg = tokio::time::timeout(Duration::from_secs(10), alice_receivers[i].recv())
            .await
            .expect(&format!("Timeout waiting for message on Alice device {}", i + 1))
            .unwrap();
        assert_eq!(msg.message, b"bob 6 -> alice");
    }

    println!("Bidirectional multi-device rotation test passed successfully!");

    // Cleanup
    for c in alice_clients {
        c.dispose().await;
    }
    for c in bob_clients {
        c.dispose().await;
    }

    cleanup_dir(&test_dir);
}

#[tokio::test]
async fn test_key_packages_and_pre_keys_not_deleted_on_reconnect() {
    let (base_url, ws_url) = match setup_server().await {
        Some(urls) => urls,
        None => return,
    };

    let test_run_id = rand::random::<u32>();
    let test_dir = format!("/tmp/firefly/reconn_clean_{}", test_run_id);
    let _ = std::fs::create_dir_all(&test_dir);
    let username = format!("reconn_user_{}", test_run_id);
    let db_path = format!("{}/user.db", test_dir);

    let (msg_tx, _msg_rx) = mpsc::channel(100);
    let (gmsg_tx, _gmsg_rx) = mpsc::channel(100);
    let callbacks = TestCallbacks {
        name: username.clone(),
        token: username.clone(),
        message_tx: msg_tx,
        group_message_tx: gmsg_tx,
    };

    // 1. First run: Start client, wait for initialization
    println!("First start: Creating client for {}", username);
    let client1 = FireflyWsClient::create(
        base_url.clone(),
        ws_url.clone(),
        1000,
        Box::new(callbacks),
        db_path.clone(),
        5000,
    )
    .await
    .expect("Failed to create client 1");
    let client1 = Arc::new(client1);
    let c1_init = client1.clone();
    tokio::spawn(async move {
        let _ = c1_init.initialize_with_retrying().await;
    });
    wait_for_init(&client1)
        .await
        .expect("Client 1 failed to initialize");

    let address_id = client1.address_id();
    assert_ne!(address_id, 0, "address_id must not be 0");

    // Fetch initial uploaded key packages from server via HTTP GET
    let token = username.clone();
    let url_kp = format!(
        "{}/group/keyPackages?address_id={}&device_id=0",
        base_url, address_id
    );
    let resp_kp = reqwest::Client::new()
        .get(&url_kp)
        .bearer_auth(&token)
        .send()
        .await
        .expect("Failed to fetch key packages from server");
    assert!(resp_kp.status().is_success());
    let kp_bytes = resp_kp.bytes().await.unwrap();
    let initial_packages = firefly_protos::deserialize_proto::<firefly_protos::firefly::GroupKeyPackages>(&kp_bytes)
        .expect("Failed to deserialize GroupKeyPackages");
    let initial_kp_ids: Vec<i32> = initial_packages.packages.iter().map(|p| p.id).collect();
    println!("Initial key package count: {}, ids: {:?}", initial_kp_ids.len(), initial_kp_ids);
    assert_eq!(initial_kp_ids.len(), 32, "Initial key packages should be 32");

    // Fetch initial uploaded pre_key_bundles from server
    let url_pk = format!(
        "{}/user/preKeyBundles?id={}&onlyIds=true",
        base_url, address_id
    );
    let resp_pk = reqwest::Client::new()
        .get(&url_pk)
        .bearer_auth(&token)
        .send()
        .await
        .expect("Failed to fetch preKeyBundles from server");
    assert!(resp_pk.status().is_success());
    let pk_bytes = resp_pk.bytes().await.unwrap();
    let initial_bundles = firefly_protos::deserialize_proto::<firefly_protos::firefly::PreKeyBundleEntries>(&pk_bytes)
        .expect("Failed to deserialize PreKeyBundleEntries");
    let initial_pk_ids: Vec<u32> = initial_bundles.entries.iter().map(|b| b.id).collect();
    println!("Initial pre_key_bundle count: {}, ids: {:?}", initial_pk_ids.len(), initial_pk_ids);
    assert_eq!(initial_pk_ids.len(), 32, "Initial pre_key_bundles should be 32");

    // Dispose client 1 (simulating app restart / disconnect)
    println!("Disposing client 1 (app closed)...");
    client1.dispose().await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 2. Second run: Restart client with SAME database (like user restarting Lupyd app)
    println!("Second start: Creating client 2 with existing database...");
    let (msg_tx2, _msg_rx2) = mpsc::channel(100);
    let (gmsg_tx2, _gmsg_rx2) = mpsc::channel(100);
    let callbacks2 = TestCallbacks {
        name: username.clone(),
        token: username.clone(),
        message_tx: msg_tx2,
        group_message_tx: gmsg_tx2,
    };
    let client2 = FireflyWsClient::create(
        base_url.clone(),
        ws_url.clone(),
        1000,
        Box::new(callbacks2),
        db_path.clone(),
        5000,
    )
    .await
    .expect("Failed to create client 2");
    let client2 = Arc::new(client2);
    let c2_init = client2.clone();
    tokio::spawn(async move {
        let _ = c2_init.initialize_with_retrying().await;
    });
    wait_for_init(&client2)
        .await
        .expect("Client 2 failed to initialize");

    let address_id2 = client2.address_id();
    assert_eq!(address_id, address_id2, "address_id must be preserved across restarts");

    // Fetch key packages from server again
    let resp_kp2 = reqwest::Client::new()
        .get(&url_kp)
        .bearer_auth(&token)
        .send()
        .await
        .expect("Failed to fetch key packages after restart");
    let kp_bytes2 = resp_kp2.bytes().await.unwrap();
    let reloaded_packages = firefly_protos::deserialize_proto::<firefly_protos::firefly::GroupKeyPackages>(&kp_bytes2)
        .expect("Failed to deserialize GroupKeyPackages");
    let reloaded_kp_ids: Vec<i32> = reloaded_packages.packages.iter().map(|p| p.id).collect();
    println!("Reloaded key package count: {}, ids: {:?}", reloaded_kp_ids.len(), reloaded_kp_ids);

    // Fetch pre_key_bundles from server again
    let resp_pk2 = reqwest::Client::new()
        .get(&url_pk)
        .bearer_auth(&token)
        .send()
        .await
        .expect("Failed to fetch preKeyBundles after restart");
    let pk_bytes2 = resp_pk2.bytes().await.unwrap();
    let reloaded_bundles = firefly_protos::deserialize_proto::<firefly_protos::firefly::PreKeyBundleEntries>(&pk_bytes2)
        .expect("Failed to deserialize PreKeyBundleEntries");
    let reloaded_pk_ids: Vec<u32> = reloaded_bundles.entries.iter().map(|b| b.id).collect();
    println!("Reloaded pre_key_bundle count: {}, ids: {:?}", reloaded_pk_ids.len(), reloaded_pk_ids);

    // CRITICAL CHECKS:
    // Key packages and pre-keys should NOT have been deleted and re-uploaded on a clean restart!
    // The IDs on the server must match the original IDs because none of them were consumed!
    assert_eq!(
        reloaded_kp_ids, initial_kp_ids,
        "Key packages MUST NOT be rotated/re-uploaded on client restart when none were used!"
    );
    assert_eq!(
        reloaded_pk_ids, initial_pk_ids,
        "Pre-key bundles MUST NOT be deleted/re-uploaded on client restart when none were used!"
    );

    client2.dispose().await;
    cleanup_dir(&test_dir);
}

#[tokio::test]
async fn test_used_key_packages_and_pre_keys_cleaned_and_sessions_persist_across_reconnect() {
    let (base_url, ws_url) = match setup_server().await {
        Some(urls) => urls,
        None => return,
    };

    let test_run_id = rand::random::<u32>();
    let test_dir = format!("/tmp/firefly/reconn_session_{}", test_run_id);
    let _ = std::fs::create_dir_all(&test_dir);

    let alice_name = format!("alice_reconn_{}", test_run_id);
    let bob_name = format!("bob_reconn_{}", test_run_id);

    let alice_db = format!("{}/alice.db", test_dir);
    let bob_db = format!("{}/bob.db", test_dir);

    // 1. Setup Alice
    let (alice_msg_tx, mut alice_msg_rx) = mpsc::channel(100);
    let (alice_gmsg_tx, mut alice_gmsg_rx) = mpsc::channel(100);
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
        alice_db.clone(),
        5000,
    )
    .await
    .expect("Failed to create Alice client");
    let alice_client = Arc::new(alice_client);
    let a_init = alice_client.clone();
    tokio::spawn(async move {
        let _ = a_init.initialize_with_retrying().await;
    });
    wait_for_init(&alice_client).await.expect("Alice failed to initialize");

    // 2. Setup Bob
    let (bob_msg_tx, mut bob_msg_rx) = mpsc::channel(100);
    let (bob_gmsg_tx, _bob_gmsg_rx) = mpsc::channel(100);
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
        bob_db.clone(),
        5000,
    )
    .await
    .expect("Failed to create Bob client");
    let bob_client = Arc::new(bob_client);
    let b_init = bob_client.clone();
    tokio::spawn(async move {
        let _ = b_init.initialize_with_retrying().await;
    });
    wait_for_init(&bob_client).await.expect("Bob failed to initialize");

    let bob_address_id = bob_client.address_id();
    let bob_token = bob_name.clone();

    // 3. Inspect Bob's initial key packages and pre-key bundles on server
    let url_bob_kp = format!(
        "{}/group/keyPackages?address_id={}&device_id=0",
        base_url, bob_address_id
    );
    let resp_kp = reqwest::Client::new()
        .get(&url_bob_kp)
        .bearer_auth(&bob_token)
        .send()
        .await
        .unwrap();
    let kp_bytes = resp_kp.bytes().await.unwrap();
    let initial_kp = firefly_protos::deserialize_proto::<firefly_protos::firefly::GroupKeyPackages>(&kp_bytes).unwrap();
    let initial_bob_kp_ids: Vec<i32> = initial_kp.packages.iter().map(|p| p.id).collect();
    assert_eq!(initial_bob_kp_ids.len(), 32, "Bob should initially have 32 key packages");

    let url_bob_pk = format!(
        "{}/user/preKeyBundles?id={}&onlyIds=true",
        base_url, bob_address_id
    );
    let resp_pk = reqwest::Client::new()
        .get(&url_bob_pk)
        .bearer_auth(&bob_token)
        .send()
        .await
        .unwrap();
    let pk_bytes = resp_pk.bytes().await.unwrap();
    let initial_pk = firefly_protos::deserialize_proto::<firefly_protos::firefly::PreKeyBundleEntries>(&pk_bytes).unwrap();
    let initial_bob_pk_ids: Vec<u32> = initial_pk.entries.iter().map(|b| b.id).collect();
    assert_eq!(initial_bob_pk_ids.len(), 32, "Bob should initially have 32 pre-key bundles");

    // 4. Alice sends 1:1 message to Bob (consumes 1 pre-key bundle of Bob from server)
    println!("Alice sending 1:1 message to Bob...");
    alice_client
        .encrypt_and_send(bob_name.clone(), b"hello bob 1:1 before reconnect".to_vec())
        .await
        .expect("Alice failed to send 1:1 message");

    let received_1_1 = tokio::time::timeout(Duration::from_secs(10), bob_msg_rx.recv())
        .await
        .expect("Timeout waiting for 1:1 message on Bob")
        .unwrap();
    assert_eq!(received_1_1.message, b"hello bob 1:1 before reconnect");

    // Verify Bob's pre-keys on server: exactly 1 consumed, 31 remaining!
    let resp_pk_after = reqwest::Client::new()
        .get(&url_bob_pk)
        .bearer_auth(&bob_token)
        .send()
        .await
        .unwrap();
    let pk_bytes_after = resp_pk_after.bytes().await.unwrap();
    let pk_after = firefly_protos::deserialize_proto::<firefly_protos::firefly::PreKeyBundleEntries>(&pk_bytes_after).unwrap();
    let bob_pk_ids_after_1_1: Vec<u32> = pk_after.entries.iter().map(|b| b.id).collect();
    assert_eq!(
        bob_pk_ids_after_1_1.len(), 31,
        "Server should have exactly 31 pre-key bundles for Bob after 1 was consumed"
    );
    for id in &bob_pk_ids_after_1_1 {
        assert!(initial_bob_pk_ids.contains(id), "Remaining pre-keys must be from the initial set");
    }

    // 5. Alice creates MLS group and adds Bob (consumes 1 key package of Bob)
    println!("Alice creating MLS group and adding Bob...");
    let group_info = alice_client
        .create_group("Reconnect Test Group".into(), "Description".into(), 0)
        .await
        .expect("Alice failed to create group");
    let group_id = group_info.id;

    alice_client
        .add_group_member(group_id, bob_name.clone(), 0)
        .await
        .expect("Alice failed to add Bob to group");

    // Bob syncs to join group
    bob_client.check_setup().await.expect("Bob failed check_setup");
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Bob sends a group message
    let group_msg = firefly_protos::firefly::GroupMessageInner {
        channelId: 0,
        message: firefly_protos::firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(
            firefly_protos::firefly::MessagePayload {
                text: "hello group from bob before reconnect".to_string().into(),
                ..Default::default()
            },
        ),
    };
    bob_client
        .upload_group_message(group_id, group_msg, 0)
        .await
        .expect("Bob failed to send group message");

    let _received_gmsg = tokio::time::timeout(Duration::from_secs(10), alice_gmsg_rx.recv())
        .await
        .expect("Timeout waiting for group message on Alice")
        .unwrap();
    println!("Alice received group message before reconnect!");

    // 6. Bob restarts (client disposed, new client started with bob_db)
    println!("Restarting Bob client...");
    bob_client.dispose().await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let (bob_msg_tx2, mut bob_msg_rx2) = mpsc::channel(100);
    let (bob_gmsg_tx2, mut bob_gmsg_rx2) = mpsc::channel(100);
    let bob_callbacks2 = TestCallbacks {
        name: bob_name.clone(),
        token: bob_name.clone(),
        message_tx: bob_msg_tx2,
        group_message_tx: bob_gmsg_tx2,
    };
    let bob_client2 = FireflyWsClient::create(
        base_url.clone(),
        ws_url.clone(),
        1000,
        Box::new(bob_callbacks2),
        bob_db.clone(),
        5000,
    )
    .await
    .expect("Failed to recreate Bob client");
    let bob_client2 = Arc::new(bob_client2);
    let b2_init = bob_client2.clone();
    tokio::spawn(async move {
        let _ = b2_init.initialize_with_retrying().await;
    });
    wait_for_init(&bob_client2).await.expect("Bob 2 failed to initialize");

    // 7. Verify Bob's pre-keys and key packages on server after restart:
    // Bob should have replenished ONLY the consumed pre-key (back to 32)
    let resp_pk_bob2 = reqwest::Client::new()
        .get(&url_bob_pk)
        .bearer_auth(&bob_token)
        .send()
        .await
        .unwrap();
    let pk_bob2_bytes = resp_pk_bob2.bytes().await.unwrap();
    let pk_bob2 = firefly_protos::deserialize_proto::<firefly_protos::firefly::PreKeyBundleEntries>(&pk_bob2_bytes).unwrap();
    let bob_pk_ids2: Vec<u32> = pk_bob2.entries.iter().map(|b| b.id).collect();
    assert_eq!(bob_pk_ids2.len(), 32, "Bob should have 32 pre-key bundles after replenishment");

    let preserved_pk_count = bob_pk_ids2.iter().filter(|id| initial_bob_pk_ids.contains(id)).count();
    assert_eq!(
        preserved_pk_count, 31,
        "Exactly 31 pre-key bundles should have been preserved across restart without rotation!"
    );

    // Bob should have replenished ONLY the consumed group key package (back to 32)
    let resp_kp_bob2 = reqwest::Client::new()
        .get(&url_bob_kp)
        .bearer_auth(&bob_token)
        .send()
        .await
        .unwrap();
    let kp_bob2_bytes = resp_kp_bob2.bytes().await.unwrap();
    let kp_bob2 = firefly_protos::deserialize_proto::<firefly_protos::firefly::GroupKeyPackages>(&kp_bob2_bytes).unwrap();
    let bob_kp_ids2: Vec<i32> = kp_bob2.packages.iter().map(|p| p.id).collect();
    assert_eq!(bob_kp_ids2.len(), 32, "Bob should have 32 key packages after replenishment");

    let preserved_kp_count = bob_kp_ids2.iter().filter(|id| initial_bob_kp_ids.contains(id)).count();
    assert_eq!(
        preserved_kp_count, 31,
        "Exactly 31 key packages should have been preserved across restart without rotation!"
    );

    // 8. SESSION PERSISTENCE: 1:1 messaging across reconnect
    println!("Testing 1:1 messaging persistence after Bob reconnect...");
    bob_client2
        .encrypt_and_send(alice_name.clone(), b"reply from bob after reconnect".to_vec())
        .await
        .expect("Bob failed to send 1:1 message after reconnect");

    let msg_alice = tokio::time::timeout(Duration::from_secs(10), alice_msg_rx.recv())
        .await
        .expect("Timeout waiting for 1:1 message on Alice from reconnected Bob")
        .unwrap();
    assert_eq!(msg_alice.message, b"reply from bob after reconnect");

    alice_client
        .encrypt_and_send(bob_name.clone(), b"alice -> bob 1:1 after reconnect".to_vec())
        .await
        .expect("Alice failed to reply to Bob after reconnect");

    let msg_bob = tokio::time::timeout(Duration::from_secs(10), bob_msg_rx2.recv())
        .await
        .expect("Timeout waiting for 1:1 message on Bob from Alice")
        .unwrap();
    assert_eq!(msg_bob.message, b"alice -> bob 1:1 after reconnect");
    println!("1:1 messaging session persisted successfully across reconnect!");

    // 9. SESSION PERSISTENCE: MLS Group messaging across reconnect
    println!("Testing MLS Group messaging persistence after Bob reconnect...");
    let gmsg_bob = firefly_protos::firefly::GroupMessageInner {
        channelId: 0,
        message: firefly_protos::firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(
            firefly_protos::firefly::MessagePayload {
                text: "group message from bob after reconnect".to_string().into(),
                ..Default::default()
            },
        ),
    };
    bob_client2
        .upload_group_message(group_id, gmsg_bob, 0)
        .await
        .expect("Bob failed to send group message after reconnect");

    let _gmsg_alice = tokio::time::timeout(Duration::from_secs(10), alice_gmsg_rx.recv())
        .await
        .expect("Timeout waiting for group message on Alice from reconnected Bob")
        .unwrap();
    println!("Alice received group message from reconnected Bob!");

    let gmsg_alice_reply = firefly_protos::firefly::GroupMessageInner {
        channelId: 0,
        message: firefly_protos::firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(
            firefly_protos::firefly::MessagePayload {
                text: "group reply from alice after reconnect".to_string().into(),
                ..Default::default()
            },
        ),
    };
    alice_client
        .upload_group_message(group_id, gmsg_alice_reply, 0)
        .await
        .expect("Alice failed to reply to group after Bob reconnect");

    let _gmsg_bob_recv = tokio::time::timeout(Duration::from_secs(10), bob_gmsg_rx2.recv())
        .await
        .expect("Timeout waiting for group message on Bob from Alice")
        .unwrap();
    println!("Bob received group reply from Alice!");

    // 10. Clean up clients and db files
    bob_client2.dispose().await;
    alice_client.dispose().await;
    cleanup_dir(&test_dir);
}
