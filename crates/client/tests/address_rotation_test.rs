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
        let _ = self.message_tx.send(message).await;
    }

    async fn on_group_message(&self, group_message: GroupMessage) {
        let _ = self.group_message_tx.send(group_message).await;
    }
}

async fn setup_server() -> Option<(String, String)> {
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

    let mut alice_clients = Vec::new();
    let mut alice_receivers = Vec::new();

    // 1. Create 5 Alice devices
    println!("Creating 5 Alice devices...");
    for i in 1..=5 {
        let (msg_tx, msg_rx) = mpsc::channel(100);
        let (gmsg_tx, _gmsg_rx) = mpsc::channel(100);
        let callbacks = TestCallbacks {
            name: "alice".into(),
            token: "alice".into(),
            message_tx: msg_tx,
            group_message_tx: gmsg_tx,
        };

        let db = format!("/tmp/alice_rot_{}_{}.db", i, test_run_id);
        let _ = std::fs::remove_file(&db);

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
        name: "bob".into(),
        token: "bob".into(),
        message_tx: bob_msg_tx,
        group_message_tx: bob_gmsg_tx,
    };
    let bob_db = format!("/tmp/bob_rot_{}.db", test_run_id);
    let _ = std::fs::remove_file(&bob_db);
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

    // 3. Bob sends message to Alice
    println!("Bob sending first message to Alice...");
    bob_client
        .encrypt_and_send("alice".to_string(), b"hello 1".to_vec())
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
        name: "alice".into(),
        token: "alice".into(),
        message_tx: msg_tx6,
        group_message_tx: gmsg_tx6,
    };

    let db6 = format!("/tmp/alice_rot_6_{}.db", test_run_id);
    let _ = std::fs::remove_file(&db6);

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

    // 7. Bob sends second message
    println!("Bob sending second message to Alice...");
    bob_client
        .encrypt_and_send("alice".to_string(), b"hello 2".to_vec())
        .await
        .expect("Bob failed to send second message");

    // Check device 1 (the oldest one)
    println!("Checking that device 1 does NOT receive the message...");
    let res1 = tokio::time::timeout(Duration::from_secs(5), alice_receivers[0].recv()).await;
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

    // Attempt cleanup of db files
    for i in 1..=5 {
        let _ = std::fs::remove_file(format!("/tmp/alice_rot_{}_{}.db", i, test_run_id));
    }
    let _ = std::fs::remove_file(format!("/tmp/alice_rot_6_{}.db", test_run_id));
    let _ = std::fs::remove_file(format!("/tmp/bob_rot_{}.db", test_run_id));
}

#[tokio::test]
async fn test_bidirectional_multi_device_rotation_and_messaging() {
    let (base_url, ws_url) = match setup_server().await {
        Some(urls) => urls,
        None => return,
    };

    let test_run_id = rand::random::<u32>();

    // 1. Create 3 initial Alice devices and 3 initial Bob devices
    let mut alice_clients = Vec::new();
    let mut alice_receivers = Vec::new();
    for i in 1..=3 {
        let (msg_tx, msg_rx) = mpsc::channel(100);
        let (gmsg_tx, _gmsg_rx) = mpsc::channel(100);
        let callbacks = TestCallbacks {
            name: "alice".into(),
            token: "alice".into(),
            message_tx: msg_tx,
            group_message_tx: gmsg_tx,
        };

        let db = format!("/tmp/alice_bidi_{}_{}.db", i, test_run_id);
        let _ = std::fs::remove_file(&db);

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

    let mut bob_clients = Vec::new();
    let mut bob_receivers = Vec::new();
    for i in 1..=3 {
        let (msg_tx, msg_rx) = mpsc::channel(100);
        let (gmsg_tx, _gmsg_rx) = mpsc::channel(100);
        let callbacks = TestCallbacks {
            name: "bob".into(),
            token: "bob".into(),
            message_tx: msg_tx,
            group_message_tx: gmsg_tx,
        };

        let db = format!("/tmp/bob_bidi_{}_{}.db", i, test_run_id);
        let _ = std::fs::remove_file(&db);

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

    // 2. Alice1 sends message to Bob (All 3 Bob devices receive)
    println!("Alice 1 sending message to Bob...");
    alice_clients[0]
        .encrypt_and_send("bob".to_string(), b"alice -> bob 1".to_vec())
        .await
        .expect("Alice 1 failed to send");

    for i in 0..3 {
        let msg = tokio::time::timeout(Duration::from_secs(10), bob_receivers[i].recv())
            .await
            .expect(&format!("Timeout waiting for message on Bob device {}", i + 1))
            .unwrap();
        assert_eq!(msg.message, b"alice -> bob 1");
    }

    // 3. Bob1 replies to Alice (All 3 Alice devices receive)
    println!("Bob 1 sending reply to Alice...");
    bob_clients[0]
        .encrypt_and_send("alice".to_string(), b"bob -> alice 1".to_vec())
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
            name: "bob".into(),
            token: "bob".into(),
            message_tx: msg_tx,
            group_message_tx: gmsg_tx,
        };

        let db = format!("/tmp/bob_bidi_{}_{}.db", i, test_run_id);
        let _ = std::fs::remove_file(&db);

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

    // 5. Alice 2 sends message to Bob -> Server will notice Bob1 is gone and return new active Bob devices
    println!("Alice 2 sending message to Bob after Bob rotation...");
    alice_clients[1]
        .encrypt_and_send("bob".to_string(), b"alice -> bob 2".to_vec())
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

    // 6. Bob 6 replies to Alice -> All active Alice devices receive
    println!("Bob 6 sending reply to Alice...");
    bob_clients[5]
        .encrypt_and_send("alice".to_string(), b"bob 6 -> alice".to_vec())
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

    for i in 1..=3 {
        let _ = std::fs::remove_file(format!("/tmp/alice_bidi_{}_{}.db", i, test_run_id));
    }
    for i in 1..=6 {
        let _ = std::fs::remove_file(format!("/tmp/bob_bidi_{}_{}.db", i, test_run_id));
    }
}
