use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;
use firefly_client::callbacks::FireflyWsClientCallback;
use firefly_client::db::{group_messages::GroupMessage, messages::UserMessage};
use firefly_client::websocket::FireflyWsClient;
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

async fn setup_server() -> Option<(String, String, String)> {
    let _ = std::fs::create_dir_all("/tmp/firefly");
    dotenv::from_filename(".env.test").ok();
    dotenv::dotenv().ok();
    if let Ok(base_url) = std::env::var("FIREFLY_BASE_URL") {
        let ws_url = std::env::var("FIREFLY_WS_URL").unwrap_or_else(|_| {
            base_url
                .replace("http://", "ws://")
                .replace("https://", "wss://")
        });
        let wt_url = std::env::var("FIREFLY_WT_URL").unwrap_or_else(|_| {
            base_url
                .replace("http://", "https://")
                .replace("ws://", "https://")
        });
        firefly_client::init_logger("/tmp/firefly/test_wt.log".to_string());
        Some((base_url, ws_url, wt_url))
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
async fn test_webtransport_client_lifecycle_and_parity() {
    let (base_url, ws_url, wt_url) = match setup_server().await {
        Some(urls) => urls,
        None => return,
    };

    let test_run_id = rand::random::<u32>();
    let test_dir = format!("/tmp/firefly/wt_{}", test_run_id);
    let _ = std::fs::create_dir_all(&test_dir);
    let alice_name = format!("alice_wt_{}", test_run_id);
    let bob_name = format!("bob_ws_{}", test_run_id);

    // 1. Alice connects via WebTransport
    let (alice_msg_tx, mut alice_msg_rx) = mpsc::channel(100);
    let (alice_gmsg_tx, _alice_gmsg_rx) = mpsc::channel(100);
    let alice_callbacks = TestCallbacks {
        name: alice_name.clone(),
        token: alice_name.clone(),
        message_tx: alice_msg_tx,
        group_message_tx: alice_gmsg_tx,
    };
    let alice_db = format!("{}/alice.db", test_dir);

    let alice_client = FireflyWsClient::create_webtransport(
        base_url.clone(),
        wt_url.clone(),
        1000,
        Box::new(alice_callbacks),
        alice_db,
        5000,
    )
    .await
    .expect("Failed to create Alice WebTransport client");
    let alice_client = Arc::new(alice_client);

    let alice_init = alice_client.clone();
    tokio::spawn(async move {
        let _ = alice_init.initialize_with_retrying().await;
    });

    wait_for_init(&alice_client)
        .await
        .expect("Alice failed to initialize via WebTransport");

    // 2. Bob connects via legacy WebSocket
    let (bob_msg_tx, mut bob_msg_rx) = mpsc::channel(100);
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
        bob_db,
        5000,
    )
    .await
    .expect("Failed to create Bob WebSocket client");
    let bob_client = Arc::new(bob_client);

    let bob_init = bob_client.clone();
    tokio::spawn(async move {
        let _ = bob_init.initialize_with_retrying().await;
    });

    wait_for_init(&bob_client)
        .await
        .expect("Bob failed to initialize via WebSocket");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // 3. Test RPC Request via WebTransport dedicated bidirectional stream:
    // Alice checks online status of [alice, bob]
    let online = alice_client
        .get_online_status(vec![alice_name.clone(), bob_name.clone()])
        .await
        .expect("Alice should fetch online status via WT bi-stream");
    assert!(online.contains(&alice_name), "Alice must be online");
    assert!(online.contains(&bob_name), "Bob must be online");

    // 4. Test Cross-Protocol Messaging: WebTransport -> WebSocket
    // Alice sends end-to-end encrypted message to Bob
    let sent_msg = alice_client
        .encrypt_and_send(bob_name.clone(), b"Hello Bob from WebTransport!".to_vec())
        .await
        .expect("Alice should send message to Bob via WT");
    assert_eq!(sent_msg.other, bob_name);

    // Bob receives message on WebSocket
    let received_by_bob = tokio::time::timeout(Duration::from_secs(10), bob_msg_rx.recv())
        .await
        .expect("Bob timeout waiting for message")
        .expect("Bob should receive message");
    assert_eq!(received_by_bob.message, b"Hello Bob from WebTransport!");
    assert_eq!(received_by_bob.other, alice_name);

    // 5. Test Cross-Protocol Messaging: WebSocket -> WebTransport Push Stream
    // Bob replies to Alice via WebSocket
    let bob_reply = bob_client
        .encrypt_and_send(alice_name.clone(), b"Hello Alice received via WS!".to_vec())
        .await
        .expect("Bob should reply to Alice via WS");
    assert_eq!(bob_reply.other, alice_name);

    // Alice receives reply on her WebTransport server-to-client push stream
    let received_by_alice = tokio::time::timeout(Duration::from_secs(10), alice_msg_rx.recv())
        .await
        .expect("Alice timeout waiting for message")
        .expect("Alice should receive message via WT push stream");
    assert_eq!(received_by_alice.message, b"Hello Alice received via WS!");
    assert_eq!(received_by_alice.other, bob_name);

    // 6. Test Concurrent RPC Requests over dedicated bidirectional streams
    let mut handles = Vec::new();
    for _ in 0..10 {
        let client = alice_client.clone();
        let an = alice_name.clone();
        let bn = bob_name.clone();
        handles.push(tokio::spawn(async move {
            client.get_online_status(vec![an, bn]).await
        }));
    }
    for handle in handles {
        let res = handle.await.unwrap().expect("Concurrent WT RPC must succeed");
        assert!(res.contains(&alice_name));
    }

    // Cleanup
    alice_client.dispose().await;
    bob_client.dispose().await;
    let _ = std::fs::remove_dir_all(&test_dir);
}

#[tokio::test]
async fn test_webtransport_auto_detection() {
    let (base_url, _ws_url, wt_url) = match setup_server().await {
        Some(urls) => urls,
        None => return,
    };

    let test_run_id = rand::random::<u32>();
    let test_dir = format!("/tmp/firefly/wt_auto_{}", test_run_id);
    let _ = std::fs::create_dir_all(&test_dir);
    let charlie_name = format!("charlie_wt_{}", test_run_id);

    let (charlie_msg_tx, _charlie_msg_rx) = mpsc::channel(100);
    let (charlie_gmsg_tx, _charlie_gmsg_rx) = mpsc::channel(100);
    let charlie_callbacks = TestCallbacks {
        name: charlie_name.clone(),
        token: charlie_name.clone(),
        message_tx: charlie_msg_tx,
        group_message_tx: charlie_gmsg_tx,
    };
    let charlie_db = format!("{}/charlie.db", test_dir);

    // Initialized via create(base_url, wt_url, ...) - auto detection should select WebTransport!
    let charlie_client = FireflyWsClient::create(
        base_url.clone(),
        wt_url.clone(),
        1000,
        Box::new(charlie_callbacks),
        charlie_db,
        5000,
    )
    .await
    .expect("Failed to create Charlie client with auto transport");
    let charlie_client = Arc::new(charlie_client);

    let charlie_init = charlie_client.clone();
    tokio::spawn(async move {
        let _ = charlie_init.initialize_with_retrying().await;
    });

    wait_for_init(&charlie_client)
        .await
        .expect("Charlie failed to initialize via auto-detected WebTransport");

    let online = charlie_client
        .get_online_status(vec![charlie_name.clone()])
        .await
        .expect("Online status check should succeed");
    assert!(online.contains(&charlie_name));

    charlie_client.dispose().await;
    let _ = std::fs::remove_dir_all(&test_dir);
}
