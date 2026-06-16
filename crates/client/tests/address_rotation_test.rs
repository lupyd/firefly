use async_trait::async_trait;
use firefly_client::callbacks::FireflyWsClientCallback;
use firefly_client::db::{group_messages::GroupMessage, messages::UserMessage};
use firefly_client::websocket::FireflyWsClient;
use firefly_server::start_http_server;
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

async fn setup_server() -> u16 {
    dotenv::from_filename(".env.test").ok();
    // Use a random port to avoid conflicts
    let port = 30000 + (rand::random::<u16>() % 10000);
    let base_url = format!("http://127.0.0.1:{}", port);

    unsafe {
        std::env::set_var("EMULATOR_MODE", "true");
        std::env::set_var("NO_TOKEN_VERIFICATION", "true");
        std::env::set_var("PORT", port.to_string());
        std::env::set_var("FIREFLY_BASE_URL", base_url);
    }
    firefly_client::init_logger("/tmp/firefly/test_rotation.log".to_string());

    tokio::spawn(async move {
        if let Err(e) = start_http_server(port).await {
            eprintln!("Server failed to start: {}", e);
        }
    });
    // Give some time for server to start
    tokio::time::sleep(Duration::from_secs(3)).await;
    port
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
    let port = setup_server().await;
    let base_url = format!("http://127.0.0.1:{}", port);
    let ws_url = format!("ws://127.0.0.1:{}/", port);

    let test_run_id = rand::random::<u32>();

    let mut alice_clients = Vec::new();
    let mut alice_receivers = Vec::new();

    // 1. Create 5 Alice devices
    println!("Creating 5 Alice devices...");
    for i in 1..=5 {
        let (msg_tx, msg_rx) = mpsc::channel(100);
        let (gmsg_tx, _gmsg_rx) = mpsc::channel(100);
        let callbacks = TestCallbacks {
            name: format!("alice_dev_{}", i),
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
    name: "alice_dev_6".into(),
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
