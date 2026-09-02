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
        firefly_client::init_logger("/tmp/firefly/test_online.log".to_string());
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
async fn test_online_status() {
    let (base_url, ws_url) = match setup_server().await {
        Some(urls) => urls,
        None => return,
    };

    let test_run_id = rand::random::<u32>();
    let alice_name = format!("alice_on_{}", test_run_id);
    let bob_name = format!("bob_on_{}", test_run_id);
    let charlie_name = format!("charlie_on_{}", test_run_id);

    // Alice Setup
    let (alice_msg_tx, _alice_msg_rx) = mpsc::channel(100);
    let (alice_gmsg_tx, _alice_gmsg_rx) = mpsc::channel(100);
    let alice_callbacks = TestCallbacks {
        name: alice_name.clone(),
        token: alice_name.clone(),
        message_tx: alice_msg_tx,
        group_message_tx: alice_gmsg_tx,
    };

    let alice_db = format!("/tmp/alice_online_test_{}.db", test_run_id);
    let _ = std::fs::remove_file(&alice_db);

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

    // Wait for Alice to connect
    wait_for_init(&alice_client)
        .await
        .expect("Alice failed to initialize");

    // Bob Setup
    let (bob_msg_tx, _bob_msg_rx) = mpsc::channel(100);
    let (bob_gmsg_tx, _bob_gmsg_rx) = mpsc::channel(100);
    let bob_callbacks = TestCallbacks {
        name: bob_name.clone(),
        token: bob_name.clone(),
        message_tx: bob_msg_tx,
        group_message_tx: bob_gmsg_tx,
    };

    let bob_db = format!("/tmp/bob_online_test_{}.db", test_run_id);
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

    // Wait for Bob to connect
    wait_for_init(&bob_client)
        .await
        .expect("Bob failed to initialize");

    // Check online status
    println!("Checking online status...");
    let usernames = vec![
        alice_name.clone(),
        bob_name.clone(),
        charlie_name.clone(),
    ];
    let online_users = alice_client
        .get_online_status(usernames.clone())
        .await
        .expect("Failed to get online status");

    println!("Online users: {:?}", online_users);
    assert!(online_users.contains(&alice_name));
    assert!(online_users.contains(&bob_name));
    assert!(!online_users.contains(&charlie_name));

    // Dispose Bob and check again
    println!("Disposing Bob...");
    bob_client.dispose().await;

    // Give some time for server to handle disconnect
    tokio::time::sleep(Duration::from_secs(1)).await;

    let online_users_after_bob_left = alice_client
        .get_online_status(usernames)
        .await
        .expect("Failed to get online status after Bob left");

    println!(
        "Online users after Bob left: {:?}",
        online_users_after_bob_left
    );
    assert!(online_users_after_bob_left.contains(&alice_name));
    assert!(!online_users_after_bob_left.contains(&bob_name));

    println!("Test passed!");

    // Cleanup
    let _ = alice_client.dispose().await;
    let _ = std::fs::remove_file(&alice_db);
    let _ = std::fs::remove_file(&bob_db);
}
