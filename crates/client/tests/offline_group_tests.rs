use async_trait::async_trait;
use firefly_client::callbacks::FireflyWsClientCallback;
use firefly_client::db::{group_messages::GroupMessage, messages::UserMessage};
use firefly_client::websocket::{ConnectionState, FfiFireflyWsClient};
use firefly_protos::firefly;
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
        firefly_client::init_logger("/tmp/firefly/test.log".to_string());
        Some((base_url, ws_url))
    } else {
        println!("Skipping integration test: FIREFLY_BASE_URL is not set.");
        None
    }
}

async fn wait_for_init(client: &FfiFireflyWsClient) -> bool {
    for _ in 0..120 {
        if client.is_initialized() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    false
}

#[tokio::test]
async fn test_offline_group_operations() {
    let (base_url, ws_url) = match setup_server().await {
        Some(urls) => urls,
        None => return,
    };

    let test_run_id = rand::random::<u32>();

    // Alice Setup
    let (alice_msg_tx, _alice_msg_rx) = mpsc::channel(100);
    let (alice_gmsg_tx, mut alice_gmsg_rx) = mpsc::channel(100);
    let alice_callbacks = TestCallbacks {
        name: "alice".into(),
        token: "alice".into(),
        message_tx: alice_msg_tx,
        group_message_tx: alice_gmsg_tx,
    };

    let alice_db = format!("/tmp/offline_alice_{}.db", test_run_id);
    let _ = std::fs::remove_file(&alice_db);

    let alice_client = FfiFireflyWsClient::create(
        base_url.clone(),
        ws_url.clone(),
        1000,
        Box::new(alice_callbacks),
        alice_db.clone(),
        5000,
    )
    .await
    .expect("Failed to create Alice client");

    // Initialize Alice
    let alice_init = alice_client.clone();
    tokio::spawn(async move {
        let _ = alice_init.initialize_with_retrying().await;
    });

    // Bob Setup
    let (bob_msg_tx, _bob_msg_rx) = mpsc::channel(100);
    let (bob_gmsg_tx, mut _bob_gmsg_rx) = mpsc::channel(100);
    let bob_callbacks = TestCallbacks {
        name: "bob".into(),
        token: "bob".into(),
        message_tx: bob_msg_tx,
        group_message_tx: bob_gmsg_tx,
    };

    let bob_db = format!("/tmp/offline_bob_{}.db", test_run_id);
    let _ = std::fs::remove_file(&bob_db);

    let bob_client = FfiFireflyWsClient::create(
        base_url.clone(),
        ws_url.clone(),
        1000,
        Box::new(bob_callbacks),
        bob_db.clone(),
        5000,
    )
    .await
    .expect("Failed to create Bob client");

    let bob_init = bob_client.clone();
    tokio::spawn(async move {
        let _ = bob_init.initialize_with_retrying().await;
    });

    // Wait for initialization
    assert!(
        wait_for_init(&alice_client).await,
        "Alice failed to initialize (MLS client)"
    );
    assert!(
        wait_for_init(&bob_client).await,
        "Bob failed to initialize (MLS client)"
    );

    // Also wait for connection for Alice if we want to create group
    let mut alice_connected = false;
    for _ in 0..60 {
        if matches!(
            alice_client.get_connection_state(),
            ConnectionState::Connected
        ) {
            alice_connected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(alice_connected, "Alice failed to connect to server");

    // Alice creates group
    let group_info = alice_client
        .create_group("Offline Test Group".into(), "Description".into(), 0)
        .await
        .expect("Alice failed to create group");
    let group_id = group_info.id;

    // Alice adds Bob
    alice_client
        .add_group_member(group_id, "bob".into(), 0)
        .await
        .expect("Alice failed to add Bob");

    // Bob should auto-join. Wait for Bob to receive the invite and join.
    println!("Bob joining group...");
    bob_client
        .check_setup()
        .await
        .expect("Bob failed to join group");
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Bob sends a message
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
    let payload = firefly_protos::serialize_proto(&group_msg)
        .unwrap()
        .to_vec();
    bob_client
        .encrypt_and_send_group(group_id, payload)
        .await
        .expect("Bob failed to send message");

    // Alice should receive the message
    let _ = tokio::time::timeout(Duration::from_secs(10), alice_gmsg_rx.recv())
        .await
        .expect("Timeout waiting for group message")
        .expect("Channel closed");

    // Now simulate Bob going offline and re-loading
    println!("Bob going offline...");
    // We can just drop the bob_client or stop it.
    std::mem::drop(bob_client);

    // Create a new Bob client pointing to a NON-EXISTENT server
    let unreachable_url = "http://127.0.0.1:1".to_string();
    let (bob_msg_tx2, _bob_msg_rx2) = mpsc::channel(100);
    let (bob_gmsg_tx2, _bob_gmsg_rx2) = mpsc::channel(100);
    let bob_callbacks2 = TestCallbacks {
        name: "bob".into(),
        token: "bob".into(),
        message_tx: bob_msg_tx2,
        group_message_tx: bob_gmsg_tx2,
    };

    let offline_bob = FfiFireflyWsClient::create(
        unreachable_url.clone(),
        unreachable_url.clone(),
        1000,
        Box::new(bob_callbacks2),
        bob_db.clone(),
        5000,
    )
    .await
    .expect("Failed to create offline Bob client");

    // Do NOT call initialize_with_retrying (which would try to connect)
    // Instead, call load_all_groups which should work OFFLINE because the identity is already in bob_db
    println!("Bob loading groups offline...");
    offline_bob
        .load_all_groups()
        .await
        .expect("Failed to load groups offline");

    // Verify Bob can see the group extension offline
    let extension = offline_bob
        .get_group_extension(group_id)
        .await
        .expect("Failed to get extension offline");
    assert!(!extension.is_empty());
    println!("Successfully retrieved group extension offline!");

    // Cleanup
    let _ = alice_client.dispose().await;
    let _ = std::fs::remove_file(&alice_db);
    let _ = std::fs::remove_file(&bob_db);
}
