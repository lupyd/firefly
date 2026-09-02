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

    let alice_db = format!("/tmp/alice_group_test_{}.db", test_run_id);
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

    // Wait for Alice to connect and register
    println!("Waiting for Alice to initialize...");
    wait_for_init(&alice_client)
        .await
        .expect("Alice failed to initialize");
    println!("Alice initialized!");

    // Bob Setup
    let (bob_msg_tx, _bob_msg_rx) = mpsc::channel(100);
    let (bob_gmsg_tx, mut bob_gmsg_rx) = mpsc::channel(100);
    let bob_callbacks = TestCallbacks {
        name: bob_name.clone(),
        token: bob_name.clone(),
        message_tx: bob_msg_tx,
        group_message_tx: bob_gmsg_tx,
    };

    let bob_db = format!("/tmp/bob_group_test_{}.db", test_run_id);
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
    let _ = std::fs::remove_file(&alice_db);
    let _ = std::fs::remove_file(&bob_db);
}
