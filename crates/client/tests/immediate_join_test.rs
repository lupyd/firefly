use async_trait::async_trait;
use firefly_client::callbacks::FireflyWsClientCallback;
use firefly_client::db::{group_messages::GroupMessage, messages::UserMessage};
use firefly_client::websocket::FireflyWsClient;
use firefly_protos::{deserialize_proto, firefly};
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
    firefly_client::init_logger("/tmp/firefly/test.log".to_string());

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
async fn test_immediate_group_join() {
    let port = setup_server().await;
    let base_url = format!("http://127.0.0.1:{}", port);
    let ws_url = format!("ws://127.0.0.1:{}/", port);

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

    let alice_db = format!("/tmp/alice_immediate_{}.db", test_run_id);
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

    wait_for_init(&alice_client)
        .await
        .expect("Alice failed to init");

    // Bob Setup
    let (bob_msg_tx, _bob_msg_rx) = mpsc::channel(100);
    let (bob_gmsg_tx, mut _bob_gmsg_rx) = mpsc::channel(100);
    let bob_callbacks = TestCallbacks {
        name: "bob".into(),
        token: "bob".into(),
        message_tx: bob_msg_tx,
        group_message_tx: bob_gmsg_tx,
    };

    let bob_db = format!("/tmp/bob_immediate_{}.db", test_run_id);
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
        .expect("Bob failed to init");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Alice creates group
    println!("Alice creating group...");
    let group_info = alice_client
        .create_group("Immediate Group".into(), "Description".into(), 0)
        .await
        .expect("Alice failed to create group");
    let group_id = group_info.id;

    // Alice adds Bob
    println!("Alice adding Bob...");
    alice_client
        .add_group_member(group_id, "bob".into(), 0)
        .await
        .expect("Alice failed to add Bob");

    // Wait for Bob to join automatically via websocket notification
    println!("Waiting for Bob to receive invite and join...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Bob sends a message to verify he is in the group
    println!("Bob sending group message...");
    let message_text = "I joined immediately!".to_string();
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
        .expect("Bob failed to send message (he might not have joined automatically)");

    // Alice should receive it
    println!("Alice waiting for message...");
    let received = tokio::time::timeout(Duration::from_secs(10), alice_gmsg_rx.recv())
        .await
        .expect("Timeout waiting for group message")
        .expect("Channel closed");

    assert_eq!(received.group_id, group_id);
    println!("Test passed!");

    // Cleanup
    let _ = alice_client.dispose().await;
    let _ = bob_client.dispose().await;
    let _ = std::fs::remove_file(&alice_db);
    let _ = std::fs::remove_file(&bob_db);
}
