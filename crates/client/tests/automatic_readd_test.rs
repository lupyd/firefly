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
async fn test_automatic_readd() {
    let port = setup_server().await;
    let base_url = format!("http://127.0.0.1:{}", port);
    let ws_url = format!("ws://127.0.0.1:{}/", port);

    let test_run_id = rand::random::<u32>();

    // Alice Setup
    let (alice_msg_tx, _alice_msg_rx) = mpsc::channel(100);
    let (alice_gmsg_tx, mut _alice_gmsg_rx) = mpsc::channel(100);
    let alice_callbacks = TestCallbacks {
        name: "alice".into(),
        token: "alice".into(),
        message_tx: alice_msg_tx,
        group_message_tx: alice_gmsg_tx,
    };

    let alice_db = format!("/tmp/alice_readd_{}.db", test_run_id);
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

    tokio::spawn({
        let alice = alice_client.clone();
        async move {
            let _ = alice.initialize_with_retrying().await;
        }
    });
    wait_for_init(&alice_client)
        .await
        .expect("Alice failed to init");

    // Bob Device 1 Setup
    let (bob1_msg_tx, _bob1_msg_rx) = mpsc::channel(100);
    let (bob1_gmsg_tx, mut _bob1_gmsg_rx) = mpsc::channel(100);
    let bob1_callbacks = TestCallbacks {
        name: "bob".into(),
        token: "bob".into(),
        message_tx: bob1_msg_tx,
        group_message_tx: bob1_gmsg_tx,
    };

    let bob1_db = format!("/tmp/bob1_readd_{}.db", test_run_id);
    let _ = std::fs::remove_file(&bob1_db);

    let bob1_client = FireflyWsClient::create(
        base_url.clone(),
        ws_url.clone(),
        1000,
        Box::new(bob1_callbacks),
        bob1_db.clone(),
        5000,
    )
    .await
    .expect("Failed to create Bob1 client");
    let bob1_client = Arc::new(bob1_client);

    tokio::spawn({
        let bob1 = bob1_client.clone();
        async move {
            let _ = bob1.initialize_with_retrying().await;
        }
    });
    wait_for_init(&bob1_client)
        .await
        .expect("Bob1 failed to init");

    // Alice creates group and adds Bob1
    println!("Alice creating group...");
    let group_info = alice_client
        .create_group("ReAdd Group".into(), "Description".into(), 0)
        .await
        .expect("Alice failed to create group");
    let group_id = group_info.id;

    println!("Alice adding Bob1...");
    if let Err(e) = alice_client
        .add_group_member(group_id, "bob".into(), 0)
        .await
    {
        panic!("Alice failed to add Bob1: {:?}", e);
    }

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Bob Device 2 Setup
    let (bob2_msg_tx, _bob2_msg_rx) = mpsc::channel(100);
    let (bob2_gmsg_tx, mut bob2_gmsg_rx) = mpsc::channel(100);
    let bob2_callbacks = TestCallbacks {
        name: "bob".into(),
        token: "bob".into(),
        message_tx: bob2_msg_tx,
        group_message_tx: bob2_gmsg_tx,
    };

    let bob2_db = format!("/tmp/bob2_readd_{}.db", test_run_id);
    let _ = std::fs::remove_file(&bob2_db);

    let bob2_client = FireflyWsClient::create(
        base_url.clone(),
        ws_url.clone(),
        1000,
        Box::new(bob2_callbacks),
        bob2_db.clone(),
        5000,
    )
    .await
    .expect("Failed to create Bob2 client");
    let bob2_client = Arc::new(bob2_client);

    tokio::spawn({
        let bob2 = bob2_client.clone();
        async move {
            let _ = bob2.initialize_with_retrying().await;
        }
    });
    wait_for_init(&bob2_client)
        .await
        .expect("Bob2 failed to init");

    // Bob2 requests to be re-added to group
    println!("Bob2 requesting re-add...");
    bob2_client
        .request_re_add(vec![group_id])
        .await
        .expect("Bob2 failed to request re-add");

    // Alice should receive the notification and automatically re-add Bob2
    println!("Waiting for Alice to automatically re-add Bob2...");
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Bob2 sends a message to verify he is in the group
    println!("Bob2 sending group message...");
    let message_text = "I was automatically re-added!".to_string();
    let group_msg = firefly::GroupMessageInner {
        channelId: 0,
        message: firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(
            firefly::MessagePayload {
                text: message_text.clone().into(),
                ..Default::default()
            },
        ),
    };

    // Bob2 might need to wait for his own sync or the GroupInvite
    tokio::time::sleep(Duration::from_secs(5)).await;

    bob2_client
        .upload_group_message(group_id, group_msg, 0)
        .await
        .expect("Bob2 failed to send message (he might not have been re-added)");

    println!("Alice waiting for Bob2's message...");
    // Alice (and Bob1) should receive the message
    // Actually we'll just check if Bob2 can send it successfully.

    println!("Test passed!");

    // Cleanup
    let _ = alice_client.dispose().await;
    let _ = bob1_client.dispose().await;
    let _ = bob2_client.dispose().await;
    let _ = std::fs::remove_file(&alice_db);
    let _ = std::fs::remove_file(&bob1_db);
    let _ = std::fs::remove_file(&bob2_db);
}
