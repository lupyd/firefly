use async_trait::async_trait;
use firefly_client::callbacks::FireflyWsClientCallback;
use firefly_client::db::{group_messages::GroupMessage, messages::UserMessage};
use firefly_client::websocket::FfiFireflyWsClient;
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

fn cleanup_dir(dir: &str) {
    if let Err(e) = std::fs::remove_dir_all(dir) {
        log::warn!("Failed to clean up test directory {}: {}", dir, e);
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

async fn wait_for_init(client: &FfiFireflyWsClient) -> anyhow::Result<()> {
    for _ in 0..60 {
        if client.is_initialized() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(anyhow::anyhow!("Client timeout waiting for initialization"))
}

#[tokio::test]
async fn test_public_link_flow() {
    let (base_url, ws_url) = match setup_server().await {
        Some(urls) => urls,
        None => return,
    };

    let test_run_id = rand::random::<u32>();
    let test_dir = format!("/tmp/firefly/link_{}", test_run_id);
    let _ = std::fs::create_dir_all(&test_dir);
    let alice_name = format!("alice_lnk_{}", test_run_id);
    let bob_name = format!("bob_lnk_{}", test_run_id);
    let charlie_name = format!("charlie_lnk_{}", test_run_id);
    let dave_name = format!("dave_lnk_{}", test_run_id);

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

    let alice_init = alice_client.clone();
    tokio::spawn(async move {
        let _ = alice_init.initialize_with_retrying().await;
    });

    wait_for_init(&alice_client)
        .await
        .expect("Alice failed to initialize");

    // Bob Setup
    let (bob_msg_tx, _bob_msg_rx) = mpsc::channel(100);
    let (bob_gmsg_tx, mut bob_gmsg_rx) = mpsc::channel(100);
    let bob_callbacks = TestCallbacks {
        name: bob_name.clone(),
        token: bob_name.clone(),
        message_tx: bob_msg_tx,
        group_message_tx: bob_gmsg_tx,
    };

    let bob_db = format!("{}/bob.db", test_dir);

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

    wait_for_init(&bob_client)
        .await
        .expect("Bob failed to initialize");

    // 1. Alice creates a group
    let group = alice_client
        .create_group("Public Group".into(), "Description".into(), 0)
        .await
        .expect("Failed to create group");

    // 2. Alice creates a join link
    let link_token = alice_client
        .create_join_link(group.id, 3600, 10)
        .await
        .expect("Failed to create join link");
    println!("Created join link: {}", link_token);

    // 3. Bob joins via link
    bob_client
        .join_via_link(&link_token)
        .await
        .expect("Bob failed to join via link");

    // Wait for Alice to process the join request and add Bob
    tokio::time::sleep(Duration::from_secs(3)).await;

    // 4. Alice sends a group message
    let alice_msg_inner = firefly_protos::firefly::GroupMessageInner {
        channelId: 0,
        message: firefly_protos::firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(
            firefly_protos::firefly::MessagePayload {
                text: std::borrow::Cow::Borrowed("Hello from Alice to the public group!"),
                ..Default::default()
            },
        ),
    };
    let alice_msg_bytes = firefly_client::utils::serialize_proto(&alice_msg_inner).unwrap();
    alice_client
        .encrypt_and_send_group(group.id, alice_msg_bytes.to_vec())
        .await
        .expect("Alice failed to send group message");

    // 5. Bob should receive and decrypt it
    let received_msg_by_bob = tokio::time::timeout(Duration::from_secs(5), bob_gmsg_rx.recv())
        .await
        .expect("Bob timed out waiting for Alice's message")
        .expect("Bob failed to receive message");

    let decoded_bob_msg = firefly_client::utils::deserialize_proto::<
        firefly_protos::firefly::GroupMessageInner,
    >(&received_msg_by_bob.message)
    .unwrap();
    if let firefly_protos::firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(payload) =
        decoded_bob_msg.message
    {
        assert_eq!(payload.text, "Hello from Alice to the public group!");
    } else {
        panic!("Unexpected message type");
    }

    // 6. Bob sends a group message
    let bob_msg_inner = firefly_protos::firefly::GroupMessageInner {
        channelId: 0,
        message: firefly_protos::firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(
            firefly_protos::firefly::MessagePayload {
                text: std::borrow::Cow::Borrowed("Hello from Bob!"),
                ..Default::default()
            },
        ),
    };
    let bob_msg_bytes = firefly_client::utils::serialize_proto(&bob_msg_inner).unwrap();
    bob_client
        .encrypt_and_send_group(group.id, bob_msg_bytes.to_vec())
        .await
        .expect("Bob failed to send group message");

    // 7. Alice should receive and decrypt it
    let received_msg_by_alice = tokio::time::timeout(Duration::from_secs(5), alice_gmsg_rx.recv())
        .await
        .expect("Alice timed out waiting for Bob's message")
        .expect("Alice failed to receive message");

    let decoded_alice_msg = firefly_client::utils::deserialize_proto::<
        firefly_protos::firefly::GroupMessageInner,
    >(&received_msg_by_alice.message)
    .unwrap();
    if let firefly_protos::firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(payload) =
        decoded_alice_msg.message
    {
        assert_eq!(payload.text, "Hello from Bob!");
    } else {
        panic!("Unexpected message type");
    }

    println!("Message exchange verified successfully!");

    // --- Test Max Uses ---
    println!("Testing max uses limit...");
    let link_max_uses = alice_client
        .create_join_link(group.id, 3600, 1)
        .await
        .expect("Failed to create max_uses link");
    // Charlie Setup
    let (charlie_msg_tx, _charlie_msg_rx) = mpsc::channel(100);
    let (charlie_gmsg_tx, _charlie_gmsg_rx) = mpsc::channel(100);
    let charlie_callbacks = TestCallbacks {
        name: charlie_name.clone(),
        token: charlie_name.clone(),
        message_tx: charlie_msg_tx,
        group_message_tx: charlie_gmsg_tx,
    };
    let charlie_db = format!("{}/charlie.db", test_dir);
    let charlie_client = FfiFireflyWsClient::create(
        base_url.clone(),
        ws_url.clone(),
        1000,
        Box::new(charlie_callbacks),
        charlie_db.clone(),
        5000,
    )
    .await
    .expect("Failed to create Charlie client");

    let charlie_init = charlie_client.clone();
    tokio::spawn(async move {
        let _ = charlie_init.initialize_with_retrying().await;
    });
    wait_for_init(&charlie_client)
        .await
        .expect("Charlie failed to initialize");

    charlie_client
        .join_via_link(&link_max_uses)
        .await
        .expect("Charlie failed to join");

    // Wait for Alice to process Charlie
    tokio::time::sleep(Duration::from_secs(3)).await;
    // Dave Setup
    let (dave_msg_tx, _dave_msg_rx) = mpsc::channel(100);
    let (dave_gmsg_tx, _dave_gmsg_rx) = mpsc::channel(100);
    let dave_callbacks = TestCallbacks {
        name: dave_name.clone(),
        token: dave_name.clone(),
        message_tx: dave_msg_tx,
        group_message_tx: dave_gmsg_tx,
    };
    let dave_db = format!("{}/dave.db", test_dir);
    let dave_client = FfiFireflyWsClient::create(
        base_url.clone(),
        ws_url.clone(),
        1000,
        Box::new(dave_callbacks),
        dave_db.clone(),
        5000,
    )
    .await
    .expect("Failed to create Dave client");

    let dave_init = dave_client.clone();
    tokio::spawn(async move {
        let _ = dave_init.initialize_with_retrying().await;
    });
    wait_for_init(&dave_client)
        .await
        .expect("Dave failed to initialize");

    let err = dave_client
        .join_via_link(&link_max_uses)
        .await
        .expect_err("Dave should have failed to join");
    assert!(
        err.to_string().contains("Invalid link"),
        "Unexpected error: {}",
        err
    );
    println!("Max uses limit verified successfully!");

    // --- Test Expiry ---
    println!("Testing link expiry...");
    let link_expiry = alice_client
        .create_join_link(group.id, 1, 10)
        .await
        .expect("Failed to create expiry link");

    tokio::time::sleep(Duration::from_secs(2)).await;

    let err2 = dave_client
        .join_via_link(&link_expiry)
        .await
        .expect_err("Dave should have failed to join expired link");
    assert!(
        err2.to_string().contains("Invalid link"),
        "Unexpected error: {}",
        err2
    );
    println!("Link expiry verified successfully!");

    // Cleanup
    let _ = alice_client.dispose().await;
    let _ = bob_client.dispose().await;
    let _ = charlie_client.dispose().await;
    let _ = dave_client.dispose().await;
    cleanup_dir(&test_dir);
}
