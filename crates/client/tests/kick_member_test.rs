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
        firefly_client::init_logger("/tmp/firefly/test_kick.log".to_string());
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
async fn test_kick_member() {
    let (base_url, ws_url) = match setup_server().await {
        Some(urls) => urls,
        None => return,
    };

    let test_run_id = rand::random::<u32>();
    let test_dir = format!("/tmp/firefly/kick_{}", test_run_id);
    let _ = std::fs::create_dir_all(&test_dir);

    // Alice Setup
    let (alice_msg_tx, _alice_msg_rx) = mpsc::channel(100);
    let (alice_gmsg_tx, mut alice_gmsg_rx) = mpsc::channel(100);
    let alice_callbacks = TestCallbacks {
        name: "alice".into(),
        token: "alice".into(),
        message_tx: alice_msg_tx,
        group_message_tx: alice_gmsg_tx,
    };

    let alice_db = format!("{}/alice.db", test_dir);

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

    let alice_init = alice_client.clone();
    tokio::spawn(async move {
        let _ = alice_init.initialize_with_retrying().await;
    });

    println!("Waiting for Alice to initialize...");
    wait_for_init(&alice_client)
        .await
        .expect("Alice failed to initialize");
    println!("Alice initialized!");

    // Bob Setup
    let (bob_msg_tx, _bob_msg_rx) = mpsc::channel(100);
    let (bob_gmsg_tx, _bob_gmsg_rx) = mpsc::channel(100);
    let bob_callbacks = TestCallbacks {
        name: "bob".into(),
        token: "bob".into(),
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

    println!("Waiting for Bob to initialize...");
    wait_for_init(&bob_client)
        .await
        .expect("Bob failed to initialize");
    println!("Bob initialized!");

    // Charles Setup
    let (charles_msg_tx, _charles_msg_rx) = mpsc::channel(100);
    let (charles_gmsg_tx, mut charles_gmsg_rx) = mpsc::channel(100);
    let charles_callbacks = TestCallbacks {
        name: "charles".into(),
        token: "charles".into(),
        message_tx: charles_msg_tx,
        group_message_tx: charles_gmsg_tx,
    };

    let charles_db = format!("{}/charles.db", test_dir);

    let charles_client = FireflyWsClient::create(
        base_url.clone(),
        ws_url.clone(),
        1000,
        Box::new(charles_callbacks),
        charles_db.clone(),
        5000,
    )
    .await
    .expect("Failed to create Charles client");
    let charles_client = Arc::new(charles_client);

    let charles_init = charles_client.clone();
    tokio::spawn(async move {
        let _ = charles_init.initialize_with_retrying().await;
    });

    println!("Waiting for Charles to initialize...");
    wait_for_init(&charles_client)
        .await
        .expect("Charles failed to initialize");
    println!("Charles initialized!");

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Alice creates group
    println!("Alice creating group...");
    let group_info = alice_client
        .create_group("Test Kick Group".into(), "Description".into(), 0)
        .await
        .expect("Alice failed to create group");
    let group_id = group_info.id;
    println!("Group created with ID: {}", group_id);

    // Alice adds Bob and Charles
    println!("Alice adding Bob...");
    alice_client
        .add_group_member(group_id, "bob".into(), 0)
        .await
        .expect("Alice failed to add Bob");

    println!("Alice adding Charles...");
    alice_client
        .add_group_member(group_id, "charles".into(), 0)
        .await
        .expect("Alice failed to add Charles");

    println!("Bob and Charles joining group...");
    bob_client
        .check_setup()
        .await
        .expect("Bob failed to join group");
    charles_client
        .check_setup()
        .await
        .expect("Charles failed to join group");
    // Charles (default member without ManageMember) attempts to kick Alice (owner) - must fail!
    println!("Charles attempting to kick Alice (should fail)...");
    let kick_alice_res = charles_client
        .kick_group_member(group_id, "alice".into())
        .await;
    assert!(
        kick_alice_res.is_err(),
        "Charles without ManageMember must NOT be able to kick owner Alice!"
    );
    println!("Charles kicking Alice correctly failed: {:?}", kick_alice_res.err().unwrap());

    // Charles (default member without ManageMember) attempts to kick Bob (active member) - must fail!
    println!("Charles attempting to kick Bob (should fail)...");
    let kick_bob_res = charles_client
        .kick_group_member(group_id, "bob".into())
        .await;
    assert!(
        kick_bob_res.is_err(),
        "Charles without ManageMember must NOT be able to kick active member Bob!"
    );
    println!("Charles kicking Bob correctly failed: {:?}", kick_bob_res.err().unwrap());

    // Alice (owner) removes Bob
    println!("Alice kicking Bob...");
    alice_client
        .kick_group_member(group_id, "bob".into())
        .await
        .expect("Alice failed to kick Bob");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify Charles synchronizes the kick
    println!("Charles checking setup to sync kick...");
    charles_client
        .check_setup()
        .await
        .expect("Charles failed to sync group");
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Verify Bob is not in the group roster on Alice's side (skip for now since no API exists)
    // let alice_roster = alice_client.get_group_members(group_id).await.expect("Failed to get Alice group roster");
    // let bob_in_roster = alice_roster.iter().any(|m| m.username == "bob");
    // assert!(!bob_in_roster, "Bob should not be in the group roster after being kicked");

    // Bob attempts to send a message
    println!("Bob sending group message (should fail or not be received by Alice and Charles)...");
    let message_text = "Hello from kicked Bob!".to_string();
    let group_msg = firefly::GroupMessageInner {
        channelId: 0,
        message: firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(
            firefly::MessagePayload {
                text: message_text.clone().into(),
                ..Default::default()
            },
        ),
    };

    // We expect upload_group_message to potentially fail since Bob is kicked
    let upload_result = bob_client
        .upload_group_message(group_id, group_msg, 0)
        .await;
    if upload_result.is_ok() {
        println!("Bob's upload succeeded (it shouldn't be processed by active members)");
    } else {
        println!(
            "Bob's upload correctly failed: {:?}",
            upload_result.err().unwrap()
        );
    }

    // Alice should NOT receive the message
    println!("Alice waiting for message (should timeout)...");
    let received = tokio::time::timeout(Duration::from_secs(5), alice_gmsg_rx.recv()).await;
    assert!(
        received.is_err(),
        "Alice received a message from a kicked member!"
    );

    // Charles should NOT receive the message
    println!("Charles waiting for message (should timeout)...");
    let received_charles =
        tokio::time::timeout(Duration::from_secs(5), charles_gmsg_rx.recv()).await;
    assert!(
        received_charles.is_err(),
        "Charles received a message from a kicked member!"
    );

    // Charles sends a message to Alice
    println!("Charles sending group message to prove he is still in the group...");
    let charles_message_text = "Hello Alice, Charles is still here!".to_string();
    let charles_group_msg = firefly::GroupMessageInner {
        channelId: 0,
        message: firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(
            firefly::MessagePayload {
                text: charles_message_text.clone().into(),
                ..Default::default()
            },
        ),
    };
    charles_client
        .upload_group_message(group_id, charles_group_msg, 0)
        .await
        .expect("Charles failed to send message");

    println!("Alice waiting for Charles's message...");
    let received_from_charles = tokio::time::timeout(Duration::from_secs(15), alice_gmsg_rx.recv())
        .await
        .expect("Timeout waiting for group message from Charles")
        .expect("Channel closed");

    assert_eq!(received_from_charles.group_id, group_id);
    let decoded_inner =
        deserialize_proto::<firefly::GroupMessageInner>(&received_from_charles.message)
            .expect("Failed to decode group message inner");
    if let firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(payload) =
        decoded_inner.message
    {
        assert_eq!(payload.text.as_ref(), charles_message_text);
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
    let _ = charles_client.dispose().await;
    cleanup_dir(&test_dir);
}
