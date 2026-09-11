use async_trait::async_trait;
use firefly_client::callbacks::FireflyWsClientCallback;
use firefly_client::db::{group_messages::GroupMessage, messages::UserMessage};
use firefly_client::websocket::FireflyWsClient;
use firefly_core::utils::HTTP_CLIENT;
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
    let _ = std::fs::create_dir_all("/tmp/firefly");
    dotenv::from_filename(".env.test").ok();
    dotenv::dotenv().ok();
    if let Ok(base_url) = std::env::var("FIREFLY_BASE_URL") {
        let ws_url = std::env::var("FIREFLY_WS_URL").unwrap_or_else(|_| {
            base_url
                .replace("http://", "ws://")
                .replace("https://", "wss://")
        });
        firefly_client::init_logger("/tmp/firefly/test_server_parity.log".to_string());
        Some((base_url, ws_url))
    } else {
        println!("Skipping integration test: FIREFLY_BASE_URL is not set.");
        None
    }
}

fn cleanup_dir(dir: &str) {
    if let Err(e) = std::fs::remove_dir_all(dir) {
        if e.kind() != std::io::ErrorKind::NotFound {
            log::warn!("Failed to cleanup test dir {}: {:?}", dir, e);
        }
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

async fn create_client_helper(
    base_url: &str,
    ws_url: &str,
    test_dir: &str,
    username: &str,
) -> (
    Arc<FireflyWsClient>,
    mpsc::Receiver<UserMessage>,
    mpsc::Receiver<GroupMessage>,
) {
    let (msg_tx, msg_rx) = mpsc::channel(100);
    let (gmsg_tx, gmsg_rx) = mpsc::channel(100);
    let callbacks = TestCallbacks {
        name: username.to_string(),
        token: username.to_string(),
        message_tx: msg_tx,
        group_message_tx: gmsg_tx,
    };

    let db_path = format!("{}/{}.db", test_dir, username);
    let client = FireflyWsClient::create(
        base_url.to_string(),
        ws_url.to_string(),
        1000,
        Box::new(callbacks),
        db_path,
        5000,
    )
    .await
    .expect("Failed to create client");
    let client = Arc::new(client);

    let client_init = client.clone();
    tokio::spawn(async move {
        let _ = client_init.initialize_with_retrying().await;
    });

    wait_for_init(&client)
        .await
        .unwrap_or_else(|_| panic!("{} failed to initialize", username));

    (client, msg_rx, gmsg_rx)
}

// =============================================================================
// TEST 1: Block Endpoint & Enforcement in MLS Groups
// =============================================================================
#[tokio::test]
async fn test_block_and_unblock_enforcement() {
    let (base_url, ws_url) = match setup_server().await {
        Some(urls) => urls,
        None => return,
    };

    let test_run_id = rand::random::<u32>();
    let test_dir = format!("/tmp/firefly/block_{}", test_run_id);
    let _ = std::fs::create_dir_all(&test_dir);

    let alice_name = format!("b_alice_{}", test_run_id);
    let charlie_name = format!("b_charlie_{}", test_run_id);

    let (alice, _, _) = create_client_helper(&base_url, &ws_url, &test_dir, &alice_name).await;
    let (charlie, _, _) = create_client_helper(&base_url, &ws_url, &test_dir, &charlie_name).await;

    // 1. Alice creates group
    let group = alice
        .create_group("Block Test Group".into(), "Description".into(), 0)
        .await
        .expect("Alice creates group");
    let group_id = group.id;

    // 2. Alice blocks Charlie via secret admin token (simulating lupyd-rust-server)
    let admin_token =
        std::env::var("SECRET_ADMIN_TOKEN").unwrap_or_else(|_| "secret_admin_token".to_string());
    let block_res = HTTP_CLIENT
        .post(format!(
            "{}/user/block?me={}&other={}&blocked=true",
            base_url, alice_name, charlie_name
        ))
        .header("Authorization", &admin_token)
        .send()
        .await
        .expect("Send block request");
    assert!(
        block_res.status().is_success(),
        "Block request should succeed with status 200"
    );

    // 3. Alice attempts to add blocked Charlie to her group -> MUST FAIL
    println!("[TEST] Alice attempting to add blocked Charlie to group...");
    let add_blocked_res = alice.add_group_member(group_id, charlie_name.clone(), 0).await;
    assert!(
        add_blocked_res.is_err(),
        "Adding blocked user to group must fail!"
    );
    println!(
        "✓ Adding blocked user correctly failed: {:?}",
        add_blocked_res.err().unwrap()
    );

    // 4. Alice unblocks Charlie via user Bearer token
    let unblock_res = HTTP_CLIENT
        .post(format!("{}/user/block?other={}&blocked=false", base_url, charlie_name))
        .bearer_auth(&alice_name)
        .send()
        .await
        .expect("Send unblock request");
    assert!(
        unblock_res.status().is_success(),
        "Unblock request should succeed with status 200"
    );

    // 5. Alice adds Charlie now -> SUCCEEDS
    println!("[TEST] Alice adding unblocked Charlie to group...");
    let add_unblocked_res = alice.add_group_member(group_id, charlie_name.clone(), 0).await;
    assert!(
        add_unblocked_res.is_ok(),
        "Adding unblocked user to group should succeed: {:?}",
        add_unblocked_res.err()
    );
    println!("✓ Adding unblocked user succeeded!");

    alice.dispose().await;
    charlie.dispose().await;
    cleanup_dir(&test_dir);
}

// =============================================================================
// TEST 2: WebRTC Call Signaling Lifecycle
// =============================================================================
#[tokio::test]
async fn test_webrtc_call_signaling_lifecycle() {
    let (base_url, ws_url) = match setup_server().await {
        Some(urls) => urls,
        None => return,
    };

    let test_run_id = rand::random::<u32>();
    let test_dir = format!("/tmp/firefly/call_{}", test_run_id);
    let _ = std::fs::create_dir_all(&test_dir);

    let alice_name = format!("call_a_{}", test_run_id);
    let bob_name = format!("call_b_{}", test_run_id);

    let (alice, _, _) = create_client_helper(&base_url, &ws_url, &test_dir, &alice_name).await;
    let (bob, _, _) = create_client_helper(&base_url, &ws_url, &test_dir, &bob_name).await;

    let call_id = (test_run_id as u64) + 10000;

    // 1. Alice initiates call to Bob
    println!("[TEST] Alice initiating call to Bob (call_id: {})...", call_id);
    let init_res = alice
        .initiate_call(call_id, bob_name.clone(), "sdp_offer_data".into())
        .await;
    assert!(
        init_res.is_ok(),
        "Alice initiate_call should succeed: {:?}",
        init_res.err()
    );
    println!("✓ Call initiated!");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // 2. Bob accepts call from Alice
    println!("[TEST] Bob accepting call from Alice...");
    let accept_res = bob
        .accept_call(call_id, alice_name.clone(), "sdp_answer_data".into())
        .await;
    assert!(
        accept_res.is_ok(),
        "Bob accept_call should succeed: {:?}",
        accept_res.err()
    );
    println!("✓ Call accepted!");

    // 3. ICE candidate exchange
    println!("[TEST] Exchanging ICE candidates...");
    let ice_res = alice
        .send_ice_candidate(call_id, bob_name.clone(), "candidate:12345".into(), "0".into(), 0)
        .await;
    assert!(
        ice_res.is_ok(),
        "send_ice_candidate should succeed: {:?}",
        ice_res.err()
    );

    // 4. Alice hangs up call
    println!("[TEST] Alice hanging up call...");
    let hangup_res = alice.hangup_call(call_id, bob_name.clone()).await;
    assert!(
        hangup_res.is_ok(),
        "hangup_call should succeed: {:?}",
        hangup_res.err()
    );
    println!("✓ Call hung up successfully!");

    // 5. Test Reject Call flow with a new call_id
    let reject_call_id = call_id + 1;
    alice
        .initiate_call(reject_call_id, bob_name.clone(), "offer".into())
        .await
        .expect("Initiate call for rejection");

    println!("[TEST] Bob rejecting call...");
    let reject_res = bob.reject_call(reject_call_id, alice_name.clone()).await;
    assert!(
        reject_res.is_ok(),
        "reject_call should succeed: {:?}",
        reject_res.err()
    );
    println!("✓ Call rejection succeeded!");

    // 6. Test Cancel Call flow
    let cancel_call_id = call_id + 2;
    alice
        .initiate_call(cancel_call_id, bob_name.clone(), "offer".into())
        .await
        .expect("Initiate call for cancellation");

    println!("[TEST] Alice cancelling call...");
    let cancel_res = alice.cancel_call(cancel_call_id, bob_name.clone()).await;
    assert!(
        cancel_res.is_ok(),
        "cancel_call should succeed: {:?}",
        cancel_res.err()
    );
    println!("✓ Call cancellation succeeded!");

    alice.dispose().await;
    bob.dispose().await;
    cleanup_dir(&test_dir);
}

// =============================================================================
// TEST 3: Group Meeting Key Export
// =============================================================================
#[tokio::test]
async fn test_group_meeting_key_export() {
    let (base_url, ws_url) = match setup_server().await {
        Some(urls) => urls,
        None => return,
    };

    let test_run_id = rand::random::<u32>();
    let test_dir = format!("/tmp/firefly/meeting_{}", test_run_id);
    let _ = std::fs::create_dir_all(&test_dir);

    let alice_name = format!("meet_a_{}", test_run_id);
    let (alice, _, _) = create_client_helper(&base_url, &ws_url, &test_dir, &alice_name).await;

    let group = alice
        .create_group("Meeting Key Test Group".into(), "Description".into(), 0)
        .await
        .expect("Alice creates group");
    let group_id = group.id;

    println!("[TEST] Exporting group meeting key for group {}...", group_id);
    let key = alice
        .export_group_meeting_key(group_id)
        .await
        .expect("Export group meeting key");

    assert!(
        !key.is_empty(),
        "Exported meeting key should not be empty"
    );
    println!("✓ Successfully exported group meeting key with length {} bytes!", key.len());

    alice.dispose().await;
    cleanup_dir(&test_dir);
}

// =============================================================================
// TEST 4: Read User Messages Upto Relay
// =============================================================================
#[tokio::test]
async fn test_read_user_messages_upto_flow() {
    let (base_url, ws_url) = match setup_server().await {
        Some(urls) => urls,
        None => return,
    };

    let test_run_id = rand::random::<u32>();
    let test_dir = format!("/tmp/firefly/read_upto_{}", test_run_id);
    let _ = std::fs::create_dir_all(&test_dir);

    let alice_name = format!("read_a_{}", test_run_id);
    let bob_name = format!("read_b_{}", test_run_id);

    let (alice, _, _) = create_client_helper(&base_url, &ws_url, &test_dir, &alice_name).await;
    let (bob, mut bob_rx, _) = create_client_helper(&base_url, &ws_url, &test_dir, &bob_name).await;

    // Alice sends DM to Bob
    println!("[TEST] Alice sending DM to Bob...");
    let send_res = alice
        .encrypt_and_send(bob_name.clone(), "Hello for read receipts!".into())
        .await;
    assert!(send_res.is_ok(), "Alice sends message: {:?}", send_res.err());

    // Bob receives message
    let received_msg = tokio::time::timeout(Duration::from_secs(10), bob_rx.recv())
        .await
        .expect("Timeout waiting for message")
        .expect("Channel closed");
    println!("✓ Bob received message ID: {}", received_msg.id);

    // Bob calls read_user_messages_upto
    println!("[TEST] Bob marking messages read upto ID {}...", received_msg.id);
    let read_res = bob
        .read_user_messages_upto(alice_name.clone(), received_msg.id)
        .await;
    assert!(
        read_res.is_ok(),
        "read_user_messages_upto should succeed: {:?}",
        read_res.err()
    );
    println!("✓ Successfully sent read_user_messages_upto!");

    alice.dispose().await;
    bob.dispose().await;
    cleanup_dir(&test_dir);
}
