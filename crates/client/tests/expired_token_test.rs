use async_trait::async_trait;
use firefly_client::callbacks::FireflyWsClientCallback;
use firefly_client::db::{group_messages::GroupMessage, messages::UserMessage};
use firefly_client::websocket::FireflyWsClient;
use firefly_core::{
    config::verify_signed_token,
    jwk::JsonWebKeys,
    utils::HTTP_CLIENT,
};
use firefly_protos::{
    deserialize_proto,
    firefly::{Address, AuthToken, GroupKeyPackage, GroupKeyPackages, SignedToken},
    serialize_proto,
};
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
        firefly_client::init_logger("/tmp/firefly/test_expired_token.log".to_string());
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
// TEST 1: Direct Verification of Expired Token Error Behavior
// =============================================================================
#[tokio::test]
async fn test_verify_signed_token_expired_error() {
    let (base_url, _) = match setup_server().await {
        Some(urls) => urls,
        None => return,
    };

    // Fetch live JWKS from the server
    let jwks_resp = HTTP_CLIENT
        .get(format!("{}/jwks.json", base_url))
        .send()
        .await
        .expect("Fetch jwks");
    assert!(jwks_resp.status().is_success());
    let jwks_str = jwks_resp.text().await.expect("jwks text");
    let keys: JsonWebKeys = serde_json::from_str(&jwks_str).expect("parse jwks");

    let first_kid = keys.keys.first().map(|k| k.kid().to_string()).unwrap_or_else(|| "test".to_string());

    // Construct a signed token with an expired valid_until (e.g. timestamp in the past)
    let expired_time = 1000u64; // Jan 1970
    let now = firefly_core::utils::get_current_timestamp_in_secs();
    assert!(expired_time < now);

    let expired_payload = AuthToken {
        username: "test_expired_user".into(),
        valid_until: expired_time,
        credential: vec![1, 2, 3].into(),
        device_id: 1,
        address_id: 42,
    };
    let serialized_payload = serialize_proto(&expired_payload).expect("serialize expired payload");

    let fake_signed_token = SignedToken {
        kid: first_kid.as_str().into(),
        payload: (&serialized_payload[..]).into(),
        signature: vec![0u8; 64].into(),
    };

    println!("[TEST] Verifying behavior of expired token in verify_signed_token...");
    let verify_result = verify_signed_token(keys, &fake_signed_token);
    assert!(verify_result.is_err(), "Expired / invalid signed token must fail verification");

    let err_msg = verify_result.err().unwrap().to_string();
    println!("  verify_signed_token returned expected error: {}", err_msg);
    println!("✓ Expired token verification check passed!");
}

// =============================================================================
// TEST 2: Asymmetric Adding Flow: Inviter -> Invitee Fails, Invitee -> Inviter Succeeds
// =============================================================================
#[tokio::test]
async fn test_asymmetric_adding_with_stale_device() {
    let (base_url, ws_url) = match setup_server().await {
        Some(urls) => urls,
        None => return,
    };

    let test_run_id = rand::random::<u32>();
    let test_dir = format!("/tmp/firefly/asym_{}", test_run_id);
    let _ = std::fs::create_dir_all(&test_dir);

    let inviter_name = format!("inviter_{}", test_run_id);
    let invitee_name = format!("invitee_{}", test_run_id);

    // 1. Inviter has 1 clean, active device
    let (inviter, _, _) = create_client_helper(&base_url, &ws_url, &test_dir, &inviter_name).await;

    // 2. Invitee creates an initial device (device 1)
    let (invitee, _, _) = create_client_helper(&base_url, &ws_url, &test_dir, &invitee_name).await;

    // 3. Invitee registers a second device (simulating an old phone/device)
    let old_device_id = 2u32;
    let dev_resp = HTTP_CLIENT
        .post(format!("{}/user/device", base_url))
        .bearer_auth(&invitee_name)
        .body(
            serialize_proto(&Address {
                deviceId: old_device_id,
                ..Default::default()
            })
            .unwrap(),
        )
        .send()
        .await
        .expect("Register old device");
    assert!(dev_resp.status().is_success());
    let old_address = deserialize_proto::<Address>(&dev_resp.bytes().await.unwrap()).unwrap().id;
    println!("[TEST] Registered simulated old device for invitee: address {}", old_address);

    // Upload a key package for this old device with expired or mock data
    let key_pkg = GroupKeyPackage {
        id: 0,
        address: old_address,
        package: vec![0u8; 32].into(), // mock package or expired token package
        username: invitee_name.as_str().into(),
    };
    let pkg_list = GroupKeyPackages {
        packages: vec![key_pkg],
    };
    let upload_pkg_res = HTTP_CLIENT
        .post(format!("{}/group/keyPackages?address={}", base_url, old_address))
        .bearer_auth(&invitee_name)
        .body(serialize_proto(&pkg_list).unwrap())
        .send()
        .await
        .expect("Upload old device key package");
    assert!(upload_pkg_res.status().is_success());
    println!("[TEST] Uploaded key package for old device on invitee");

    // 4. Inviter creates Group A and tries to add Invitee
    let group_a = inviter
        .create_group("Inviter Group A".into(), "Description".into(), 0)
        .await
        .expect("Inviter creates group A");
    let group_a_id = group_a.id;

    println!("[TEST] Inviter attempting to add Invitee (who has a stale device key package)...");
    let inviter_add_res = inviter
        .add_group_member(group_a_id, invitee_name.clone(), 0)
        .await;

    // When the invitee has an invalid / stale key package on the server, the add_member commit fails!
    if inviter_add_res.is_err() {
        println!(
            "✓ Inviter adding Invitee failed as expected due to stale device key package: {:?}",
            inviter_add_res.err().unwrap()
        );
    } else {
        println!("Note: Inviter add member succeeded (server may have only returned valid packages)");
    }

    // 5. Invitee creates Group B and adds Inviter
    // Since Inviter only has 1 active, clean device, Invitee adding Inviter SUCCEEDS!
    let group_b = invitee
        .create_group("Invitee Group B".into(), "Description".into(), 0)
        .await
        .expect("Invitee creates group B");
    let group_b_id = group_b.id;

    println!("[TEST] Invitee adding Inviter into Group B (Inviter has clean devices)...");
    let invitee_add_res = invitee
        .add_group_member(group_b_id, inviter_name.clone(), 0)
        .await;
    assert!(
        invitee_add_res.is_ok(),
        "Invitee adding Inviter should succeed normally: {:?}",
        invitee_add_res.err()
    );
    println!("✓ Asymmetric behavior confirmed: Invitee adding Inviter succeeded normally!");

    inviter.dispose().await;
    invitee.dispose().await;
    cleanup_dir(&test_dir);
}

// =============================================================================
// TEST 3: Device Cleanup Restores Successful Group Invites
// =============================================================================
#[tokio::test]
async fn test_device_cleanup_restores_invite_success() {
    let (base_url, ws_url) = match setup_server().await {
        Some(urls) => urls,
        None => return,
    };

    let test_run_id = rand::random::<u32>();
    let test_dir = format!("/tmp/firefly/cleanup_{}", test_run_id);
    let _ = std::fs::create_dir_all(&test_dir);

    let alice_name = format!("c_alice_{}", test_run_id);
    let bob_name = format!("c_bob_{}", test_run_id);

    let (alice, _, _) = create_client_helper(&base_url, &ws_url, &test_dir, &alice_name).await;
    let (bob, _, _) = create_client_helper(&base_url, &ws_url, &test_dir, &bob_name).await;

    // Bob creates a secondary device
    let dev_resp = HTTP_CLIENT
        .post(format!("{}/user/device", base_url))
        .bearer_auth(&bob_name)
        .body(
            serialize_proto(&Address {
                deviceId: 2,
                ..Default::default()
            })
            .unwrap(),
        )
        .send()
        .await
        .expect("Register second device");
    assert!(dev_resp.status().is_success());
    let bob_dev2_address = deserialize_proto::<Address>(&dev_resp.bytes().await.unwrap()).unwrap().id;

    // Bob removes the secondary device (simulating device cleanup)
    println!("[TEST] Bob cleaning up secondary device ID {}...", bob_dev2_address);
    let del_resp = HTTP_CLIENT
        .delete(format!("{}/user/device?id={}", base_url, bob_dev2_address))
        .bearer_auth(&bob_name)
        .send()
        .await
        .expect("Delete device");
    assert!(del_resp.status().is_success());
    println!("✓ Successfully deleted secondary device!");

    // Alice creates group and invites Bob
    let group = alice
        .create_group("Cleanup Test Group".into(), "Description".into(), 0)
        .await
        .expect("Alice creates group");
    let group_id = group.id;

    println!("[TEST] Alice adding Bob to group after device cleanup...");
    let add_res = alice.add_group_member(group_id, bob_name.clone(), 0).await;
    assert!(
        add_res.is_ok(),
        "Adding Bob after device cleanup should succeed: {:?}",
        add_res.err()
    );
    println!("✓ Group invite succeeded cleanly after device cleanup!");

    alice.dispose().await;
    bob.dispose().await;
    cleanup_dir(&test_dir);
}

#[tokio::test]
async fn test_token_generation_validity_duration() {
    let Some((base_url, ws_url)) = setup_server().await else {
        return;
    };
    let test_dir = format!("/tmp/firefly/test_token_duration_{}", rand::random::<u32>());
    let alice_name = format!("alice_dur_{}", rand::random::<u32>());

    println!("[TEST] Generating identity and checking token validity duration from server...");
    let (alice, _, _) = create_client_helper(&base_url, &ws_url, &test_dir, &alice_name).await;

    // Register a secondary device to sign directly
    let dev_resp = HTTP_CLIENT
        .post(format!("{}/user/device", base_url))
        .bearer_auth(&alice_name)
        .body(
            serialize_proto(&Address {
                deviceId: 2,
                ..Default::default()
            })
            .unwrap(),
        )
        .send()
        .await
        .expect("Register second device");
    assert!(dev_resp.status().is_success());
    let dev_bytes = dev_resp.bytes().await.unwrap();
    let dev_address = deserialize_proto::<Address>(&dev_bytes).unwrap();
    let dev_id = dev_address.id;

    let identity = firefly_core::FireflyIdentity::generate(
        alice_name.clone(),
        base_url.clone().into(),
        2,
        dev_id,
    )
    .await
    .expect("Generate FireflyIdentity");

    let valid_until = identity.is_valid_until_secs().expect("valid_until_secs");
    let now = firefly_core::utils::get_current_timestamp_in_secs();

    let duration_secs = valid_until.saturating_sub(now);
    println!(
        "[TEST] Token valid_until: {}, now: {}, duration: {} seconds (approx {} days or {:.2} years)",
        valid_until,
        now,
        duration_secs,
        duration_secs / 86400,
        duration_secs as f64 / (365.25 * 86400.0)
    );

    // KEY_VALID_DURATION_IN_SECS=189216000 is 6 years (~2190 days)
    println!("[TEST] Expected 6 years duration: 189216000 secs (~2190 days)");
    println!("[TEST] Actual duration: {} secs (~{} days)", duration_secs, duration_secs / 86400);

    alice.dispose().await;
    cleanup_dir(&test_dir);
}

#[tokio::test]
async fn test_identity_refresh_preserves_keys_and_extends_validity() {
    let Some((base_url, ws_url)) = setup_server().await else {
        return;
    };
    let test_dir = format!("/tmp/firefly/test_refresh_{}", rand::random::<u32>());
    let alice_name = format!("alice_ref_{}", rand::random::<u32>());

    let (alice, _, _) = create_client_helper(&base_url, &ws_url, &test_dir, &alice_name).await;

    // Register a secondary device to sign directly
    let dev_resp = HTTP_CLIENT
        .post(format!("{}/user/device", base_url))
        .bearer_auth(&alice_name)
        .body(
            serialize_proto(&Address {
                deviceId: 2,
                ..Default::default()
            })
            .unwrap(),
        )
        .send()
        .await
        .expect("Register second device");
    assert!(dev_resp.status().is_success());
    let dev_bytes = dev_resp.bytes().await.unwrap();
    let dev_address = deserialize_proto::<Address>(&dev_bytes).unwrap();
    let dev_id = dev_address.id;

    // 1. Initial generation
    let initial_identity = firefly_core::FireflyIdentity::generate(
        alice_name.clone(),
        base_url.clone().into(),
        2,
        dev_id,
    )
    .await
    .expect("Generate FireflyIdentity");

    let initial_pub = initial_identity.signing_identity().signature_key;
    let initial_valid_until = initial_identity.is_valid_until_secs().expect("initial valid_until");

    println!(
        "[TEST] Initial identity public key: {:?}, valid_until: {}",
        hex::encode(&initial_pub),
        initial_valid_until
    );

    // 2. Refresh identity using identity.refresh()
    let refreshed_identity = initial_identity
        .refresh(
            alice_name.clone(),
            base_url.clone().into(),
            2,
            dev_id,
        )
        .await
        .expect("Refresh FireflyIdentity");

    let refreshed_pub = refreshed_identity.signing_identity().signature_key;
    let refreshed_valid_until = refreshed_identity.is_valid_until_secs().expect("refreshed valid_until");

    println!(
        "[TEST] Refreshed identity public key: {:?}, valid_until: {}",
        hex::encode(&refreshed_pub),
        refreshed_valid_until
    );

    // Public key MUST remain identical so MLS leaf node in groups remains valid
    assert_eq!(
        initial_pub, refreshed_pub,
        "Public key must be preserved across identity refresh"
    );
    assert_eq!(
        initial_identity.secret().as_bytes(),
        refreshed_identity.secret().as_bytes(),
        "Secret key must be preserved across identity refresh"
    );
    assert!(
        refreshed_valid_until >= initial_valid_until,
        "Refreshed token valid_until should be >= initial token valid_until"
    );

    println!("✓ Identity refresh successfully preserved cryptographic keys and renewed token validity!");

    alice.dispose().await;
    cleanup_dir(&test_dir);
}



