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
        if message.sent_by_other {
            let _ = self.message_tx.send(message).await;
        }
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
        firefly_client::init_logger("/tmp/firefly/test_rotation.log".to_string());
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

/// One of an account's devices, with the direct messages it receives.
struct Device {
    client: Arc<FireflyWsClient>,
    messages: mpsc::Receiver<UserMessage>,
}

/// Creates a device and starts connecting it in the background; it registers with the server
/// when the account has room for it.
async fn new_device(base_url: &str, ws_url: &str, username: &str, db: String) -> Device {
    let (message_tx, messages) = mpsc::channel(100);
    let (group_message_tx, _) = mpsc::channel(100);
    let callbacks = TestCallbacks {
        name: username.to_string(),
        token: username.to_string(),
        message_tx,
        group_message_tx,
    };
    let client = Arc::new(
        FireflyWsClient::create(base_url.to_string(), ws_url.to_string(), 1000, Box::new(callbacks), db, 5000)
            .await
            .expect("failed to create client"),
    );
    let connecting = client.clone();
    tokio::spawn(async move {
        let _ = connecting.initialize_with_retrying().await;
    });
    Device { client, messages }
}

/// A device that registered.
async fn started_device(base_url: &str, ws_url: &str, username: &str, db: String) -> Device {
    let device = new_device(base_url, ws_url, username, db).await;
    wait_for_init(&device.client).await.expect("device failed to initialize");
    device
}

/// The account's devices as the server has them (address ids), as the apps list them.
async fn devices_on_server(base_url: &str, username: &str) -> Vec<u64> {
    let bytes = reqwest::Client::new()
        .get(format!("{base_url}/user/devices"))
        .bearer_auth(username)
        .send()
        .await
        .expect("GET /user/devices")
        .error_for_status()
        .expect("GET /user/devices status")
        .bytes()
        .await
        .unwrap();
    let addresses = firefly_protos::deserialize_proto::<firefly_protos::firefly::Addresses>(&bytes).unwrap();
    addresses.addresses.iter().map(|a| a.id).collect()
}

/// The user removes one of their devices (Settings > Devices in the apps).
async fn remove_device(base_url: &str, username: &str, address_id: u64) {
    reqwest::Client::new()
        .delete(format!("{base_url}/user/device?id={address_id}"))
        .bearer_auth(username)
        .send()
        .await
        .expect("DELETE /user/device")
        .error_for_status()
        .expect("DELETE /user/device status");
}

async fn expect_message(device: &mut Device, label: &str, text: &[u8]) {
    let message = tokio::time::timeout(Duration::from_secs(15), device.messages.recv())
        .await
        .unwrap_or_else(|_| panic!("{label} got no message"))
        .unwrap();
    assert_eq!(message.message, text, "{label}");
}

async fn expect_no_message(device: &mut Device, label: &str) {
    if let Ok(Some(message)) = tokio::time::timeout(Duration::from_secs(5), device.messages.recv()).await {
        panic!("{label} should get nothing, got {:?}", String::from_utf8_lossy(&message.message));
    }
}

/// A sixth device isn't admitted, and doesn't push out an older one: it waits until the user
/// removes one of the five (the apps list the devices with their last connection and let the
/// user pick). Then it registers and gets what is sent to the account; the removed one doesn't.
#[tokio::test]
async fn a_sixth_device_waits_until_the_user_removes_one() {
    let (base_url, ws_url) = match setup_server().await {
        Some(urls) => urls,
        None => return,
    };
    let run = rand::random::<u32>();
    let dir = format!("/tmp/firefly/limit_{run}");
    let _ = std::fs::create_dir_all(&dir);
    let alice = format!("alice_limit_{run}");
    let bob = format!("bob_limit_{run}");

    let mut alices = Vec::new();
    for i in 1..=5 {
        alices.push(started_device(&base_url, &ws_url, &alice, format!("{dir}/alice_{i}.db")).await);
    }
    let bob_device = started_device(&base_url, &ws_url, &bob, format!("{dir}/bob.db")).await;
    assert_eq!(devices_on_server(&base_url, &alice).await.len(), 5);

    bob_device.client.encrypt_and_send(alice.clone(), b"hello 1".to_vec()).await.unwrap();
    for (i, device) in alices.iter_mut().enumerate() {
        expect_message(device, &format!("alice {}", i + 1), b"hello 1").await;
    }

    // At the limit: the sixth device stays unregistered and nobody is pushed out.
    let mut sixth = new_device(&base_url, &ws_url, &alice, format!("{dir}/alice_6.db")).await;
    tokio::time::sleep(Duration::from_secs(8)).await;
    assert!(!sixth.client.is_initialized(), "a sixth device registered without one being removed");
    let mut before: Vec<u64> = Vec::new();
    for device in &alices {
        before.push(device.client.address_id());
    }
    before.sort();
    let mut on_server = devices_on_server(&base_url, &alice).await;
    on_server.sort();
    assert_eq!(on_server, before, "the server's devices changed without the user removing one");

    bob_device.client.encrypt_and_send(alice.clone(), b"hello 2".to_vec()).await.unwrap();
    for (i, device) in alices.iter_mut().enumerate() {
        expect_message(device, &format!("alice {}", i + 1), b"hello 2").await;
    }
    expect_no_message(&mut sixth, "the unregistered sixth device").await;

    // The user removes the oldest device; the sixth then registers in its place.
    let first = alices[0].client.address_id();
    remove_device(&base_url, &alice, first).await;
    wait_for_init(&sixth.client).await.expect("the sixth device didn't register after one was removed");
    let sixth_id = sixth.client.address_id();
    let on_server = devices_on_server(&base_url, &alice).await;
    assert_eq!(on_server.len(), 5, "{on_server:?}");
    assert!(on_server.contains(&sixth_id) && !on_server.contains(&first), "{on_server:?}");

    bob_device.client.encrypt_and_send(alice.clone(), b"hello 3".to_vec()).await.unwrap();
    for (i, device) in alices.iter_mut().enumerate().skip(1) {
        expect_message(device, &format!("alice {}", i + 1), b"hello 3").await;
    }
    expect_message(&mut sixth, "alice 6", b"hello 3").await;
    expect_no_message(&mut alices[0], "the removed device").await;

    for device in alices {
        device.client.dispose().await;
    }
    sixth.client.dispose().await;
    bob_device.client.dispose().await;
    cleanup_dir(&dir);
}

/// Both people keep talking while one replaces a device at the limit: the other side's next
/// message reaches the new device and not the removed one, and the new device can answer.
#[tokio::test]
async fn conversation_continues_after_replacing_a_device_at_the_limit() {
    let (base_url, ws_url) = match setup_server().await {
        Some(urls) => urls,
        None => return,
    };
    let run = rand::random::<u32>();
    let dir = format!("/tmp/firefly/limit_bidi_{run}");
    let _ = std::fs::create_dir_all(&dir);
    let alice = format!("alice_bidi_{run}");
    let bob = format!("bob_bidi_{run}");

    let mut alices = Vec::new();
    for i in 1..=2 {
        alices.push(started_device(&base_url, &ws_url, &alice, format!("{dir}/alice_{i}.db")).await);
    }
    let mut bobs = Vec::new();
    for i in 1..=5 {
        bobs.push(started_device(&base_url, &ws_url, &bob, format!("{dir}/bob_{i}.db")).await);
    }

    alices[0].client.encrypt_and_send(bob.clone(), b"hi bob".to_vec()).await.unwrap();
    for (i, device) in bobs.iter_mut().enumerate() {
        expect_message(device, &format!("bob {}", i + 1), b"hi bob").await;
    }
    bobs[0].client.encrypt_and_send(alice.clone(), b"hi alice".to_vec()).await.unwrap();
    for (i, device) in alices.iter_mut().enumerate() {
        expect_message(device, &format!("alice {}", i + 1), b"hi alice").await;
    }

    // Bob gets a new phone: it waits until he removes his first device.
    let mut new_phone = new_device(&base_url, &ws_url, &bob, format!("{dir}/bob_6.db")).await;
    tokio::time::sleep(Duration::from_secs(8)).await;
    assert!(!new_phone.client.is_initialized(), "a sixth device registered without one being removed");
    let first = bobs[0].client.address_id();
    remove_device(&base_url, &bob, first).await;
    wait_for_init(&new_phone.client).await.expect("the new device didn't register after one was removed");

    // Alice's devices were last told about Bob's old device list; the server points them at the new one.
    alices[1].client.encrypt_and_send(bob.clone(), b"welcome back".to_vec()).await.unwrap();
    expect_message(&mut new_phone, "bob's new phone", b"welcome back").await;
    for (i, device) in bobs.iter_mut().enumerate().skip(1) {
        expect_message(device, &format!("bob {}", i + 1), b"welcome back").await;
    }
    expect_no_message(&mut bobs[0], "bob's removed device").await;

    new_phone.client.encrypt_and_send(alice.clone(), b"from my new phone".to_vec()).await.unwrap();
    for (i, device) in alices.iter_mut().enumerate() {
        expect_message(device, &format!("alice {}", i + 1), b"from my new phone").await;
    }

    for device in alices.into_iter().chain(bobs) {
        device.client.dispose().await;
    }
    new_phone.client.dispose().await;
    cleanup_dir(&dir);
}


#[tokio::test]
async fn test_key_packages_and_pre_keys_not_deleted_on_reconnect() {
    let (base_url, ws_url) = match setup_server().await {
        Some(urls) => urls,
        None => return,
    };

    let test_run_id = rand::random::<u32>();
    let test_dir = format!("/tmp/firefly/reconn_clean_{}", test_run_id);
    let _ = std::fs::create_dir_all(&test_dir);
    let username = format!("reconn_user_{}", test_run_id);
    let db_path = format!("{}/user.db", test_dir);

    let (msg_tx, _msg_rx) = mpsc::channel(100);
    let (gmsg_tx, _gmsg_rx) = mpsc::channel(100);
    let callbacks = TestCallbacks {
        name: username.clone(),
        token: username.clone(),
        message_tx: msg_tx,
        group_message_tx: gmsg_tx,
    };

    // 1. First run: Start client, wait for initialization
    println!("First start: Creating client for {}", username);
    let client1 = FireflyWsClient::create(
        base_url.clone(),
        ws_url.clone(),
        1000,
        Box::new(callbacks),
        db_path.clone(),
        5000,
    )
    .await
    .expect("Failed to create client 1");
    let client1 = Arc::new(client1);
    let c1_init = client1.clone();
    tokio::spawn(async move {
        let _ = c1_init.initialize_with_retrying().await;
    });
    wait_for_init(&client1)
        .await
        .expect("Client 1 failed to initialize");

    let address_id = client1.address_id();
    assert_ne!(address_id, 0, "address_id must not be 0");

    // Fetch initial uploaded key packages from server via HTTP GET
    let token = username.clone();
    let url_kp = format!(
        "{}/group/keyPackages?address_id={}&device_id=0",
        base_url, address_id
    );
    let resp_kp = reqwest::Client::new()
        .get(&url_kp)
        .bearer_auth(&token)
        .send()
        .await
        .expect("Failed to fetch key packages from server");
    assert!(resp_kp.status().is_success());
    let kp_bytes = resp_kp.bytes().await.unwrap();
    let initial_packages = firefly_protos::deserialize_proto::<firefly_protos::firefly::GroupKeyPackages>(&kp_bytes)
        .expect("Failed to deserialize GroupKeyPackages");
    let initial_kp_ids: Vec<i32> = initial_packages.packages.iter().map(|p| p.id).collect();
    println!("Initial key package count: {}, ids: {:?}", initial_kp_ids.len(), initial_kp_ids);
    assert_eq!(initial_kp_ids.len(), 32, "Initial key packages should be 32");

    // Fetch initial uploaded pre_key_bundles from server
    let url_pk = format!(
        "{}/user/preKeyBundles?id={}&onlyIds=true",
        base_url, address_id
    );
    let resp_pk = reqwest::Client::new()
        .get(&url_pk)
        .bearer_auth(&token)
        .send()
        .await
        .expect("Failed to fetch preKeyBundles from server");
    assert!(resp_pk.status().is_success());
    let pk_bytes = resp_pk.bytes().await.unwrap();
    let initial_bundles = firefly_protos::deserialize_proto::<firefly_protos::firefly::PreKeyBundleEntries>(&pk_bytes)
        .expect("Failed to deserialize PreKeyBundleEntries");
    let initial_pk_ids: Vec<u32> = initial_bundles.entries.iter().map(|b| b.id).collect();
    println!("Initial pre_key_bundle count: {}, ids: {:?}", initial_pk_ids.len(), initial_pk_ids);
    assert_eq!(initial_pk_ids.len(), 32, "Initial pre_key_bundles should be 32");

    // Dispose client 1 (simulating app restart / disconnect)
    println!("Disposing client 1 (app closed)...");
    client1.dispose().await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 2. Second run: Restart client with SAME database (like user restarting Lupyd app)
    println!("Second start: Creating client 2 with existing database...");
    let (msg_tx2, _msg_rx2) = mpsc::channel(100);
    let (gmsg_tx2, _gmsg_rx2) = mpsc::channel(100);
    let callbacks2 = TestCallbacks {
        name: username.clone(),
        token: username.clone(),
        message_tx: msg_tx2,
        group_message_tx: gmsg_tx2,
    };
    let client2 = FireflyWsClient::create(
        base_url.clone(),
        ws_url.clone(),
        1000,
        Box::new(callbacks2),
        db_path.clone(),
        5000,
    )
    .await
    .expect("Failed to create client 2");
    let client2 = Arc::new(client2);
    let c2_init = client2.clone();
    tokio::spawn(async move {
        let _ = c2_init.initialize_with_retrying().await;
    });
    wait_for_init(&client2)
        .await
        .expect("Client 2 failed to initialize");

    let address_id2 = client2.address_id();
    assert_eq!(address_id, address_id2, "address_id must be preserved across restarts");

    // Fetch key packages from server again
    let resp_kp2 = reqwest::Client::new()
        .get(&url_kp)
        .bearer_auth(&token)
        .send()
        .await
        .expect("Failed to fetch key packages after restart");
    let kp_bytes2 = resp_kp2.bytes().await.unwrap();
    let reloaded_packages = firefly_protos::deserialize_proto::<firefly_protos::firefly::GroupKeyPackages>(&kp_bytes2)
        .expect("Failed to deserialize GroupKeyPackages");
    let reloaded_kp_ids: Vec<i32> = reloaded_packages.packages.iter().map(|p| p.id).collect();
    println!("Reloaded key package count: {}, ids: {:?}", reloaded_kp_ids.len(), reloaded_kp_ids);

    // Fetch pre_key_bundles from server again
    let resp_pk2 = reqwest::Client::new()
        .get(&url_pk)
        .bearer_auth(&token)
        .send()
        .await
        .expect("Failed to fetch preKeyBundles after restart");
    let pk_bytes2 = resp_pk2.bytes().await.unwrap();
    let reloaded_bundles = firefly_protos::deserialize_proto::<firefly_protos::firefly::PreKeyBundleEntries>(&pk_bytes2)
        .expect("Failed to deserialize PreKeyBundleEntries");
    let reloaded_pk_ids: Vec<u32> = reloaded_bundles.entries.iter().map(|b| b.id).collect();
    println!("Reloaded pre_key_bundle count: {}, ids: {:?}", reloaded_pk_ids.len(), reloaded_pk_ids);

    // CRITICAL CHECKS:
    // Key packages and pre-keys should NOT have been deleted and re-uploaded on a clean restart!
    // The IDs on the server must match the original IDs because none of them were consumed!
    assert_eq!(
        reloaded_kp_ids, initial_kp_ids,
        "Key packages MUST NOT be rotated/re-uploaded on client restart when none were used!"
    );
    assert_eq!(
        reloaded_pk_ids, initial_pk_ids,
        "Pre-key bundles MUST NOT be deleted/re-uploaded on client restart when none were used!"
    );

    client2.dispose().await;
    cleanup_dir(&test_dir);
}

#[tokio::test]
async fn test_used_key_packages_and_pre_keys_cleaned_and_sessions_persist_across_reconnect() {
    let (base_url, ws_url) = match setup_server().await {
        Some(urls) => urls,
        None => return,
    };

    let test_run_id = rand::random::<u32>();
    let test_dir = format!("/tmp/firefly/reconn_session_{}", test_run_id);
    let _ = std::fs::create_dir_all(&test_dir);

    let alice_name = format!("alice_reconn_{}", test_run_id);
    let bob_name = format!("bob_reconn_{}", test_run_id);

    let alice_db = format!("{}/alice.db", test_dir);
    let bob_db = format!("{}/bob.db", test_dir);

    // 1. Setup Alice
    let (alice_msg_tx, mut alice_msg_rx) = mpsc::channel(100);
    let (alice_gmsg_tx, mut alice_gmsg_rx) = mpsc::channel(100);
    let alice_callbacks = TestCallbacks {
        name: alice_name.clone(),
        token: alice_name.clone(),
        message_tx: alice_msg_tx,
        group_message_tx: alice_gmsg_tx,
    };
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
    let a_init = alice_client.clone();
    tokio::spawn(async move {
        let _ = a_init.initialize_with_retrying().await;
    });
    wait_for_init(&alice_client).await.expect("Alice failed to initialize");

    // 2. Setup Bob
    let (bob_msg_tx, mut bob_msg_rx) = mpsc::channel(100);
    let (bob_gmsg_tx, _bob_gmsg_rx) = mpsc::channel(100);
    let bob_callbacks = TestCallbacks {
        name: bob_name.clone(),
        token: bob_name.clone(),
        message_tx: bob_msg_tx,
        group_message_tx: bob_gmsg_tx,
    };
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
    let b_init = bob_client.clone();
    tokio::spawn(async move {
        let _ = b_init.initialize_with_retrying().await;
    });
    wait_for_init(&bob_client).await.expect("Bob failed to initialize");

    let bob_address_id = bob_client.address_id();
    let bob_token = bob_name.clone();

    // 3. Inspect Bob's initial key packages and pre-key bundles on server
    let url_bob_kp = format!(
        "{}/group/keyPackages?address_id={}&device_id=0",
        base_url, bob_address_id
    );
    let resp_kp = reqwest::Client::new()
        .get(&url_bob_kp)
        .bearer_auth(&bob_token)
        .send()
        .await
        .unwrap();
    let kp_bytes = resp_kp.bytes().await.unwrap();
    let initial_kp = firefly_protos::deserialize_proto::<firefly_protos::firefly::GroupKeyPackages>(&kp_bytes).unwrap();
    let initial_bob_kp_ids: Vec<i32> = initial_kp.packages.iter().map(|p| p.id).collect();
    assert_eq!(initial_bob_kp_ids.len(), 32, "Bob should initially have 32 key packages");

    let url_bob_pk = format!(
        "{}/user/preKeyBundles?id={}&onlyIds=true",
        base_url, bob_address_id
    );
    let resp_pk = reqwest::Client::new()
        .get(&url_bob_pk)
        .bearer_auth(&bob_token)
        .send()
        .await
        .unwrap();
    let pk_bytes = resp_pk.bytes().await.unwrap();
    let initial_pk = firefly_protos::deserialize_proto::<firefly_protos::firefly::PreKeyBundleEntries>(&pk_bytes).unwrap();
    let initial_bob_pk_ids: Vec<u32> = initial_pk.entries.iter().map(|b| b.id).collect();
    assert_eq!(initial_bob_pk_ids.len(), 32, "Bob should initially have 32 pre-key bundles");

    // 4. Alice sends 1:1 message to Bob (consumes 1 pre-key bundle of Bob from server)
    println!("Alice sending 1:1 message to Bob...");
    alice_client
        .encrypt_and_send(bob_name.clone(), b"hello bob 1:1 before reconnect".to_vec())
        .await
        .expect("Alice failed to send 1:1 message");

    let received_1_1 = tokio::time::timeout(Duration::from_secs(10), bob_msg_rx.recv())
        .await
        .expect("Timeout waiting for 1:1 message on Bob")
        .unwrap();
    assert_eq!(received_1_1.message, b"hello bob 1:1 before reconnect");

    // Verify Bob's pre-keys on server: exactly 1 consumed, 31 remaining!
    let resp_pk_after = reqwest::Client::new()
        .get(&url_bob_pk)
        .bearer_auth(&bob_token)
        .send()
        .await
        .unwrap();
    let pk_bytes_after = resp_pk_after.bytes().await.unwrap();
    let pk_after = firefly_protos::deserialize_proto::<firefly_protos::firefly::PreKeyBundleEntries>(&pk_bytes_after).unwrap();
    let bob_pk_ids_after_1_1: Vec<u32> = pk_after.entries.iter().map(|b| b.id).collect();
    assert_eq!(
        bob_pk_ids_after_1_1.len(), 31,
        "Server should have exactly 31 pre-key bundles for Bob after 1 was consumed"
    );
    for id in &bob_pk_ids_after_1_1 {
        assert!(initial_bob_pk_ids.contains(id), "Remaining pre-keys must be from the initial set");
    }

    // 5. Alice creates MLS group and adds Bob (consumes 1 key package of Bob)
    println!("Alice creating MLS group and adding Bob...");
    let group_info = alice_client
        .create_group("Reconnect Test Group".into(), "Description".into(), 0)
        .await
        .expect("Alice failed to create group");
    let group_id = group_info.id;

    alice_client
        .add_group_member(group_id, bob_name.clone(), 0)
        .await
        .expect("Alice failed to add Bob to group");

    // Bob syncs to join group
    bob_client.check_setup().await.expect("Bob failed check_setup");
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Bob sends a group message
    let group_msg = firefly_protos::firefly::GroupMessageInner {
        channelId: 0,
        message: firefly_protos::firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(
            firefly_protos::firefly::MessagePayload {
                text: "hello group from bob before reconnect".to_string().into(),
                ..Default::default()
            },
        ),
        message_type: 0,
    };
    bob_client
        .upload_group_message(group_id, group_msg, 0)
        .await
        .expect("Bob failed to send group message");

    let _received_gmsg = tokio::time::timeout(Duration::from_secs(10), alice_gmsg_rx.recv())
        .await
        .expect("Timeout waiting for group message on Alice")
        .unwrap();
    println!("Alice received group message before reconnect!");

    // 6. Bob restarts (client disposed, new client started with bob_db)
    println!("Restarting Bob client...");
    bob_client.dispose().await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let (bob_msg_tx2, mut bob_msg_rx2) = mpsc::channel(100);
    let (bob_gmsg_tx2, mut bob_gmsg_rx2) = mpsc::channel(100);
    let bob_callbacks2 = TestCallbacks {
        name: bob_name.clone(),
        token: bob_name.clone(),
        message_tx: bob_msg_tx2,
        group_message_tx: bob_gmsg_tx2,
    };
    let bob_client2 = FireflyWsClient::create(
        base_url.clone(),
        ws_url.clone(),
        1000,
        Box::new(bob_callbacks2),
        bob_db.clone(),
        5000,
    )
    .await
    .expect("Failed to recreate Bob client");
    let bob_client2 = Arc::new(bob_client2);
    let b2_init = bob_client2.clone();
    tokio::spawn(async move {
        let _ = b2_init.initialize_with_retrying().await;
    });
    wait_for_init(&bob_client2).await.expect("Bob 2 failed to initialize");

    // 7. Verify Bob's pre-keys and key packages on server after restart:
    // Bob should have replenished ONLY the consumed pre-key (back to 32)
    let resp_pk_bob2 = reqwest::Client::new()
        .get(&url_bob_pk)
        .bearer_auth(&bob_token)
        .send()
        .await
        .unwrap();
    let pk_bob2_bytes = resp_pk_bob2.bytes().await.unwrap();
    let pk_bob2 = firefly_protos::deserialize_proto::<firefly_protos::firefly::PreKeyBundleEntries>(&pk_bob2_bytes).unwrap();
    let bob_pk_ids2: Vec<u32> = pk_bob2.entries.iter().map(|b| b.id).collect();
    assert_eq!(bob_pk_ids2.len(), 32, "Bob should have 32 pre-key bundles after replenishment");

    let preserved_pk_count = bob_pk_ids2.iter().filter(|id| initial_bob_pk_ids.contains(id)).count();
    assert_eq!(
        preserved_pk_count, 31,
        "Exactly 31 pre-key bundles should have been preserved across restart without rotation!"
    );

    // Bob should have replenished ONLY the consumed group key package (back to 32)
    let mut bob_kp_ids2: Vec<i32> = Vec::new();
    for _ in 0..50 {
        let resp_kp_bob2 = reqwest::Client::new()
            .get(&url_bob_kp)
            .bearer_auth(&bob_token)
            .send()
            .await
            .unwrap();
        let kp_bob2_bytes = resp_kp_bob2.bytes().await.unwrap();
        let kp_bob2 = firefly_protos::deserialize_proto::<firefly_protos::firefly::GroupKeyPackages>(&kp_bob2_bytes).unwrap();
        bob_kp_ids2 = kp_bob2.packages.iter().map(|p| p.id).collect();
        if bob_kp_ids2.len() >= 32 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(bob_kp_ids2.len() >= 32, "Bob should have at least 32 key packages after replenishment");

    let preserved_kp_count = bob_kp_ids2.iter().filter(|id| initial_bob_kp_ids.contains(id)).count();
    assert_eq!(
        preserved_kp_count, 31,
        "Exactly 31 key packages should have been preserved across restart without rotation!"
    );

    // 8. SESSION PERSISTENCE: 1:1 messaging across reconnect
    println!("Testing 1:1 messaging persistence after Bob reconnect...");
    bob_client2
        .encrypt_and_send(alice_name.clone(), b"reply from bob after reconnect".to_vec())
        .await
        .expect("Bob failed to send 1:1 message after reconnect");

    let msg_alice = tokio::time::timeout(Duration::from_secs(10), alice_msg_rx.recv())
        .await
        .expect("Timeout waiting for 1:1 message on Alice from reconnected Bob")
        .unwrap();
    assert_eq!(msg_alice.message, b"reply from bob after reconnect");

    alice_client
        .encrypt_and_send(bob_name.clone(), b"alice -> bob 1:1 after reconnect".to_vec())
        .await
        .expect("Alice failed to reply to Bob after reconnect");

    let msg_bob = tokio::time::timeout(Duration::from_secs(10), bob_msg_rx2.recv())
        .await
        .expect("Timeout waiting for 1:1 message on Bob from Alice")
        .unwrap();
    assert_eq!(msg_bob.message, b"alice -> bob 1:1 after reconnect");
    println!("1:1 messaging session persisted successfully across reconnect!");

    // 9. SESSION PERSISTENCE: MLS Group messaging across reconnect
    println!("Testing MLS Group messaging persistence after Bob reconnect...");
    let gmsg_bob = firefly_protos::firefly::GroupMessageInner {
        channelId: 0,
        message: firefly_protos::firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(
            firefly_protos::firefly::MessagePayload {
                text: "group message from bob after reconnect".to_string().into(),
                ..Default::default()
            },
        ),
        message_type: 0,
    };
    bob_client2
        .upload_group_message(group_id, gmsg_bob, 0)
        .await
        .expect("Bob failed to send group message after reconnect");

    let _gmsg_alice = tokio::time::timeout(Duration::from_secs(10), alice_gmsg_rx.recv())
        .await
        .expect("Timeout waiting for group message on Alice from reconnected Bob")
        .unwrap();
    println!("Alice received group message from reconnected Bob!");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let gmsg_alice_reply = firefly_protos::firefly::GroupMessageInner {
        channelId: 0,
        message: firefly_protos::firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(
            firefly_protos::firefly::MessagePayload {
                text: "group reply from alice after reconnect".to_string().into(),
                ..Default::default()
            },
        ),
        message_type: 0,
    };
    alice_client
        .upload_group_message(group_id, gmsg_alice_reply, 0)
        .await
        .expect("Alice failed to reply to group after Bob reconnect");

    let _gmsg_bob_recv = tokio::time::timeout(Duration::from_secs(10), bob_gmsg_rx2.recv())
        .await
        .expect("Timeout waiting for group message on Bob from Alice")
        .unwrap();
    println!("Bob received group reply from Alice!");

    // 10. Clean up clients and db files
    bob_client2.dispose().await;
    alice_client.dispose().await;
    cleanup_dir(&test_dir);
}
