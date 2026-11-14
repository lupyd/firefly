//! A new device of an account gets none of the account's history (direct history pages, the
//! direct-backup key) and is not believed (its "sent by me" copies) until the user approves it
//! on a device that is trusted; the approval reaches the account's other devices.
//!
//! Runs against a local server (EMULATOR_MODE: token == username):
//!   FIREFLY_BASE_URL=http://127.0.0.1:39205 EMULATOR_MODE=true cargo test -p firefly-client \
//!     --features test-hooks --test own_device_trust_test -- --test-threads=1

use async_trait::async_trait;
use firefly_client::callbacks::{DirectHistoryPage, DirectHistoryRequest, FireflyWsClientCallback, OwnDeviceTrustRequest};
use firefly_client::db::messages::UserMessage;
use firefly_client::db::own_devices::OwnDeviceTrust;
use firefly_client::websocket::FfiFireflyWsClient;
use firefly_protos::firefly;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::mpsc;

struct Cb {
    name: String,
    client: Arc<OnceLock<FfiFireflyWsClient>>,
    dm_tx: mpsc::UnboundedSender<UserMessage>,
    page_tx: mpsc::UnboundedSender<DirectHistoryPage>,
    trust_tx: mpsc::UnboundedSender<OwnDeviceTrustRequest>,
    changed_tx: mpsc::UnboundedSender<()>,
    key_tx: mpsc::UnboundedSender<Vec<u8>>,
}

#[async_trait]
impl FireflyWsClientCallback for Cb {
    fn name(&self) -> &str {
        &self.name
    }
    async fn get_access_token(&self) -> Option<String> {
        Some(self.name.clone())
    }
    async fn on_message(&self, message: UserMessage) {
        if let Some(c) = self.client.get() {
            let _ = c.messages_store().insert_user_message(message.clone()).await;
        }
        let _ = self.dm_tx.send(message);
    }
    async fn on_group_message(&self, _m: firefly_client::db::group_messages::GroupMessage) {}
    // Like the app (lupyd-desktop app-core answer_direct_history).
    async fn on_direct_history_request(&self, request: DirectHistoryRequest) {
        let Some(c) = self.client.get().cloned() else { return };
        tokio::spawn(async move {
            let before = request.before_id.min(i64::MAX as u64) as i64;
            let msgs = c.messages_store().get_last_messages_of(&request.other, before, 50).await.unwrap_or_default();
            if let Err(e) = c.send_direct_history_page(&request, &msgs, false).await {
                println!("answer failed: {e:#}");
            }
        });
    }
    async fn on_direct_history_page(&self, page: DirectHistoryPage) {
        let _ = self.page_tx.send(page);
    }
    async fn on_own_device_trust_request(&self, request: OwnDeviceTrustRequest) {
        let _ = self.trust_tx.send(request);
    }
    async fn on_own_device_trust_changed(&self) {
        let _ = self.changed_tx.send(());
    }
    async fn on_direct_backup_key_sync(&self, sync: firefly::DirectBackupKeySync<'static>) {
        let _ = self.key_tx.send(sync.key.to_vec());
    }
}

struct Peer {
    client: FfiFireflyWsClient,
    dm_rx: mpsc::UnboundedReceiver<UserMessage>,
    page_rx: mpsc::UnboundedReceiver<DirectHistoryPage>,
    trust_rx: mpsc::UnboundedReceiver<OwnDeviceTrustRequest>,
    changed_rx: mpsc::UnboundedReceiver<()>,
    key_rx: mpsc::UnboundedReceiver<Vec<u8>>,
}

impl Peer {
    async fn device(&self) -> (u64, u32) {
        (self.client.address_id(), self.client.get_device_id().await)
    }
}

async fn peer(name: &str, db: &str, base: &str) -> Peer {
    let ws = base.replace("http://", "ws://");
    let (dm_tx, dm_rx) = mpsc::unbounded_channel();
    let (page_tx, page_rx) = mpsc::unbounded_channel();
    let (trust_tx, trust_rx) = mpsc::unbounded_channel();
    let (changed_tx, changed_rx) = mpsc::unbounded_channel();
    let (key_tx, key_rx) = mpsc::unbounded_channel();
    let cell = Arc::new(OnceLock::new());
    let cb = Cb { name: name.into(), client: cell.clone(), dm_tx, page_tx, trust_tx, changed_tx, key_tx };
    let client = FfiFireflyWsClient::create(base.into(), ws, 1000, Box::new(cb), db.into(), 5000).await.expect("create client");
    let _ = cell.set(client.clone());
    let c2 = client.clone();
    tokio::spawn(async move {
        let _ = c2.initialize_with_retrying().await;
    });
    for _ in 0..120 {
        if client.is_initialized() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(client.is_initialized(), "{name} did not initialize");
    Peer { client, dm_rx, page_rx, trust_rx, changed_rx, key_rx }
}

fn base() -> Option<String> {
    std::env::var("FIREFLY_BASE_URL").ok()
}

fn dir(tag: &str) -> String {
    let d = format!("/tmp/firefly/own_trust_{}_{}", tag, rand::random::<u32>());
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn dm(text: &str) -> Vec<u8> {
    firefly_protos::serialize_proto(&firefly::UserMessageInner {
        message: firefly::mod_UserMessageInner::OneOfmessage::messagePayload(firefly::MessagePayload {
            text: text.into(),
            ..Default::default()
        }),
        nonce: rand::random(),
        message_type: 0,
    })
    .unwrap()
    .to_vec()
}

fn text_of(bytes: &[u8]) -> String {
    match firefly_protos::deserialize_proto::<firefly::UserMessageInner>(bytes).map(|i| i.message) {
        Ok(firefly::mod_UserMessageInner::OneOfmessage::messagePayload(p)) => p.text.to_string(),
        _ => String::new(),
    }
}

async fn recv<T>(rx: &mut mpsc::UnboundedReceiver<T>, secs: u64) -> Option<T> {
    tokio::time::timeout(Duration::from_secs(secs), rx.recv()).await.ok().flatten()
}

/// Waits for a message whose text contains `needle`, skipping others.
async fn recv_text(rx: &mut mpsc::UnboundedReceiver<UserMessage>, needle: &str, secs: u64) -> Option<UserMessage> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        let m = tokio::time::timeout(left, rx.recv()).await.ok().flatten()?;
        if text_of(&m.message).contains(needle) {
            return Some(m);
        }
    }
}

fn backup_key(key: &[u8]) -> firefly::DirectBackupKeySync<'static> {
    firefly::DirectBackupKeySync {
        version: 1,
        epoch: "e1".to_string().into(),
        key: key.to_vec().into(),
        request_key: false,
        request_id: String::new().into(),
    }
}

#[tokio::test]
async fn new_device_gets_nothing_until_the_user_approves_it() {
    let Some(base) = base() else { return };
    let d = dir("approve");
    let id = rand::random::<u32>();
    let alice_n = format!("alice_t{id}");
    let bob_n = format!("bob_t{id}");

    let alice = peer(&alice_n, &format!("{d}/alice.db"), &base).await;
    let mut bob = peer(&bob_n, &format!("{d}/bob.db"), &base).await;
    alice.client.encrypt_and_send(bob_n.clone(), dm("secret-1: the launch code is 0000")).await.expect("send");
    recv_text(&mut bob.dm_rx, "secret-1", 20).await.expect("bob receives");

    // A second device registers under Bob's account (anyone with Bob's token could).
    let mut ghost = peer(&bob_n, &format!("{d}/ghost.db"), &base).await;
    let (ghost_address, ghost_device) = ghost.device().await;
    // The new device trusts the device that already existed.
    let (bob_address, bob_device) = bob.device().await;
    assert_eq!(ghost.client.own_device_trust(bob_address, bob_device).await, OwnDeviceTrust::Trusted);

    // Bob's device asks the user about it (the new device announces itself).
    let asked = recv(&mut bob.trust_rx, 30).await.expect("bob is asked about the new device");
    assert_eq!(asked, OwnDeviceTrustRequest { address_id: ghost_address, device_id: ghost_device });
    assert_eq!(bob.client.own_device_trust(ghost_address, ghost_device).await, OwnDeviceTrust::Unknown);

    // 1) No history for it.
    let (_req, status) = ghost.client.request_direct_history(&alice_n, u64::MAX, 50).await.expect("request");
    assert_eq!(status, 0, "bob's device is connected");
    assert!(recv(&mut ghost.page_rx, 8).await.is_none(), "an unapproved device got history");

    // 2) Its "sent by me" copies are not believed.
    while bob.dm_rx.try_recv().is_ok() {}
    ghost.client.encrypt_and_send(alice_n.clone(), dm("from-ghost-1")).await.expect("ghost sends");
    assert!(recv_text(&mut bob.dm_rx, "from-ghost-1", 6).await.is_none(), "an unapproved device's copy was shown");

    // 3) No direct-backup key for it.
    bob.client.send_backup_key_sync(backup_key(&[7; 32])).await.expect("key sync");
    assert!(recv(&mut ghost.key_rx, 6).await.is_none(), "an unapproved device got the backup key");

    // The user approves it on Bob's device: the held request is answered.
    bob.client.trust_own_device(ghost_address, ghost_device).await.expect("trust");
    assert_eq!(bob.client.own_device_trust(ghost_address, ghost_device).await, OwnDeviceTrust::Trusted);
    let page = recv(&mut ghost.page_rx, 30).await.expect("history after approval");
    assert!(page.messages.iter().any(|m| text_of(&m.message).contains("launch code")));

    ghost.client.encrypt_and_send(alice_n.clone(), dm("from-ghost-2")).await.expect("ghost sends");
    let copy = recv_text(&mut bob.dm_rx, "from-ghost-2", 20).await.expect("approved device's copy shown");
    assert!(!copy.sent_by_other);

    bob.client.send_backup_key_sync(backup_key(&[8; 32])).await.expect("key sync");
    assert_eq!(recv(&mut ghost.key_rx, 20).await.expect("backup key after approval"), vec![8; 32]);
    let _ = alice;
}

#[tokio::test]
async fn approval_and_rejection_reach_the_accounts_other_devices() {
    let Some(base) = base() else { return };
    let d = dir("propagate");
    let bob_n = format!("bob_p{}", rand::random::<u32>());

    let bob1 = peer(&bob_n, &format!("{d}/bob1.db"), &base).await;
    let mut bob2 = peer(&bob_n, &format!("{d}/bob2.db"), &base).await;
    let (b2_address, b2_device) = bob2.device().await;
    let (b1_address, b1_device) = bob1.device().await;
    // bob2 came later: it trusts bob1; bob1 approves it.
    assert_eq!(bob2.client.own_device_trust(b1_address, b1_device).await, OwnDeviceTrust::Trusted);
    bob1.client.trust_own_device(b2_address, b2_device).await.expect("trust bob2");

    let ghost = peer(&bob_n, &format!("{d}/ghost.db"), &base).await;
    let (g_address, g_device) = ghost.device().await;
    assert_eq!(bob2.client.own_device_trust(g_address, g_device).await, OwnDeviceTrust::Unknown);
    while bob2.changed_rx.try_recv().is_ok() {}

    // Approved on bob1: bob2 learns it (no second prompt needed).
    bob1.client.trust_own_device(g_address, g_device).await.expect("trust ghost");
    let mut trusted = false;
    for _ in 0..40 {
        if bob2.client.own_device_trust(g_address, g_device).await == OwnDeviceTrust::Trusted {
            trusted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(trusted, "bob2 did not learn the approval");
    assert!(recv(&mut bob2.changed_rx, 5).await.is_some());

    // Revoked on bob2: bob1 learns it too.
    bob2.client.reject_own_device(g_address, g_device).await.expect("reject");
    let mut rejected = false;
    for _ in 0..40 {
        if bob1.client.own_device_trust(g_address, g_device).await == OwnDeviceTrust::Rejected {
            rejected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(rejected, "bob1 did not learn the rejection");
    let list = bob1.client.own_device_trust_list().await.unwrap();
    assert!(list.iter().any(|e| e.address_id == g_address && !e.trusted));
    assert!(list.iter().any(|e| e.address_id == b2_address && e.trusted));
}
