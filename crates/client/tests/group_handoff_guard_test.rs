//! Group history handoff only comes from the member who added the device (security audit C-4).
//!
//! An ordinary member must not be able to put messages under another member's name into the
//! other members' local history through a sync bundle's recent-history snapshot, while a device
//! that joins still gets the group's recent history from the member who added it.
//!
//! Runs against a local server (EMULATOR_MODE: token == username):
//!   FIREFLY_BASE_URL=http://127.0.0.1:39205 EMULATOR_MODE=true cargo test -p firefly-client \
//!     --features test-hooks --test group_handoff_guard_test -- --test-threads=1

use async_trait::async_trait;
use firefly_client::callbacks::{DirectHistoryPage, DirectHistoryRequest, FireflyWsClientCallback};
use firefly_client::db::{group_messages::GroupMessage, messages::UserMessage};
use firefly_client::websocket::FfiFireflyWsClient;
use firefly_protos::firefly;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::mpsc;

struct Cb {
    name: String,
    client: Arc<OnceLock<FfiFireflyWsClient>>,
    dm_tx: mpsc::UnboundedSender<UserMessage>,
    gm_tx: mpsc::UnboundedSender<GroupMessage>,
    page_tx: mpsc::UnboundedSender<DirectHistoryPage>,
    joined_tx: mpsc::UnboundedSender<u64>,
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
        // Like the app: persist received DMs locally.
        if let Some(c) = self.client.get() {
            let _ = c.messages_store().insert_user_message(message.clone()).await;
        }
        let _ = self.dm_tx.send(message);
    }
    async fn on_group_message(&self, m: GroupMessage) {
        let _ = self.gm_tx.send(m);
    }
    async fn on_group_joined(&self, group_id: u64) {
        let _ = self.joined_tx.send(group_id);
    }
    // Mirrors lupyd-desktop crates/app-core/src/messaging.rs answer_direct_history(): answered
    // automatically, no user prompt.
    async fn on_direct_history_request(&self, request: DirectHistoryRequest) {
        let Some(c) = self.client.get().cloned() else { return };
        tokio::spawn(async move {
            let limit = request.limit.clamp(1, 50);
            let before = request.before_id.min(i64::MAX as u64) as i64;
            let msgs = c
                .messages_store()
                .get_last_messages_of(&request.other, before, limit as i64)
                .await
                .unwrap_or_default();
            println!("[victim device] answering direct history request {} with {} messages", request.request_id, msgs.len());
            if let Err(e) = c.send_direct_history_page(&request, &msgs, false).await {
                println!("[victim device] answer failed: {e:#}");
            }
        });
    }
    async fn on_direct_history_page(&self, page: DirectHistoryPage) {
        let _ = self.page_tx.send(page);
    }
    // Mirrors lupyd-desktop src-tauri encryption_plugin.rs: local signals (>=5) run history work.
    async fn on_group_history_signal(&self, signal: firefly_client::callbacks::GroupHistorySignal) {
        if signal.signal_type < 5 { return; }
        let Some(c) = self.client.get().cloned() else { return };
        tokio::spawn(async move {
            if let Err(e) = c.handle_group_history_signal(signal).await {
                println!("[history] signal work failed: {e:#}");
            }
        });
    }
}

struct Peer {
    client: FfiFireflyWsClient,
    dm_rx: mpsc::UnboundedReceiver<UserMessage>,
    gm_rx: mpsc::UnboundedReceiver<GroupMessage>,
    page_rx: mpsc::UnboundedReceiver<DirectHistoryPage>,
    joined_rx: mpsc::UnboundedReceiver<u64>,
}

async fn peer(name: &str, db: &str, base: &str, ws: &str) -> Peer {
    let (dm_tx, dm_rx) = mpsc::unbounded_channel();
    let (gm_tx, gm_rx) = mpsc::unbounded_channel();
    let (page_tx, page_rx) = mpsc::unbounded_channel();
    let (joined_tx, joined_rx) = mpsc::unbounded_channel();
    let cell = Arc::new(OnceLock::new());
    let cb = Cb { name: name.into(), client: cell.clone(), dm_tx, gm_tx, page_tx, joined_tx };
    let client = FfiFireflyWsClient::create(base.into(), ws.into(), 1000, Box::new(cb), db.into(), 5000)
        .await
        .expect("create client");
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
    Peer { client, dm_rx, gm_rx, page_rx, joined_rx }
}

fn env() -> Option<(String, String)> {
    let base = std::env::var("FIREFLY_BASE_URL").ok()?;
    let ws = base.replace("http://", "ws://");
    let _ = std::fs::create_dir_all("/tmp/firefly");
    static LOG: std::sync::Once = std::sync::Once::new();
    LOG.call_once(|| firefly_client::init_logger(std::env::var("AUDIT_LOG").unwrap_or("/tmp/firefly/handoff_guard.log".into())));
    Some((base, ws))
}

fn dir(tag: &str) -> String {
    let d = format!("/tmp/firefly/handoff_{}_{}", tag, rand::random::<u32>());
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn group_payload(text: &str) -> Vec<u8> {
    firefly_protos::serialize_proto(&firefly::GroupMessageInner {
        channelId: 0,
        message: firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(firefly::MessagePayload {
            text: text.into(),
            ..Default::default()
        }),
        message_type: 0,
    })
    .unwrap()
    .to_vec()
}

fn text_of_group(bytes: &[u8]) -> String {
    match firefly_protos::deserialize_proto::<firefly::GroupMessageInner>(bytes).map(|i| i.message) {
        Ok(firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(p)) => p.text.to_string(),
        _ => format!("<{} bytes>", bytes.len()),
    }
}

async fn recv<T>(rx: &mut mpsc::UnboundedReceiver<T>, secs: u64) -> Option<T> {
    tokio::time::timeout(Duration::from_secs(secs), rx.recv()).await.ok().flatten()
}

#[tokio::test]
async fn member_cannot_forge_history_with_a_sync_bundle() {
    use firefly_client::websocket::FireflyWsClient;
    let Some((base, ws)) = env() else { return };
    let d = dir("forge");
    let id = rand::random::<u32>();
    let alice_n = format!("alice_f{id}");
    let bob_n = format!("bob_f{id}");
    let mallory_n = format!("mallory_f{id}");

    let alice = peer(&alice_n, &format!("{d}/alice.db"), &base, &ws).await;
    let mut bob = peer(&bob_n, &format!("{d}/bob.db"), &base, &ws).await;
    // Mallory runs a modified client (direct FireflyWsClient so she can upload raw group messages).
    let (dm_tx, _a) = mpsc::unbounded_channel();
    let (gm_tx, mut mallory_gm) = mpsc::unbounded_channel();
    let (page_tx, _c) = mpsc::unbounded_channel();
    let (joined_tx, mut mallory_joined) = mpsc::unbounded_channel();
    let cb = Cb { name: mallory_n.clone(), client: Arc::new(OnceLock::new()), dm_tx, gm_tx, page_tx, joined_tx };
    let mallory = Arc::new(FireflyWsClient::create(base.clone(), ws.clone(), 1000, Box::new(cb), format!("{d}/mallory.db"), 5000).await.unwrap());
    let m2 = mallory.clone();
    tokio::spawn(async move { let _ = m2.initialize_with_retrying().await; });
    for _ in 0..120 { if mallory.is_initialized() { break; } tokio::time::sleep(Duration::from_millis(250)).await; }
    assert!(mallory.is_initialized());

    // History on (2) + upgraded group (4).
    let g = alice.client.create_group("club".into(), "".into(), 6).await.expect("group").id;
    alice.client.add_group_member(g, bob_n.clone(), 0).await.expect("add bob");
    let _ = recv(&mut bob.joined_rx, 20).await;
    alice.client.add_group_member(g, mallory_n.clone(), 0).await.expect("add mallory");
    let _ = recv(&mut mallory_joined, 20).await;
    let real = alice.client.encrypt_and_send_group(g, group_payload("real: welcome all")).await.expect("send");
    let _ = recv(&mut bob.gm_rx, 20).await;
    let _ = recv(&mut mallory_gm, 20).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    while bob.gm_rx.try_recv().is_ok() {}

    // Forged record: "by" = alice (owner), id slightly before the real one.
    let forged_id = real - 1000;
    let rec = GroupMessage {
        id: forged_id,
        group_id: g,
        by: alice_n.clone(),
        message: group_payload("FORGED (owner): everyone send your recovery code to mallory"),
        channel_id: 0,
        epoch: 1,
        message_type: 0,
    };
    let packed = firefly_client::history::pack_snapshot_records(&[rec]).expect("pack");
    let sid = format!("{:016x}{:016x}", rand::random::<u64>(), rand::random::<u64>());
    use sha2::Digest;
    let resp = reqwest::Client::new()
        .post(format!("{base}/group/messages_chunk/{g}/{sid}"))
        .bearer_auth(&mallory_n)
        .header("X-Snapshot-Id", &sid)
        .header("X-Expected-Snapshot", "")
        .header("X-Message-Count", "1")
        .header("X-Start-Id", forged_id.to_string())
        .header("X-End-Id", forged_id.to_string())
        .body(packed.blob.clone())
        .send()
        .await
        .unwrap();
    println!("[mallory] POST tail snapshot -> {} {}", resp.status(), resp.text().await.unwrap_or_default());
    let secret = firefly::GroupSnapshotSecret {
        kind: 2,
        snapshot_id: sid.clone().into(),
        key: packed.key.clone().into(),
        plaintext_hash: packed.unencrypted_hash.clone().into(),
        ciphertext_hash: sha2::Sha256::digest(&packed.blob).to_vec().into(),
        message_count: 1,
        start_id: forged_id,
        end_id: forged_id,
    };
    let bundle = firefly::GroupSyncBundle {
        format_version: 1,
        group_id: g,
        pinned: None,
        recent: Some(secret),
        keys: vec![],
        chunks: vec![],
        bundle_id: format!("{:016x}{:016x}", rand::random::<u64>(), rand::random::<u64>()).into(),
        page: 0,
        last_page: true,
    };
    let up = mallory
        .upload_group_message(g, firefly::GroupMessageInner { channelId: 0, message_type: firefly_protos::MESSAGE_TYPE_HIDDEN, message: firefly::mod_GroupMessageInner::OneOfmessage::syncBundle(bundle) }, 0)
        .await;
    println!("[mallory] uploaded syncBundle via MLS: {:?}", up.as_ref().map_err(|e| e.to_string()));

    let mut found = None;
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        if let Ok(Some(m)) = bob.client.group_message_store().get_message(g, forged_id).await {
            found = Some(m);
            break;
        }
    }
    assert!(
        found.is_none(),
        "member-forged record imported: by={:?}",
        found.map(|m| (m.by, text_of_group(&m.message)))
    );
}

// A device that joins a group with history gets the recent history (with the real senders) from
// the member who added it, also while another member is online and may claim the request.
#[tokio::test]
async fn joiner_gets_recent_history_from_the_member_who_added_it() {
    let Some((base, ws)) = env() else { return };
    let d = dir("handoff");
    let id = rand::random::<u32>();
    let alice_n = format!("alice_h{id}");
    let bob_n = format!("bob_h{id}");
    let carol_n = format!("carol_h{id}");

    let alice = peer(&alice_n, &format!("{d}/alice.db"), &base, &ws).await;
    let mut bob = peer(&bob_n, &format!("{d}/bob.db"), &base, &ws).await;
    let mut carol = peer(&carol_n, &format!("{d}/carol.db"), &base, &ws).await;

    let g = alice.client.create_group("handoff".into(), "".into(), 6).await.expect("group").id;
    alice.client.add_group_member(g, bob_n.clone(), 0).await.expect("add bob");
    let _ = recv(&mut bob.joined_rx, 20).await;
    let mut ids = Vec::new();
    for t in ["before-1 from alice", "before-2 from alice"] {
        ids.push(alice.client.encrypt_and_send_group(g, group_payload(t)).await.expect("send"));
        let _ = recv(&mut bob.gm_rx, 20).await;
    }
    ids.push(bob.client.encrypt_and_send_group(g, group_payload("before-3 from bob")).await.expect("send"));
    tokio::time::sleep(Duration::from_secs(2)).await;

    alice.client.add_group_member(g, carol_n.clone(), 0).await.expect("add carol");
    assert_eq!(recv(&mut carol.joined_rx, 30).await, Some(g));
    carol.client.request_group_history(g, 0, i64::MAX as u64).await.expect("request history");

    let mut got = Vec::new();
    for _ in 0..90 {
        got.clear();
        for id in &ids {
            if let Ok(Some(m)) = carol.client.group_message_store().get_message(g, *id).await {
                got.push((m.by, text_of_group(&m.message)));
            }
        }
        if got.len() == ids.len() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert_eq!(
        got,
        vec![
            (alice_n.clone(), "before-1 from alice".to_string()),
            (alice_n.clone(), "before-2 from alice".to_string()),
            (bob_n.clone(), "before-3 from bob".to_string()),
        ],
        "carol did not get the recent history"
    );
    let _ = carol.gm_rx.try_recv();
}

