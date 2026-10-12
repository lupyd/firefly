//! Message requests: call gating, "requests disabled", implicit/explicit accepts and the ordering
//! of call signals over WebTransport. Requires a firefly-mls server (see `FIREFLY_BASE_URL`);
//! call gating is enforced by default.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use firefly_client::callbacks::{CallSignal, FireflyWsClientCallback};
use firefly_client::db::{group_messages::GroupMessage, messages::UserMessage};
use firefly_client::websocket::{ActiveTransport, FireflyWsClient};
use tokio::sync::mpsc;

const CALL_REQUEST: i32 = 0;
const CALL_REJECT: i32 = 2;
const CALL_ICECANDIDATE: i32 = 6;
const REJECT_NOT_ACCEPTED: i32 = 1;
const REJECT_BLOCKED: i32 = 2;

struct Callbacks {
    name: String,
    message_tx: mpsc::Sender<UserMessage>,
    call_tx: mpsc::Sender<CallSignal>,
}

#[async_trait]
impl FireflyWsClientCallback for Callbacks {
    fn name(&self) -> &str {
        &self.name
    }
    async fn get_access_token(&self) -> Option<String> {
        Some(self.name.clone())
    }
    async fn on_message(&self, message: UserMessage) {
        let _ = self.message_tx.send(message).await;
    }
    async fn on_group_message(&self, _group_message: GroupMessage) {}
    async fn on_call_signal(&self, signal: CallSignal) {
        let _ = self.call_tx.send(signal).await;
    }
}

struct Peer {
    name: String,
    client: Arc<FireflyWsClient>,
    messages: mpsc::Receiver<UserMessage>,
    calls: mpsc::Receiver<CallSignal>,
}

fn urls() -> Option<(String, String, String)> {
    let _ = std::fs::create_dir_all("/tmp/firefly");
    dotenv::from_filename(".env.test").ok();
    let base = std::env::var("FIREFLY_BASE_URL").ok()?;
    let ws = std::env::var("FIREFLY_WS_URL").unwrap_or_else(|_| base.replace("http://", "ws://"));
    let wt = std::env::var("FIREFLY_WT_URL").unwrap_or_else(|_| base.replace("http://", "https://"));
    if std::env::var("FIREFLY_WT_CERT_SHA256").is_err() {
        // SAFETY: set before any client thread is spawned in this test.
        unsafe { std::env::set_var("FIREFLY_WT_INSECURE", "1") };
    }
    firefly_client::init_logger("/tmp/firefly/test_requests.log".to_string());
    Some((base, ws, wt))
}

async fn peer(prefix: &str, run: u32, dir: &str, wt: bool) -> Peer {
    let (base, ws, wt_url) = urls().expect("urls");
    let name = format!("{prefix}_{run}");
    let (message_tx, messages) = mpsc::channel(256);
    let (call_tx, calls) = mpsc::channel(1024);
    let client = FireflyWsClient::create_with_endpoints(
        base,
        ws,
        wt.then_some(wt_url),
        1000,
        Box::new(Callbacks {
            name: name.clone(),
            message_tx,
            call_tx,
        }),
        format!("{dir}/{name}.db"),
        5000,
    )
    .await
    .expect("create client");
    let client = Arc::new(client);
    let init = client.clone();
    tokio::spawn(async move {
        let _ = init.initialize_with_retrying().await;
    });
    for _ in 0..80 {
        if client.is_initialized() && client.current_transport().is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(client.is_initialized(), "{name} failed to initialize");
    if wt {
        assert_eq!(client.current_transport(), Some(ActiveTransport::WebTransport));
    }
    Peer {
        name,
        client,
        messages,
        calls,
    }
}

/// Call ids unique to this run: the server remembers call ids, and a long-running test server
/// would otherwise confuse this run's calls with an earlier run's.
fn call_id(n: u64) -> u64 {
    static BASE: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *BASE.get_or_init(|| (rand::random::<u32>() as u64) << 20) + n
}

async fn next_call(rx: &mut mpsc::Receiver<CallSignal>, secs: u64) -> Option<CallSignal> {
    tokio::time::timeout(Duration::from_secs(secs), rx.recv())
        .await
        .ok()
        .flatten()
}

fn dir(tag: &str) -> (u32, String) {
    let run = rand::random::<u32>();
    let dir = format!("/tmp/firefly/{tag}_{run}");
    let _ = std::fs::create_dir_all(&dir);
    (run, dir)
}

#[tokio::test]
async fn unaccepted_caller_is_rejected_and_callee_never_rings() {
    if urls().is_none() {
        return;
    }
    let (run, dir) = dir("req_call");
    let mut alice = peer("alice_rc", run, &dir, true).await;
    let mut bob = peer("bob_rc", run, &dir, true).await;

    alice
        .client
        .initiate_call(call_id(1001), bob.name.clone(), "offer".into())
        .await
        .unwrap();
    // A refused caller keeps sending ICE; none of it may reach the callee.
    for i in 0..5 {
        alice
            .client
            .send_ice_candidate(call_id(1001), bob.name.clone(), format!("c{i}"), "0".into(), 0)
            .await
            .unwrap();
    }

    let reject = next_call(&mut alice.calls, 10).await.expect("caller gets a reject");
    assert_eq!(reject.signal_type, CALL_REJECT);
    assert_eq!(reject.reject_reason, REJECT_NOT_ACCEPTED);
    assert_eq!(reject.call_id, call_id(1001));
    assert!(
        next_call(&mut bob.calls, 3).await.is_none(),
        "callee must not see anything from an unaccepted caller"
    );

    // After bob accepts alice, the call rings and ICE arrives in order.
    bob.client.set_user_accepted(&alice.name, true).await.unwrap();
    assert!(bob.client.is_user_accepted(&alice.name).await);

    alice
        .client
        .initiate_call(call_id(1002), bob.name.clone(), "offer".into())
        .await
        .unwrap();
    for i in 0..200 {
        alice
            .client
            .send_ice_candidate(call_id(1002), bob.name.clone(), format!("c{i}"), "0".into(), 0)
            .await
            .unwrap();
    }
    let request = next_call(&mut bob.calls, 10).await.expect("callee rings");
    assert_eq!(request.signal_type, CALL_REQUEST);
    assert_eq!(request.call_id, call_id(1002));
    for i in 0..200 {
        let ice = next_call(&mut bob.calls, 10).await.expect("ice candidate");
        assert_eq!(ice.signal_type, CALL_ICECANDIDATE);
        assert_eq!(ice.candidate, format!("c{i}"), "ICE candidates must arrive in order");
    }

    alice.client.dispose().await;
    bob.client.dispose().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn messaging_first_implicitly_accepts_for_calls() {
    if urls().is_none() {
        return;
    }
    let (run, dir) = dir("req_implicit");
    let mut carol = peer("carol_ri", run, &dir, true).await;
    let mut dave = peer("dave_ri", run, &dir, false).await;

    tokio::time::sleep(Duration::from_millis(500)).await;
    // carol messages dave first: carol accepted dave, dave did not accept carol.
    carol
        .client
        .encrypt_and_send(dave.name.clone(), b"hello dave, first contact".to_vec())
        .await
        .unwrap();
    let got = tokio::time::timeout(Duration::from_secs(10), dave.messages.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.message, b"hello dave, first contact");
    assert!(carol.client.is_user_accepted(&dave.name).await);
    assert!(!dave.client.is_user_accepted(&carol.name).await);
    // dave's client auto-replied to carol's prekey message; that must not accept carol.
    let dave_accepted = dave.client.refresh_accepted_users().await.unwrap();
    assert!(!dave_accepted.contains(&carol.name), "automatic reply accepted the request: {dave_accepted:?}");

    // dave may call carol, carol may not call dave.
    dave.client
        .initiate_call(call_id(2001), carol.name.clone(), "offer".into())
        .await
        .unwrap();
    let ring = next_call(&mut carol.calls, 10).await.expect("carol rings");
    assert_eq!(ring.signal_type, CALL_REQUEST);

    carol
        .client
        .initiate_call(call_id(2002), dave.name.clone(), "offer".into())
        .await
        .unwrap();
    let reject = next_call(&mut carol.calls, 10).await.expect("carol is refused");
    assert_eq!(reject.signal_type, CALL_REJECT);
    assert_eq!(reject.reject_reason, REJECT_NOT_ACCEPTED);

    // The server list is the source of truth after a refresh.
    let accepted = carol.client.refresh_accepted_users().await.unwrap();
    assert_eq!(accepted, vec![dave.name.clone()]);

    carol.client.dispose().await;
    dave.client.dispose().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn disabled_requests_refuse_strangers_but_not_accepted_users() {
    if urls().is_none() {
        return;
    }
    let (run, dir) = dir("req_disabled");
    let mut erin = peer("erin_rd", run, &dir, true).await;
    let frank = peer("frank_rd", run, &dir, true).await;
    let grace = peer("grace_rd", run, &dir, false).await;

    erin.client.set_requests_disabled(true).await.unwrap();
    assert!(erin.client.get_requests_disabled().await.unwrap());
    erin.client.set_user_accepted(&grace.name, true).await.unwrap();

    let err = frank
        .client
        .encrypt_and_send(erin.name.clone(), b"stranger".to_vec())
        .await
        .expect_err("stranger must be refused");
    assert!(
        err.to_string().contains("not accepting message requests"),
        "unexpected error: {err}"
    );
    assert!(!frank.client.is_user_accepted(&erin.name).await, "a refused send is not an accept");

    grace
        .client
        .encrypt_and_send(erin.name.clone(), b"friend".to_vec())
        .await
        .expect("accepted user may message");
    let got = tokio::time::timeout(Duration::from_secs(10), erin.messages.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.message, b"friend");

    erin.client.set_requests_disabled(false).await.unwrap();
    frank
        .client
        .encrypt_and_send(erin.name.clone(), b"now allowed".to_vec())
        .await
        .expect("requests re-enabled");

    erin.client.dispose().await;
    frank.client.dispose().await;
    grace.client.dispose().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn blocked_caller_is_rejected() {
    if urls().is_none() {
        return;
    }
    let (base, _, _) = urls().unwrap();
    let (run, dir) = dir("req_block");
    let mut heidi = peer("heidi_rb", run, &dir, true).await;
    let ivan = peer("ivan_rb", run, &dir, true).await;

    ivan.client.set_user_accepted(&heidi.name, true).await.unwrap();
    let admin = std::env::var("SECRET_ADMIN_TOKEN").unwrap_or_else(|_| "secret_admin_token".into());
    let status = reqwest::Client::new()
        .post(format!(
            "{base}/user/block?me={}&other={}&blocked=true",
            ivan.name, heidi.name
        ))
        .header("Authorization", admin)
        .send()
        .await
        .unwrap()
        .status();
    assert!(status.is_success(), "block failed: {status}");

    heidi
        .client
        .initiate_call(call_id(3001), ivan.name.clone(), "offer".into())
        .await
        .unwrap();
    let reject = next_call(&mut heidi.calls, 10).await.expect("blocked caller is refused");
    assert_eq!(reject.signal_type, CALL_REJECT);
    assert_eq!(reject.reject_reason, REJECT_BLOCKED);

    heidi.client.dispose().await;
    ivan.client.dispose().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn forged_follow_up_signals_never_reach_an_unaccepting_callee() {
    if urls().is_none() {
        return;
    }
    let (run, dir) = dir("req_forged");
    let mallory = peer("mallory_rf", run, &dir, true).await;
    let mut victim = peer("victim_rf", run, &dir, true).await;

    // No CALL_REQUEST at all: signals with fresh call ids and no live call session.
    for i in 0..5u64 {
        mallory
            .client
            .send_ice_candidate(call_id(9000 + i), victim.name.clone(), format!("c{i}"), "0".into(), 0)
            .await
            .unwrap();
        mallory.client.hangup_call(call_id(9100 + i), victim.name.clone()).await.unwrap();
    }
    let leaked = next_call(&mut victim.calls, 4).await;
    assert!(
        leaked.is_none(),
        "forged signals from an unaccepted user must be dropped, got {leaked:?}"
    );

    mallory.client.dispose().await;
    victim.client.dispose().await;
    let _ = std::fs::remove_dir_all(&dir);
}
