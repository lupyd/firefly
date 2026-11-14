//! A member uploads a commit with a corrupted tag (the tag_brick PoC). The server can't check the
//! tag and accepts it; the other members' clients fail it, dispute it on their own, the server
//! withdraws it, and the group goes on: messages flow and the next legitimate commit (adding a
//! member) is processed by everyone. The tampering member's device gets re-added automatically.
//!
//! Needs a firefly-mls server with commit disputes (FIREFLY_BASE_URL), ideally started with a
//! short COMMIT_DISPUTE_GRACE_MS.
use async_trait::async_trait;
use firefly_client::callbacks::FireflyWsClientCallback;
use firefly_client::db::{group_messages::GroupMessage, messages::UserMessage};
use firefly_client::websocket::FireflyWsClient;
use firefly_core::{
    FireflyIdentity,
    client::load_client,
    config::FireflyIdentityProvider,
    storage_provider::{
        FfiGroupStateStorage, FfiKeyPackageStorage, FfiPreSharedKeyStorage, MlsGroupStateStorage,
        MlsKeyPackageStorage, MlsPreSharedKeyStorage,
    },
    utils::HTTP_CLIENT,
};
use firefly_protos::{deserialize_proto, firefly, serialize_proto};
use mls_rs::{MlsMessage, mls_rs_codec::MlsEncode};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::mpsc;
use zeroize::Zeroizing;

struct TestCallbacks {
    name: String,
    group_message_tx: mpsc::Sender<GroupMessage>,
}

#[async_trait]
impl FireflyWsClientCallback for TestCallbacks {
    fn name(&self) -> &str {
        &self.name
    }
    async fn get_access_token(&self) -> Option<String> {
        Some(self.name.clone())
    }
    async fn on_message(&self, _message: UserMessage) {}
    async fn on_group_message(&self, group_message: GroupMessage) {
        let _ = self.group_message_tx.send(group_message).await;
    }
}

#[derive(Default)]
struct Stores {
    kp: tokio::sync::Mutex<HashMap<Vec<u8>, Vec<u8>>>,
}

#[async_trait]
impl MlsKeyPackageStorage for Stores {
    async fn insert(&self, id: Vec<u8>, data: Vec<u8>) -> bool {
        self.kp.lock().await.insert(id, data).is_none()
    }
    async fn delete(&self, id: Vec<u8>) -> bool {
        self.kp.lock().await.remove(&id).is_some()
    }
    async fn get(&self, id: Vec<u8>) -> Option<Vec<u8>> {
        self.kp.lock().await.get(&id).cloned()
    }
}

#[async_trait]
impl MlsPreSharedKeyStorage for Stores {
    async fn get(&self, _id: Vec<u8>) -> Option<Vec<u8>> {
        None
    }
}

#[async_trait]
impl MlsGroupStateStorage for Stores {
    async fn state(&self, _: Vec<u8>) -> Option<Zeroizing<Vec<u8>>> {
        None
    }
    async fn epoch(&self, _: Vec<u8>, _: u64) -> Option<Zeroizing<Vec<u8>>> {
        None
    }
    async fn write(
        &self,
        _: Vec<u8>,
        _: Zeroizing<Vec<u8>>,
        _: HashMap<u64, Zeroizing<Vec<u8>>>,
        _: HashMap<u64, Zeroizing<Vec<u8>>>,
    ) -> bool {
        true
    }
    async fn max_epoch_id(&self, _: Vec<u8>) -> Option<u64> {
        None
    }
}

async fn setup_server() -> Option<(String, String)> {
    let _ = std::fs::create_dir_all("/tmp/firefly");
    dotenv::from_filename(".env.test").ok();
    let base_url = std::env::var("FIREFLY_BASE_URL").ok()?;
    let ws_url = std::env::var("FIREFLY_WS_URL")
        .unwrap_or_else(|_| base_url.replace("http://", "ws://").replace("https://", "wss://"));
    Some((base_url, ws_url))
}

async fn new_client(
    name: &str,
    base_url: &str,
    ws_url: &str,
    dir: &str,
) -> (Arc<FireflyWsClient>, mpsc::Receiver<GroupMessage>) {
    let (tx, rx) = mpsc::channel(100);
    let client = FireflyWsClient::create(
        base_url.to_string(),
        ws_url.to_string(),
        1000,
        Box::new(TestCallbacks { name: name.to_string(), group_message_tx: tx }),
        format!("{dir}/{name}.db"),
        5000,
    )
    .await
    .expect("client");
    let client = Arc::new(client);
    tokio::spawn({
        let c = client.clone();
        async move {
            let _ = c.initialize_with_retrying().await;
        }
    });
    for _ in 0..60 {
        if client.is_initialized() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(client.is_initialized(), "{name} failed to initialize");
    (client, rx)
}

fn text(t: &str) -> firefly::GroupMessageInner<'static> {
    firefly::GroupMessageInner {
        channelId: 0,
        message: firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(firefly::MessagePayload {
            text: t.to_string().into(),
            ..Default::default()
        }),
        message_type: 0,
    }
}

async fn expect_text(rx: &mut mpsc::Receiver<GroupMessage>, group_id: u64, want: &str, who: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let received = tokio::time::timeout_at(deadline, rx.recv())
            .await
            .unwrap_or_else(|_| panic!("{who} did not receive {want:?}"))
            .expect("channel");
        if received.group_id != group_id {
            continue;
        }
        let inner = deserialize_proto::<firefly::GroupMessageInner>(&received.message).unwrap();
        if let firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(p) = inner.message {
            if p.text == want {
                return;
            }
        }
    }
}

async fn invites(base_url: &str, name: &str, address: u64) -> Vec<firefly::GroupInvite<'static>> {
    let response = HTTP_CLIENT
        .get(format!("{base_url}/group/invites?address={address}"))
        .bearer_auth(name)
        .send()
        .await
        .unwrap();
    let bytes = response.bytes().await.unwrap();
    deserialize_proto::<firefly::GroupInvites>(&bytes)
        .unwrap()
        .invites
        .into_iter()
        .map(|i| firefly::GroupInvite {
            groupId: i.groupId,
            inviter: i.inviter.into_owned().into(),
            invitee: i.invitee.into_owned().into(),
            welcomeMessage: i.welcomeMessage.into_owned().into(),
            commitId: i.commitId,
        })
        .collect()
}

#[tokio::test]
async fn tampered_commit_is_disputed_withdrawn_and_group_moves_on() {
    let Some((base_url, ws_url)) = setup_server().await else {
        println!("Skipping: FIREFLY_BASE_URL is not set");
        return;
    };
    let run = rand::random::<u32>();
    let dir = format!("/tmp/firefly/dispute_{run}");
    let _ = std::fs::create_dir_all(&dir);
    let alice = format!("alice_d{run}");
    let bob = format!("bob_d{run}");
    let carol = format!("carol_d{run}");
    let mallory = format!("mallory_d{run}");

    let (alice_c, mut alice_rx) = new_client(&alice, &base_url, &ws_url, &dir).await;
    let (bob_c, mut bob_rx) = new_client(&bob, &base_url, &ws_url, &dir).await;
    let (carol_c, mut carol_rx) = new_client(&carol, &base_url, &ws_url, &dir).await;

    // Mallory: an ordinary mls-rs client with Firefly's rules, so the test can corrupt a commit.
    let mallory_address = {
        let response = HTTP_CLIENT
            .post(format!("{base_url}/user/device"))
            .bearer_auth(&mallory)
            .body(serialize_proto(&firefly::Address { deviceId: 1, ..Default::default() }).unwrap())
            .send()
            .await
            .unwrap();
        deserialize_proto::<firefly::Address>(&response.bytes().await.unwrap()).unwrap().id
    };
    let identity = FireflyIdentity::generate(mallory.clone(), base_url.clone().into(), 1, mallory_address)
        .await
        .unwrap();
    let stores = Arc::new(Stores::default());
    let mallory_mls = load_client(
        identity,
        FfiKeyPackageStorage::new(stores.clone() as Arc<dyn MlsKeyPackageStorage>),
        FfiGroupStateStorage::new(stores.clone() as Arc<dyn MlsGroupStateStorage>),
        FfiPreSharedKeyStorage::new(stores as Arc<dyn MlsPreSharedKeyStorage>),
        FireflyIdentityProvider::new(base_url.clone().into()),
    )
    .unwrap();
    let upload_key_packages = |n: usize| {
        let mallory = mallory.clone();
        let base_url = base_url.clone();
        let mls = &mallory_mls;
        async move {
            let mut packages = Vec::new();
            for i in 0..n {
                let kp = mls
                    .generate_key_package_message(Default::default(), Default::default(), None)
                    .await
                    .unwrap();
                packages.push(firefly::GroupKeyPackage {
                    id: (rand::random::<u16>() % 30_000) as i32 + i as i32,
                    package: kp.to_bytes().unwrap().into(),
                    address: mallory_address,
                    ..Default::default()
                });
            }
            let response = HTTP_CLIENT
                .post(format!("{base_url}/group/keyPackages?address={mallory_address}"))
                .bearer_auth(&mallory)
                .body(serialize_proto(&firefly::GroupKeyPackages { packages }).unwrap())
                .send()
                .await
                .unwrap();
            assert!(response.status().is_success());
        }
    };
    upload_key_packages(2).await;

    let group_id = alice_c
        .create_group("Disputes".into(), "".into(), 0)
        .await
        .expect("create group")
        .id;
    alice_c.add_group_member(group_id, bob.clone(), 0).await.expect("add bob");
    bob_c.check_setup().await.expect("bob joins");
    alice_c.add_group_member(group_id, mallory.clone(), 0).await.expect("add mallory");
    let invite = invites(&base_url, &mallory, mallory_address)
        .await
        .into_iter()
        .find(|i| i.groupId == group_id)
        .expect("mallory's invite");
    let welcome = MlsMessage::from_bytes(&invite.welcomeMessage).unwrap();
    let (mut mallory_group, _) = mallory_mls.join_group(None, &welcome, None).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Everyone is in sync.
    alice_c.upload_group_message(group_id, text("before"), 0).await.expect("alice sends");
    expect_text(&mut bob_rx, group_id, "before", "bob").await;

    // Mallory's empty commit with one byte of the confirmation tag flipped (tag_brick).
    let commit = mallory_group.commit_builder().build().await.unwrap();
    let mut bytes = commit.commit_message.mls_encode_to_vec().unwrap();
    let n = bytes.len();
    bytes[n - 34] ^= 0xff;
    let response = HTTP_CLIENT
        .post(format!("{base_url}/group/commit"))
        .bearer_auth(&mallory)
        .body(
            serialize_proto(&firefly::GroupMessage {
                groupId: group_id,
                message: bytes.clone().into(),
                ..Default::default()
            })
            .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success(), "the server can't tell: {}", response.status());
    mallory_group.apply_pending_commit().await.unwrap();
    println!("tampered commit accepted by the server; waiting for the members' disputes");

    // Alice's and bob's clients fail it, dispute it, and the server withdraws it. Their messages
    // then go through again (until the withdrawal the server refuses them as stale).
    let mut sent = false;
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        if alice_c.upload_group_message(group_id, text("after"), 0).await.is_ok() {
            sent = true;
            break;
        }
    }
    assert!(sent, "alice could never send again: the group stayed bricked");
    expect_text(&mut bob_rx, group_id, "after", "bob").await;
    bob_c.upload_group_message(group_id, text("bob after"), 0).await.expect("bob sends");
    expect_text(&mut alice_rx, group_id, "bob after", "alice").await;

    // The next legitimate commit: alice adds carol. Bob processes it; carol joins; all talk.
    alice_c.add_group_member(group_id, carol.clone(), 0).await.expect("add carol");
    carol_c.check_setup().await.expect("carol joins");
    tokio::time::sleep(Duration::from_secs(2)).await;
    carol_c.upload_group_message(group_id, text("hi from carol"), 0).await.expect("carol sends");
    expect_text(&mut alice_rx, group_id, "hi from carol", "alice").await;
    expect_text(&mut bob_rx, group_id, "hi from carol", "bob").await;
    bob_c.upload_group_message(group_id, text("hi carol"), 0).await.expect("bob sends");
    expect_text(&mut carol_rx, group_id, "hi carol", "carol").await;

    // Mallory's device applied the withdrawn commit; alice (who manages members) re-adds it on
    // the usual re-add request: a fresh invite shows up for mallory. (A claim on the request from
    // an earlier attempt holds it for a while, as in the existing re-add flow.)
    upload_key_packages(2).await;
    let _ = alice_c.sync_group_joins_and_readds().await;
    let mut re_added = false;
    for _ in 0..90 {
        if invites(&base_url, &mallory, mallory_address)
            .await
            .iter()
            .any(|i| i.groupId == group_id && i.commitId != invite.commitId)
        {
            re_added = true;
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        let _ = alice_c.sync_group_joins_and_readds().await;
    }
    assert!(re_added, "mallory's device was not re-added");

    for c in [alice_c, bob_c, carol_c] {
        c.dispose().await;
    }
    let _ = std::fs::remove_dir_all(&dir);
}
