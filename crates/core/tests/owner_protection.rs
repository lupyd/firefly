//! Real MLS groups (firefly-core clients, the server's ExternalGroup) against a fake JWKS on
//! 127.0.0.1: who may remove the owner (M-3) and processing commits late (M-4).
//!
//! "Old client" below is a client that sends under the rules every member applies on receipt
//! (what deployed clients send); the server refuses what breaks the stricter rules.

use std::{
    collections::HashMap,
    io::{Read, Write},
    sync::{Arc, OnceLock},
    time::Duration,
};

use firefly_core::{
    FireflyIdentity,
    client::{FireflyClient, FireflyClientMlsConfig, load_client},
    config::{
        CIPHERSUITE, FireflyIdentityProvider, UpdateChannelProposal, UpdateRoleInChannelProposal,
        UpdateRoleProposal, UpdateUserProposal,
    },
    extension::{FireflyGroupExtension, FireflyGroupExtensionWrapper},
    jwk::{JsonWebKeys, ecdsa_to_jwk, generate_key},
    rules::{FireflyMlsRules, filter_proposals_with},
    storage_provider::{
        FfiGroupStateStorage, FfiKeyPackageStorage, FfiPreSharedKeyStorage, MlsGroupStateStorage,
        MlsKeyPackageStorage, MlsPreSharedKeyStorage,
    },
    utils::{get_current_timestamp_in_secs as now, with_commit_time},
};
use firefly_protos::{
    firefly::{AuthToken, FireflyGroupMember, FireflyGroupRole, SignedToken},
    serialize_proto,
};
use mls_rs::{
    CipherSuiteProvider, CryptoProvider, ExtensionList, Group, MlsMessage, MlsRules,
    client_builder::{BaseConfig, WithCryptoProvider, WithIdentityProvider, WithMlsRules},
    extension::{MlsCodecExtension, MlsExtension},
    group::{GroupContext, Roster, proposal::MlsCustomProposal},
    mls_rules::{CommitDirection, CommitOptions, CommitSource, EncryptionOptions, ProposalBundle},
};
use p256::ecdsa::{Signature, SigningKey, signature::hazmat::PrehashSigner};
use sha2::Digest;
use zeroize::Zeroizing;

const DAY: u64 = 24 * 60 * 60;

struct Jwks {
    key: SigningKey,
    base: String,
}

/// One fake JWKS for the whole test binary (make_server reads FIREFLY_BASE_URL once per call).
fn jwks() -> &'static Jwks {
    static JWKS: OnceLock<Jwks> = OnceLock::new();
    JWKS.get_or_init(|| {
        let key = generate_key().unwrap();
        let body = serde_json::to_string(&JsonWebKeys { keys: vec![ecdsa_to_jwk(key.verifying_key(), "k1")] })
            .unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
                let _ = write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nCache-Control: max-age=3600\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
            }
        });
        unsafe { std::env::set_var("FIREFLY_BASE_URL", &base) };
        Jwks { key, base }
    })
}

async fn identity(user: &str, address_id: u64, valid_until: u64) -> FireflyIdentity {
    let cs = mls_rs_crypto_rustcrypto::RustCryptoProvider::default()
        .cipher_suite_provider(CIPHERSUITE)
        .unwrap();
    let (secret, public) = cs.signature_key_generate().await.unwrap();
    let token = AuthToken {
        username: user.into(),
        valid_until,
        credential: public.as_bytes().to_vec().into(),
        address_id,
        device_id: 1,
    };
    let payload = serialize_proto(&token).unwrap().to_vec();
    let signature: Signature = jwks().key.sign_prehash(&sha2::Sha256::digest(&payload)).unwrap();
    let signed = SignedToken {
        kid: "k1".into(),
        payload: payload.into(),
        signature: signature.to_bytes().to_vec().into(),
    };
    let id = firefly_protos::firefly::FireflyIdentity {
        secret: secret.as_bytes().into(),
        public: public.as_bytes().into(),
        credential: serialize_proto(&signed).unwrap().to_vec().into(),
    };
    FireflyIdentity::from_vec(serialize_proto(&id).unwrap().to_vec()).unwrap()
}

#[derive(Default)]
struct Stores {
    kp: tokio::sync::Mutex<HashMap<Vec<u8>, Vec<u8>>>,
}

#[async_trait::async_trait]
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

#[async_trait::async_trait]
impl MlsPreSharedKeyStorage for Stores {
    async fn get(&self, _id: Vec<u8>) -> Option<Vec<u8>> {
        None
    }
}

#[async_trait::async_trait]
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

fn client(identity: FireflyIdentity) -> FireflyClient {
    let s = Arc::new(Stores::default());
    load_client(
        identity,
        FfiKeyPackageStorage::new(s.clone() as Arc<dyn MlsKeyPackageStorage>),
        FfiGroupStateStorage::new(s.clone() as Arc<dyn MlsGroupStateStorage>),
        FfiPreSharedKeyStorage::new(s as Arc<dyn MlsPreSharedKeyStorage>),
        FireflyIdentityProvider::new(jwks().base.clone().into()),
    )
    .unwrap()
}

/// Sends under the receive rules only: what a deployed (pre-StrictRules) client sends.
#[derive(Clone)]
struct OldClientRules;

#[async_trait::async_trait]
impl MlsRules for OldClientRules {
    type Error = firefly_core::config::FireflyError;
    async fn filter_proposals(
        &self,
        direction: CommitDirection,
        source: CommitSource,
        roster: &Roster,
        context: &GroupContext,
        proposals: ProposalBundle,
    ) -> Result<ProposalBundle, Self::Error> {
        filter_proposals_with(direction, source, roster, context, proposals, false)
    }
    fn commit_options(&self, r: &Roster, c: &GroupContext, p: &ProposalBundle) -> Result<CommitOptions, Self::Error> {
        FireflyMlsRules.commit_options(r, c, p)
    }
    fn encryption_options(&self, r: &Roster, c: &GroupContext) -> Result<EncryptionOptions, Self::Error> {
        FireflyMlsRules.encryption_options(r, c)
    }
}

type OldConfig = WithIdentityProvider<
    FireflyIdentityProvider,
    WithCryptoProvider<mls_rs_crypto_rustcrypto::RustCryptoProvider, WithMlsRules<OldClientRules, BaseConfig>>,
>;

fn old_client(identity: FireflyIdentity) -> mls_rs::Client<OldConfig> {
    mls_rs::Client::builder()
        .extension_type(<FireflyGroupExtension as MlsCodecExtension>::extension_type())
        .mls_rules(OldClientRules)
        .identity_provider(FireflyIdentityProvider::new(jwks().base.clone().into()))
        .crypto_provider(mls_rs_crypto_rustcrypto::RustCryptoProvider::default())
        .custom_proposal_type(UpdateUserProposal::proposal_type())
        .custom_proposal_type(UpdateRoleProposal::proposal_type())
        .custom_proposal_type(UpdateChannelProposal::proposal_type())
        .custom_proposal_type(UpdateRoleInChannelProposal::proposal_type())
        .signing_identity(identity.signing_identity(), identity.secret().clone(), CIPHERSUITE)
        .build()
}

/// What `create_group_with_settings` makes, plus an "admin" role with every permission (the same
/// permissions as the owner role) held by `admin`.
fn initial_extension(owner: &str, admin: Option<&str>) -> ExtensionList {
    let mut ext = FireflyGroupExtensionWrapper::new(Default::default());
    ext.update_group("g".into(), firefly_core::config::DEFAULT_GROUP_PERMISSIONS);
    ext.update_role(FireflyGroupRole { id: 1, name: "owner".into(), permissions: u32::MAX, color: 0 });
    ext.update_role(FireflyGroupRole { id: 2, name: "admin".into(), permissions: u32::MAX, color: 0 });
    ext.update_member(FireflyGroupMember { username: owner.to_string().into(), role: 1 });
    if let Some(admin) = admin {
        ext.update_member(FireflyGroupMember { username: admin.to_string().into(), role: 2 });
    }
    let mut list = ExtensionList::new();
    list.set(FireflyGroupExtension::new(ext).unwrap().into_extension().unwrap());
    list
}

fn owners<C: mls_rs::client_builder::MlsConfig>(group: &Group<C>) -> Vec<String> {
    let ext = group.context().extensions().get_as::<FireflyGroupExtension>().unwrap().unwrap();
    let w = ext.deserialize().unwrap();
    w.inner().members.iter().filter(|m| m.role == 1).map(|m| m.username.to_string()).collect()
}

async fn kp(c: &FireflyClient) -> MlsMessage {
    c.generate_key_package_message(Default::default(), Default::default(), None).await.unwrap()
}

type G = Group<FireflyClientMlsConfig>;
type Server = mls_rs::external_client::ExternalGroup<firefly_core::server::FireflyServerConfig>;

/// owner (one or two devices), the group's server, and `others` added by the owner. Returns the
/// owner's groups, the server's group, and each other's group (joined from the Welcome).
async fn setup(owner_devices: &[FireflyClient], others: &[&FireflyClient], admin: Option<&str>) -> (Vec<G>, Server, Vec<G>) {
    jwks();
    let mut og = owner_devices[0].create_group(initial_extension("owner", admin), Default::default(), None).await.unwrap();
    let mut sg = firefly_core::server::make_server()
        .observe_group(og.group_info_message(true).await.unwrap(), None, None)
        .await
        .unwrap();
    let mut builder = og.commit_builder();
    for device in &owner_devices[1..] {
        builder = builder.add_member(kp(device).await).unwrap();
    }
    for other in others {
        builder = builder.add_member(kp(other).await).unwrap();
    }
    let commit = builder.build().await.unwrap();
    sg.process_incoming_message(commit.commit_message.clone()).await.unwrap();
    og.apply_pending_commit().await.unwrap();
    let mut owner_groups = vec![og];
    let mut groups = Vec::new();
    if let Some(welcome) = commit.welcome_messages.first() {
        for device in &owner_devices[1..] {
            owner_groups.push(device.join_group(None, welcome, None).await.unwrap().0);
        }
        for other in others {
            groups.push(other.join_group(None, welcome, None).await.unwrap().0);
        }
    }
    (owner_groups, sg, groups)
}

fn leaf_of<C: mls_rs::client_builder::MlsConfig>(group: &Group<C>) -> u32 {
    group.current_member_index()
}

#[tokio::test]
async fn members_cannot_remove_the_owners_live_device_even_an_older_one() {
    let t = now();
    let owner_phone = client(identity("owner", 1, t + 20 * DAY).await); // renewed earlier
    let owner_laptop = client(identity("owner", 2, t + 29 * DAY).await);
    let alice = client(identity("alice", 3, t + 30 * DAY).await);
    let admin = client(identity("admin", 4, t + 30 * DAY).await);
    let (owner_groups, mut sg, mut groups) = setup(&[owner_phone, owner_laptop], &[&alice, &admin], Some("admin")).await;
    let phone = leaf_of(&owner_groups[0]);

    // A new client refuses to build either removal.
    let err = groups[0].commit_builder().remove_member(phone).unwrap().build().await.unwrap_err();
    assert!(format!("{err:?}").contains("only the owner can remove a live device"), "{err:?}");
    // An admin with every permission the owner has can't kick the owner either.
    let err = groups[1].commit_builder().remove_member(phone).unwrap().build().await.unwrap_err();
    assert!(format!("{err:?}").contains("only the owner can remove a live device"), "{err:?}");

    let mut og_laptop = owner_groups.into_iter().nth(1).unwrap();
    let before = sg.group_context().epoch;
    // Owner's laptop removes the owner's own phone: always allowed.
    let own = og_laptop.commit_builder().remove_member(phone).unwrap().build().await.unwrap();
    sg.process_incoming_message(own.commit_message.clone()).await.unwrap();
    og_laptop.apply_pending_commit().await.unwrap();
    groups[0].process_incoming_message(own.commit_message.clone()).await.unwrap();
    assert_eq!(sg.group_context().epoch, before + 1);
    assert_eq!(owners(&groups[0]), vec!["owner".to_string()]);
}

/// An old client's commit that the server refuses, built from the same group state.
#[tokio::test]
async fn server_refuses_old_clients_removal_of_owner_device_but_receivers_stay_lenient() {
    let t = now();
    let owner_phone = client(identity("owner", 1, t + 20 * DAY).await);
    let owner_laptop = client(identity("owner", 2, t + 29 * DAY).await);
    let alice_identity = identity("alice", 3, t + 30 * DAY).await;
    let alice_old = old_client(alice_identity.clone());
    let bob = client(identity("bob", 5, t + 30 * DAY).await);

    // Built by hand so alice runs the old client.
    jwks();
    let mut og = owner_phone.create_group(initial_extension("owner", None), Default::default(), None).await.unwrap();
    let mut sg = firefly_core::server::make_server()
        .observe_group(og.group_info_message(true).await.unwrap(), None, None)
        .await
        .unwrap();
    let alice_kp = alice_old.generate_key_package_message(Default::default(), Default::default(), None).await.unwrap();
    let commit = og
        .commit_builder()
        .add_member(kp(&owner_laptop).await).unwrap()
        .add_member(alice_kp).unwrap()
        .add_member(kp(&bob).await).unwrap()
        .build()
        .await
        .unwrap();
    sg.process_incoming_message(commit.commit_message.clone()).await.unwrap();
    og.apply_pending_commit().await.unwrap();
    let welcome = &commit.welcome_messages[0];
    let (mut alice_group, _) = alice_old.join_group(None, welcome, None).await.unwrap();
    let (mut bob_group, _) = bob.join_group(None, welcome, None).await.unwrap();

    let phone = og.current_member_index();
    let stale = alice_group.commit_builder().remove_member(phone).unwrap().build().await.unwrap();
    let err = sg.process_incoming_message(stale.commit_message.clone()).await.unwrap_err();
    assert!(format!("{err:?}").contains("only the owner can remove a live device"), "{err:?}");
    // The server's group is untouched by the refused commit.
    assert_eq!(sg.group_context().epoch, og.current_epoch());

    // Had an old server accepted it (it is in some backlog), a new member still applies it, and
    // computes the same group state (extension included) as the old committer: nothing forks.
    bob_group.process_incoming_message(stale.commit_message.clone()).await.unwrap();
    alice_group.apply_pending_commit().await.unwrap();
    assert_eq!(bob_group.epoch_authenticator().unwrap(), alice_group.epoch_authenticator().unwrap());
    assert_eq!(owners(&bob_group), vec!["owner".to_string()]);
}

#[tokio::test]
async fn owners_expired_devices_go_but_the_last_one_stays() {
    let t = now();
    let owner_old = client(identity("owner", 1, t + 2).await); // expires in 2 s
    let owner_new = client(identity("owner", 2, t + 3).await); // and this one in 3 s
    let alice = client(identity("alice", 3, t + 30 * DAY).await);
    let carol = client(identity("carol", 4, t + 30 * DAY).await);
    let (owner_groups, mut sg, mut groups) = setup(&[owner_old, owner_new], &[&alice, &carol], None).await;
    let old_leaf = leaf_of(&owner_groups[0]);
    let new_leaf = leaf_of(&owner_groups[1]);
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Both expired: removing both would delete the owner role. The last device stays.
    let err = groups[0]
        .commit_builder()
        .remove_member(old_leaf).unwrap()
        .remove_member(new_leaf).unwrap()
        .build()
        .await
        .unwrap_err();
    assert!(format!("{err:?}").contains("last device stays"), "{err:?}");

    // The older one alone is fine.
    let c = groups[0].commit_builder().remove_member(old_leaf).unwrap().build().await.unwrap();
    sg.process_incoming_message(c.commit_message.clone()).await.unwrap();
    groups[0].apply_pending_commit().await.unwrap();
    groups[1].process_incoming_message(c.commit_message.clone()).await.unwrap();
    assert_eq!(owners(&groups[1]), vec!["owner".to_string()]);

    // Now the remaining (expired) one is the last.
    let err = groups[0].commit_builder().remove_member(new_leaf).unwrap().build().await.unwrap_err();
    assert!(format!("{err:?}").contains("last device stays"), "{err:?}");

    // A new member's device still joins: the expired owner leaf doesn't block the Welcome.
    let carol_phone = client(identity("carol", 5, now() + 30 * DAY).await);
    let add = groups[1].commit_builder().add_member(kp(&carol_phone).await).unwrap().build().await.unwrap();
    sg.process_incoming_message(add.commit_message.clone()).await.unwrap();
    groups[1].apply_pending_commit().await.unwrap();
    let (joined, _) = carol_phone.join_group(None, &add.welcome_messages[0], None).await.unwrap();
    assert_eq!(owners(&joined), vec!["owner".to_string()]);
}

#[tokio::test]
async fn non_owner_stale_cleanup_is_unchanged() {
    // PoC S1 for a user who is not the owner: the older (live) device can go, the newest stays.
    let t = now();
    let owner = client(identity("owner", 1, t + 30 * DAY).await);
    let alice = client(identity("alice", 2, t + 30 * DAY).await);
    let bob_phone = client(identity("bob", 3, t + 20 * DAY).await);
    let bob_laptop = client(identity("bob", 4, t + 29 * DAY).await);
    let (mut owner_groups, mut sg, mut groups) = setup(&[owner], &[&alice, &bob_phone, &bob_laptop], None).await;
    let phone = leaf_of(&groups[1]);
    let laptop = leaf_of(&groups[2]);

    let err = groups[0].commit_builder().remove_member(laptop).unwrap().build().await.unwrap_err();
    assert!(format!("{err:?}").contains("permission to manage members"), "{err:?}");

    let c = groups[0].commit_builder().remove_member(phone).unwrap().build().await.unwrap();
    sg.process_incoming_message(c.commit_message.clone()).await.unwrap();
    groups[0].apply_pending_commit().await.unwrap();
    owner_groups[0].process_incoming_message(c.commit_message.clone()).await.unwrap();
    groups[2].process_incoming_message(c.commit_message.clone()).await.unwrap();
    assert_eq!(owner_groups[0].epoch_authenticator().unwrap(), groups[2].epoch_authenticator().unwrap());
}

#[tokio::test]
async fn non_owners_cannot_touch_the_owner_role() {
    let t = now();
    let owner = client(identity("owner", 1, t + 30 * DAY).await);
    let admin = client(identity("admin", 2, t + 30 * DAY).await);
    let (_owner_groups, _sg, mut groups) = setup(&[owner], &[&admin], Some("admin")).await;
    let proposals = [
        UpdateUserProposal { username: "owner".into(), role_id: 2 }.to_custom_proposal().unwrap(),
        UpdateUserProposal { username: "admin".into(), role_id: 1 }.to_custom_proposal().unwrap(),
        UpdateRoleProposal { name: "owner".into(), role_id: 1, permissions: 1, delete: false, color: 0 }
            .to_custom_proposal()
            .unwrap(),
        UpdateRoleProposal { name: "owner".into(), role_id: 1, permissions: 0, delete: true, color: 0 }
            .to_custom_proposal()
            .unwrap(),
    ];
    for proposal in proposals {
        let err = groups[0].commit_builder().custom_proposal(proposal).build().await.unwrap_err();
        assert!(format!("{err:?}").contains("only the owner can change the owner role"), "{err:?}");
    }
}

#[tokio::test]
async fn late_processing_uses_the_commits_server_time() {
    // M-4 PoC: carol is added with a credential that is valid then, and expires before bob (who
    // was offline) processes the commit.
    let t = now() + 30 * DAY;
    let owner = client(identity("owner", 1, t).await);
    let bob = client(identity("bob", 2, t).await);
    let (mut owner_groups, mut sg, mut groups) = setup(&[owner], &[&bob], None).await;
    let carol = client(identity("carol", 3, now() + 2).await);
    let add = owner_groups[0].commit_builder().add_member(kp(&carol).await).unwrap().build().await.unwrap();
    let accepted_at = now();
    sg.process_incoming_message(add.commit_message.clone()).await.unwrap();
    owner_groups[0].apply_pending_commit().await.unwrap();
    tokio::time::sleep(Duration::from_secs(4)).await;

    // Judged by bob's clock: stuck, and the failure is classified as permanent (re-add needed).
    let err = groups[0].process_incoming_message(add.commit_message.clone()).await.unwrap_err();
    assert!(format!("{err:?}").contains("token expired"), "{err:?}");
    assert!(firefly_core::is_permanent_commit_failure(&anyhow::Error::from(err)));

    // Judged at the time the server accepted it: applied, same state as everyone else.
    with_commit_time(Some(accepted_at), groups[0].process_incoming_message(add.commit_message.clone()))
        .await
        .unwrap();
    assert_eq!(groups[0].epoch_authenticator().unwrap(), owner_groups[0].epoch_authenticator().unwrap());
    assert_eq!(groups[0].current_epoch(), sg.group_context().epoch);
}

#[tokio::test]
async fn a_late_or_skewed_committer_cant_get_an_expired_credential_past_the_server() {
    let t = now() + 30 * DAY;
    let owner = client(identity("owner", 1, t).await);
    let (mut owner_groups, mut sg, _) = setup(&[owner], &[], None).await;
    let carol = client(identity("carol", 3, now() + 1).await);
    let carol_kp = kp(&carol).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    // The committer's clock is behind (here: it judges at an earlier commit time): it builds the
    // add, and the server, on its own clock, refuses it.
    let add = with_commit_time(Some(now() - 60 * 60), async {
        owner_groups[0].commit_builder().add_member(carol_kp).unwrap().build().await
    })
    .await
    .unwrap();
    let err = sg.process_incoming_message(add.commit_message.clone()).await.unwrap_err();
    assert!(format!("{err:?}").contains("token expired"), "{err:?}");
}
