use std::{collections::HashMap, sync::Arc};

use firefly_core::{
    FireflyAuthTokenCallback, FireflyIdentity, FireflyMlsClient,
    config::{UpdateChannelProposal, UpdateRoleInChannelProposal, UpdateRoleProposal, UserPermission},
    ensure_application_message_is_current, ensure_key_package_belongs_to,
    extension::{FireflyGroupExtension, FireflyGroupExtensionWrapper},
    rules::{check_group_proposal, handle_custom_proposal, should_drop_invalid_proposal},
    storage_provider::{MlsGroupStateStorage, MlsKeyPackageStorage, MlsPreSharedKeyStorage},
};
use firefly_protos::{
    firefly::{AuthToken, FireflyGroupChannel, FireflyGroupMember, FireflyGroupRole, SignedToken},
    serialize_proto,
};
use mls_rs::{
    CipherSuiteProvider, CryptoProvider, ExtensionList, MlsMessage,
    extension::MlsExtension,
    group::proposal::{MlsCustomProposal, Proposal, ReInitProposal},
    mls_rules::{CommitDirection, ProposalRef, ProposalSource},
};
use mls_rs_codec::MlsDecode;
use zeroize::Zeroizing;

const SEE: u32 = UserPermission::SeeMessage as u32;
const ADD: u32 = UserPermission::AddMessage as u32;
const CHANNEL: u32 = UserPermission::ManageChannel as u32;
const ROLE: u32 = UserPermission::ManageRole as u32;
const GROUP: u32 = UserPermission::ManageGroup as u32;

const OWNER: &str = "owner";
const MOD: &str = "mod";
const MOD_PERMS: u32 = SEE | ADD | CHANNEL | ROLE | GROUP;

/// owner (role 1), mod (role 2: ManageChannel + ManageRole + ManageGroup), channel 7 with
/// a channel override restricting mod to SEE | ADD | ROLE.
fn group() -> FireflyGroupExtensionWrapper<'static> {
    let mut ext = FireflyGroupExtensionWrapper::new(Default::default());
    ext.update_group("group".into(), SEE | ADD);
    ext.update_role(FireflyGroupRole {
        id: 1,
        name: "owner".into(),
        permissions: u32::MAX,
        ..Default::default()
    });
    ext.update_role(FireflyGroupRole {
        id: 2,
        name: "mod".into(),
        permissions: MOD_PERMS,
        ..Default::default()
    });
    ext.update_member(FireflyGroupMember { username: OWNER.into(), role: 1 });
    ext.update_member(FireflyGroupMember { username: MOD.into(), role: 2 });
    ext.update_channel(FireflyGroupChannel {
        id: 7,
        name: "seven".into(),
        default_permissions: SEE,
        ..Default::default()
    });
    ext.update_channel_role_permissions(7, 2, SEE | ADD | ROLE);
    assert!(ext.is_valid());
    ext
}

fn perms_of(ext: &FireflyGroupExtensionWrapper, user: &str) -> u32 {
    let role = ext.get_role_of_user(user).unwrap_or(0);
    ext.get_permissions_from_role_id(role).unwrap()
}

fn channel_role(channel_id: u32, role_id: u32, permissions: u32) -> Proposal {
    Proposal::Custom(
        UpdateRoleInChannelProposal {
            channel_id,
            role_proposal: UpdateRoleProposal {
                name: String::new(),
                role_id,
                permissions,
                delete: false,
                color: 0,
            },
        }
        .to_custom_proposal()
        .unwrap(),
    )
}

fn channel(id: u32, default_permissions: u32) -> Proposal {
    Proposal::Custom(
        UpdateChannelProposal {
            id,
            delete: false,
            name: "chan".into(),
            channel_ty: 1,
            default_permissions,
        }
        .to_custom_proposal()
        .unwrap(),
    )
}

fn apply(ext: &mut FireflyGroupExtensionWrapper, sender: &str, p: &Proposal) -> bool {
    check_group_proposal(p, ext, sender).is_ok()
}

#[test]
fn channel_role_zero_updates_only_that_channels_default() {
    let mut ext = group();
    // owner rewrites channel 7's default; the group-wide default must stay untouched
    assert!(apply(&mut ext, OWNER, &channel_role(7, 0, SEE | ADD)));
    assert_eq!(ext.default_permissions(), SEE | ADD);
    assert_eq!(ext.get_channel(7).unwrap().default_permissions, SEE | ADD);

    // even an owner-sized grant via role 0 never reaches the group-wide default
    assert!(apply(&mut ext, OWNER, &channel_role(7, 0, u32::MAX)));
    assert_eq!(ext.default_permissions(), SEE | ADD);
    assert!(ext.is_valid());
}

#[test]
fn channel_role_grants_are_bounded_by_senders_channel_permissions() {
    let mut ext = group();
    // mod holds root ManageChannel but only SEE | ADD | ROLE in channel 7
    assert!(!apply(&mut ext, MOD, &channel_role(7, 0, SEE | ADD | CHANNEL)));
    assert!(!apply(&mut ext, MOD, &channel_role(7, 0, MOD_PERMS)));
    assert_eq!(ext.get_channel(7).unwrap().default_permissions, SEE);
    // a subset of what mod holds in the channel is fine
    assert!(apply(&mut ext, MOD, &channel_role(7, 0, SEE | ADD)));
    // mod cannot rewrite the owner's role inside the channel
    assert!(!apply(&mut ext, MOD, &channel_role(7, 1, SEE)));
    // default members (no ManageRole anywhere) cannot touch channel roles
    assert!(!apply(&mut ext, "nobody", &channel_role(7, 0, SEE)));
    assert_eq!(ext.default_permissions(), SEE | ADD);
}

#[test]
fn channel_overrides_for_unknown_roles_are_rejected() {
    let mut ext = group();
    assert!(!apply(&mut ext, OWNER, &channel_role(7, 42, SEE)));
    assert!(ext.get_channel(7).unwrap().roles.iter().all(|r| r.id != 42));
    assert!(ext.is_valid());
}

#[test]
fn channel_defaults_cannot_mint_permissions_the_sender_lacks() {
    let mut ext = group();
    // creation: mod lacks ManageMember, so it cannot be a channel default
    let manage_member = UserPermission::ManageMember as u32;
    assert!(!apply(&mut ext, MOD, &channel(9, SEE | manage_member)));
    assert!(ext.get_channel(9).is_none());
    assert!(apply(&mut ext, MOD, &channel(9, SEE | ROLE)));
    // update of an existing channel through root ManageChannel is bounded the same way
    assert!(!apply(&mut ext, MOD, &channel(9, SEE | manage_member)));
    assert_eq!(ext.get_channel(9).unwrap().default_permissions, SEE | ROLE);
    // the owner is unrestricted
    assert!(apply(&mut ext, OWNER, &channel(9, u32::MAX)));
    assert_eq!(ext.default_permissions(), SEE | ADD);
}

#[test]
fn custom_proposals_are_checked_with_the_senders_permissions() {
    let mut ext = group();
    let p = channel(10, SEE);
    let Proposal::Custom(custom) = &p else { unreachable!() };
    assert!(handle_custom_proposal(custom, &mut ext, perms_of(&group(), "nobody"), "nobody").is_err());
    assert!(handle_custom_proposal(custom, &mut ext, perms_of(&group(), MOD), MOD).unwrap());
}

fn reinit() -> Proposal {
    // group_id = [], version = MLS 1.0, cipher suite 2, no extensions
    let bytes = [0u8, 0, 1, 0, 2, 0];
    Proposal::ReInit(ReInitProposal::mls_decode(&mut bytes.as_slice()).unwrap())
}

#[test]
fn reinit_is_always_rejected() {
    let mut ext = group();
    assert!(check_group_proposal(&reinit(), &mut ext, OWNER).is_err());
    assert!(check_group_proposal(&reinit(), &mut ext, MOD).is_err());
    assert!(check_group_proposal(&reinit(), &mut ext, "nobody").is_err());
}

fn gce(ext: Option<FireflyGroupExtension>) -> Proposal {
    let mut list = ExtensionList::new();
    if let Some(ext) = ext {
        list.set(ext.into_extension().unwrap());
    }
    Proposal::GroupContextExtensions(list)
}

#[test]
fn group_context_extensions_are_owner_only_and_validated() {
    let mut ext = group();
    let valid = || Some(FireflyGroupExtension::new(group()).unwrap());

    // ManageGroup alone is not enough to replace the whole rule set
    assert!(has_group(&perms_of(&ext, MOD)));
    assert!(check_group_proposal(&gce(valid()), &mut ext, MOD).is_err());
    assert!(check_group_proposal(&gce(valid()), &mut ext, "nobody").is_err());
    assert!(check_group_proposal(&gce(valid()), &mut ext, OWNER).is_ok());

    // the owner must keep a Firefly extension
    assert!(check_group_proposal(&gce(None), &mut ext, OWNER).is_err());

    // and it must be well formed (here: members out of order)
    let mut raw = group().inner().clone();
    raw.members.reverse();
    let broken = FireflyGroupExtensionWrapper::new(raw);
    assert!(!broken.is_valid());
    let broken = FireflyGroupExtension::new(broken).unwrap();
    assert!(check_group_proposal(&gce(Some(broken)), &mut ext, OWNER).is_err());
}

fn has_group(perms: &u32) -> bool {
    perms & GROUP == GROUP
}

#[test]
fn invalid_by_reference_proposals_are_dropped_only_when_sending() {
    let mut raw_ref = vec![32u8];
    raw_ref.extend_from_slice(&[0u8; 32]);
    let by_ref = ProposalSource::ByReference(ProposalRef::mls_decode(&mut raw_ref.as_slice()).unwrap());
    assert!(should_drop_invalid_proposal(CommitDirection::Send, &by_ref));
    assert!(!should_drop_invalid_proposal(CommitDirection::Receive, &by_ref));
    assert!(!should_drop_invalid_proposal(CommitDirection::Send, &ProposalSource::ByValue));
    assert!(!should_drop_invalid_proposal(CommitDirection::Receive, &ProposalSource::ByValue));
    assert!(!should_drop_invalid_proposal(CommitDirection::Send, &ProposalSource::Local));
}

/// A PrivateMessage frame; only its cleartext header is inspected.
fn private_message(epoch: u64, content_type: u8) -> MlsMessage {
    let mut bytes = vec![0, 1, 0, 2, 1, 0xAA];
    bytes.extend_from_slice(&epoch.to_be_bytes());
    bytes.extend_from_slice(&[content_type, 0, 0, 0]);
    MlsMessage::mls_decode(&mut bytes.as_slice()).unwrap()
}

#[test]
fn application_messages_from_other_epochs_are_rejected() {
    assert!(ensure_application_message_is_current(&private_message(5, 1), 5).is_ok());
    assert!(ensure_application_message_is_current(&private_message(4, 1), 5).is_err());
    assert!(ensure_application_message_is_current(&private_message(6, 1), 5).is_err());
    // handshake messages are left to mls-rs
    assert!(ensure_application_message_is_current(&private_message(4, 3), 5).is_ok());
    assert!(ensure_application_message_is_current(&private_message(4, 2), 5).is_ok());
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

struct NoToken;

#[async_trait::async_trait]
impl FireflyAuthTokenCallback for NoToken {
    async fn token(&self) -> anyhow::Result<String> {
        Ok(String::new())
    }
}

async fn key_package(username: &str, address_id: u64) -> MlsMessage {
    let cs = mls_rs_crypto_rustcrypto::RustCryptoProvider::default()
        .cipher_suite_provider(firefly_core::config::CIPHERSUITE)
        .unwrap();
    let (secret, public) = cs.signature_key_generate().await.unwrap();
    let token = AuthToken {
        username: username.into(),
        valid_until: u64::MAX,
        credential: public.to_vec().into(),
        address_id,
        device_id: 1,
    };
    let signed = SignedToken {
        kid: "kid".into(),
        payload: serialize_proto(&token).unwrap().to_vec().into(),
        signature: vec![0u8; 64].into(),
    };
    let identity = firefly_protos::firefly::FireflyIdentity {
        secret: secret.as_bytes().into(),
        public: public.as_bytes().into(),
        credential: serialize_proto(&signed).unwrap().to_vec().into(),
    };
    let identity = FireflyIdentity::from_vec(serialize_proto(&identity).unwrap().to_vec()).unwrap();
    let stores = Arc::new(Stores::default());
    let client = FireflyMlsClient::load(
        "http://127.0.0.1:1".into(),
        Arc::new(identity),
        stores.clone(),
        stores.clone(),
        stores,
        Arc::new(NoToken),
    )
    .unwrap();
    MlsMessage::from_bytes(&client.generate_key_package().await.unwrap()).unwrap()
}

#[tokio::test]
async fn substituted_key_packages_are_rejected() {
    let kp = key_package("bob", 11).await;
    assert!(ensure_key_package_belongs_to(&kp, "bob", 11).is_ok());
    assert!(ensure_key_package_belongs_to(&kp, "mallory", 11).is_err());
    assert!(ensure_key_package_belongs_to(&kp, "bob", 12).is_err());
}
