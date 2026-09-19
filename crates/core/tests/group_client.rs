use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};

use firefly_core::{
    FireflyAuthTokenCallback, FireflyIdentity, FireflyMlsClient, FireflyMlsGroup,
    config::{FireflyCredential, UpdateRoleProposal, UpdateUserProposal, UserPermission},
    extension::FireflyGroupExtensionWrapper,
    storage_provider::{MlsGroupStateStorage, MlsKeyPackageStorage, MlsPreSharedKeyStorage},
    utils::HTTP_CLIENT,
};
use firefly_protos::{
    deserialize_proto,
    firefly::{
        Address, FireflyGroupChannel, FireflyGroupExtension, FireflyGroupMember, FireflyGroupRole,
        GroupInvites, GroupKeyPackage, GroupKeyPackages, GroupMemberUpdate, GroupMessages,
        ServerMessage,
    },
    serialize_proto,
};
use zeroize::Zeroizing;

#[derive(Default)]
struct InMemoryKeyPackageStore {
    inner: tokio::sync::Mutex<HashMap<Vec<u8>, Vec<u8>>>,
}

#[async_trait::async_trait]
impl MlsKeyPackageStorage for InMemoryKeyPackageStore {
    async fn insert(&self, id: Vec<u8>, key_package_data: Vec<u8>) -> bool {
        self.inner
            .lock()
            .await
            .insert(id, key_package_data)
            .is_none()
    }

    async fn delete(&self, id: Vec<u8>) -> bool {
        self.inner.lock().await.remove(&id).is_some()
    }

    async fn get(&self, id: Vec<u8>) -> Option<Vec<u8>> {
        self.inner.lock().await.get(&id).cloned()
    }
}

#[derive(Default)]
struct InMemoryPskStore {}

#[async_trait::async_trait]
impl MlsPreSharedKeyStorage for InMemoryPskStore {
    async fn get(&self, _id: Vec<u8>) -> Option<Vec<u8>> {
        None
    }
}

#[derive(Default)]
struct InMemoryGroupStateStorage {
    inner: tokio::sync::Mutex<
        HashMap<Vec<u8>, (Zeroizing<Vec<u8>>, BTreeMap<u64, Zeroizing<Vec<u8>>>)>,
    >,
}

#[async_trait::async_trait]
impl MlsGroupStateStorage for InMemoryGroupStateStorage {
    async fn state(&self, group_id: Vec<u8>) -> Option<Zeroizing<Vec<u8>>> {
        self.inner
            .lock()
            .await
            .get(&group_id)
            .map(|(state, _)| state.clone())
    }
    async fn epoch(&self, group_id: Vec<u8>, epoch_id: u64) -> Option<Zeroizing<Vec<u8>>> {
        self.inner
            .lock()
            .await
            .get(&group_id)?
            .1
            .get(&epoch_id)
            .cloned()
    }
    async fn write(
        &self,
        group_id: Vec<u8>,
        state_data: Zeroizing<Vec<u8>>,
        epoch_inserts: HashMap<u64, Zeroizing<Vec<u8>>>,
        epoch_updates: HashMap<u64, Zeroizing<Vec<u8>>>,
    ) -> bool {
        let mut inner = self.inner.lock().await;
        let (state, epochs) = inner
            .entry(group_id)
            .or_insert_with(|| (Zeroizing::new(Vec::new()), BTreeMap::new()));
        *state = state_data;
        for (epoch_id, epoch_data) in epoch_inserts {
            epochs.insert(epoch_id, epoch_data);
        }
        for (epoch_id, epoch_data) in epoch_updates {
            epochs.insert(epoch_id, epoch_data);
        }
        true
    }
    async fn max_epoch_id(&self, group_id: Vec<u8>) -> Option<u64> {
        self.inner
            .lock()
            .await
            .get(&group_id)?
            .1
            .keys()
            .max()
            .copied()
    }
}

struct TokenCallbacks {
    token: String,
}

#[async_trait::async_trait]
impl FireflyAuthTokenCallback for TokenCallbacks {
    async fn token(&self) -> anyhow::Result<String> {
        Ok(self.token.clone())
    }
}

fn get_base_url() -> String {
    std::env::var("FIREFLY_BASE_URL").unwrap_or_else(|_| "http://127.0.0.1:39206".to_string())
}
// just to get this test work without pain
lazy_static::lazy_static! {
    static ref store_map: std::sync::Mutex<HashMap<String, (
        Arc<InMemoryGroupStateStorage>,
        Arc<InMemoryKeyPackageStore>,
        Arc<InMemoryPskStore>,
    )>> = Default::default();
}

async fn new_test_user(
    username: &str,
    device_id: u8,
    identity: Option<FireflyIdentity>,
) -> (FireflyMlsClient, u64) {
    // with NO_TOKEN_VERIFICATION env var + EMULATOR_MODE env var set
    let token = username;
    let (identity, address) = if let Some(identity) = identity {
        let credential =
            FireflyCredential::from_signing_identity(&identity.signing_identity()).unwrap();
        let signed_token = credential.signed_token().unwrap();
        let token =
            deserialize_proto::<firefly_protos::firefly::AuthToken>(&signed_token.payload).unwrap();
        (identity, token.address_id)
    } else {
        let address = {
            let response = HTTP_CLIENT
                .post(format!("{}/user/device", get_base_url()))
                .bearer_auth(token)
                .body(
                    serialize_proto(&Address {
                        deviceId: device_id as u32,
                        ..Default::default()
                    })
                    .unwrap(),
                )
                .send()
                .await
                .unwrap();

            assert!(response.status().is_success());

            deserialize_proto::<Address>(&response.bytes().await.unwrap())
                .unwrap()
                .id
        };

        let identity =
            FireflyIdentity::generate(token.into(), get_base_url().into(), device_id, address)
                .await
                .unwrap();
        (identity, address)
    };

    let auth_token_callbacks = Arc::new(TokenCallbacks {
        token: token.to_string(),
    });

    let (gc, kp, psk) = store_map
        .lock()
        .unwrap()
        .entry(username.to_string())
        .or_insert(Default::default())
        .clone();

    let client = FireflyMlsClient::load(
        get_base_url().into(),
        Arc::new(identity),
        kp,
        gc,
        psk,
        auth_token_callbacks,
    )
    .unwrap();

    {
        let package = client.generate_key_package().await.unwrap();

        let response = HTTP_CLIENT
            .post(format!(
                "{}/group/keyPackages?address={}",
                get_base_url(),
                address
            ))
            .bearer_auth(token)
            .body(
                serialize_proto(&GroupKeyPackages {
                    packages: vec![GroupKeyPackage {
                        id: rand::random_range(0..30_000),
                        package: package.into(),
                        address,
                        ..Default::default()
                    }],
                })
                .unwrap(),
            )
            .send()
            .await
            .unwrap();

        assert!(response.status().is_success());
    }

    (client, address)
}

async fn setup_server() -> bool {
    dotenv::from_filename(".env.test").ok();
    dotenv::dotenv().ok();
    if std::env::var("FIREFLY_BASE_URL").is_err() {
        println!("Skipping server integration test: FIREFLY_BASE_URL is not set.");
        false
    } else {
        true
    }
}

#[tokio::test]
async fn group_flow() {
    if !setup_server().await {
        return;
    }
    env_logger::Builder::from_default_env()
        .format(|buf, record| {
            use std::io::Write;
            writeln!(
                buf,
                "[{} {}:{}] {}",
                record.level(),
                record.file().unwrap_or("?"),
                record.line().unwrap_or(0),
                record.args()
            )
        })
        .init();

    let test_run_id = rand::random::<u32>();
    let alice_name = format!("alice_gc_{}", test_run_id);
    let bob_name = format!("bob_gc_{}", test_run_id);
    let charles_name = format!("charles_gc_{}", test_run_id);
    let dave_name = format!("dave_gc_{}", test_run_id);

    let mut wrapper = FireflyGroupExtensionWrapper::new(Default::default());
    wrapper.update_group("alice's group".into(), firefly_core::config::DEFAULT_GROUP_PERMISSIONS);
    wrapper.update_role(FireflyGroupRole {
        id: 1,
        name: "owner".into(),
        permissions: u32::MAX,
        color: Default::default(),
    });
    wrapper.update_member(FireflyGroupMember {
        username: alice_name.clone().into(),
        role: 1,
    });
    wrapper.update_channel(FireflyGroupChannel {
        id: 1,
        name: "general".into(),
        type_pb: 1, // Type::Text
        roles: Default::default(),
        default_permissions: UserPermission::AddMessage as u32,
    });

    log::info!("{:#?}", wrapper.inner());

    let (alice, alice_address) = new_test_user(&alice_name, 1, None).await;
    let alice_identity = alice.get_identity().as_ref().clone();
    let alice_group = alice.create_group(wrapper.inner().clone()).await.unwrap();

    async fn add_member_server(username: &str, address: u64, group: &FireflyMlsGroup) {
        let response = HTTP_CLIENT
            .post(format!(
                "{}/group/member?address={}",
                get_base_url(),
                address
            ))
            .bearer_auth(username)
            .body(
                serialize_proto(&GroupMemberUpdate {
                    group_id: group.group_id(),
                    last_message_seen: 0,
                    last_epoch: group.epoch().await as u32,
                })
                .unwrap(),
            )
            .send()
            .await
            .unwrap();

        assert!(response.status().is_success());
    }

    add_member_server(&alice_name, alice_address, &alice_group).await;

    // just waste some time for keys to expire, they'll expire in 8 seconds
    tokio::time::sleep(Duration::from_secs(5)).await;
    let (bob, bob_address) = new_test_user(&bob_name, 1, None).await;
    let bob_identity = bob.get_identity().as_ref().clone();

    alice_group.add_member(bob_name.clone(), 0).await.unwrap();

    let bob_group = {
        let response = HTTP_CLIENT
            .get(format!(
                "{}/group/invites?address={}",
                get_base_url(),
                bob_address
            ))
            .bearer_auth(&bob_name)
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());

        let body = response.bytes().await.unwrap();
        let invites = deserialize_proto::<GroupInvites>(&body).unwrap();

        let invite = &invites.invites[0];

        bob.join_group(invite.groupId, invite.welcomeMessage.to_vec())
            .await
            .unwrap()
    };

    // We don't need this, since the server automatically adds members
    // add_member_server("bob", bob_address, &bob_group).await;
    let encrypted_by_bob = bob_group.encrypt("Hello".as_bytes()).await.unwrap();

    let message = alice_group.process(&encrypted_by_bob).await.unwrap();
    log::info!("{:#?}", message);

    async fn print_extension(group: &FireflyMlsGroup) {
        let ext = group.extension().await.unwrap();
        log::info!(
            "{:#?}",
            deserialize_proto::<FireflyGroupExtension>(&ext).unwrap()
        );
    }

    let (charles, charles_address) = new_test_user(&charles_name, 1, None).await;
    let charles_identity = charles.get_identity().as_ref().clone();

    match bob_group.add_member(charles_name.clone(), 0).await {
        Err(err) => log::info!("{:?}", err),
        Ok(_) => {
            panic!("this shouldn't succeed, because bob doesn't have the required permissions");
        }
    };

    alice_group
        .update_roles(
            [UpdateRoleProposal {
                name: "manager".into(),
                role_id: 2,
                permissions: (UserPermission::ManageMember as u32
                    | UserPermission::AddMessage as u32),
                delete: false,
                color: Default::default(),
            }]
            .into_iter(),
        )
        .await
        .unwrap();

    alice_group
        .update_users(
            [UpdateUserProposal {
                username: bob_name.clone(),
                role_id: 2,
            }]
            .into_iter(),
        )
        .await
        .unwrap();

    log::info!("Alice's extension:");
    print_extension(&alice_group).await;

    async fn sync_groups(address: u64, username: &str, group: &FireflyMlsGroup) {
        let response = HTTP_CLIENT
            .get(format!(
                "{}/group/syncCommits?address={}&limit=50",
                get_base_url(),
                address
            ))
            .bearer_auth(username)
            .send()
            .await
            .unwrap();

        assert!(response.status().is_success());

        let body = response.bytes().await.unwrap();
        let server_msg = deserialize_proto::<ServerMessage>(&body).unwrap();
        let commits = match server_msg.message {
            firefly_protos::firefly::mod_ServerMessage::OneOfmessage::groupMessages(m) => m,
            _ => panic!("Expected GroupMessages, got {:?}", server_msg.message),
        };

        log::info!("synchronising {} commits", commits.messages.len());

        for commit in commits.messages {
            let epoch = group.epoch().await;
            log::info!(
                "processing group, current epoch: {}, commit epoch: {}",
                epoch,
                commit.epoch
            );

            let msg = group.process(&commit.message).await.unwrap();
            log::info!("processed msg: {:#?}", msg);
        }
    }

    sync_groups(bob_address, &bob_name, &bob_group).await;

    log::info!("Bob's extension:");
    print_extension(&bob_group).await;

    // now this should fail, because role 1 is owner, has too many permissions, that bob can't give to charles
    match bob_group.add_member(charles_name.clone(), 1).await {
        Ok(_) => panic!(
            "this should fail, because role 1 is owner, has too many permissions, that bob can't give to charles"
        ),
        Err(err) => log::info!("{}", err),
    }

    bob_group.add_member(charles_name.clone(), 2).await.unwrap(); // can succeed giving same role

    match bob_group
        .update_channel(2, false, "manager-message-only".into(), 1, 0)
        .await
    {
        Ok(_) => panic!("this should fail, bob doesn't have ManageChannel Permission"),
        Err(err) => log::info!("{}", err),
    }

    match bob_group
        .update_users(
            [UpdateUserProposal {
                username: bob_name.clone(),
                role_id: 1,
            }]
            .into_iter(),
        )
        .await
    {
        Ok(_) => panic!("bob should not be able to escalate his own role with more permissions"),
        Err(err) => log::info!("{:?}", err),
    };

    match bob_group
        .update_roles(
            [UpdateRoleProposal {
                name: "higher-manager".into(),
                role_id: 2,
                permissions: 0,
                delete: false,
                color: Default::default(),
            }]
            .into_iter(),
        )
        .await
    {
        Ok(_) => panic!("bob should not be able to update any role without ManageRole Permission"),
        Err(err) => log::info!("{:?}", err),
    };

    add_member_server(&alice_name, alice_address, &alice_group).await;
    sync_groups(alice_address, &alice_name, &alice_group).await;

    alice_group
        .update_roles(
            [UpdateRoleProposal {
                name: "super-manager".into(),
                role_id: 2,
                permissions: (UserPermission::AddMessage as u32)
                    | (UserPermission::ManageChannel as u32)
                    | (UserPermission::ManageRole as u32),
                delete: false,
                color: Default::default(),
            }]
            .into_iter(),
        )
        .await
        .unwrap();

    add_member_server(&bob_name, bob_address, &bob_group).await;
    add_member_server(&alice_name, alice_address, &alice_group).await;

    sync_groups(bob_address, &bob_name, &bob_group).await;

    log::info!("bob extension: ");
    print_extension(&bob_group).await;

    // bob can now update channels
    bob_group
        .update_channel(2, false, "manager-channel".into(), 1, 0)
        .await
        .unwrap();

    // bob can update roles but only with permissions he has
    bob_group
        .update_roles(
            [UpdateRoleProposal {
                name: "junior-manager".into(),
                role_id: 3,
                permissions: UserPermission::AddMessage as u32,
                delete: false,
                color: Default::default(),
            }]
            .into_iter(),
        )
        .await
        .unwrap();

    // bob cannot create role with permissions he doesn't have
    match bob_group
        .update_roles(
            [UpdateRoleProposal {
                name: "admin".into(),
                role_id: 4,
                permissions: UserPermission::ManageMember as u32,
                delete: false,
                color: Default::default(),
            }]
            .into_iter(),
        )
        .await
    {
        Ok(_) => panic!("bob should not be able to create role with ManageMember permission"),
        Err(err) => log::error!("{:?}", err),
    };

    let charles_group = {
        let response = HTTP_CLIENT
            .get(format!(
                "{}/group/invites?address={}",
                get_base_url(),
                charles_address
            ))
            .bearer_auth(&charles_name)
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());

        let body = response.bytes().await.unwrap();
        let invites = deserialize_proto::<GroupInvites>(&body).unwrap();
        let invite = &invites.invites[0];

        charles
            .join_group(invite.groupId, invite.welcomeMessage.to_vec())
            .await
            .unwrap()
    };

    add_member_server(&charles_name, charles_address, &charles_group).await;
    sync_groups(charles_address, &charles_name, &charles_group).await;

    // charles has role 2 (manager), should not be able to add members
    match charles_group.add_member(dave_name.clone(), 0).await {
        Ok(_) => {
            panic!("charles should not be able to add members without ManageMember permission")
        }
        Err(err) => log::error!("{:?}", err),
    };

    // charles cannot kick active member alice (owner)
    match charles_group.kick_member(&alice_name).await {
        Ok(_) => {
            panic!("charles should not be able to kick owner alice")
        }
        Err(err) => log::info!("Expected error when charles kicks alice: {:?}", err),
    };

    // charles cannot kick active member bob
    match charles_group.kick_member(&bob_name).await {
        Ok(_) => {
            panic!("charles should not be able to kick active member bob without ManageMember")
        }
        Err(err) => log::info!("Expected error when charles kicks bob: {:?}", err),
    };

    // bob (has ManageMember) cannot kick owner alice (role 1)
    match bob_group.kick_member(&alice_name).await {
        Ok(_) => {
            panic!("bob should not be able to kick owner alice")
        }
        Err(err) => log::info!("Expected error when bob kicks alice: {:?}", err),
    };

    // charles cannot create role with permissions he doesn't have (ManageMember)
    match charles_group
        .update_roles(
            [UpdateRoleProposal {
                name: "test".into(),
                role_id: 5,
                permissions: UserPermission::ManageMember as u32,
                delete: false,
                color: Default::default(),
            }]
            .into_iter(),
        )
        .await
    {
        Ok(_) => panic!("charles should not be able to update roles with permissions he lacks"),
        Err(err) => log::error!("{:?}", err),
    };

    // charles cannot escalate his own permissions
    match charles_group
        .update_users(
            [UpdateUserProposal {
                username: charles_name.clone(),
                role_id: 1,
            }]
            .into_iter(),
        )
        .await
    {
        Ok(_) => panic!("charles should not be able to escalate to owner role"),
        Err(err) => log::error!("{:?}", err),
    };

    add_member_server(&charles_name, charles_address, &charles_group).await;
    sync_groups(charles_address, &charles_name, &charles_group).await;
    // charles can update channels since he has ManageChannel
    charles_group
        .update_channel(3, false, "charles-channel".into(), 1, 0)
        .await
        .unwrap();

    add_member_server(&alice_name, alice_address, &alice_group).await;
    sync_groups(alice_address, &alice_name, &alice_group).await;
    // alice removes ManageChannel from role 2
    alice_group
        .update_roles(
            [UpdateRoleProposal {
                name: "super-manager".into(),
                role_id: 2,
                permissions: (UserPermission::AddMessage as u32)
                    | (UserPermission::ManageRole as u32),
                delete: false,
                color: Default::default(),
            }]
            .into_iter(),
        )
        .await
        .unwrap();

    add_member_server(&alice_name, alice_address, &alice_group).await;
    sync_groups(alice_address, &alice_name, &alice_group).await;
    add_member_server(&bob_name, bob_address, &bob_group).await;
    sync_groups(bob_address, &bob_name, &bob_group).await;
    add_member_server(&charles_name, charles_address, &charles_group).await;
    sync_groups(charles_address, &charles_name, &charles_group).await;

    // now charles cannot update channels
    match charles_group
        .update_channel(4, false, "fail-channel".into(), 1, 0)
        .await
    {
        Ok(_) => panic!("charles should not be able to update channels after permission removed"),
        Err(err) => log::error!("{:?}", err),
    };

    // the old jwk would've expired but still in "retention" so you can update with new jwk
    tokio::time::sleep(Duration::from_secs(5)).await;

    let (bob, bob_address, bob_group) = {
        // let bob get a new identity

        let (bob, bob_address) = new_test_user(&bob_name, 1, Some(bob_identity)).await;

        bob_group.save().await.unwrap();

        let group = bob
            .load_group(
                bob_group.group_id(),
                bob_group.group_identifier().await.unwrap(),
            )
            .await
            .unwrap();

        (bob, bob_address, group)
    };

    add_member_server(&bob_name, bob_address, &bob_group).await;
    sync_groups(bob_address, &bob_name, &bob_group).await;

    // update to new key
    bob_group.update_leaf(&bob.get_identity()).await.unwrap();

    // bob cannot delete role 1 (owner) as he doesn't have those permissions
    match bob_group
        .update_roles(
            [UpdateRoleProposal {
                name: "".into(),
                role_id: 1,
                permissions: 0,
                delete: true,
                color: Default::default(),
            }]
            .into_iter(),
        )
        .await
    {
        Ok(_) => panic!("bob should not be able to delete owner role"),
        Err(err) => log::error!("{:?}", err),
    };

    // bob can delete role 3 that he created
    bob_group
        .update_roles(
            [UpdateRoleProposal {
                name: "".into(),
                role_id: 3,
                permissions: 0,
                delete: true,
                color: Default::default(),
            }]
            .into_iter(),
        )
        .await
        .unwrap();

    log::info!("All permission tests passed!");
}

/// An unchecked MLS peer represents an old/malicious sender and the relay never
/// participates in authorization. The receiving Firefly rules must reject it.
#[tokio::test]
async fn message_permissions_reject_unchecked_mls_peers() {
    use firefly_core::storage_provider::{
        FfiGroupStateStorage, FfiKeyPackageStorage, FfiPreSharedKeyStorage,
    };
    use firefly_core::{
        client::load_client, config::FireflyIdentityProvider,
        extension::FireflyGroupExtension as MlsExtension, rules::MessagePermissionDenied,
    };
    use firefly_protos::firefly::{GroupMessageInner, MessagePayload, mod_GroupMessageInner};
    use mls_rs::extension::MlsExtension as _;
    use mls_rs::{ExtensionList, MlsMessage};

    if !setup_server().await {
        return;
    }
    let run = rand::random::<u32>();
    let alice_name = format!("rules_a_{run}");
    let bob_name = format!("rules_b_{run}");
    let (alice, _) = new_test_user(&alice_name, 1, None).await;
    let (bob, _) = new_test_user(&bob_name, 1, None).await;
    let raw_client = |client: &FireflyMlsClient, name: &str| {
        let (gs, kp, psk) = store_map.lock().unwrap().get(name).unwrap().clone();
        load_client(
            client.get_identity().as_ref().clone(),
            FfiKeyPackageStorage::new(kp as Arc<dyn MlsKeyPackageStorage>),
            FfiGroupStateStorage::new(gs as Arc<dyn MlsGroupStateStorage>),
            FfiPreSharedKeyStorage::new(psk as Arc<dyn MlsPreSharedKeyStorage>),
            FireflyIdentityProvider::new(get_base_url().into()),
        )
        .unwrap()
    };
    let raw_a = raw_client(&alice, &alice_name);
    let raw_b = raw_client(&bob, &bob_name);
    let payload = |outer, nested| {
        serialize_proto(&GroupMessageInner {
            channelId: 0,
            message_type: outer,
            message: mod_GroupMessageInner::OneOfmessage::messagePayload(MessagePayload {
                text: "authenticated ciphertext".into(),
                message_type: nested,
                ..Default::default()
            }),
        })
        .unwrap()
        .to_vec()
    };
    let normal = payload(0, 0);
    let outer_pin = payload(1, 0);
    let nested_pin = payload(0, 1);

    for mask in 0..8 {
        let mut extension = FireflyGroupExtensionWrapper::new(FireflyGroupExtension {
            default_permissions: mask,
            ..Default::default()
        });
        extension.update_role(FireflyGroupRole {
            id: 1,
            name: "owner".into(),
            permissions: u32::MAX,
            ..Default::default()
        });
        extension.update_member(FireflyGroupMember {
            username: alice_name.clone().into(),
            role: 1,
        });
        let mut extensions = ExtensionList::new();
        extensions.set(
            MlsExtension::new(extension)
                .unwrap()
                .into_extension()
                .unwrap(),
        );
        let mut ga = raw_a
            .create_group(extensions, Default::default(), None)
            .await
            .unwrap();
        let kp = bob.generate_key_package().await.unwrap();
        let commit = ga
            .commit_builder()
            .add_member(MlsMessage::from_bytes(&kp).unwrap())
            .unwrap()
            .build()
            .await
            .unwrap();
        ga.apply_pending_commit().await.unwrap();
        let (mut gb, _) = raw_b
            .join_group(None, &commit.welcome_messages[0], None)
            .await
            .unwrap();
        let forged_pin = gb
            .encrypt_application_message(&nested_pin, Vec::new())
            .await
            .unwrap()
            .to_bytes()
            .unwrap();
        let forged_normal = gb
            .encrypt_application_message(&normal, Vec::new())
            .await
            .unwrap()
            .to_bytes()
            .unwrap();
        let from_owner = ga
            .encrypt_application_message(&normal, Vec::new())
            .await
            .unwrap()
            .to_bytes()
            .unwrap();
        let pin_from_owner = ga
            .encrypt_application_message(&outer_pin, Vec::new())
            .await
            .unwrap()
            .to_bytes()
            .unwrap();
        let group_a = FireflyMlsGroup::new(
            mask as u64,
            ga,
            get_base_url().into(),
            Arc::new(TokenCallbacks {
                token: alice_name.clone(),
            }),
        );
        let group_b = FireflyMlsGroup::new(
            mask as u64,
            gb,
            get_base_url().into(),
            Arc::new(TokenCallbacks {
                token: bob_name.clone(),
            }),
        );
        let can_send = mask & 5 == 5;
        let can_pin = mask & 7 == 7;
        let can_read = mask & 1 != 0;
        assert_eq!(
            group_b.encrypt(&normal).await.is_ok(),
            can_send,
            "send mask={mask}"
        );
        assert_eq!(
            group_b.encrypt(&outer_pin).await.is_ok(),
            can_pin,
            "outer pin mask={mask}"
        );
        assert_eq!(
            group_b.encrypt(&nested_pin).await.is_ok(),
            can_pin,
            "nested pin mask={mask}"
        );
        let received = group_a.process(&forged_pin).await;
        assert_eq!(received.is_ok(), can_pin, "unchecked pin mask={mask}");
        if let Err(err) = received {
            assert!(err.downcast_ref::<MessagePermissionDenied>().is_some());
        }
        assert_eq!(
            group_a.process(&forged_normal).await.is_ok(),
            can_send,
            "unchecked send mask={mask}"
        );
        assert_eq!(
            group_b.process(&from_owner).await.is_ok(),
            can_read,
            "receive mask={mask}"
        );
        assert_eq!(
            group_b.process(&pin_from_owner).await.is_ok(),
            can_read,
            "receive pin needs SeeMessage, not PinMessage mask={mask}"
        );
        assert_eq!(group_b.can_see_message(0).await.unwrap(), can_read);
        assert!(!group_b.can_see_message(999).await.unwrap());
    }
}
