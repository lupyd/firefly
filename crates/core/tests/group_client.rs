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

async fn setup_server() {
    dotenv::from_filename(".env.test").ok();
    let port = std::env::var("PORT")
        .map(|x| x.parse::<u16>().unwrap_or(39206))
        .unwrap_or(39206);
    tokio::spawn(async move {
        firefly_server::start_http_server(port).await.unwrap();
    });
    // Give some time for server to start
    tokio::time::sleep(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn group_flow() {
    setup_server().await;
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

    let mut wrapper = FireflyGroupExtensionWrapper::new(Default::default());
    wrapper.update_group("alice's group".into(), UserPermission::AddMessage as u32);
    wrapper.update_role(FireflyGroupRole {
        id: 1,
        name: "owner".into(),
        permissions: u32::MAX,
        color: Default::default(),
    });
    wrapper.update_member(FireflyGroupMember {
        username: "alice".into(),
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

    let (alice, alice_address) = new_test_user("alice", 1, None).await;
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

    add_member_server("alice", alice_address, &alice_group).await;

    // just waste some time for keys to expire, they'll expire in 8 seconds
    tokio::time::sleep(Duration::from_secs(5)).await;
    let (bob, bob_address) = new_test_user("bob", 1, None).await;
    let bob_identity = bob.get_identity().as_ref().clone();

    alice_group.add_member("bob".into(), 0).await.unwrap();

    let bob_group = {
        let response = HTTP_CLIENT
            .get(format!(
                "{}/group/invites?address={}",
                get_base_url(),
                bob_address
            ))
            .bearer_auth("bob")
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

    let (charles, charles_address) = new_test_user("charles", 1, None).await;
    let charles_identity = charles.get_identity().as_ref().clone();

    match bob_group.add_member("charles".into(), 0).await {
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
                username: "bob".into(),
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

    sync_groups(bob_address, "bob", &bob_group).await;

    log::info!("Bob's extension:");
    print_extension(&bob_group).await;

    // now this should fail, because role 1 is owner, has too many permissions, that bob can't give to charles
    match bob_group.add_member("charles".into(), 1).await {
        Ok(_) => panic!(
            "this should fail, because role 1 is owner, has too many permissions, that bob can't give to charles"
        ),
        Err(err) => log::info!("{}", err),
    }

    bob_group.add_member("charles".into(), 2).await.unwrap(); // can succeed giving same role

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
                username: "bob".into(),
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

    add_member_server("alice", alice_address, &alice_group).await;
    sync_groups(alice_address, "alice", &alice_group).await;

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

    add_member_server("bob", bob_address, &bob_group).await;
    add_member_server("alice", alice_address, &alice_group).await;

    sync_groups(bob_address, "bob", &bob_group).await;

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
            .bearer_auth("charles")
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

    // add_member_server("charles", charles_address, &charles_group).await;

    // charles has role 2 (manager), should not be able to add members
    match charles_group.add_member("dave".into(), 0).await {
        Ok(_) => {
            panic!("charles should not be able to add members without ManageMember permission")
        }
        Err(err) => log::error!("{:?}", err),
    };

    // charles cannot update roles
    match charles_group
        .update_roles(
            [UpdateRoleProposal {
                name: "test".into(),
                role_id: 5,
                permissions: 0,
                delete: false,
                color: Default::default(),
            }]
            .into_iter(),
        )
        .await
    {
        Ok(_) => panic!("charles should not be able to update roles without ManageRole permission"),
        Err(err) => log::error!("{:?}", err),
    };

    // charles cannot escalate his own permissions
    match charles_group
        .update_users(
            [UpdateUserProposal {
                username: "charles".into(),
                role_id: 1,
            }]
            .into_iter(),
        )
        .await
    {
        Ok(_) => panic!("charles should not be able to escalate to owner role"),
        Err(err) => log::error!("{:?}", err),
    };

    add_member_server("charles", charles_address, &charles_group).await;
    sync_groups(charles_address, "charles", &charles_group).await;
    // charles can update channels since he has ManageChannel
    charles_group
        .update_channel(3, false, "charles-channel".into(), 1, 0)
        .await
        .unwrap();

    add_member_server("alice", alice_address, &alice_group).await;
    sync_groups(alice_address, "alice", &alice_group).await;
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

    add_member_server("alice", alice_address, &alice_group).await;
    sync_groups(alice_address, "alice", &alice_group).await;
    add_member_server("bob", bob_address, &bob_group).await;
    sync_groups(bob_address, "bob", &bob_group).await;
    add_member_server("charles", charles_address, &charles_group).await;
    sync_groups(charles_address, "charles", &charles_group).await;

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

        let (bob, bob_address) = new_test_user("bob", 1, Some(bob_identity)).await;

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

    add_member_server("bob", bob_address, &bob_group).await;
    sync_groups(bob_address, "bob", &bob_group).await;

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
