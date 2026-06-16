use std::collections::HashMap;

use firefly_core::storage_provider::MlsGroupStateStorage;
use firefly_core::storage_provider::MlsKeyPackageStorage;
use firefly_core::storage_provider::MlsPreSharedKeyStorage;
use sqlx::Executor;
use sqlx::SqlitePool;
use sqlx::prelude::*;
use zeroize::Zeroizing;

pub struct GroupStateStore {
    pool: SqlitePool,
}

impl GroupStateStore {
    pub async fn new(pool: SqlitePool) -> anyhow::Result<Self> {
        pool.execute(
            r#"
        CREATE TABLE IF NOT EXISTS group_states (
            id BLOB PRIMARY KEY,
            state BLOB NOT NULL
        )"#,
        )
        .await?;

        pool.execute(
            r#"
        CREATE TABLE IF NOT EXISTS group_epoch_states (
            id BLOB NOT NULL,
            epoch INTEGER NOT NULL,
            state BLOB NOT NULL,
            PRIMARY KEY (id, epoch),
            FOREIGN KEY (id) REFERENCES group_states(id) ON DELETE CASCADE
        )"#,
        )
        .await?;

        Ok(Self { pool })
    }

    pub async fn get_state(&self, id: &[u8]) -> anyhow::Result<Vec<u8>> {
        log::info!("store select: group_state id={:?}", id);
        let row = sqlx::query("SELECT state FROM group_states WHERE id = ?")
            .bind(id)
            .fetch_one(&self.pool)
            .await?;
        let state = row.try_get(0)?;
        Ok(state)
    }

    pub async fn set_state(
        &self,
        group_id: &[u8],
        state_data: &[u8],
        epoch_inserts: HashMap<u64, Vec<u8>>,
        epoch_updates: HashMap<u64, Vec<u8>>,
    ) -> anyhow::Result<()> {
        log::info!("store insert: group_state id={:?}", group_id);
        let mut tx = self.pool.begin().await?;

        sqlx::query("INSERT OR REPLACE INTO group_states (id, state) VALUES (?, ?)")
            .bind(&group_id)
            .bind(&state_data)
            .execute(&mut *tx)
            .await?;

        for (epoch_id, state) in epoch_inserts {
            log::info!(
                "store insert: group_epoch_state id={:?} epoch={}",
                group_id,
                epoch_id
            );
            sqlx::query(
                "INSERT OR REPLACE INTO group_epoch_states (id, epoch, state) VALUES (?, ?, ?)",
            )
            .bind(&group_id)
            .bind(epoch_id as i64)
            .bind(state)
            .execute(&mut *tx)
            .await?;
        }
        for (epoch_id, state) in epoch_updates {
            log::info!(
                "store update: group_epoch_state id={:?} epoch={}",
                group_id,
                epoch_id
            );
            sqlx::query(
                "INSERT OR REPLACE INTO group_epoch_states (id, epoch, state) VALUES (?, ?, ?)",
            )
            .bind(&group_id)
            .bind(epoch_id as i64)
            .bind(state)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;

        Ok(())
    }

    pub async fn get_epoch_state(&self, id: &[u8], epoch_id: u64) -> anyhow::Result<Vec<u8>> {
        log::info!(
            "store select: group_epoch_state id={:?} epoch={}",
            id,
            epoch_id
        );
        let row = sqlx::query("SELECT state FROM group_epoch_states WHERE id = ? AND epoch = ?")
            .bind(id)
            .bind(epoch_id as i64)
            .fetch_one(&self.pool)
            .await?;

        Ok(row.try_get(0)?)
    }

    pub async fn get_max_epoch_id(&self, group_id: &[u8]) -> anyhow::Result<Option<u64>> {
        log::info!("store select: group_max_epoch id={:?}", group_id);
        let row = sqlx::query("SELECT MAX(epoch) FROM group_epoch_states WHERE id = ?")
            .bind(&group_id)
            .fetch_one(&self.pool)
            .await?;

        let id: Option<i64> = row.try_get(0)?;

        Ok(id.map(|val| val as u64))
    }
}

#[async_trait::async_trait]
impl MlsGroupStateStorage for GroupStateStore {
    async fn state(&self, group_id: Vec<u8>) -> Option<Zeroizing<Vec<u8>>> {
        self.get_state(&group_id).await.ok().map(Zeroizing::new)
    }
    async fn epoch(&self, group_id: Vec<u8>, epoch_id: u64) -> Option<Zeroizing<Vec<u8>>> {
        self.get_epoch_state(&group_id, epoch_id)
            .await
            .ok()
            .map(Zeroizing::new)
    }
    async fn write(
        &self,
        group_id: Vec<u8>,
        state_data: Zeroizing<Vec<u8>>,
        epoch_inserts: HashMap<u64, Zeroizing<Vec<u8>>>,
        epoch_updates: HashMap<u64, Zeroizing<Vec<u8>>>,
    ) -> bool {
        let inserts = epoch_inserts
            .into_iter()
            .map(|(id, data)| (id, (*data).clone()))
            .collect();
        let updates = epoch_updates
            .into_iter()
            .map(|(id, data)| (id, (*data).clone()))
            .collect();

        self.set_state(&group_id, &state_data, inserts, updates)
            .await
            .is_ok()
    }
    async fn max_epoch_id(&self, group_id: Vec<u8>) -> Option<u64> {
        self.get_max_epoch_id(&group_id).await.ok().flatten()
    }
}

pub struct GroupPskStore {}

#[async_trait::async_trait]
impl MlsPreSharedKeyStorage for GroupPskStore {
    async fn get(&self, _id: Vec<u8>) -> Option<Vec<u8>> {
        None
    }
}

pub struct GroupKeyPackageStore {
    pool: SqlitePool,
}

impl GroupKeyPackageStore {
    pub async fn new(pool: SqlitePool) -> anyhow::Result<Self> {
        pool.execute(
            r#"
            CREATE TABLE IF NOT EXISTS group_key_packages(
                id BLOB PRIMARY KEY NOT NULL,
                key_package BLOB NOT NULL
            )
            "#,
        )
        .await?;

        Ok(Self { pool })
    }
}

#[async_trait::async_trait]
impl MlsKeyPackageStorage for GroupKeyPackageStore {
    async fn insert(&self, id: Vec<u8>, key_package_data: Vec<u8>) -> bool {
        log::info!("store insert: group_key_package id={:?}", id);
        sqlx::query(
            r#"
                INSERT INTO group_key_packages (id, key_package)
                VALUES (?, ?)
                "#,
        )
        .bind(id)
        .bind(key_package_data)
        .execute(&self.pool)
        .await
        .is_ok()
    }

    async fn delete(&self, id: Vec<u8>) -> bool {
        log::info!("store delete: group_key_package id={:?}", id);
        sqlx::query(
            r#"
                DELETE FROM group_key_packages
                WHERE id = ?
                "#,
        )
        .bind(id)
        .execute(&self.pool)
        .await
        .is_ok()
    }

    async fn get(&self, id: Vec<u8>) -> Option<Vec<u8>> {
        log::info!("store select: group_key_package id={:?}", id);
        let row = sqlx::query(
            r#"
                SELECT key_package FROM group_key_packages
                WHERE id = ?
                "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .ok()??;

        let key_package: Vec<u8> = row.try_get(0).ok()?;

        Some(key_package)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use firefly_core::{
        FireflyAuthTokenCallback, FireflyIdentity, FireflyMlsClient, FireflyMlsGroup,
        config::{FireflyCredential, UpdateRoleProposal, UpdateUserProposal, UserPermission},
        extension::FireflyGroupExtensionWrapper,
        utils::HTTP_CLIENT,
    };
    use firefly_protos::{
        deserialize_proto,
        firefly::{
            Address, FireflyGroupChannel, FireflyGroupExtension, FireflyGroupMember,
            FireflyGroupRole, GroupInvites, GroupKeyPackage, GroupKeyPackages, GroupMemberUpdate,
            GroupMessages, ServerMessage,
        },
        serialize_proto,
    };
    use sqlx::{SqlitePool, sqlite::SqlitePoolOptions};
    use std::{collections::HashMap, sync::Arc, time::Duration};

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
        std::env::var("FIREFLY_BASE_URL").unwrap_or_else(|_| "http://localhost:39205".to_string())
    }

    lazy_static::lazy_static! {
        static ref STORE_MAP: std::sync::Mutex<HashMap<String, SqlitePool>> = Default::default();
    }

    async fn new_test_user(
        username: &str,
        device_id: u8,
        identity: Option<FireflyIdentity>,
    ) -> (FireflyMlsClient, u64) {
        let token = username;
        let (identity, address) = if let Some(identity) = identity {
            let credential =
                FireflyCredential::from_signing_identity(&identity.signing_identity()).unwrap();
            let signed_token = credential.signed_token().unwrap();
            let token =
                deserialize_proto::<firefly_protos::firefly::AuthToken>(&signed_token.payload)
                    .unwrap();
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

        let pool = STORE_MAP
            .lock()
            .unwrap()
            .entry(username.to_string())
            .or_insert_with(|| {
                SqlitePoolOptions::new()
                    .max_connections(1)
                    .connect_lazy(":memory:")
                    .unwrap()
            })
            .clone();

        let gc = Arc::new(GroupStateStore::new(pool.clone()).await.unwrap());
        let kp = Arc::new(GroupKeyPackageStore::new(pool.clone()).await.unwrap());
        let psk = Arc::new(GroupPskStore {});

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
        });
        wrapper.update_member(FireflyGroupMember {
            username: "alice".into(),
            role: 1,
        });
        wrapper.update_channel(FireflyGroupChannel {
            id: 1,
            name: "general".into(),
            type_pb: 1,
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

        match bob_group.add_member("charles".into(), 1).await {
            Ok(_) => panic!(
                "this should fail, because role 1 is owner, has too many permissions, that bob can't give to charles"
            ),
            Err(err) => log::info!("{}", err),
        }

        bob_group.add_member("charles".into(), 2).await.unwrap();

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
            Ok(_) => {
                panic!("bob should not be able to escalate his own role with more permissions")
            }
            Err(err) => log::info!("{:?}", err),
        };

        match bob_group
            .update_roles(
                [UpdateRoleProposal {
                    name: "higher-manager".into(),
                    role_id: 2,
                    permissions: 0,
                    delete: false,
                }]
                .into_iter(),
            )
            .await
        {
            Ok(_) => {
                panic!("bob should not be able to update any role without ManageRole Permission")
            }
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

        bob_group
            .update_channel(2, false, "manager-channel".into(), 1, 0)
            .await
            .unwrap();

        bob_group
            .update_roles(
                [UpdateRoleProposal {
                    name: "junior-manager".into(),
                    role_id: 3,
                    permissions: UserPermission::AddMessage as u32,
                    delete: false,
                }]
                .into_iter(),
            )
            .await
            .unwrap();

        match bob_group
            .update_roles(
                [UpdateRoleProposal {
                    name: "admin".into(),
                    role_id: 4,
                    permissions: UserPermission::ManageMember as u32,
                    delete: false,
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

        match charles_group.add_member("dave".into(), 0).await {
            Ok(_) => {
                panic!("charles should not be able to add members without ManageMember permission")
            }
            Err(err) => log::error!("{:?}", err),
        };

        match charles_group
            .update_roles(
                [UpdateRoleProposal {
                    name: "test".into(),
                    role_id: 5,
                    permissions: 0,
                    delete: false,
                }]
                .into_iter(),
            )
            .await
        {
            Ok(_) => {
                panic!("charles should not be able to update roles without ManageRole permission")
            }
            Err(err) => log::error!("{:?}", err),
        };

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

        charles_group
            .update_channel(3, false, "charles-channel".into(), 1, 0)
            .await
            .unwrap();

        add_member_server("alice", alice_address, &alice_group).await;
        sync_groups(alice_address, "alice", &alice_group).await;

        alice_group
            .update_roles(
                [UpdateRoleProposal {
                    name: "super-manager".into(),
                    role_id: 2,
                    permissions: (UserPermission::AddMessage as u32)
                        | (UserPermission::ManageRole as u32),
                    delete: false,
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

        match charles_group
            .update_channel(4, false, "fail-channel".into(), 1, 0)
            .await
        {
            Ok(_) => {
                panic!("charles should not be able to update channels after permission removed")
            }
            Err(err) => log::error!("{:?}", err),
        };

        tokio::time::sleep(Duration::from_secs(5)).await;

        let (bob, bob_address, bob_group) = {
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

        bob_group.update_leaf(&bob.get_identity()).await.unwrap();

        match bob_group
            .update_roles(
                [UpdateRoleProposal {
                    name: "".into(),
                    role_id: 1,
                    permissions: 0,
                    delete: true,
                }]
                .into_iter(),
            )
            .await
        {
            Ok(_) => panic!("bob should not be able to delete owner role"),
            Err(err) => log::error!("{:?}", err),
        };

        bob_group
            .update_roles(
                [UpdateRoleProposal {
                    name: "".into(),
                    role_id: 3,
                    permissions: 0,
                    delete: true,
                }]
                .into_iter(),
            )
            .await
            .unwrap();

        log::info!("All permission tests passed!");
    }
}
