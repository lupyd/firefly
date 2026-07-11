use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use base64::{Engine, prelude::BASE64_URL_SAFE_NO_PAD};
use firefly_core::{
    FireflyAuthTokenCallback, FireflyIdentity, FireflyMlsClient, FireflyMlsGroup,
    config::{UpdateRoleInChannelProposal, UpdateRoleProposal, UpdateUserProposal, UserPermission},
    extension::FireflyGroupExtensionWrapper,
};
use firefly_protos::firefly::{FireflyGroupMember, FireflyGroupRole};
use mls_rs::identity::SigningIdentity;
use sqlx::SqlitePool;

use crate::{
    callbacks::FireflyWsClientCallback,
    db::{
        group_stores::{GroupInfoStore, GroupKeyPackageStore, GroupPskStore, GroupStateStore},
        keyvalue::KeyValueStore,
    },
    error,
};

#[derive(Debug)]
pub struct EncryptedGroupMessage {
    pub sender: String,
    pub message: Vec<u8>,
}

#[derive(Debug)]
pub enum FireflyMlsReceivedMessage {
    Message(EncryptedGroupMessage),
    Commit,
    Proposal,
    GroupInfo,
    Welcome,
    KeyPackage,
}

pub struct FfiMlsClient {
    client: FireflyMlsClient,
    base_url: Arc<str>,
    group_info_state: GroupInfoStore,

    // figure out a better way to sync groups
    loaded_groups: std::sync::Mutex<HashMap<u64, Arc<FfiMlsGroup>>>,
}

impl FfiMlsClient {
    pub fn credential(&self) {
        self.client.get_identity();
    }

    pub async fn initialize(
        device_id: u8,
        address_id: u64,
        callbacks: Arc<dyn FireflyWsClientCallback>,
        key_value_store: KeyValueStore,
        base_url: String,
        pool: SqlitePool,
    ) -> anyhow::Result<Self> {
        const GROUP_IDENTITY_KEY: &str = "group_identity_b64";
        let base_url: Arc<str> = base_url.into();

        let identity = if let Ok(identity_b64) = key_value_store.get(GROUP_IDENTITY_KEY).await {
            let identity = BASE64_URL_SAFE_NO_PAD.decode(&identity_b64)?;
            let identity = FireflyIdentity::from_vec(identity)?;

            // regenerate identity before it expires
            // a week before actual validity, these are long credentials
            let current_timestamp_seconds = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();

            if identity.is_valid_until_secs().unwrap_or_default() > current_timestamp_seconds + 5 {
                identity
            } else if let Some(token) = callbacks.get_access_token().await {
                let identity = FireflyIdentity::generate(
                    token.clone(),
                    base_url.clone(),
                    device_id,
                    address_id,
                )
                .await?;

                let serialized_identity = identity.to_vec()?;

                let serialized_base64_identity =
                    BASE64_URL_SAFE_NO_PAD.encode(&serialized_identity);

                key_value_store
                    .set(GROUP_IDENTITY_KEY, &serialized_base64_identity)
                    .await?;

                identity
            } else {
                identity
            }
        } else {
            log::info!("no group identity found in key_value_store, generating...");
            let token = callbacks
                .get_access_token()
                .await
                .context("token not found")?;

            let identity =
                FireflyIdentity::generate(token.clone(), base_url.clone(), device_id, address_id)
                    .await?;

            let serialized_identity = identity.to_vec()?;

            let serialized_base64_identity = BASE64_URL_SAFE_NO_PAD.encode(&serialized_identity);

            key_value_store
                .set(GROUP_IDENTITY_KEY, &serialized_base64_identity)
                .await?;

            identity
        };

        let client = FireflyMlsClient::load(
            base_url.to_string(),
            identity.into(),
            Arc::new(GroupKeyPackageStore::new(pool.clone()).await?),
            Arc::new(GroupStateStore::new(pool.clone()).await?),
            Arc::new(GroupPskStore {}),
            Arc::new(AuthCallback {
                callbacks: callbacks.clone(),
            }),
        )?;

        let group_info_state = GroupInfoStore::new(pool.clone()).await?;

        Ok(Self {
            client,
            base_url,
            group_info_state,
            loaded_groups: Default::default(),
        })
    }
}

struct AuthCallback {
    callbacks: Arc<dyn FireflyWsClientCallback>,
}

#[async_trait::async_trait]
impl FireflyAuthTokenCallback for AuthCallback {
    async fn token(&self) -> anyhow::Result<String> {
        self.callbacks
            .get_access_token()
            .await
            .context("token not found")
    }
}

pub struct FfiMlsGroup {
    group: FireflyMlsGroup,
    #[allow(unused)]
    base_url: Arc<str>,
}

impl FfiMlsClient {
    pub fn username(&self) -> Option<String> {
        self.client.username()
    }

    pub fn signing_identity(&self) -> SigningIdentity {
        self.client.get_identity().signing_identity()
    }

    pub async fn create_group(&self, group_name: String) -> anyhow::Result<Arc<FfiMlsGroup>> {
        let Some(username) = self.username() else {
            return Err(anyhow::anyhow!("user not authenticated"));
        };

        let mut ext = FireflyGroupExtensionWrapper::new(Default::default());

        ext.update_group(group_name.clone(), UserPermission::AddMessage as u32);

        ext.update_role(FireflyGroupRole {
            id: 1,
            name: "owner".into(),
            permissions: u32::MAX,
        });

        ext.update_member(FireflyGroupMember {
            username: username.into(),
            role: 1,
        });

        let group = self.client.create_group(ext.inner().clone()).await?;

        self.group_info_state
            .set(
                group.group_id(),
                group_name,
                String::new(),
                group.group_identifier().await?,
            )
            .await?;
        let group = FfiMlsGroup {
            group,
            base_url: self.base_url.clone(),
        };
        let group = Arc::new(group);
        {
            self.loaded_groups
                .lock()
                .unwrap()
                .insert(group.group_id(), group.clone());
        }

        Ok(group)
    }

    pub async fn generate_key_package(&self) -> anyhow::Result<Vec<u8>> {
        self.client.generate_key_package().await
    }

    pub async fn join_group(
        &self,
        group_id: u64,
        welcome_message: Vec<u8>,
    ) -> anyhow::Result<Arc<FfiMlsGroup>> {
        let group = self.client.join_group(group_id, welcome_message).await?;

        let group = Arc::new(FfiMlsGroup {
            group,
            base_url: self.base_url.clone(),
        });
        {
            self.loaded_groups
                .lock()
                .unwrap()
                .insert(group_id, group.clone());
        }

        Ok(group)
    }

    pub async fn load_group(
        &self,
        group_id: u64,
        group_identifier: Vec<u8>,
    ) -> anyhow::Result<Arc<FfiMlsGroup>> {
        if let Some(group) = self.loaded_groups.lock().unwrap().get(&group_id).cloned() {
            return Ok(group);
        }

        let group = self.client.load_group(group_id, group_identifier).await?;
        let group = Arc::new(FfiMlsGroup {
            group,
            base_url: self.base_url.clone(),
        });

        self.loaded_groups
            .lock()
            .unwrap()
            .insert(group_id, group.clone());

        Ok(group)
    }

    pub async fn load_all_groups(&self) -> anyhow::Result<Vec<Arc<FfiMlsGroup>>> {
        let group_infos = self.group_info_state.get_all().await?;
        let mut groups = Vec::new();

        for info in group_infos {
            let group = self.load_group(info.id, info.identifier).await?;
            groups.push(group);
        }

        Ok(groups)
    }

    pub async fn is_valid_until_secs(&self) -> anyhow::Result<u64> {
        self.client.is_valid_until_secs().await
    }
}

impl FfiMlsGroup {
    pub async fn state(&self) -> anyhow::Result<Vec<u8>> {
        self.group.state().await
    }

    pub async fn extension(&self) -> anyhow::Result<Vec<u8>> {
        self.group.extension().await
    }

    pub async fn save(&self) -> anyhow::Result<()> {
        self.group.save().await
    }

    pub async fn encrypt(&self, data: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        self.group.encrypt(&data).await
    }

    pub async fn re_add_member(&self, username: String, address: u64) -> anyhow::Result<u64> {
        self.group.re_add_member(username, address).await
    }

    pub async fn process(&self, message: Vec<u8>) -> anyhow::Result<FireflyMlsReceivedMessage> {
        let result = self.group.process(&message).await?;
        Ok(match result {
            firefly_core::FireflyMlsReceivedMessage::Message(msg) => {
                FireflyMlsReceivedMessage::Message(EncryptedGroupMessage {
                    sender: msg.sender,
                    message: msg.message,
                })
            }
            firefly_core::FireflyMlsReceivedMessage::Commit => {
                self.group.save().await?;
                FireflyMlsReceivedMessage::Commit
            }

            firefly_core::FireflyMlsReceivedMessage::Proposal => {
                FireflyMlsReceivedMessage::Proposal
            }
            firefly_core::FireflyMlsReceivedMessage::GroupInfo => {
                FireflyMlsReceivedMessage::GroupInfo
            }
            firefly_core::FireflyMlsReceivedMessage::Welcome => FireflyMlsReceivedMessage::Welcome,
            firefly_core::FireflyMlsReceivedMessage::KeyPackage => {
                FireflyMlsReceivedMessage::KeyPackage
            }
        })
    }

    pub async fn add_member(&self, username: String, role_id: u32) -> anyhow::Result<u64> {
        self.group.add_member(username, role_id).await
    }

    pub async fn kick_member(&self, username: String) -> anyhow::Result<u64> {
        self.group.kick_member(&username).await
    }

    pub async fn update_channel(
        &self,
        id: u32,
        delete: bool,
        name: String,
        channel_ty: u8,
        default_permissions: u32,
    ) -> anyhow::Result<u64> {
        self.group
            .update_channel(id, delete, name, channel_ty, default_permissions)
            .await
    }

    pub async fn update_roles(&self, roles: Vec<UpdateRoleProposalFfi>) -> anyhow::Result<u64> {
        self.group
            .update_roles(roles.into_iter().map(|role| UpdateRoleProposal {
                role_id: role.role_id,
                name: role.name,
                permissions: role.permissions,
                delete: role.delete,
            }))
            .await
    }

    pub async fn update_users(&self, users: Vec<UpdateUserProposalFfi>) -> anyhow::Result<u64> {
        self.group
            .update_users(users.into_iter().map(|user| UpdateUserProposal {
                username: user.username,
                role_id: user.role_id,
            }))
            .await
    }

    pub async fn update_roles_in_channel(
        &self,
        channel_id: u32,
        roles: Vec<UpdateRoleProposalFfi>,
    ) -> anyhow::Result<u64> {
        self.group
            .update_roles_in_channel(roles.into_iter().map(|x| UpdateRoleInChannelProposal {
                channel_id,
                role_proposal: UpdateRoleProposal {
                    name: x.name,
                    role_id: x.role_id,
                    permissions: x.permissions,
                    delete: x.delete,
                },
            }))
            .await
    }

    pub async fn group_identifier(&self) -> anyhow::Result<Vec<u8>> {
        self.group.group_identifier().await
    }

    pub fn group_id(&self) -> u64 {
        self.group.group_id()
    }

    pub async fn epoch(&self) -> u64 {
        self.group.epoch().await
    }

    pub async fn export_secret(
        &self,
        label: &str,
        context: &[u8],
        key_length: usize,
    ) -> anyhow::Result<Vec<u8>> {
        self.group.export_secret(label, context, key_length).await
    }
}

pub struct UpdateRoleProposalFfi {
    pub name: String,
    pub role_id: u32,
    pub permissions: u32,
    pub delete: bool,
}

pub struct UpdateUserProposalFfi {
    pub username: String,
    pub role_id: u32,
}
