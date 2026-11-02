use std::{
    collections::HashMap,
    sync::Arc,
};

use anyhow::Context;
use base64::{Engine, prelude::BASE64_URL_SAFE_NO_PAD};
use firefly_core::{
    FireflyAuthTokenCallback, FireflyIdentity, FireflyMlsClient, FireflyMlsGroup,
    config::{UpdateRoleInChannelProposal, UpdateRoleProposal, UpdateUserProposal},
    extension::FireflyGroupExtensionWrapper,
};
use firefly_protos::firefly::{FireflyGroupMember, FireflyGroupRole};
use mls_rs::identity::SigningIdentity;

use firefly_core::storage_provider::{
    MlsGroupStateStorage, MlsKeyPackageStorage, MlsPreSharedKeyStorage,
};
use crate::storage::{
    GroupDepartureStorage, GroupInfoStorage, KeyValueGroupDepartureStore, KeyValueStorage,
};

use crate::sql::SqlitePool;

use crate::db::{
    group_stores::{
        GroupDepartureStore, GroupInfoStore, GroupKeyPackageStore, GroupPskStore, GroupStateStore,
    },
    keyvalue::KeyValueStore,
};

use crate::callbacks::FireflyWsClientCallback;

/// Where the device's group identity (signing key and credential) is kept.
pub const GROUP_IDENTITY_KEY: &str = "group_identity_b64";
/// When the current credential was issued to this device (unix seconds), for renewal.
pub const GROUP_IDENTITY_OBTAINED_AT_KEY: &str = "group_identity_obtained_at";

/// Whether a group credential valid until `valid_until` should be renewed now: once three
/// quarters of its life have passed, so a device that is only online now and then still renews
/// long before it expires. For credentials issued before renewal existed (`obtained_at`
/// unknown), in its last 30 days.
pub fn credential_due(valid_until: u64, obtained_at: Option<u64>, now: u64) -> bool {
    let margin = match obtained_at {
        Some(obtained) if obtained < valid_until => (valid_until - obtained) / 4,
        _ => 30 * 24 * 60 * 60,
    };
    now.saturating_add(margin) >= valid_until
}

/// Builds the MLS client around an identity, over this device's stores.
type BuildClient = Box<dyn Fn(Arc<FireflyIdentity>) -> anyhow::Result<FireflyMlsClient> + Send + Sync>;

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
    /// Replaced when the group credential is renewed (renew_credential). Groups keep their own
    /// signer in their state, so this only decides new key packages and new groups.
    client: std::sync::RwLock<Arc<FireflyMlsClient>>,
    build_client: BuildClient,
    base_url: Arc<str>,
    group_info_state: Arc<dyn GroupInfoStorage>,
    departure_store: Arc<dyn GroupDepartureStorage>,

    // figure out a better way to sync groups
    loaded_groups: std::sync::Mutex<HashMap<u64, Arc<FfiMlsGroup>>>,
}

impl FfiMlsClient {
    pub fn credential(&self) {
        self.inner().get_identity();
    }

    fn inner(&self) -> Arc<FireflyMlsClient> {
        self.client.read().unwrap().clone()
    }

    /// This device's group identity (signing key and credential).
    pub fn identity(&self) -> Arc<FireflyIdentity> {
        self.inner().get_identity()
    }

    /// Gets a new credential for the same signing key and uses it from now on (new key packages,
    /// new groups). Groups the device is already in take it with a commit (update_leaf).
    pub async fn renew_credential(
        &self,
        token: String,
        device_id: u8,
        address_id: u64,
    ) -> anyhow::Result<Arc<FireflyIdentity>> {
        let renewed = Arc::new(
            self.identity()
                .refresh(token, self.base_url.clone(), device_id, address_id)
                .await?,
        );
        let client = (self.build_client)(renewed.clone())?;
        *self.client.write().unwrap() = Arc::new(client);
        Ok(renewed)
    }

    pub async fn initialize(
        device_id: u8,
        address_id: u64,
        callbacks: Arc<dyn FireflyWsClientCallback>,
        key_value_store: KeyValueStore,
        base_url: String,
        pool: SqlitePool,
    ) -> anyhow::Result<Self> {
        let base_url: Arc<str> = base_url.into();

        let identity = if let Ok(identity_b64) = key_value_store.get(GROUP_IDENTITY_KEY).await {
            let identity = BASE64_URL_SAFE_NO_PAD.decode(&identity_b64)?;
            let identity = FireflyIdentity::from_vec(identity)?;

            let current_timestamp_seconds = firefly_core::utils::get_current_timestamp_in_secs();

            if identity.is_valid_until_secs().unwrap_or_default() > current_timestamp_seconds + 5 {
                identity
            } else if let Some(token) = callbacks.get_access_token().await {
                let identity = identity
                    .refresh(
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
                key_value_store
                    .set(
                        GROUP_IDENTITY_OBTAINED_AT_KEY,
                        &firefly_core::utils::get_current_timestamp_in_secs().to_string(),
                    )
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
            key_value_store
                .set(
                    GROUP_IDENTITY_OBTAINED_AT_KEY,
                    &firefly_core::utils::get_current_timestamp_in_secs().to_string(),
                )
                .await?;

            identity
        };

        let key_packages: Arc<dyn MlsKeyPackageStorage> = Arc::new(GroupKeyPackageStore::new(pool.clone()).await?);
        let group_states: Arc<dyn MlsGroupStateStorage> = Arc::new(GroupStateStore::new(pool.clone()).await?);
        let auth: Arc<dyn FireflyAuthTokenCallback> = Arc::new(AuthCallback {
            callbacks: callbacks.clone(),
        });
        let load_base_url = base_url.to_string();
        let build_client: BuildClient = Box::new(move |identity| {
            FireflyMlsClient::load(
                load_base_url.clone(),
                identity,
                key_packages.clone(),
                group_states.clone(),
                Arc::new(GroupPskStore {}),
                auth.clone(),
            )
        });
        let client = build_client(identity.into())?;

        let group_info_state: Arc<dyn GroupInfoStorage> =
            Arc::new(GroupInfoStore::new(pool.clone()).await?);
        let departure_store: Arc<dyn GroupDepartureStorage> =
            Arc::new(GroupDepartureStore::new(pool.clone()).await?);

        Ok(Self {
            client: std::sync::RwLock::new(Arc::new(client)),
            build_client,
            base_url,
            group_info_state,
            departure_store,
            loaded_groups: Default::default(),
        })
    }


    pub async fn initialize_with_stores(
        device_id: u8,
        address_id: u64,
        callbacks: Arc<dyn FireflyWsClientCallback>,
        key_value_store: Arc<dyn KeyValueStorage>,
        key_package_storage: Arc<dyn MlsKeyPackageStorage>,
        group_state_storage: Arc<dyn MlsGroupStateStorage>,
        psk_storage: Arc<dyn MlsPreSharedKeyStorage>,
        group_info_state: Arc<dyn GroupInfoStorage>,
        base_url: String,
    ) -> anyhow::Result<Self> {
        let base_url: Arc<str> = base_url.into();

        let identity = if let Ok(identity_b64) = key_value_store.get(GROUP_IDENTITY_KEY).await {
            let identity = BASE64_URL_SAFE_NO_PAD.decode(&identity_b64)?;
            let identity = FireflyIdentity::from_vec(identity)?;

            let current_timestamp_seconds = firefly_core::utils::get_current_timestamp_in_secs();

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
                key_value_store
                    .set(
                        GROUP_IDENTITY_OBTAINED_AT_KEY,
                        &firefly_core::utils::get_current_timestamp_in_secs().to_string(),
                    )
                    .await?;

                identity
            } else {
                identity
            }
        } else {
            log::info!("no group identity found in storage, generating...");
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
            key_value_store
                .set(
                    GROUP_IDENTITY_OBTAINED_AT_KEY,
                    &firefly_core::utils::get_current_timestamp_in_secs().to_string(),
                )
                .await?;

            identity
        };

        let departure_store: Arc<dyn GroupDepartureStorage> =
            Arc::new(KeyValueGroupDepartureStore::new(key_value_store.clone()));

        let auth: Arc<dyn FireflyAuthTokenCallback> = Arc::new(AuthCallback {
            callbacks: callbacks.clone(),
        });
        let load_base_url = base_url.to_string();
        let build_client: BuildClient = Box::new(move |identity| {
            FireflyMlsClient::load(
                load_base_url.clone(),
                identity,
                key_package_storage.clone(),
                group_state_storage.clone(),
                psk_storage.clone(),
                auth.clone(),
            )
        });
        let client = build_client(identity.into())?;

        Ok(Self {
            client: std::sync::RwLock::new(Arc::new(client)),
            build_client,
            base_url,
            group_info_state,
            departure_store,
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
    departure_store: Arc<dyn GroupDepartureStorage>,
}

impl FfiMlsClient {
    pub fn username(&self) -> Option<String> {
        self.inner().username()
    }

    pub fn signing_identity(&self) -> SigningIdentity {
        self.inner().get_identity().signing_identity()
    }

    pub async fn create_group(&self, group_name: String) -> anyhow::Result<Arc<FfiMlsGroup>> {
        self.create_group_with_settings(group_name,String::new(),0).await
    }

    pub async fn create_group_with_settings(&self, group_name:String, description:String, settings:u32)->anyhow::Result<Arc<FfiMlsGroup>> {
        let Some(username) = self.username() else {
            return Err(anyhow::anyhow!("user not authenticated"));
        };

        let mut ext = FireflyGroupExtensionWrapper::new(Default::default());

        ext.update_group(group_name.clone(), firefly_core::config::DEFAULT_GROUP_PERMISSIONS);

        ext.update_role(FireflyGroupRole {
            id: 1,
            name: "owner".into(),
            permissions: u32::MAX,
            color: Default::default(),
        });

        ext.update_member(FireflyGroupMember {
            username: username.into(),
            role: 1,
        });

        let group = self.inner().create_group_with_settings(ext.inner().clone(),description.clone(),settings).await?;

        self.group_info_state
            .set(
                group.group_id(),
                group_name,
                description,
                group.group_identifier().await?,
            )
            .await?;
        let group = FfiMlsGroup {
            group,
            base_url: self.base_url.clone(),
            departure_store: self.departure_store.clone(),
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
        self.inner().generate_key_package().await
    }

    pub async fn join_group(
        &self,
        group_id: u64,
        welcome_message: Vec<u8>,
    ) -> anyhow::Result<Arc<FfiMlsGroup>> {
        let group = self.inner().join_group(group_id, welcome_message).await?;

        let group = Arc::new(FfiMlsGroup {
            group,
            base_url: self.base_url.clone(),
            departure_store: self.departure_store.clone(),
        });
        {
            self.loaded_groups
                .lock()
                .unwrap()
                .insert(group_id, group.clone());
        }

        Ok(group)
    }

    /// The group currently cached for `group_id`, if any.
    pub fn loaded_group(&self, group_id: u64) -> Option<Arc<FfiMlsGroup>> {
        self.loaded_groups.lock().unwrap().get(&group_id).cloned()
    }

    /// Restores (or clears) the cached group for `group_id`, e.g. after refusing a Welcome.
    pub fn restore_loaded_group(&self, group_id: u64, group: Option<Arc<FfiMlsGroup>>) {
        let mut loaded = self.loaded_groups.lock().unwrap();
        match group {
            Some(group) => loaded.insert(group_id, group),
            None => loaded.remove(&group_id),
        };
    }

    pub async fn load_group(
        &self,
        group_id: u64,
        group_identifier: Vec<u8>,
    ) -> anyhow::Result<Arc<FfiMlsGroup>> {
        if let Some(group) = self.loaded_groups.lock().unwrap().get(&group_id).cloned() {
            return Ok(group);
        }

        let group = self.inner().load_group(group_id, group_identifier).await?;
        let group = Arc::new(FfiMlsGroup {
            group,
            base_url: self.base_url.clone(),
            departure_store: self.departure_store.clone(),
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
        self.inner().is_valid_until_secs().await
    }
}

impl FfiMlsGroup {
    /// When the credential in this device's own leaf of the group expires (unix seconds).
    pub async fn own_credential_valid_until(&self) -> anyhow::Result<u64> {
        self.group.own_credential_valid_until().await
    }

    /// Replaces this device's leaf with `identity` (same member, a renewed credential).
    pub async fn update_leaf(&self, identity: &FireflyIdentity) -> anyhow::Result<u64> {
        self.group.update_leaf(identity).await
    }

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

    pub async fn can_see_message(&self, channel_id: u32) -> anyhow::Result<bool> {
        self.group.can_see_message(channel_id).await
    }

    pub async fn has_full_channel_access(&self) -> anyhow::Result<bool> {
        self.group.has_full_channel_access().await
    }

    pub async fn is_owner(&self) -> anyhow::Result<bool> {
        self.group.is_owner().await
    }

    /// Persists the departures recorded by commits applied through this group since the last
    /// call. Called after every operation that may apply a commit.
    async fn persist_departures(&self) {
        let group_id = self.group.group_id();
        for departure in self.group.take_departures() {
            if let Err(err) = self.departure_store.record(group_id, &departure).await {
                // Losing a record only makes a later re-add of that user fail closed.
                log::error!(
                    "failed to record departure {:?} from group {}: {:?}",
                    departure, group_id, err
                );
            }
        }
    }

    /// Authorization gate for re-adds requested by the (untrusted) server. Allows `username`
    /// only if they still have a device in the roster (reinstall / address rotation), or this
    /// client saw them leave through a stale-device cleanup (every removed device's credential
    /// had expired) with no later kick/leave. Fails closed when there is no record.
    pub async fn re_add_decision(&self, username: &str) -> ReAddDecision {
        self.persist_departures().await;
        if self.group.has_member_user(username).await {
            return ReAddDecision::AllowedMember;
        }
        match self
            .departure_store
            .lookup(self.group.group_id(), username)
            .await
        {
            Ok(Some(true)) => ReAddDecision::AllowedStaleDeparture,
            Ok(Some(false)) => ReAddDecision::Refused,
            Ok(None) => ReAddDecision::NoRecord,
            Err(err) => {
                log::warn!("departure lookup failed for {}: {:?}", username, err);
                ReAddDecision::NoRecord
            }
        }
    }

    pub async fn may_auto_re_add(&self, username: &str) -> bool {
        self.re_add_decision(username).await.is_allowed()
    }

    /// Re-adds a single device of `username`. Callers acting on server-supplied requests must
    /// gate this with [`Self::may_auto_re_add`].
    pub async fn re_add_member(&self, username: String, address: u64) -> anyhow::Result<u64> {
        let res = self.group.re_add_member(username, address).await;
        self.persist_departures().await;
        res
    }

    pub async fn process(&self, message: Vec<u8>) -> anyhow::Result<FireflyMlsReceivedMessage> {
        let result = self.group.process(&message).await;
        self.persist_departures().await;
        let result = result?;
        Ok(match result {
            firefly_core::FireflyMlsReceivedMessage::Message(msg) => {
                if let Err(err) = self.group.save().await {
                    log::warn!("Failed to save group state after process message: {:?}", err);
                }
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
        let res = self.group.add_member(username, role_id).await;
        self.persist_departures().await;
        res
    }

    /// `(address_id, device_id, username)` of the other members' leaves.
    pub async fn member_device_addresses(&self) -> Vec<(u64, u32, String)> {
        self.group.member_device_addresses().await
    }

    pub async fn remove_gone_devices(&self, my_username: &str, gone: &[u64]) -> anyhow::Result<Option<u64>> {
        let res = self.group.remove_gone_devices(my_username, gone).await;
        self.persist_departures().await;
        res
    }

    pub async fn kick_member(&self, username: String) -> anyhow::Result<u64> {
        let res = self.group.kick_member(&username).await;
        self.persist_departures().await;
        res
    }

    pub async fn update_channel(
        &self,
        id: u32,
        delete: bool,
        name: String,
        channel_ty: u8,
        default_permissions: u32,
    ) -> anyhow::Result<u64> {
        let res = self.group
            .update_channel(id, delete, name, channel_ty, default_permissions)
            .await;
        self.persist_departures().await;
        res
    }

    pub async fn update_roles(&self, roles: Vec<UpdateRoleProposalFfi>) -> anyhow::Result<u64> {
        let res = self.group
            .update_roles(roles.into_iter().map(|role| UpdateRoleProposal {
                role_id: role.role_id,
                name: role.name,
                permissions: role.permissions,
                delete: role.delete,
                color: role.color,
            }))
            .await;
        self.persist_departures().await;
        res
    }

    pub async fn update_users(&self, users: Vec<UpdateUserProposalFfi>) -> anyhow::Result<u64> {
        let res = self.group
            .update_users(users.into_iter().map(|user| UpdateUserProposal {
                username: user.username,
                role_id: user.role_id,
            }))
            .await;
        self.persist_departures().await;
        res
    }

    pub async fn update_roles_in_channel(
        &self,
        channel_id: u32,
        roles: Vec<UpdateRoleProposalFfi>,
    ) -> anyhow::Result<u64> {
        let res = self.group
            .update_roles_in_channel(roles.into_iter().map(|x| UpdateRoleInChannelProposal {
                channel_id,
                role_proposal: UpdateRoleProposal {
                    name: x.name,
                    role_id: x.role_id,
                    permissions: x.permissions,
                    delete: x.delete,
                    color: x.color,
                },
            }))
            .await;
        self.persist_departures().await;
        res
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

/// Outcome of [`FfiMlsGroup::re_add_decision`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReAddDecision {
    /// The user still has at least one device in the roster.
    AllowedMember,
    /// The user's last recorded departure was a stale-device cleanup.
    AllowedStaleDeparture,
    /// The user's last recorded departure was a kick or voluntary leave.
    Refused,
    /// This client never saw the user depart (e.g. it joined later, or they were never a
    /// member); another member's client may hold the record.
    NoRecord,
}

impl ReAddDecision {
    pub fn is_allowed(self) -> bool {
        matches!(self, Self::AllowedMember | Self::AllowedStaleDeparture)
    }
}

pub struct UpdateRoleProposalFfi {
    pub name: String,
    pub role_id: u32,
    pub permissions: u32,
    pub delete: bool,
    pub color: u32,
}

pub struct UpdateUserProposalFfi {
    pub username: String,
    pub role_id: u32,
}

#[cfg(test)]
mod credential_renewal_tests {
    use super::credential_due;

    const DAY: u64 = 24 * 60 * 60;

    #[test]
    fn renews_after_three_quarters_of_its_life() {
        let obtained = 1_000 * DAY;
        let valid_until = obtained + 365 * DAY;
        assert!(!credential_due(valid_until, Some(obtained), obtained + 200 * DAY));
        assert!(!credential_due(valid_until, Some(obtained), obtained + 273 * DAY));
        assert!(credential_due(valid_until, Some(obtained), obtained + 274 * DAY));
        assert!(credential_due(valid_until, Some(obtained), valid_until + DAY));
    }

    #[test]
    fn short_lived_credentials_renew_in_time_too() {
        assert!(!credential_due(1_240, Some(1_000), 1_100));
        assert!(credential_due(1_240, Some(1_000), 1_180));
    }

    #[test]
    fn credentials_from_before_renewal_renew_in_their_last_30_days() {
        let valid_until = 1_000 * DAY;
        assert!(!credential_due(valid_until, None, valid_until - 31 * DAY));
        assert!(credential_due(valid_until, None, valid_until - 29 * DAY));
    }
}
