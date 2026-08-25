#![cfg(all(mls_build_async))]

use crate::client::FireflyClient;
use crate::client::{FireflyClientMlsConfig, load_client};
use crate::config::{
    CIPHERSUITE, FireflyCredential, FireflyError, FireflyIdentityProvider, UpdateChannelProposal,
    UpdateRoleInChannelProposal, UpdateRoleProposal, UpdateUserInChannelProposal,
    UpdateUserProposal,
};
use crate::extension::{FireflyGroupExtension, FireflyGroupExtensionWrapper};
use crate::rules::{
    does_signing_identity_has_this_username, get_auth_token_from_signing_identity,
    get_username_from_signing_identity,
};
use crate::storage_provider::{
    FfiGroupStateStorage, FfiKeyPackageStorage, FfiPreSharedKeyStorage, MlsGroupStateStorage,
    MlsKeyPackageStorage, MlsPreSharedKeyStorage,
};
use crate::utils::HTTP_CLIENT;
use anyhow::Context;
use firefly_protos::firefly::{
    GroupCommitAndWelcome, GroupKeyPackage, GroupKeyPackages, GroupMessage,
};
use firefly_protos::{self as protos, firefly};
use firefly_protos::{deserialize_proto, serialize_proto};
use mls_rs::group::CommitOutput;
use mls_rs::identity::{Credential, CustomCredential, MlsCredential, SigningIdentity};
use mls_rs::{
    CipherSuiteProvider, CryptoProvider, ExtensionList, Group, MlsMessage,
    crypto::{SignaturePublicKey, SignatureSecretKey},
    extension::MlsExtension,
    group::proposal::MlsCustomProposal,
};
use mls_rs_codec::MlsDecode;
use std::sync::Arc;

pub mod client;
pub mod config;
pub mod extension;
pub mod jwk;
pub mod rules;
pub mod server;
pub mod sorted_search;
pub mod storage_provider;
pub mod utils;

#[derive(Debug)]
pub struct EncryptedMessage {
    pub sender: String,
    pub message: Vec<u8>,
}

#[derive(Debug)]
pub enum FireflyMlsReceivedMessage {
    Message(EncryptedMessage),
    Commit,
    Proposal,
    GroupInfo,
    Welcome,
    KeyPackage,
}

pub struct FireflyMlsClient {
    client: FireflyClient,
    identity: Arc<FireflyIdentity>,
    base_url: Arc<str>,
    auth_token_callbacks: Arc<dyn FireflyAuthTokenCallback>,
}

#[derive(Clone)]
pub struct FireflyIdentity {
    secret: SignatureSecretKey,
    public: SignaturePublicKey,
    credential: Vec<u8>,
}

impl FireflyIdentity {
    pub async fn generate(
        token: String,
        base_url: Arc<str>,
        device_id: u8,
        address_id: u64,
    ) -> anyhow::Result<Self> {
        let crypto_provider = mls_rs_crypto_rustcrypto::RustCryptoProvider::default();

        let cipher_suite = crypto_provider
            .cipher_suite_provider(CIPHERSUITE)
            .ok_or(anyhow::anyhow!("missing cipher_suite"))?;
        let (secret, public) = cipher_suite.signature_key_generate().await?;
        let credential = FireflyIdentityProvider::new(base_url)
            .get_credential(public.to_vec(), token, device_id, address_id)
            .await?;

        Ok(Self {
            credential,
            secret,
            public,
        })
    }

    pub async fn refresh(
        &self,
        token: String,
        base_url: Arc<str>,
        device_id: u8,
        address_id: u64,
    ) -> anyhow::Result<Self> {
        let credential = FireflyIdentityProvider::new(base_url)
            .get_credential(self.public.to_vec(), token, device_id, address_id)
            .await?;

        Ok(Self {
            credential: credential,
            secret: self.secret.clone(),
            public: self.public.clone(),
        })
    }

    pub fn to_vec(&self) -> anyhow::Result<Vec<u8>> {
        let s = protos::firefly::FireflyIdentity {
            secret: self.secret.as_bytes().into(),
            public: self.public.as_bytes().into(),
            credential: (&self.credential).into(),
        };
        Ok(serialize_proto(&s)?.to_vec())
    }

    pub fn from_vec(v: Vec<u8>) -> anyhow::Result<Self> {
        let s: protos::firefly::FireflyIdentity = deserialize_proto(&v)?;
        Ok(Self {
            secret: s.secret.to_vec().into(),
            public: s.public.to_vec().into(),
            credential: s.credential.to_vec(),
        })
    }

    pub fn is_valid_until_secs(&self) -> anyhow::Result<u64> {
        // TODO: make this efficient
        Ok(FireflyCredential::new(self.credential.clone())?.valid_until_secs()?)
    }

    #[inline(always)]
    pub const fn secret(&self) -> &SignatureSecretKey {
        &self.secret
    }

    pub fn signing_identity(&self) -> SigningIdentity {
        let credential = Credential::Custom(CustomCredential::new(
            FireflyCredential::credential_type(),
            self.credential.clone(),
        ));
        let signature_key = self.public.clone();
        SigningIdentity::new(credential, signature_key)
    }
}

impl FireflyMlsClient {
    pub fn get_identity(&self) -> Arc<FireflyIdentity> {
        self.identity.clone()
    }

    pub fn load(
        base_url: String,
        identity: Arc<FireflyIdentity>,
        key_package_repo: Arc<dyn MlsKeyPackageStorage>,
        group_state_storage: Arc<dyn MlsGroupStateStorage>,
        psk_store: Arc<dyn MlsPreSharedKeyStorage>,
        auth_token_callbacks: Arc<dyn FireflyAuthTokenCallback>,
    ) -> anyhow::Result<Self> {
        let base_url: Arc<str> = base_url.into();
        let f = Self {
            client: load_client(
                FireflyIdentity::clone(&identity),
                FfiKeyPackageStorage::new(key_package_repo),
                FfiGroupStateStorage::new(group_state_storage),
                FfiPreSharedKeyStorage::new(psk_store),
                FireflyIdentityProvider::new(base_url.clone()),
            )?,
            identity,
            base_url: base_url.clone(),
            auth_token_callbacks,
        };

        Ok(f)
    }

    pub fn username(&self) -> Option<String> {
        Some(get_username_from_signing_identity(self.client.signing_identity().ok()?.0).ok()?)
    }

    pub async fn create_group(
        &self,
        extension: protos::firefly::FireflyGroupExtension<'_>,
    ) -> anyhow::Result<FireflyMlsGroup> {
        let group_name = extension.name.clone();
        let mut extensions = ExtensionList::new();
        extensions.set(
            FireflyGroupExtension::new(FireflyGroupExtensionWrapper::new(extension))?
                .into_extension()?,
        );
        let mut group = self
            .client
            .create_group(extensions, Default::default(), None)
            .await?;

        let group_info_message = group.group_info_message(true).await?.to_bytes()?;

        let url = format!("{}/group", self.base_url);
        let token = self.auth_token_callbacks.token().await?;

        let body = firefly::Group {
            name: group_name,
            state: group_info_message.into(),
            ..Default::default()
        };

        let response = HTTP_CLIENT
            .post(url)
            .bearer_auth(token)
            .body(serialize_proto(&body)?)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "unexpected status code [{}]: {}",
                response.status(),
                response.text().await?
            ));
        }

        let response_body = response.bytes().await?;

        let group_body = deserialize_proto::<firefly::Group>(&response_body)?;

        group.write_to_storage().await?;

        Ok(FireflyMlsGroup::new(
            group_body.id,
            group,
            self.base_url.clone(),
            self.auth_token_callbacks.clone(),
        ))
    }

    pub async fn generate_key_package(&self) -> anyhow::Result<Vec<u8>> {
        let package = self
            .client
            .generate_key_package_message(Default::default(), Default::default(), None)
            .await?;

        Ok(package.to_bytes()?)
    }

    pub fn key_package_info_credential(message: &[u8]) -> anyhow::Result<FireflyCredential> {
        let message = MlsMessage::from_bytes(message)?;
        let key_package = message.as_key_package().context("not a key package")?;

        FireflyCredential::from_signing_identity(key_package.signing_identity())
    }

    pub async fn join_group(
        &self,
        group_id: u64,
        welcome_message: Vec<u8>,
    ) -> anyhow::Result<FireflyMlsGroup> {
        let welcome_message: MlsMessage = MlsMessage::mls_decode(&mut welcome_message.as_slice())?;

        let (group, _) = self.client.join_group(None, &welcome_message, None).await?;
        Ok(FireflyMlsGroup::new(
            group_id,
            group,
            self.base_url.clone(),
            self.auth_token_callbacks.clone(),
        ))
    }

    pub async fn load_group(
        &self,
        group_id: u64,
        group_identifier: Vec<u8>,
    ) -> anyhow::Result<FireflyMlsGroup> {
        let group = self.client.load_group(&group_identifier).await?;
        Ok(FireflyMlsGroup::new(
            group_id,
            group,
            self.base_url.clone(),
            self.auth_token_callbacks.clone(),
        ))
    }

    pub async fn is_valid_until_secs(&self) -> anyhow::Result<u64> {
        let (identity, _) = self.client.signing_identity()?;
        let credential = FireflyCredential::from_signing_identity(identity)?;
        Ok(credential.valid_until_secs()?)
    }
}

#[cfg(target_arch = "wasm32")]
#[async_trait::async_trait(?Send)]
pub trait FireflyAuthTokenCallback {
    async fn token(&self) -> anyhow::Result<String>;
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait::async_trait]
pub trait FireflyAuthTokenCallback: Send + Sync {
    async fn token(&self) -> anyhow::Result<String>;
}

pub struct FireflyMlsGroup {
    group: tokio::sync::Mutex<Group<FireflyClientMlsConfig>>,
    base_url: Arc<str>,
    auth_token_callback: Arc<dyn FireflyAuthTokenCallback>,
    group_id: u64,
}

impl FireflyMlsGroup {
    pub async fn epoch(&self) -> u64 {
        let group = self.group.lock().await;
        group.context().epoch()
    }

    pub async fn export_secret(
        &self,
        label: &str,
        context: &[u8],
        key_length: usize,
    ) -> anyhow::Result<Vec<u8>> {
        let group = self.group.lock().await;
        let secret = group
            .export_secret(label.as_bytes(), context, key_length)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        Ok(secret.as_bytes().to_vec())
    }

    pub fn new(
        group_id: u64,
        group: Group<FireflyClientMlsConfig>,
        base_url: Arc<str>,
        auth_token_callback: Arc<dyn FireflyAuthTokenCallback>,
    ) -> Self {
        Self {
            group: tokio::sync::Mutex::new(group),
            base_url,
            auth_token_callback,
            group_id,
        }
    }
}

pub struct CommitAndWelcomeMessage {
    pub commit_message: Vec<u8>,
    pub welcome_message: Vec<u8>,
}

impl FireflyMlsGroup {
    pub async fn state(&self) -> anyhow::Result<Vec<u8>> {
        let msg = self.group.lock().await.group_info_message(true).await?;
        let body = msg.to_bytes()?;
        Ok(body)
    }

    pub async fn extension(&self) -> anyhow::Result<Vec<u8>> {
        let group = self.group.lock().await;
        let extension = group
            .context()
            .extensions()
            .get_as::<FireflyGroupExtension>()?
            .ok_or(anyhow::anyhow!("no firefly extension on group"))?;

        let mut wrapper = extension.deserialize()?;

        for member in group.roster().members() {
            let token = get_auth_token_from_signing_identity(&member.signing_identity)?;

            if wrapper.has_member(&token.username) {
                continue;
            } else {
                wrapper.update_member_even_if_default_role(protos::firefly::FireflyGroupMember {
                    username: token.username.into(),
                    role: 0,
                });
            }
        }

        let result = wrapper.serialize()?;

        Ok(result)
    }

    pub async fn save(&self) -> anyhow::Result<()> {
        Ok(self.group.lock().await.write_to_storage().await?)
    }

    pub async fn encrypt(&self, data: &[u8]) -> anyhow::Result<Vec<u8>> {
        Ok(self
            .group
            .lock()
            .await
            .encrypt_application_message(data, Vec::new())
            .await?
            .to_bytes()?)
    }

    pub async fn process(&self, message: &[u8]) -> anyhow::Result<FireflyMlsReceivedMessage> {
        let mut group = self.group.lock().await;

        let msg = group
            .process_incoming_message(MlsMessage::from_bytes(message)?)
            .await?;
        let out = match msg {
            mls_rs::group::ReceivedMessage::ApplicationMessage(application_message_description) => {
                let sender_username = get_username_from_signing_identity(
                    &group
                        .member_at_index(application_message_description.sender_index)
                        .ok_or(anyhow::anyhow!("member at index doesn't exist"))?
                        .signing_identity,
                )?;

                FireflyMlsReceivedMessage::Message(EncryptedMessage {
                    sender: sender_username,
                    message: application_message_description.data().into(),
                })
            }
            mls_rs::group::ReceivedMessage::Commit(_commit_message_description) => {
                FireflyMlsReceivedMessage::Commit
            }
            mls_rs::group::ReceivedMessage::Proposal(_proposal_message_description) => {
                FireflyMlsReceivedMessage::Proposal
            }
            mls_rs::group::ReceivedMessage::GroupInfo(_) => FireflyMlsReceivedMessage::GroupInfo,
            mls_rs::group::ReceivedMessage::Welcome => FireflyMlsReceivedMessage::Welcome,
            mls_rs::group::ReceivedMessage::KeyPackage(_) => FireflyMlsReceivedMessage::KeyPackage,
        };
        Ok(out)
    }

    pub async fn re_add_member(&self, username: String, address_id: u64) -> anyhow::Result<u64> {
        let url = format!(
            "{}/group/keyPackages?other={}&address={}",
            self.base_url, username, address_id
        );

        let token = self.auth_token_callback.token().await?;

        let response = HTTP_CLIENT.get(url).bearer_auth(&token).send().await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "unexpected status code: [{}]: {}",
                response.status(),
                response.text().await?
            ));
        }

        let mut group = self.group.lock().await;

        let mut commit_builder = group.commit_builder();

        let body = response.bytes().await?;

        let package = deserialize_proto::<GroupKeyPackage>(&body)?;

        let mut invitee_addresses = Vec::new();

        commit_builder = commit_builder.add_member(MlsMessage::from_bytes(&package.package)?)?;

        invitee_addresses.push(package.address);

        let commit = commit_builder.build().await?;

        match self
            .send_commit_and_welcome_to_server(commit, &username, invitee_addresses)
            .await
        {
            Ok(id) => {
                group.apply_pending_commit().await?;

                group.write_to_storage().await?;

                return Ok(id);
            }

            Err(err) => {
                group.clear_pending_commit();

                return Err(err);
            }
        };
    }

    pub async fn add_member(&self, username: String, role_id: u32) -> anyhow::Result<u64> {
        let url = format!(
            "{}/group/keyPackages?other={}&all=true",
            self.base_url, username
        );

        let token = self.auth_token_callback.token().await?;

        let response = HTTP_CLIENT.get(url).bearer_auth(&token).send().await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "unexpected status code: [{}]: {}",
                response.status(),
                response.text().await?
            ));
        }

        let body = response.bytes().await?;

        let packages = deserialize_proto::<GroupKeyPackages>(&body)?;

        if packages.packages.is_empty() {
            return Err(anyhow::anyhow!("No key packages found"));
        }

        let mut group = self.group.lock().await;

        let current_addresses: Vec<u64> = group
            .roster()
            .members_iter()
            .filter_map(|m| {
                crate::rules::get_auth_token_from_signing_identity(&m.signing_identity)
                    .ok()
                    .map(|t| t.address_id)
            })
            .collect();

        let mut commit_builder = group.commit_builder();

        let mut invitee_addresses = Vec::new();

        for package in packages.packages {
            if current_addresses.contains(&package.address) {
                log::info!("Skip adding member {}, already in group", package.address);
                continue;
            }

            commit_builder =
                commit_builder.add_member(MlsMessage::from_bytes(&package.package)?)?;

            invitee_addresses.push(package.address);
        }

        if invitee_addresses.is_empty() {
            log::info!("No new members to add, skipping commit");
            return Ok(0);
        }

        commit_builder = commit_builder.custom_proposal(
            UpdateUserProposal {
                username: username.clone(),
                role_id,
            }
            .to_custom_proposal()?,
        );

        let commit = commit_builder.build().await?;

        match self
            .send_commit_and_welcome_to_server(commit, &username, invitee_addresses)
            .await
        {
            Ok(id) => {
                group.apply_pending_commit().await?;
                group.write_to_storage().await?;

                return Ok(id);
            }
            Err(err) => {
                group.clear_pending_commit();
                return Err(err);
            }
        };
    }

    pub async fn kick_member(&self, username: &str) -> anyhow::Result<u64> {
        let mut indices = Vec::new();

        let mut group = self.group.lock().await;
        for member in group.roster().members_iter() {
            if does_signing_identity_has_this_username(username, &member.signing_identity) {
                indices.push(member.index);
            }
        }

        if indices.is_empty() {
            return Err(anyhow::anyhow!("user does not exist"));
        }

        let mut builder = group.commit_builder();

        for index in indices {
            builder = builder.remove_member(index)?;
        }

        let commit = builder.build().await?;

        self.send_commit_to_server_and_apply_commit(&mut *group, commit)
            .await
    }

    pub async fn update_channel(
        &self,
        id: u32,
        delete: bool,
        name: String,
        channel_ty: u8,
        default_permissions: u32,
    ) -> anyhow::Result<u64> {
        self.commit_custom_proposals(std::iter::once(UpdateChannelProposal {
            delete,
            id,
            name,
            channel_ty,
            default_permissions,
        }))
        .await
    }

    async fn commit_custom_proposals<I, C>(&self, proposals: I) -> anyhow::Result<u64>
    where
        I: Iterator<Item = C>,
        C: MlsCustomProposal,
    {
        let mut group = self.group.lock().await;
        let mut commit_builder = group.commit_builder();
        for proposal in proposals {
            commit_builder = commit_builder.custom_proposal(proposal.to_custom_proposal()?);
        }

        let commit = commit_builder.build().await?;

        self.send_commit_to_server_and_apply_commit(&mut *group, commit)
            .await
    }

    pub async fn update_roles(
        &self,
        proposals: impl Iterator<Item = UpdateRoleProposal>,
    ) -> anyhow::Result<u64> {
        self.commit_custom_proposals(proposals).await
    }

    pub async fn update_roles_in_channel(
        &self,
        proposals: impl Iterator<Item = UpdateRoleInChannelProposal>,
    ) -> anyhow::Result<u64> {
        self.commit_custom_proposals(proposals).await
    }

    pub async fn update_users_in_channel(
        &self,
        proposals: impl Iterator<Item = UpdateUserInChannelProposal>,
    ) -> anyhow::Result<u64> {
        self.commit_custom_proposals(proposals).await
    }

    pub async fn update_users(
        &self,
        proposals: impl Iterator<Item = UpdateUserProposal>,
    ) -> anyhow::Result<u64> {
        self.commit_custom_proposals(proposals).await
    }

    pub async fn update_leaf(&self, identity: &FireflyIdentity) -> anyhow::Result<u64> {
        let mut group = self.group.lock().await;

        let signing_identity = identity.signing_identity();

        let secret = identity.secret().to_owned();
        let commit = group
            .commit_builder()
            .set_new_signing_identity(secret, signing_identity)
            .build()
            .await?;

        self.send_commit_to_server_and_apply_commit(&mut group, commit)
            .await
    }

    pub async fn group_identifier(&self) -> anyhow::Result<Vec<u8>> {
        let group_id = self.group.lock().await.group_id().to_vec();
        Ok(group_id)
    }

    pub fn group_id(&self) -> u64 {
        self.group_id
    }

    async fn send_commit_to_server_and_apply_commit(
        &self,
        group: &mut Group<FireflyClientMlsConfig>,
        commit: CommitOutput,
    ) -> anyhow::Result<u64> {
        log::info!(
            "sending commit to server: group_epoch: {}, commit_epoch: {:?}",
            group.current_epoch(),
            commit.commit_message.epoch(),
        );

        match self.send_commit_to_server(commit).await {
            Ok(id) => {
                log::info!("server accepted commit: msg_id: {}", id);
                log::info!(
                    "has pending_commit: {}, applying pending commit",
                    group.has_pending_commit()
                );

                let desc = group.apply_pending_commit().await?;

                log::info!("applied pending commit: {:?}", desc);

                group.write_to_storage().await?;

                return Ok(id);
            }
            Err(err) => {
                log::info!("server rejected commit: {:?}", err);
                group.clear_pending_commit();
                return Err(anyhow::anyhow!(err));
            }
        }
    }

    async fn send_commit_to_server(&self, commit: CommitOutput) -> anyhow::Result<u64> {
        let token = self.auth_token_callback.token().await?;

        let url = format!("{}/group/commit", self.base_url);

        let message = GroupMessage {
            id: 0,
            groupId: self.group_id,
            message: commit.commit_message.to_bytes()?.into(),
            epoch: commit.commit_message.epoch().unwrap_or_default() as u32,
        };

        let response = HTTP_CLIENT
            .post(url)
            .bearer_auth(token)
            .body(serialize_proto(&message)?)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "unexpected status: [{}] {}",
                response.status(),
                response.text().await?
            ));
        }

        let response_body = response.bytes().await?;

        let message = deserialize_proto::<GroupMessage>(&response_body)?;

        return Ok(message.id);
    }
    async fn send_commit_and_welcome_to_server(
        &self,
        commit: CommitOutput,
        invitee: &str,
        invitee_ids: Vec<u64>,
    ) -> anyhow::Result<u64> {
        let token = self.auth_token_callback.token().await?;

        let url = format!("{}/group/commitAndWelcome", self.base_url);

        log::info!("welcome messages length: {}", commit.welcome_messages.len());

        if commit.welcome_messages.is_empty() {
            return Err(anyhow::anyhow!(
                "unexpected welcome messages length: {}",
                commit.welcome_messages.len()
            ));
        }

        let welcome_messages: Vec<Vec<u8>> = commit
            .welcome_messages
            .iter()
            .map(|m| m.to_bytes().unwrap())
            .collect();
        let commit_message = commit.commit_message.to_bytes()?;

        let message = GroupCommitAndWelcome {
            groupId: self.group_id,
            commitMessage: commit_message.into(),
            welcomeMessages: welcome_messages.into_iter().map(|v| v.into()).collect(),
            invitee: invitee.into(),
            inviteeAddresses: invitee_ids,
            ..Default::default()
        };

        if commit.welcome_messages.len() > 1 {
            log::info!("sending {} welcome messages", commit.welcome_messages.len());
        }

        let response = HTTP_CLIENT
            .post(url)
            .bearer_auth(token)
            .body(serialize_proto(&message)?)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "unexpected status: [{}] {}",
                response.status(),
                response.text().await?
            ));
        }

        let response_body = response.bytes().await?;

        let message = deserialize_proto::<GroupMessage>(&response_body)?;

        return Ok(message.id);
    }
}
