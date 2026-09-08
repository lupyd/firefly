use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;
use rand::RngCore;
use libsignal_protocol::{kem::KeyType, *};
use zeroize::Zeroizing;

use crate::{
    EncryptedMessage, FfiPreKeyBundle,
    utils::{self, get_current_timestamp_millis_since_epoch},
};

pub use firefly_core::storage_provider::{
    MlsGroupStateStorage, MlsKeyPackageStorage, MlsPreSharedKeyStorage,
};

// ---------------------------------------------------------------------------
// Common Data Models
// ---------------------------------------------------------------------------

#[cfg(not(target_arch = "wasm32"))]
pub use crate::db::messages::UserMessage;

#[cfg(target_arch = "wasm32")]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct UserMessage {
    pub id: u64,
    pub other: String,
    pub message: Vec<u8>,
    pub sent_by_other: bool,
}

#[cfg(not(target_arch = "wasm32"))]
pub use crate::db::group_messages::GroupMessage;

#[cfg(target_arch = "wasm32")]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct GroupMessage {
    pub id: u64,
    pub group_id: u64,
    pub by: String,
    pub message: Vec<u8>,
    pub channel_id: u32,
    pub epoch: u32,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct GroupInfo {
    pub id: u64,
    pub identifier: Vec<u8>,
    pub name: String,
    pub description: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AddressIdAndDeviceId {
    pub address_id: u64,
    pub device_id: u8,
    pub username: String,
}

#[derive(Default, Clone, Copy, Debug)]
pub struct ConversationSettings {
    pub inner: u64,
}

impl ConversationSettings {
    pub fn new(settings: u64) -> Self {
        Self { inner: settings }
    }
}

// ---------------------------------------------------------------------------
// Key-Value Store Trait & Memory Implementation
// ---------------------------------------------------------------------------

pub const KEY_LAST_RECEIVED_MESSAGE_ID: &str = "last_received_message_id";
pub const KEY_LAST_RECEIVED_GROUP_MESSAGE_ID: &str = "last_received_group_message_id";
pub const KEY_FCM_TOKEN: &str = "fcm_token";

#[async_trait::async_trait]
pub trait KeyValueStorage: Send + Sync {
    async fn get(&self, key: &str) -> anyhow::Result<String>;
    async fn set(&self, key: &str, value: &str) -> anyhow::Result<()>;
    async fn update_last_received_message_id(
        &self,
        last_received_message_id: u64,
    ) -> anyhow::Result<()>;
}

#[derive(Clone, Default)]
pub struct MemoryKeyValueStore {
    data: Arc<RwLock<HashMap<String, String>>>,
}

impl MemoryKeyValueStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl KeyValueStorage for MemoryKeyValueStore {
    async fn get(&self, key: &str) -> anyhow::Result<String> {
        let guard = self.data.read().await;
        guard
            .get(key)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("key not found: {}", key))
    }

    async fn set(&self, key: &str, value: &str) -> anyhow::Result<()> {
        let mut guard = self.data.write().await;
        guard.insert(key.to_string(), value.to_string());
        Ok(())
    }

    async fn update_last_received_message_id(
        &self,
        last_received_message_id: u64,
    ) -> anyhow::Result<()> {
        if let Ok(existing_str) = self.get(KEY_LAST_RECEIVED_MESSAGE_ID).await {
            if let Ok(existing) = existing_str.parse::<u64>() {
                if existing >= last_received_message_id {
                    return Ok(());
                }
            }
        }
        self.set(KEY_LAST_RECEIVED_MESSAGE_ID, &last_received_message_id.to_string())
            .await
    }
}

// ---------------------------------------------------------------------------
// MLS KeyPackage Storage - Memory Implementation
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
pub struct MemoryMlsKeyPackageStorage {
    data: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>,
}

impl MemoryMlsKeyPackageStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl MlsKeyPackageStorage for MemoryMlsKeyPackageStorage {
    async fn insert(&self, id: Vec<u8>, key_package_data: Vec<u8>) -> bool {
        let mut guard = self.data.write().await;
        guard.insert(id, key_package_data);
        true
    }

    async fn delete(&self, id: Vec<u8>) -> bool {
        let mut guard = self.data.write().await;
        guard.remove(&id).is_some()
    }

    async fn get(&self, id: Vec<u8>) -> Option<Vec<u8>> {
        let guard = self.data.read().await;
        guard.get(&id).cloned()
    }
}

// ---------------------------------------------------------------------------
// MLS PreSharedKey Storage - Memory Implementation
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
pub struct MemoryMlsPreSharedKeyStorage {
    data: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>,
}

impl MemoryMlsPreSharedKeyStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl MlsPreSharedKeyStorage for MemoryMlsPreSharedKeyStorage {
    async fn get(&self, id: Vec<u8>) -> Option<Vec<u8>> {
        let guard = self.data.read().await;
        guard.get(&id).cloned()
    }
}

// ---------------------------------------------------------------------------
// MLS GroupState Storage - Memory Implementation
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
pub struct MemoryMlsGroupStateStorage {
    states: Arc<RwLock<HashMap<Vec<u8>, Zeroizing<Vec<u8>>>>>,
    epochs: Arc<RwLock<HashMap<(Vec<u8>, u64), Zeroizing<Vec<u8>>>>>,
}

impl MemoryMlsGroupStateStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl MlsGroupStateStorage for MemoryMlsGroupStateStorage {
    async fn state(&self, group_id: Vec<u8>) -> Option<Zeroizing<Vec<u8>>> {
        let guard = self.states.read().await;
        guard.get(&group_id).cloned()
    }

    async fn epoch(&self, group_id: Vec<u8>, epoch_id: u64) -> Option<Zeroizing<Vec<u8>>> {
        let guard = self.epochs.read().await;
        guard.get(&(group_id, epoch_id)).cloned()
    }

    async fn write(
        &self,
        group_id: Vec<u8>,
        state_data: Zeroizing<Vec<u8>>,
        epoch_inserts: HashMap<u64, Zeroizing<Vec<u8>>>,
        epoch_updates: HashMap<u64, Zeroizing<Vec<u8>>>,
    ) -> bool {
        {
            let mut states_guard = self.states.write().await;
            states_guard.insert(group_id.clone(), state_data);
        }

        let mut epochs_guard = self.epochs.write().await;
        for (epoch_id, data) in epoch_inserts {
            epochs_guard.insert((group_id.clone(), epoch_id), data);
        }
        for (epoch_id, data) in epoch_updates {
            epochs_guard.insert((group_id.clone(), epoch_id), data);
        }

        true
    }

    async fn max_epoch_id(&self, group_id: Vec<u8>) -> Option<u64> {
        let guard = self.epochs.read().await;
        guard
            .keys()
            .filter(|(gid, _)| gid == &group_id)
            .map(|(_, epoch)| *epoch)
            .max()
    }
}

// ---------------------------------------------------------------------------
// Group Info Store Trait & Memory Implementation
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
pub trait GroupInfoStorage: Send + Sync {
    async fn get_all(&self) -> anyhow::Result<Vec<GroupInfo>>;
    async fn get(&self, id: u64) -> anyhow::Result<GroupInfo>;
    async fn set(
        &self,
        id: u64,
        name: String,
        description: String,
        group_state_id: Vec<u8>,
    ) -> anyhow::Result<()>;
    async fn delete(&self, id: u64) -> anyhow::Result<()>;
}

#[derive(Clone, Default)]
pub struct MemoryGroupInfoStore {
    data: Arc<RwLock<HashMap<u64, GroupInfo>>>,
}

impl MemoryGroupInfoStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl GroupInfoStorage for MemoryGroupInfoStore {
    async fn get_all(&self) -> anyhow::Result<Vec<GroupInfo>> {
        let guard = self.data.read().await;
        Ok(guard.values().cloned().collect())
    }

    async fn get(&self, id: u64) -> anyhow::Result<GroupInfo> {
        let guard = self.data.read().await;
        guard
            .get(&id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("GroupInfo not found for {}", id))
    }

    async fn set(
        &self,
        id: u64,
        name: String,
        description: String,
        group_state_id: Vec<u8>,
    ) -> anyhow::Result<()> {
        let mut guard = self.data.write().await;
        guard.insert(
            id,
            GroupInfo {
                id,
                name,
                description,
                identifier: group_state_id,
            },
        );
        Ok(())
    }

    async fn delete(&self, id: u64) -> anyhow::Result<()> {
        let mut guard = self.data.write().await;
        guard.remove(&id);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Group Messages Store Trait & Memory Implementation
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
pub trait GroupMessageStorage: Send + Sync {
    async fn add(
        &self,
        id: u64,
        group_id: u64,
        channel_id: u32,
        epoch: u32,
        by: &str,
        message: &[u8],
    ) -> anyhow::Result<()>;
    async fn get(
        &self,
        group_id: u64,
        start_before: u64,
        limit: u32,
    ) -> anyhow::Result<Vec<GroupMessage>>;
    async fn get_last_message_of_group(&self, group_id: u64) -> anyhow::Result<GroupMessage>;
    async fn delete_by_group_id(&self, group_id: u64) -> anyhow::Result<()>;
    async fn update_cursor(&self, id: u64, group_id: u64, epoch: u32) -> anyhow::Result<()>;
}

#[derive(Clone, Default)]
pub struct MemoryGroupMessageStore {
    messages: Arc<RwLock<Vec<GroupMessage>>>,
    cursors: Arc<RwLock<HashMap<u64, (u64, u32)>>>,
}

impl MemoryGroupMessageStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl GroupMessageStorage for MemoryGroupMessageStore {
    async fn add(
        &self,
        id: u64,
        group_id: u64,
        channel_id: u32,
        epoch: u32,
        by: &str,
        message: &[u8],
    ) -> anyhow::Result<()> {
        let mut guard = self.messages.write().await;
        guard.push(GroupMessage {
            id,
            group_id,
            by: by.to_string(),
            message: message.to_vec(),
            channel_id,
            epoch,
        });
        Ok(())
    }

    async fn get(
        &self,
        group_id: u64,
        start_before: u64,
        limit: u32,
    ) -> anyhow::Result<Vec<GroupMessage>> {
        let guard = self.messages.read().await;
        let mut matching: Vec<GroupMessage> = guard
            .iter()
            .filter(|m| m.group_id == group_id && m.id < start_before)
            .cloned()
            .collect();
        matching.sort_by(|a, b| b.id.cmp(&a.id));
        matching.truncate(limit as usize);
        Ok(matching)
    }

    async fn get_last_message_of_group(&self, group_id: u64) -> anyhow::Result<GroupMessage> {
        let res = self.get(group_id, u64::MAX, 1).await?;
        res.into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("no group messages"))
    }

    async fn delete_by_group_id(&self, group_id: u64) -> anyhow::Result<()> {
        let mut guard = self.messages.write().await;
        guard.retain(|m| m.group_id != group_id);
        Ok(())
    }

    async fn update_cursor(&self, id: u64, group_id: u64, epoch: u32) -> anyhow::Result<()> {
        let mut guard = self.cursors.write().await;
        guard.insert(group_id, (id, epoch));
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// User Messages Store Trait & Memory Implementation
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
pub trait UserMessageStorage: Send + Sync {
    async fn add(
        &self,
        id: u64,
        other: &str,
        message: &[u8],
        sent_by_other: bool,
    ) -> anyhow::Result<()>;
    async fn get_last_messages_of(
        &self,
        other: &str,
        before: i64,
        limit: i64,
    ) -> anyhow::Result<Vec<UserMessage>>;
}

#[derive(Clone, Default)]
pub struct MemoryUserMessageStore {
    messages: Arc<RwLock<Vec<UserMessage>>>,
}

impl MemoryUserMessageStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl UserMessageStorage for MemoryUserMessageStore {
    async fn add(
        &self,
        id: u64,
        other: &str,
        message: &[u8],
        sent_by_other: bool,
    ) -> anyhow::Result<()> {
        let mut guard = self.messages.write().await;
        guard.push(UserMessage {
            id,
            other: other.to_string(),
            message: message.to_vec(),
            sent_by_other,
        });
        Ok(())
    }

    async fn get_last_messages_of(
        &self,
        other: &str,
        before: i64,
        limit: i64,
    ) -> anyhow::Result<Vec<UserMessage>> {
        let guard = self.messages.read().await;
        let before_u64 = if before < 0 { u64::MAX } else { before as u64 };
        let mut matching: Vec<UserMessage> = guard
            .iter()
            .filter(|m| m.other == other && m.id < before_u64)
            .cloned()
            .collect();
        matching.sort_by(|a, b| b.id.cmp(&a.id));
        matching.truncate(limit as usize);
        Ok(matching)
    }
}

// ---------------------------------------------------------------------------
// Address Store - Memory Implementation
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
pub struct MemoryAddressStore {
    by_id: Arc<RwLock<HashMap<u64, AddressIdAndDeviceId>>>,
    by_username: Arc<RwLock<HashMap<String, Vec<AddressIdAndDeviceId>>>>,
}

impl MemoryAddressStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn add(&self, id: u64, username: &str, device_id: u8) -> anyhow::Result<()> {
        let item = AddressIdAndDeviceId {
            address_id: id,
            device_id,
            username: username.to_string(),
        };
        {
            let mut guard = self.by_id.write().await;
            guard.insert(id, item.clone());
        }
        {
            let mut guard = self.by_username.write().await;
            let list = guard.entry(username.to_string()).or_default();
            list.retain(|a| a.device_id != device_id);
            list.push(item);
        }
        Ok(())
    }

    pub async fn get(&self, username: &str) -> anyhow::Result<Vec<AddressIdAndDeviceId>> {
        let guard = self.by_username.read().await;
        Ok(guard.get(username).cloned().unwrap_or_default())
    }

    pub async fn get_by_id(&self, id: u64) -> anyhow::Result<Option<AddressIdAndDeviceId>> {
        let guard = self.by_id.read().await;
        Ok(guard.get(&id).cloned())
    }
}

// ---------------------------------------------------------------------------
// Self Group KeyPackage Store - Memory Implementation
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
pub struct MemorySelfGroupKeyPackageStore {
    data: Arc<RwLock<HashMap<i32, Vec<u8>>>>,
}

impl MemorySelfGroupKeyPackageStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn set(&self, id: i32, key_package_data: &[u8]) -> anyhow::Result<()> {
        let mut guard = self.data.write().await;
        guard.insert(id, key_package_data.to_vec());
        Ok(())
    }

    pub async fn get(&self, id: i32) -> anyhow::Result<Vec<u8>> {
        let guard = self.data.read().await;
        guard
            .get(&id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("key package not found"))
    }

    pub async fn delete(&self, id: i32) -> anyhow::Result<()> {
        let mut guard = self.data.write().await;
        guard.remove(&id);
        Ok(())
    }

    pub async fn delete_many(&self, ids: &[i32]) -> anyhow::Result<()> {
        let mut guard = self.data.write().await;
        for id in ids {
            guard.remove(id);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Conversation Store Trait & Memory Implementation
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
pub trait ConversationStorage: Send + Sync {
    async fn get_conversation(
        &self,
        username: &str,
    ) -> anyhow::Result<Option<ConversationSettings>>;
    async fn set_conversation(
        &self,
        username: &str,
        settings: ConversationSettings,
    ) -> anyhow::Result<()>;
}

#[derive(Clone, Default)]
pub struct MemoryConversationStore {
    data: Arc<RwLock<HashMap<String, ConversationSettings>>>,
}

impl MemoryConversationStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl ConversationStorage for MemoryConversationStore {
    async fn get_conversation(
        &self,
        username: &str,
    ) -> anyhow::Result<Option<ConversationSettings>> {
        let guard = self.data.read().await;
        Ok(guard.get(username).copied())
    }

    async fn set_conversation(
        &self,
        username: &str,
        settings: ConversationSettings,
    ) -> anyhow::Result<()> {
        let mut guard = self.data.write().await;
        guard.insert(username.to_string(), settings);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Signal Stores - Memory Implementation
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
pub struct MemoryPreKeyDb {
    data: Arc<RwLock<HashMap<u32, Vec<u8>>>>,
}

impl MemoryPreKeyDb {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn get_pre_key(&self, prekey_id: PreKeyId) -> Result<PreKeyRecord, SignalProtocolError> {
        let key = u32::from(prekey_id);
        let guard = self.data.read().await;
        let bytes = guard
            .get(&key)
            .ok_or(SignalProtocolError::InvalidPreKeyId)?;
        PreKeyRecord::deserialize(bytes)
    }

    pub async fn save_pre_key(
        &mut self,
        prekey_id: PreKeyId,
        record: &PreKeyRecord,
    ) -> Result<(), SignalProtocolError> {
        let key = u32::from(prekey_id);
        let bytes = record.serialize()?;
        let mut guard = self.data.write().await;
        guard.insert(key, bytes);
        Ok(())
    }

    pub async fn remove_pre_key(&mut self, prekey_id: PreKeyId) -> Result<(), SignalProtocolError> {
        let key = u32::from(prekey_id);
        let mut guard = self.data.write().await;
        guard.remove(&key);
        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl PreKeyStore for MemoryPreKeyDb {
    async fn get_pre_key(&self, prekey_id: PreKeyId) -> Result<PreKeyRecord, SignalProtocolError> {
        self.get_pre_key(prekey_id).await
    }

    async fn save_pre_key(
        &mut self,
        prekey_id: PreKeyId,
        record: &PreKeyRecord,
    ) -> Result<(), SignalProtocolError> {
        self.save_pre_key(prekey_id, record).await
    }

    async fn remove_pre_key(&mut self, prekey_id: PreKeyId) -> Result<(), SignalProtocolError> {
        self.remove_pre_key(prekey_id).await
    }
}

#[derive(Clone, Default)]
pub struct MemorySignedPreKeyDb {
    data: Arc<RwLock<HashMap<u32, Vec<u8>>>>,
}

impl MemorySignedPreKeyDb {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn get_signed_pre_key(
        &self,
        signed_prekey_id: SignedPreKeyId,
    ) -> Result<SignedPreKeyRecord, SignalProtocolError> {
        let key = u32::from(signed_prekey_id);
        let guard = self.data.read().await;
        let bytes = guard
            .get(&key)
            .ok_or(SignalProtocolError::InvalidSignedPreKeyId)?;
        SignedPreKeyRecord::deserialize(bytes)
    }

    pub async fn save_signed_pre_key(
        &mut self,
        signed_prekey_id: SignedPreKeyId,
        record: &SignedPreKeyRecord,
    ) -> Result<(), SignalProtocolError> {
        let key = u32::from(signed_prekey_id);
        let bytes = record.serialize()?;
        let mut guard = self.data.write().await;
        guard.insert(key, bytes);
        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl SignedPreKeyStore for MemorySignedPreKeyDb {
    async fn get_signed_pre_key(
        &self,
        signed_prekey_id: SignedPreKeyId,
    ) -> Result<SignedPreKeyRecord, SignalProtocolError> {
        self.get_signed_pre_key(signed_prekey_id).await
    }

    async fn save_signed_pre_key(
        &mut self,
        signed_prekey_id: SignedPreKeyId,
        record: &SignedPreKeyRecord,
    ) -> Result<(), SignalProtocolError> {
        self.save_signed_pre_key(signed_prekey_id, record).await
    }
}

#[derive(Clone, Default)]
pub struct MemoryKyberPreKeyDb {
    data: Arc<RwLock<HashMap<u32, Vec<u8>>>>,
}

impl MemoryKyberPreKeyDb {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn get_kyber_pre_key(
        &self,
        kyber_prekey_id: KyberPreKeyId,
    ) -> Result<KyberPreKeyRecord, SignalProtocolError> {
        let key = u32::from(kyber_prekey_id);
        let guard = self.data.read().await;
        let bytes = guard
            .get(&key)
            .ok_or(SignalProtocolError::InvalidKyberPreKeyId)?;
        KyberPreKeyRecord::deserialize(bytes)
    }

    pub async fn save_kyber_pre_key(
        &mut self,
        kyber_prekey_id: KyberPreKeyId,
        record: &KyberPreKeyRecord,
    ) -> Result<(), SignalProtocolError> {
        let key = u32::from(kyber_prekey_id);
        let bytes = record.serialize()?;
        let mut guard = self.data.write().await;
        guard.insert(key, bytes);
        Ok(())
    }

    pub async fn mark_kyber_pre_key_used(
        &mut self,
        _kyber_prekey_id: KyberPreKeyId,
        _ec_prekey_id: SignedPreKeyId,
        _base_key: &PublicKey,
    ) -> Result<(), SignalProtocolError> {
        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl KyberPreKeyStore for MemoryKyberPreKeyDb {
    async fn get_kyber_pre_key(
        &self,
        kyber_prekey_id: KyberPreKeyId,
    ) -> Result<KyberPreKeyRecord, SignalProtocolError> {
        self.get_kyber_pre_key(kyber_prekey_id).await
    }

    async fn save_kyber_pre_key(
        &mut self,
        kyber_prekey_id: KyberPreKeyId,
        record: &KyberPreKeyRecord,
    ) -> Result<(), SignalProtocolError> {
        self.save_kyber_pre_key(kyber_prekey_id, record).await
    }

    async fn mark_kyber_pre_key_used(
        &mut self,
        kyber_prekey_id: KyberPreKeyId,
        ec_prekey_id: SignedPreKeyId,
        base_key: &PublicKey,
    ) -> Result<(), SignalProtocolError> {
        self.mark_kyber_pre_key_used(kyber_prekey_id, ec_prekey_id, base_key).await
    }
}

#[derive(Clone, Default)]
pub struct MemorySessionDb {
    data: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl MemorySessionDb {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn load_session(
        &self,
        address: &ProtocolAddress,
    ) -> Result<Option<SessionRecord>, SignalProtocolError> {
        let guard = self.data.read().await;
        if let Some(bytes) = guard.get(&address.to_string()) {
            Ok(Some(SessionRecord::deserialize(bytes)?))
        } else {
            Ok(None)
        }
    }

    pub async fn store_session(
        &mut self,
        address: &ProtocolAddress,
        record: &SessionRecord,
    ) -> Result<(), SignalProtocolError> {
        let bytes = record.serialize()?;
        let mut guard = self.data.write().await;
        guard.insert(address.to_string(), bytes);
        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl SessionStore for MemorySessionDb {
    async fn load_session(
        &self,
        address: &ProtocolAddress,
    ) -> Result<Option<SessionRecord>, SignalProtocolError> {
        self.load_session(address).await
    }

    async fn store_session(
        &mut self,
        address: &ProtocolAddress,
        record: &SessionRecord,
    ) -> Result<(), SignalProtocolError> {
        self.store_session(address, record).await
    }
}

#[derive(Clone)]
pub struct IdentityKeyPairRow {
    pub id: i64,
    pub keypair: IdentityKeyPair,
    pub registration_id: u32,
    pub device_id: u8,
    pub username: String,
}

#[derive(Clone, Default)]
pub struct MemoryIdentityDb {
    row: Arc<RwLock<Option<IdentityKeyPairRow>>>,
    identities: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl MemoryIdentityDb {
    pub async fn new() -> anyhow::Result<Self> {
        let store = Self::default();
        let mut rng = utils::rng();
        let keypair = IdentityKeyPair::generate(&mut rng);
        let registration_id = rng.next_u32() % 32000;
        let device_id = 1 + (rng.next_u32() % 126) as u8;
        *store.row.write().await = Some(IdentityKeyPairRow {
            id: 0,
            keypair,
            registration_id,
            device_id,
            username: String::new(),
        });
        Ok(store)
    }

    pub async fn get_full_identity_key_pair(&self) -> anyhow::Result<IdentityKeyPairRow> {
        let guard = self.row.read().await;
        guard
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Identity key pair not found"))
    }

    pub async fn update_registration_for_keypair(
        &self,
        id: i64,
        username: &str,
        device_id: u8,
    ) -> anyhow::Result<()> {
        let mut guard = self.row.write().await;
        if let Some(ref mut row) = *guard {
            row.id = id;
            row.username = username.to_string();
            row.device_id = device_id;
        }
        Ok(())
    }

    pub async fn update_id_for_keypair(&self, id: i64, username: &str) -> anyhow::Result<()> {
        let mut guard = self.row.write().await;
        if let Some(ref mut row) = *guard {
            row.id = id;
            row.username = username.to_string();
        }
        Ok(())
    }

    pub async fn get_identity_key_pair(&self) -> Result<IdentityKeyPair, SignalProtocolError> {
        let row = self
            .get_full_identity_key_pair()
            .await
            .map_err(|e| SignalProtocolError::FfiBindingError(e.to_string()))?;
        Ok(row.keypair)
    }

    pub async fn get_local_registration_id(&self) -> Result<u32, SignalProtocolError> {
        let row = self
            .get_full_identity_key_pair()
            .await
            .map_err(|e| SignalProtocolError::FfiBindingError(e.to_string()))?;
        Ok(row.registration_id)
    }

    pub async fn save_identity(
        &mut self,
        address: &ProtocolAddress,
        identity: &IdentityKey,
    ) -> Result<IdentityChange, SignalProtocolError> {
        let key = address.to_string();
        let bytes = identity.serialize().to_vec();
        let mut guard = self.identities.write().await;
        let existing = guard.insert(key, bytes);
        Ok(IdentityChange::from_changed(existing.is_none()))
    }

    pub async fn is_trusted_identity(
        &self,
        _address: &ProtocolAddress,
        _identity: &IdentityKey,
        _direction: Direction,
    ) -> Result<bool, SignalProtocolError> {
        Ok(true)
    }

    pub async fn get_identity(
        &self,
        address: &ProtocolAddress,
    ) -> Result<Option<IdentityKey>, SignalProtocolError> {
        let key = address.to_string();
        let guard = self.identities.read().await;
        if let Some(bytes) = guard.get(&key) {
            Ok(Some(IdentityKey::decode(bytes)?))
        } else {
            Ok(None)
        }
    }
}

#[async_trait::async_trait(?Send)]
impl IdentityKeyStore for MemoryIdentityDb {
    async fn get_identity_key_pair(&self) -> Result<IdentityKeyPair, SignalProtocolError> {
        self.get_identity_key_pair().await
    }

    async fn get_local_registration_id(&self) -> Result<u32, SignalProtocolError> {
        self.get_local_registration_id().await
    }

    async fn save_identity(
        &mut self,
        address: &ProtocolAddress,
        identity: &IdentityKey,
    ) -> Result<IdentityChange, SignalProtocolError> {
        self.save_identity(address, identity).await
    }

    async fn is_trusted_identity(
        &self,
        address: &ProtocolAddress,
        identity: &IdentityKey,
        direction: Direction,
    ) -> Result<bool, SignalProtocolError> {
        self.is_trusted_identity(address, identity, direction).await
    }

    async fn get_identity(
        &self,
        address: &ProtocolAddress,
    ) -> Result<Option<IdentityKey>, SignalProtocolError> {
        self.get_identity(address).await
    }
}

// ---------------------------------------------------------------------------
// Memory Key Stores
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct MemoryKeyStores {
    pub identity_store: MemoryIdentityDb,
    pub session_store: MemorySessionDb,
    pub signed_prekey_store: MemorySignedPreKeyDb,
    pub prekey_store: MemoryPreKeyDb,
    pub kyber_key_store: MemoryKyberPreKeyDb,
    pub address_store: MemoryAddressStore,
    pub conversation_store: MemoryConversationStore,
}

impl MemoryKeyStores {
    pub async fn new() -> anyhow::Result<Self> {
        let identity_store = MemoryIdentityDb::new().await?;
        let session_store = MemorySessionDb::new();
        let signed_prekey_store = MemorySignedPreKeyDb::new();
        let prekey_store = MemoryPreKeyDb::new();
        let kyber_key_store = MemoryKyberPreKeyDb::new();
        let address_store = MemoryAddressStore::new();
        let conversation_store = MemoryConversationStore::new();

        Ok(Self {
            identity_store,
            session_store,
            signed_prekey_store,
            prekey_store,
            kyber_key_store,
            address_store,
            conversation_store,
        })
    }

    pub async fn decrypt(
        &mut self,
        other: ProtocolAddress,
        cipher_text: Vec<u8>,
        ty: u8,
    ) -> anyhow::Result<Vec<u8>> {
        let cipher_text_type = CiphertextMessageType::try_from(ty)?;
        let remote_address = other;
        let mut rng = utils::rng();

        let f = self.identity_store.get_full_identity_key_pair().await?;
        let local_address = ProtocolAddress::new(f.username.clone(), f.device_id.try_into()?);

        match cipher_text_type {
            CiphertextMessageType::Whisper => {
                let message = SignalMessage::try_from(cipher_text.as_ref())?;
                let decrypted = message_decrypt_signal(
                    &message,
                    &remote_address,
                    &local_address,
                    &mut self.session_store,
                    &mut self.identity_store,
                    &mut rng,
                )
                .await?;
                Ok(decrypted)
            }
            CiphertextMessageType::PreKey => {
                let message = PreKeySignalMessage::try_from(cipher_text.as_ref())?;
                let decrypted = message_decrypt_prekey(
                    &message,
                    &remote_address,
                    &local_address,
                    &mut self.session_store,
                    &mut self.identity_store,
                    &mut self.prekey_store,
                    &self.signed_prekey_store,
                    &mut self.kyber_key_store,
                    &mut rng,
                )
                .await?;
                Ok(decrypted)
            }
            _ => Err(anyhow::anyhow!("Invalid message type")),
        }
    }

    pub async fn encrypt(
        &mut self,
        other: ProtocolAddress,
        ptext: Vec<u8>,
    ) -> anyhow::Result<EncryptedMessage> {
        let f = self.identity_store.get_full_identity_key_pair().await?;
        let local_address = ProtocolAddress::new(f.username.clone(), f.device_id.try_into()?);
        let remote_address = other;
        let mut rng = utils::rng();

        let encrypted = message_encrypt(
            &ptext,
            &remote_address,
            &local_address,
            &mut self.session_store,
            &mut self.identity_store,
            crate::utils::now_system_time(),
            &mut rng,
        )
        .await?;

        Ok(EncryptedMessage {
            cipher_text: encrypted.serialize().to_vec(),
            ty: encrypted.message_type() as u8,
        })
    }

    pub async fn process_pre_key_bundle(
        &mut self,
        other: String,
        bundle: FfiPreKeyBundle,
    ) -> anyhow::Result<()> {
        let device_id = DeviceId::new(bundle.device_id)?;
        let remote_address = ProtocolAddress::new(other, device_id);

        let f = self.identity_store.get_full_identity_key_pair().await?;
        let local_address = ProtocolAddress::new(f.username.clone(), f.device_id.try_into()?);

        let bundle = PreKeyBundle::new(
            bundle.registration_id,
            device_id,
            Some((
                PreKeyId::from(bundle.pre_key_id),
                PublicKey::try_from(bundle.pre_key.as_ref())?,
            )),
            SignedPreKeyId::from(bundle.signed_pre_key_id),
            PublicKey::try_from(bundle.signed_pre_key_public.as_ref())?,
            bundle.signed_pre_key_signature,
            KyberPreKeyId::from(bundle.kyber_pre_key_id),
            kem::PublicKey::try_from(bundle.kyber_pre_key_public.as_ref())?,
            bundle.kyber_pre_key_signature,
            IdentityKey::decode(bundle.identity_key.as_ref())?,
        )?;

        process_prekey_bundle(
            &remote_address,
            &local_address,
            &mut self.session_store,
            &mut self.identity_store,
            &bundle,
            crate::utils::now_system_time(),
            &mut utils::rng(),
        )
        .await?;

        Ok(())
    }

    pub async fn generate_prekey_bundle(&mut self) -> anyhow::Result<FfiPreKeyBundle> {
        let mut rng = utils::rng();

        let full_identity_key_pair = self.identity_store.get_full_identity_key_pair().await?;
        let device_id = full_identity_key_pair.device_id;
        let identity_key_pair = full_identity_key_pair.keypair;
        let registration_id = full_identity_key_pair.registration_id;

        const MAX_KEY_ID: u32 = 32000;
        let pre_key_id = rng.next_u32() % MAX_KEY_ID;
        let kyber_key_id = rng.next_u32() % MAX_KEY_ID;
        let signed_pre_key_id = rng.next_u32() % MAX_KEY_ID;

        let pre_key = KeyPair::generate(&mut rng);
        let pre_key_record = PreKeyRecord::new(PreKeyId::from(pre_key_id), &pre_key);
        self.prekey_store
            .save_pre_key(PreKeyId::from(pre_key_id), &pre_key_record)
            .await?;

        let signed_pre_key = KeyPair::generate(&mut rng);
        let kyber_pre_key = kem::KeyPair::generate(KeyType::Kyber1024, &mut rng);

        let signed_pre_key_public = signed_pre_key.public_key.serialize();
        let signed_pre_key_signature = identity_key_pair
            .private_key()
            .calculate_signature(&signed_pre_key_public, &mut rng)?;

        let ts = Timestamp::from_epoch_millis(get_current_timestamp_millis_since_epoch());

        let signed_pre_key_record = SignedPreKeyRecord::new(
            SignedPreKeyId::from(signed_pre_key_id),
            ts,
            &signed_pre_key,
            signed_pre_key_signature.as_ref(),
        );

        self.signed_prekey_store
            .save_signed_pre_key(
                SignedPreKeyId::from(signed_pre_key_id),
                &signed_pre_key_record,
            )
            .await?;

        let kyber_pre_key_public = kyber_pre_key.public_key.serialize();
        let kyber_pre_key_signature = identity_key_pair
            .private_key()
            .calculate_signature(&kyber_pre_key_public, &mut rng)?;

        let kyber_pre_key_record = KyberPreKeyRecord::new(
            KyberPreKeyId::from(kyber_key_id),
            ts,
            &kyber_pre_key,
            kyber_pre_key_signature.as_ref(),
        );
        self.kyber_key_store
            .save_kyber_pre_key(KyberPreKeyId::from(kyber_key_id), &kyber_pre_key_record)
            .await?;

        Ok(FfiPreKeyBundle {
            registration_id,
            device_id,
            pre_key_id,
            pre_key: pre_key.public_key.serialize().into(),
            signed_pre_key_id,
            signed_pre_key_public: signed_pre_key_public.into(),
            signed_pre_key_signature: signed_pre_key_signature.into(),
            kyber_pre_key_id: kyber_key_id,
            kyber_pre_key_public: kyber_pre_key_public.into(),
            kyber_pre_key_signature: kyber_pre_key_signature.into(),
            identity_key: identity_key_pair.public_key().serialize().into(),
        })
    }
}
