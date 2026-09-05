use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;
use rand::RngCore;
use libsignal_protocol::{kem::KeyType, *};
use crate::{
    EncryptedMessage, FfiPreKeyBundle,
    utils::{self, get_current_timestamp_millis_since_epoch},
};

#[async_trait::async_trait]
pub trait FireflyStorage: Send + Sync {
    async fn get(&self, table: &str, key: &str) -> Option<Vec<u8>>;
    async fn set(&self, table: &str, key: &str, value: Vec<u8>);
    async fn delete(&self, table: &str, key: &str);
    async fn get_all(&self, table: &str) -> Vec<(String, Vec<u8>)>;
}

#[derive(Clone, Default)]
pub struct MemoryStorage {
    data: Arc<RwLock<HashMap<String, HashMap<String, Vec<u8>>>>>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl FireflyStorage for MemoryStorage {
    async fn get(&self, table: &str, key: &str) -> Option<Vec<u8>> {
        let guard = self.data.read().await;
        guard.get(table).and_then(|t| t.get(key).cloned())
    }

    async fn set(&self, table: &str, key: &str, value: Vec<u8>) {
        let mut guard = self.data.write().await;
        guard
            .entry(table.to_string())
            .or_default()
            .insert(key.to_string(), value);
    }

    async fn delete(&self, table: &str, key: &str) {
        let mut guard = self.data.write().await;
        if let Some(t) = guard.get_mut(table) {
            t.remove(key);
        }
    }

    async fn get_all(&self, table: &str) -> Vec<(String, Vec<u8>)> {
        let guard = self.data.read().await;
        guard
            .get(table)
            .map(|t| t.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default()
    }
}

// ---------------------------------------------------------------------------
// MLS Storage Provider Adapters
// ---------------------------------------------------------------------------

use firefly_core::storage_provider::{
    MlsGroupStateStorage, MlsKeyPackageStorage, MlsPreSharedKeyStorage,
};
use zeroize::Zeroizing;

pub struct GenericMlsKeyPackageStorage {
    storage: Arc<dyn FireflyStorage>,
}

impl GenericMlsKeyPackageStorage {
    pub fn new(storage: Arc<dyn FireflyStorage>) -> Self {
        Self { storage }
    }
}

#[async_trait::async_trait]
impl MlsKeyPackageStorage for GenericMlsKeyPackageStorage {
    async fn insert(&self, id: Vec<u8>, key_package_data: Vec<u8>) -> bool {
        let hex_id = hex::encode(&id);
        self.storage.set("mls_key_packages", &hex_id, key_package_data).await;
        true
    }

    async fn delete(&self, id: Vec<u8>) -> bool {
        let hex_id = hex::encode(&id);
        self.storage.delete("mls_key_packages", &hex_id).await;
        true
    }

    async fn get(&self, id: Vec<u8>) -> Option<Vec<u8>> {
        let hex_id = hex::encode(&id);
        self.storage.get("mls_key_packages", &hex_id).await
    }
}

pub struct GenericMlsPreSharedKeyStorage {
    storage: Arc<dyn FireflyStorage>,
}

impl GenericMlsPreSharedKeyStorage {
    pub fn new(storage: Arc<dyn FireflyStorage>) -> Self {
        Self { storage }
    }
}

#[async_trait::async_trait]
impl MlsPreSharedKeyStorage for GenericMlsPreSharedKeyStorage {
    async fn get(&self, id: Vec<u8>) -> Option<Vec<u8>> {
        let hex_id = hex::encode(&id);
        self.storage.get("mls_psk", &hex_id).await
    }
}

pub struct GenericMlsGroupStateStorage {
    storage: Arc<dyn FireflyStorage>,
}

impl GenericMlsGroupStateStorage {
    pub fn new(storage: Arc<dyn FireflyStorage>) -> Self {
        Self { storage }
    }
}

#[async_trait::async_trait]
impl MlsGroupStateStorage for GenericMlsGroupStateStorage {
    async fn state(&self, group_id: Vec<u8>) -> Option<Zeroizing<Vec<u8>>> {
        let hex_id = hex::encode(&group_id);
        self.storage
            .get("mls_group_states", &hex_id)
            .await
            .map(Zeroizing::new)
    }

    async fn epoch(&self, group_id: Vec<u8>, epoch_id: u64) -> Option<Zeroizing<Vec<u8>>> {
        let key = format!("{}:{}", hex::encode(&group_id), epoch_id);
        self.storage
            .get("mls_epochs", &key)
            .await
            .map(Zeroizing::new)
    }

    async fn write(
        &self,
        group_id: Vec<u8>,
        state_data: Zeroizing<Vec<u8>>,
        epoch_inserts: HashMap<u64, Zeroizing<Vec<u8>>>,
        epoch_updates: HashMap<u64, Zeroizing<Vec<u8>>>,
    ) -> bool {
        let hex_id = hex::encode(&group_id);
        self.storage
            .set("mls_group_states", &hex_id, state_data.to_vec())
            .await;

        for (epoch_id, data) in epoch_inserts {
            let key = format!("{}:{}", hex_id, epoch_id);
            self.storage.set("mls_epochs", &key, data.to_vec()).await;
        }

        for (epoch_id, data) in epoch_updates {
            let key = format!("{}:{}", hex_id, epoch_id);
            self.storage.set("mls_epochs", &key, data.to_vec()).await;
        }

        true
    }

    async fn max_epoch_id(&self, group_id: Vec<u8>) -> Option<u64> {
        let prefix = format!("{}:", hex::encode(&group_id));
        let all = self.storage.get_all("mls_epochs").await;
        all.into_iter()
            .filter_map(|(k, _)| {
                if k.starts_with(&prefix) {
                    k[prefix.len()..].parse::<u64>().ok()
                } else {
                    None
                }
            })
            .max()
    }
}

// ---------------------------------------------------------------------------
// Generic Key-Value Store
// ---------------------------------------------------------------------------

pub const KEY_LAST_RECEIVED_MESSAGE_ID: &str = "last_received_message_id";
pub const KEY_LAST_RECEIVED_GROUP_MESSAGE_ID: &str = "last_received_group_message_id";
pub const KEY_FCM_TOKEN: &str = "fcm_token";

#[derive(Clone)]
pub struct GenericKeyValueStore {
    storage: Arc<dyn FireflyStorage>,
}

impl GenericKeyValueStore {
    pub fn new(storage: Arc<dyn FireflyStorage>) -> Self {
        Self { storage }
    }

    pub async fn get(&self, key: &str) -> anyhow::Result<String> {
        let val = self
            .storage
            .get("key_value_store", key)
            .await
            .ok_or_else(|| anyhow::anyhow!("key not found: {}", key))?;
        String::from_utf8(val).map_err(|e| anyhow::anyhow!(e))
    }

    pub async fn set(&self, key: &str, value: &str) -> anyhow::Result<()> {
        self.storage
            .set("key_value_store", key, value.as_bytes().to_vec())
            .await;
        Ok(())
    }

    pub async fn update_last_received_message_id(
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

// ---------------------------------------------------------------------------
// Generic Group Info Store
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct GenericGroupInfoStore {
    storage: Arc<dyn FireflyStorage>,
}

impl GenericGroupInfoStore {
    pub fn new(storage: Arc<dyn FireflyStorage>) -> Self {
        Self { storage }
    }

    pub async fn get_all(&self) -> anyhow::Result<Vec<GroupInfo>> {
        let rows = self.storage.get_all("group_infos").await;
        let mut list = Vec::new();
        for (_, bytes) in rows {
            if let Ok(info) = serde_json::from_slice::<GroupInfo>(&bytes) {
                list.push(info);
            }
        }
        Ok(list)
    }

    pub async fn get(&self, id: u64) -> anyhow::Result<GroupInfo> {
        let bytes = self
            .storage
            .get("group_infos", &id.to_string())
            .await
            .ok_or_else(|| anyhow::anyhow!("GroupInfo not found for {}", id))?;
        serde_json::from_slice(&bytes).map_err(|e| anyhow::anyhow!(e))
    }

    pub async fn set(
        &self,
        id: u64,
        name: String,
        description: String,
        group_state_id: Vec<u8>,
    ) -> anyhow::Result<()> {
        let info = GroupInfo {
            id,
            name,
            description,
            identifier: group_state_id,
        };
        let bytes = serde_json::to_vec(&info)?;
        self.storage.set("group_infos", &id.to_string(), bytes).await;
        Ok(())
    }

    pub async fn delete(&self, id: u64) -> anyhow::Result<()> {
        self.storage.delete("group_infos", &id.to_string()).await;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Generic Group Messages Store
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct GenericGroupMessagesStore {
    storage: Arc<dyn FireflyStorage>,
}

impl GenericGroupMessagesStore {
    pub fn new(storage: Arc<dyn FireflyStorage>) -> Self {
        Self { storage }
    }

    pub async fn add(
        &self,
        id: u64,
        group_id: u64,
        channel_id: u32,
        epoch: u32,
        by: &str,
        message: &[u8],
    ) -> anyhow::Result<()> {
        let msg = GroupMessage {
            id,
            group_id,
            by: by.to_string(),
            message: message.to_vec(),
            channel_id,
            epoch,
        };
        let key = format!("{}:{:020}", group_id, id);
        let bytes = serde_json::to_vec(&msg)?;
        self.storage.set("group_messages", &key, bytes).await;
        Ok(())
    }

    pub async fn get(
        &self,
        group_id: u64,
        start_before: u64,
        limit: u32,
    ) -> anyhow::Result<Vec<GroupMessage>> {
        let prefix = format!("{}:", group_id);
        let all = self.storage.get_all("group_messages").await;
        let mut matching: Vec<GroupMessage> = all
            .into_iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .filter_map(|(_, v)| serde_json::from_slice(&v).ok())
            .filter(|m: &GroupMessage| m.id < start_before)
            .collect();
        matching.sort_by(|a, b| b.id.cmp(&a.id));
        matching.truncate(limit as usize);
        Ok(matching)
    }

    pub async fn get_last_message_of_group(&self, group_id: u64) -> anyhow::Result<GroupMessage> {
        let res = self.get(group_id, u64::MAX, 1).await?;
        res.into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("no group messages"))
    }

    pub async fn delete_by_group_id(&self, group_id: u64) -> anyhow::Result<()> {
        let prefix = format!("{}:", group_id);
        let all = self.storage.get_all("group_messages").await;
        for (k, _) in all {
            if k.starts_with(&prefix) {
                self.storage.delete("group_messages", &k).await;
            }
        }
        Ok(())
    }

    pub async fn update_cursor(&self, id: u64, group_id: u64, epoch: u32) -> anyhow::Result<()> {
        let cursor = serde_json::json!({ "id": id, "epoch": epoch });
        let key = format!("cursor:{}", group_id);
        let bytes = serde_json::to_vec(&cursor)?;
        self.storage.set("group_messages_cursor", &key, bytes).await;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Generic User Messages Store
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct GenericMessagesStore {
    storage: Arc<dyn FireflyStorage>,
}

impl GenericMessagesStore {
    pub fn new(storage: Arc<dyn FireflyStorage>) -> Self {
        Self { storage }
    }

    pub async fn add(
        &self,
        id: u64,
        other: &str,
        message: &[u8],
        sent_by_other: bool,
    ) -> anyhow::Result<()> {
        let msg = UserMessage {
            id,
            other: other.to_string(),
            message: message.to_vec(),
            sent_by_other,
        };
        let key = format!("{}:{:020}", other, id);
        let bytes = serde_json::to_vec(&msg)?;
        self.storage.set("user_messages", &key, bytes).await;
        Ok(())
    }

    pub async fn get_last_messages_of(
        &self,
        other: &str,
        before: i64,
        limit: i64,
    ) -> anyhow::Result<Vec<UserMessage>> {
        let prefix = format!("{}:", other);
        let all = self.storage.get_all("user_messages").await;
        let before_u64 = if before < 0 { u64::MAX } else { before as u64 };
        let mut matching: Vec<UserMessage> = all
            .into_iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .filter_map(|(_, v)| serde_json::from_slice(&v).ok())
            .filter(|m: &UserMessage| m.id < before_u64)
            .collect();
        matching.sort_by(|a, b| b.id.cmp(&a.id));
        matching.truncate(limit as usize);
        Ok(matching)
    }
}

// ---------------------------------------------------------------------------
// Generic Address Store
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct GenericAddressStore {
    storage: Arc<dyn FireflyStorage>,
}

impl GenericAddressStore {
    pub fn new(storage: Arc<dyn FireflyStorage>) -> Self {
        Self { storage }
    }

    pub async fn add(&self, id: u64, username: &str, device_id: u8) -> anyhow::Result<()> {
        let item = AddressIdAndDeviceId {
            address_id: id,
            device_id,
            username: username.to_string(),
        };
        let bytes = serde_json::to_vec(&item)?;
        self.storage.set("addresses", &id.to_string(), bytes.clone()).await;
        let un_key = format!("{}:{}", username, device_id);
        self.storage.set("addresses_by_username", &un_key, bytes).await;
        Ok(())
    }

    pub async fn get(&self, username: &str) -> anyhow::Result<Vec<AddressIdAndDeviceId>> {
        let prefix = format!("{}:", username);
        let all = self.storage.get_all("addresses_by_username").await;
        let mut res = Vec::new();
        for (k, v) in all {
            if k.starts_with(&prefix) {
                if let Ok(item) = serde_json::from_slice(&v) {
                    res.push(item);
                }
            }
        }
        Ok(res)
    }

    pub async fn get_by_id(&self, id: u64) -> anyhow::Result<Option<AddressIdAndDeviceId>> {
        if let Some(bytes) = self.storage.get("addresses", &id.to_string()).await {
            Ok(Some(serde_json::from_slice(&bytes)?))
        } else {
            Ok(None)
        }
    }
}

// ---------------------------------------------------------------------------
// Generic Self Group KeyPackage Store
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct GenericSelfGroupKeyPackageStore {
    storage: Arc<dyn FireflyStorage>,
}

impl GenericSelfGroupKeyPackageStore {
    pub fn new(storage: Arc<dyn FireflyStorage>) -> Self {
        Self { storage }
    }

    pub async fn set(&self, id: i32, key_package_data: &[u8]) -> anyhow::Result<()> {
        self.storage
            .set("self_group_key_packages", &id.to_string(), key_package_data.to_vec())
            .await;
        Ok(())
    }

    pub async fn get(&self, id: i32) -> anyhow::Result<Vec<u8>> {
        self.storage
            .get("self_group_key_packages", &id.to_string())
            .await
            .ok_or_else(|| anyhow::anyhow!("key package not found"))
    }

    pub async fn delete(&self, id: i32) -> anyhow::Result<()> {
        self.storage
            .delete("self_group_key_packages", &id.to_string())
            .await;
        Ok(())
    }

    pub async fn delete_many(&self, ids: &[i32]) -> anyhow::Result<()> {
        for id in ids {
            self.delete(*id).await?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Generic Conversation Store
// ---------------------------------------------------------------------------

#[derive(Default, Clone, Copy, Debug)]
pub struct ConversationSettings {
    pub inner: u64,
}

impl ConversationSettings {
    pub fn new(settings: u64) -> Self {
        Self { inner: settings }
    }
}

#[derive(Clone)]
pub struct GenericConversationStore {
    storage: Arc<dyn FireflyStorage>,
}

impl GenericConversationStore {
    pub fn new(storage: Arc<dyn FireflyStorage>) -> Self {
        Self { storage }
    }

    pub async fn get_conversation(
        &self,
        username: &str,
    ) -> anyhow::Result<Option<ConversationSettings>> {
        if let Some(bytes) = self.storage.get("conversations", username).await {
            let val = String::from_utf8(bytes)?.parse::<u64>()?;
            Ok(Some(ConversationSettings::new(val)))
        } else {
            Ok(None)
        }
    }

    pub async fn set_conversation(
        &self,
        username: &str,
        settings: ConversationSettings,
    ) -> anyhow::Result<()> {
        self.storage
            .set(
                "conversations",
                username,
                settings.inner.to_string().into_bytes(),
            )
            .await;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Generic Signal Stores
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct GenericPreKeyDb {
    storage: Arc<dyn FireflyStorage>,
}

impl GenericPreKeyDb {
    pub fn new(storage: Arc<dyn FireflyStorage>) -> Self {
        Self { storage }
    }

    pub async fn get_pre_key(&self, prekey_id: PreKeyId) -> Result<PreKeyRecord, SignalProtocolError> {
        let key = u32::from(prekey_id).to_string();
        let bytes = self
            .storage
            .get("pre_keys", &key)
            .await
            .ok_or_else(|| SignalProtocolError::InvalidPreKeyId)?;
        PreKeyRecord::deserialize(&bytes)
    }

    pub async fn save_pre_key(
        &mut self,
        prekey_id: PreKeyId,
        record: &PreKeyRecord,
    ) -> Result<(), SignalProtocolError> {
        let key = u32::from(prekey_id).to_string();
        let bytes = record.serialize()?;
        self.storage.set("pre_keys", &key, bytes).await;
        Ok(())
    }

    pub async fn remove_pre_key(&mut self, prekey_id: PreKeyId) -> Result<(), SignalProtocolError> {
        let key = u32::from(prekey_id).to_string();
        self.storage.delete("pre_keys", &key).await;
        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl PreKeyStore for GenericPreKeyDb {
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

#[derive(Clone)]
pub struct GenericSignedPreKeyDb {
    storage: Arc<dyn FireflyStorage>,
}

impl GenericSignedPreKeyDb {
    pub fn new(storage: Arc<dyn FireflyStorage>) -> Self {
        Self { storage }
    }

    pub async fn get_signed_pre_key(
        &self,
        signed_prekey_id: SignedPreKeyId,
    ) -> Result<SignedPreKeyRecord, SignalProtocolError> {
        let key = u32::from(signed_prekey_id).to_string();
        let bytes = self
            .storage
            .get("signed_pre_keys", &key)
            .await
            .ok_or_else(|| SignalProtocolError::InvalidSignedPreKeyId)?;
        SignedPreKeyRecord::deserialize(&bytes)
    }

    pub async fn save_signed_pre_key(
        &mut self,
        signed_prekey_id: SignedPreKeyId,
        record: &SignedPreKeyRecord,
    ) -> Result<(), SignalProtocolError> {
        let key = u32::from(signed_prekey_id).to_string();
        let bytes = record.serialize()?;
        self.storage.set("signed_pre_keys", &key, bytes).await;
        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl SignedPreKeyStore for GenericSignedPreKeyDb {
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

#[derive(Clone)]
pub struct GenericKyberPreKeyDb {
    storage: Arc<dyn FireflyStorage>,
}

impl GenericKyberPreKeyDb {
    pub fn new(storage: Arc<dyn FireflyStorage>) -> Self {
        Self { storage }
    }

    pub async fn get_kyber_pre_key(
        &self,
        kyber_prekey_id: KyberPreKeyId,
    ) -> Result<KyberPreKeyRecord, SignalProtocolError> {
        let key = u32::from(kyber_prekey_id).to_string();
        let bytes = self
            .storage
            .get("kyber_pre_keys", &key)
            .await
            .ok_or_else(|| SignalProtocolError::InvalidKyberPreKeyId)?;
        KyberPreKeyRecord::deserialize(&bytes)
    }

    pub async fn save_kyber_pre_key(
        &mut self,
        kyber_prekey_id: KyberPreKeyId,
        record: &KyberPreKeyRecord,
    ) -> Result<(), SignalProtocolError> {
        let key = u32::from(kyber_prekey_id).to_string();
        let bytes = record.serialize()?;
        self.storage.set("kyber_pre_keys", &key, bytes).await;
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
impl KyberPreKeyStore for GenericKyberPreKeyDb {
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

#[derive(Clone)]
pub struct GenericSessionDb {
    storage: Arc<dyn FireflyStorage>,
}

impl GenericSessionDb {
    pub fn new(storage: Arc<dyn FireflyStorage>) -> Self {
        Self { storage }
    }

    pub async fn load_session(
        &self,
        address: &ProtocolAddress,
    ) -> Result<Option<SessionRecord>, SignalProtocolError> {
        if let Some(bytes) = self.storage.get("sessions", &address.to_string()).await {
            Ok(Some(SessionRecord::deserialize(&bytes)?))
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
        self.storage.set("sessions", &address.to_string(), bytes).await;
        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl SessionStore for GenericSessionDb {
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

#[derive(serde::Serialize, serde::Deserialize)]
struct StoredIdentityKeyPair {
    id: i64,
    keypair: Vec<u8>,
    registration_id: u32,
    device_id: u8,
    username: String,
}

#[derive(Clone)]
pub struct GenericIdentityDb {
    storage: Arc<dyn FireflyStorage>,
}

impl GenericIdentityDb {
    pub async fn new(storage: Arc<dyn FireflyStorage>) -> anyhow::Result<Self> {
        let store = Self { storage };
        if store.get_stored().await.is_none() {
            let mut rng = utils::rng();
            let keypair = IdentityKeyPair::generate(&mut rng);
            let registration_id = rng.next_u32() % 32000;
            let device_id = 1 + (rng.next_u32() % 126) as u8;
            let stored = StoredIdentityKeyPair {
                id: 0,
                keypair: keypair.serialize().to_vec(),
                registration_id,
                device_id,
                username: String::new(),
            };
            store.save_stored(&stored).await?;
        }
        Ok(store)
    }

    async fn get_stored(&self) -> Option<StoredIdentityKeyPair> {
        let bytes = self.storage.get("identity_keypair", "local").await?;
        serde_json::from_slice(&bytes).ok()
    }

    async fn save_stored(&self, stored: &StoredIdentityKeyPair) -> anyhow::Result<()> {
        let bytes = serde_json::to_vec(stored)?;
        self.storage.set("identity_keypair", "local", bytes).await;
        Ok(())
    }

    pub async fn get_full_identity_key_pair(&self) -> anyhow::Result<IdentityKeyPairRow> {
        let stored = self
            .get_stored()
            .await
            .ok_or_else(|| anyhow::anyhow!("Identity key pair not found"))?;
        Ok(IdentityKeyPairRow {
            id: stored.id,
            keypair: IdentityKeyPair::try_from(stored.keypair.as_slice())?,
            registration_id: stored.registration_id,
            device_id: stored.device_id,
            username: stored.username,
        })
    }

    pub async fn update_registration_for_keypair(
        &self,
        id: i64,
        username: &str,
        device_id: u8,
    ) -> anyhow::Result<()> {
        if let Some(mut stored) = self.get_stored().await {
            stored.id = id;
            stored.username = username.to_string();
            stored.device_id = device_id;
            self.save_stored(&stored).await?;
        }
        Ok(())
    }

    pub async fn update_id_for_keypair(&self, id: i64, username: &str) -> anyhow::Result<()> {
        if let Some(mut stored) = self.get_stored().await {
            stored.id = id;
            stored.username = username.to_string();
            self.save_stored(&stored).await?;
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
        let existing = self.storage.get("identities", &key).await;
        self.storage
            .set("identities", &key, identity.serialize().to_vec())
            .await;
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
        if let Some(bytes) = self.storage.get("identities", &key).await {
            Ok(Some(IdentityKey::decode(&bytes)?))
        } else {
            Ok(None)
        }
    }
}

#[async_trait::async_trait(?Send)]
impl IdentityKeyStore for GenericIdentityDb {
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
// Generic Key Stores
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct GenericKeyStores {
    pub identity_store: GenericIdentityDb,
    pub session_store: GenericSessionDb,
    pub signed_prekey_store: GenericSignedPreKeyDb,
    pub prekey_store: GenericPreKeyDb,
    pub kyber_key_store: GenericKyberPreKeyDb,
    pub address_store: GenericAddressStore,
    pub conversation_store: GenericConversationStore,
}

impl GenericKeyStores {
    pub async fn new(storage: Arc<dyn FireflyStorage>) -> anyhow::Result<Self> {
        let identity_store = GenericIdentityDb::new(storage.clone()).await?;
        let session_store = GenericSessionDb::new(storage.clone());
        let signed_prekey_store = GenericSignedPreKeyDb::new(storage.clone());
        let prekey_store = GenericPreKeyDb::new(storage.clone());
        let kyber_key_store = GenericKyberPreKeyDb::new(storage.clone());
        let address_store = GenericAddressStore::new(storage.clone());
        let conversation_store = GenericConversationStore::new(storage.clone());

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



