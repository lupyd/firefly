use std::collections::HashMap;
use std::sync::Arc;

use mls_rs::psk::{ExternalPskId, PreSharedKey};
use mls_rs::{GroupStateStorage, KeyPackageStorage, PreSharedKeyStorage};
use mls_rs_codec::{MlsDecode, MlsEncode};
use mls_rs_core::group::{EpochRecord, GroupState};
use mls_rs_core::key_package::KeyPackageData;
use zeroize::Zeroizing;

use crate::FireflyError;

#[async_trait::async_trait]
pub trait MlsKeyPackageStorage: Send + Sync {
    async fn insert(&self, id: Vec<u8>, key_package_data: Vec<u8>) -> bool;

    async fn delete(&self, id: Vec<u8>) -> bool;

    async fn get(&self, id: Vec<u8>) -> Option<Vec<u8>>;
}

#[derive(Clone)]
pub struct FfiKeyPackageStorage {
    pub storage: Arc<dyn MlsKeyPackageStorage>,
}

impl FfiKeyPackageStorage {
    pub fn new(storage: impl Into<Arc<dyn MlsKeyPackageStorage>>) -> Self {
        Self {
            storage: storage.into(),
        }
    }
}

#[maybe_async::must_be_async]
impl KeyPackageStorage for FfiKeyPackageStorage {
    type Error = FireflyError;

    async fn delete(&mut self, id: &[u8]) -> Result<(), Self::Error> {
        self.storage.delete(id.to_vec()).await;
        Ok(())
    }

    async fn insert(&mut self, id: Vec<u8>, pkg: KeyPackageData) -> Result<(), Self::Error> {
        self.storage.insert(id, pkg.mls_encode_to_vec()?).await;
        Ok(())
    }

    async fn get(&self, id: &[u8]) -> Result<Option<KeyPackageData>, Self::Error> {
        let res = self.storage.get(id.to_vec()).await;
        if let Some(data) = res {
            Ok(Some(KeyPackageData::mls_decode(&mut &*data)?))
        } else {
            Ok(None)
        }
    }
}

#[async_trait::async_trait]
pub trait MlsPreSharedKeyStorage: Send + Sync {
    async fn get(&self, id: Vec<u8>) -> Option<Vec<u8>>;
}

#[derive(Clone)]
pub struct FfiPreSharedKeyStorage {
    pub storage: Arc<dyn MlsPreSharedKeyStorage>,
}

impl FfiPreSharedKeyStorage {
    pub fn new(storage: impl Into<Arc<dyn MlsPreSharedKeyStorage>>) -> Self {
        Self {
            storage: storage.into(),
        }
    }
}

#[maybe_async::must_be_async]
impl PreSharedKeyStorage for FfiPreSharedKeyStorage {
    type Error = FireflyError;

    async fn get(&self, id: &ExternalPskId) -> Result<Option<PreSharedKey>, Self::Error> {
        if let Some(data) = self.storage.get(id.to_vec()).await {
            Ok(Some(PreSharedKey::mls_decode(&mut &*data)?))
        } else {
            Ok(None)
        }
    }
}

#[async_trait::async_trait]
pub trait MlsGroupStateStorage: Send + Sync {
    async fn state(&self, group_id: Vec<u8>) -> Option<Zeroizing<Vec<u8>>>;
    async fn epoch(&self, group_id: Vec<u8>, epoch_id: u64) -> Option<Zeroizing<Vec<u8>>>;
    async fn write(
        &self,
        group_id: Vec<u8>,
        state_data: Zeroizing<Vec<u8>>,
        epoch_inserts: HashMap<u64, Zeroizing<Vec<u8>>>,
        epoch_updates: HashMap<u64, Zeroizing<Vec<u8>>>,
    ) -> bool;
    async fn max_epoch_id(&self, group_id: Vec<u8>) -> Option<u64>;
}

#[derive(Clone)]
pub struct FfiGroupStateStorage {
    pub storage: Arc<dyn MlsGroupStateStorage>,
}

impl FfiGroupStateStorage {
    pub fn new(storage: impl Into<Arc<dyn MlsGroupStateStorage>>) -> Self {
        Self {
            storage: storage.into(),
        }
    }
}

#[maybe_async::must_be_async]
impl GroupStateStorage for FfiGroupStateStorage {
    type Error = FireflyError;

    async fn state(&self, group_id: &[u8]) -> Result<Option<Zeroizing<Vec<u8>>>, Self::Error> {
        Ok(self.storage.state(group_id.to_vec()).await)
    }

    async fn epoch(
        &self,
        group_id: &[u8],
        epoch_id: u64,
    ) -> Result<Option<Zeroizing<Vec<u8>>>, Self::Error> {
        Ok(self.storage.epoch(group_id.to_vec(), epoch_id).await)
    }

    async fn write(
        &mut self,
        state: GroupState,
        epoch_inserts: Vec<EpochRecord>,
        epoch_updates: Vec<EpochRecord>,
    ) -> Result<(), Self::Error> {
        let inserts: HashMap<u64, Zeroizing<Vec<u8>>> =
            epoch_inserts.into_iter().map(|e| (e.id, e.data)).collect();
        let updates: HashMap<u64, Zeroizing<Vec<u8>>> =
            epoch_updates.into_iter().map(|e| (e.id, e.data)).collect();

        if self
            .storage
            .write(state.id, state.data, inserts, updates)
            .await
        {
            Ok(())
        } else {
            Err(FireflyError::Custom(
                "Failed to write group state".to_string(),
            ))
        }
    }

    async fn max_epoch_id(&self, group_id: &[u8]) -> Result<Option<u64>, Self::Error> {
        Ok(self.storage.max_epoch_id(group_id.to_vec()).await)
    }
}
