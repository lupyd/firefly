use std::time::SystemTime;

use libsignal_protocol::{kem::KeyType, *};
use rand::RngCore;
use sqlx::{SqlitePool, prelude::*};

use crate::{
    EncryptedMessage, FfiPreKeyBundle,
    db::{address::AddressStore, conversations::ConversationStore},
    utils::{self, get_current_timestamp_millis_since_epoch},
};

#[derive(Clone)]
pub struct PreKeyDb {
    pool: SqlitePool,
}

impl PreKeyDb {
    pub async fn new(pool: SqlitePool) -> anyhow::Result<Self> {
        let sql = "CREATE TABLE IF NOT EXISTS pre_keys (\n    id INTEGER PRIMARY KEY,\n    record BLOB NOT NULL\n)";
        let mut connection = pool.acquire().await?;
        for stmt in sql.split(';') {
            connection.execute(stmt).await?;
        }

        Ok(Self { pool })
    }

    pub async fn get_pre_key(
        &self,
        prekey_id: PreKeyId,
    ) -> Result<PreKeyRecord, SignalProtocolError> {
        let result = sqlx::query("SELECT record FROM pre_keys WHERE id = ?")
            .bind(u32::from(prekey_id))
            .fetch_one(&self.pool)
            .await
            .map_err(|err| SignalProtocolError::FfiBindingError(err.to_string()))?;
        let record: &[u8] = result
            .try_get(0)
            .map_err(|err| SignalProtocolError::FfiBindingError(err.to_string()))?;
        return Ok(PreKeyRecord::deserialize(record)?);
    }

    pub async fn save_pre_key(
        &mut self,
        prekey_id: PreKeyId,
        record: &PreKeyRecord,
    ) -> Result<(), SignalProtocolError> {
        log::info!("store insert: pre_key id={}", u32::from(prekey_id));
        sqlx::query("INSERT OR REPLACE INTO pre_keys (id, record) VALUES (?, ?)")
            .bind(u32::from(prekey_id))
            .bind(record.serialize()?)
            .execute(&self.pool)
            .await
            .map_err(|err| SignalProtocolError::FfiBindingError(err.to_string()))?;
        Ok(())
    }

    pub async fn remove_pre_key(&mut self, prekey_id: PreKeyId) -> Result<(), SignalProtocolError> {
        log::info!("store delete: pre_key id={}", u32::from(prekey_id));
        let _ = sqlx::query("DELETE FROM pre_keys WHERE id = ?")
            .bind(u32::from(prekey_id))
            .execute(&self.pool)
            .await
            .map_err(|err| SignalProtocolError::FfiBindingError(err.to_string()))?;

        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl PreKeyStore for PreKeyDb {
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
pub struct SignedPreKeyDb {
    pool: SqlitePool,
}

impl SignedPreKeyDb {
    pub async fn new(pool: SqlitePool) -> anyhow::Result<Self> {
        let sql = "CREATE TABLE IF NOT EXISTS signed_pre_keys (\n    id INTEGER PRIMARY KEY,\n    record BLOB NOT NULL\n)";
        let mut connection = pool.acquire().await?;
        for stmt in sql.split(';') {
            connection.execute(stmt).await?;
        }
        Ok(Self { pool })
    }

    pub async fn get_signed_pre_key(
        &self,
        signed_prekey_id: SignedPreKeyId,
    ) -> Result<SignedPreKeyRecord, SignalProtocolError> {
        let result = sqlx::query("SELECT record FROM signed_pre_keys WHERE id = ?")
            .bind(u32::from(signed_prekey_id))
            .fetch_one(&self.pool)
            .await
            .map_err(|err| SignalProtocolError::FfiBindingError(err.to_string()))?;
        let record: &[u8] = result
            .try_get(0)
            .map_err(|err| SignalProtocolError::FfiBindingError(err.to_string()))?;
        Ok(SignedPreKeyRecord::deserialize(record)?)
    }

    pub async fn save_signed_pre_key(
        &mut self,
        signed_prekey_id: SignedPreKeyId,
        record: &SignedPreKeyRecord,
    ) -> Result<(), SignalProtocolError> {
        log::info!(
            "store insert: signed_pre_key id={}",
            u32::from(signed_prekey_id)
        );
        let record = record.serialize()?;
        sqlx::query("INSERT OR REPLACE INTO signed_pre_keys (id, record) VALUES (?, ?)")
            .bind(u32::from(signed_prekey_id))
            .bind(&record)
            .execute(&self.pool)
            .await
            .map_err(|err| SignalProtocolError::FfiBindingError(err.to_string()))?;
        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl SignedPreKeyStore for SignedPreKeyDb {
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
pub struct SessionDb {
    pool: SqlitePool,
}

impl SessionDb {
    pub async fn new(pool: SqlitePool) -> anyhow::Result<Self> {
        let sql = "CREATE TABLE IF NOT EXISTS sessions (\n    address TEXT PRIMARY KEY,\n    record BLOB NOT NULL\n)";
        let mut connection = pool.acquire().await?;
        for stmt in sql.split(';') {
            connection.execute(stmt).await?;
        }
        Ok(Self { pool })
    }

    pub async fn load_session(
        &self,
        address: &ProtocolAddress,
    ) -> Result<Option<SessionRecord>, SignalProtocolError> {
        let result = sqlx::query("SELECT record FROM sessions WHERE address = ?")
            .bind(address.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(|err| SignalProtocolError::FfiBindingError(err.to_string()))?;

        log::info!(
            "store select: session address={}, found: {}",
            address,
            result.is_some()
        );
        match result {
            Some(row) => {
                let record: &[u8] = row
                    .try_get(0)
                    .map_err(|err| SignalProtocolError::FfiBindingError(err.to_string()))?;
                Ok(Some(SessionRecord::deserialize(record)?))
            }
            None => Ok(None),
        }
    }

    pub async fn store_session(
        &mut self,
        address: &ProtocolAddress,
        record: &SessionRecord,
    ) -> Result<(), SignalProtocolError> {
        log::info!("store insert: session address={}", address);
        sqlx::query("INSERT OR REPLACE INTO sessions (address, record) VALUES (?, ?)")
            .bind(address.to_string())
            .bind(record.serialize()?)
            .execute(&self.pool)
            .await
            .map_err(|err| SignalProtocolError::FfiBindingError(err.to_string()))?;
        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl SessionStore for SessionDb {
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

#[derive(sqlx::FromRow)]
pub struct IdentityKeyPairRow {
    pub id: i64,
    pub keypair: IdentityKeyPair,
    pub registration_id: u32,
    pub device_id: u8,
    pub username: String,
}

#[derive(Clone)]
pub struct IdentityDb {
    pool: SqlitePool,
}

impl IdentityDb {
    pub async fn new(pool: SqlitePool) -> anyhow::Result<Self> {
        let mut connection = pool.acquire().await?;
        let sql = r#"CREATE TABLE IF NOT EXISTS identities (
                address TEXT PRIMARY KEY,
                identity_key BLOB NOT NULL
            )"#;
        connection.execute(sql).await?;
        let sql = r#"CREATE TABLE IF NOT EXISTS identity_keypair (
                id INTEGER NOT NULL,
                keypair BLOB NOT NULL,
                registration_id INTEGER NOT NULL,
                device_id INTEGER NOT NULL
            )"#;
        connection.execute(sql).await?;

        // it will return error in case of duplicate column, just ignoring
        let sql = "ALTER TABLE identity_keypair ADD COLUMN username TEXT NOT NULL DEFAULT ''";
        let _ = connection.execute(sql).await;

        {
            connection
                .transaction(|txn| {
                    Box::pin(async move {
                        
                        let row = sqlx::query("SELECT keypair FROM identity_keypair")
                            .fetch_optional(&mut **txn)
                            .await?;

                        if row.is_none() {
                            let mut rng = utils::rng();
                            let keypair = IdentityKeyPair::generate(&mut rng);
                            let registration_id = rng.next_u32() % 32000;
                            let device_id = 1 + (rng.next_u32() % 126) as u8;

                            sqlx::query("INSERT INTO identity_keypair (id, keypair, registration_id, device_id, username) VALUES (?, ?, ?, ?, ?)")
                                .bind(0)
                                .bind(keypair.serialize())
                                .bind(registration_id)
                                .bind(device_id)
                                .bind("")
                                .execute(&mut **txn)

                                .await?;
                        }



                        Ok::<_, sqlx::Error>(())
                    })
                })
                .await?;
        }

        Ok(Self { pool })
    }

    pub async fn update_registration_for_keypair(
        &self,
        id: i64,
        username: &str,
        device_id: u8,
    ) -> anyhow::Result<()> {
        log::info!(
            "store update: identity_keypair id={} username={} device_id={}",
            id,
            username,
            device_id
        );
        let mut db = self.pool.acquire().await?;
        sqlx::query("UPDATE identity_keypair SET id = ?, username = ?, device_id = ?")
            .bind(id)
            .bind(username)
            .bind(device_id)
            .execute(&mut *db)
            .await?;

        log::info!("store delete: identity_keypair id!={}", id);
        sqlx::query("DELETE FROM identity_keypair WHERE id <> ?")
            .bind(id)
            .execute(&mut *db)
            .await?;
        Ok(())
    }

    pub async fn update_id_for_keypair(&self, id: i64, username: &str) -> anyhow::Result<()> {
        log::info!(
            "store update: identity_keypair id={} username={}",
            id,
            username
        );
        let mut db = self.pool.acquire().await?;
        sqlx::query("UPDATE identity_keypair SET id = ?, username = ?")
            .bind(id)
            .bind(username)
            .execute(&mut *db)
            .await?;

        log::info!("store delete: identity_keypair id!={}", id);
        sqlx::query("DELETE FROM identity_keypair WHERE id <> ?")
            .bind(id)
            .execute(&mut *db)
            .await?;
        Ok(())
    }

    pub async fn get_full_identity_key_pair(&self) -> anyhow::Result<IdentityKeyPairRow> {
        let row = sqlx::query(
            "SELECT id, keypair, registration_id, device_id, username FROM identity_keypair",
        )
        .fetch_one(&self.pool)
        .await?;
        let id: i64 = row.try_get(0)?;
        let keypair: &[u8] = row.try_get(1)?;
        let registration_id: u32 = row.try_get(2)?;
        let device_id: u8 = row.try_get(3)?;
        let username: String = row.try_get(4)?;

        let row = IdentityKeyPairRow {
            id,
            keypair: IdentityKeyPair::try_from(keypair)?,
            registration_id,
            device_id,
            username,
        };

        Ok(row)
    }

    pub async fn get_identity_key_pair(&self) -> Result<IdentityKeyPair, SignalProtocolError> {
        let row = self
            .get_full_identity_key_pair()
            .await
            .map_err(|err| SignalProtocolError::FfiBindingError(err.to_string()))?;
        Ok(row.keypair)
    }

    pub async fn get_local_registration_id(&self) -> Result<u32, SignalProtocolError> {
        let row = self
            .get_full_identity_key_pair()
            .await
            .map_err(|err| SignalProtocolError::FfiBindingError(err.to_string()))?;
        Ok(row.registration_id)
    }

    pub async fn save_identity(
        &mut self,
        address: &ProtocolAddress,
        identity: &IdentityKey,
    ) -> Result<IdentityChange, SignalProtocolError> {
        log::info!("store insert: identity address={}", address);
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|err| SignalProtocolError::FfiBindingError(err.to_string()))?;

        let existing = sqlx::query("SELECT identity_key FROM identities WHERE address = ?")
            .bind(address.to_string())
            .fetch_optional(&mut *tx)
            .await
            .map_err(|err| SignalProtocolError::FfiBindingError(err.to_string()))?;

        sqlx::query("INSERT OR REPLACE INTO identities (address, identity_key) VALUES (?, ?)")
            .bind(address.to_string())
            .bind(identity.serialize())
            .execute(&mut *tx)
            .await
            .map_err(|err| SignalProtocolError::FfiBindingError(err.to_string()))?;

        tx.commit()
            .await
            .map_err(|err| SignalProtocolError::FfiBindingError(err.to_string()))?;

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
        let result = sqlx::query("SELECT identity_key FROM identities WHERE address = ?")
            .bind(address.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(|err| SignalProtocolError::FfiBindingError(err.to_string()))?;
        match result {
            Some(row) => {
                let key: &[u8] = row
                    .try_get(0)
                    .map_err(|err| SignalProtocolError::FfiBindingError(err.to_string()))?;
                Ok(Some(IdentityKey::decode(key)?))
            }
            None => Ok(None),
        }
    }
}

#[async_trait::async_trait(?Send)]
impl IdentityKeyStore for IdentityDb {
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

pub struct SenderKeyDb {
    #[allow(unused)]
    pool: SqlitePool,
}

impl SenderKeyDb {
    pub async fn new(pool: SqlitePool) -> anyhow::Result<Self> {
        let sql = "CREATE TABLE IF NOT EXISTS sender_keys (\n    sender_id TEXT NOT NULL,\n    distribution_id TEXT NOT NULL,\n    record BLOB NOT NULL,\n    PRIMARY KEY (sender_id, distribution_id)\n)";
        let mut connection = pool.acquire().await?;
        for stmt in sql.split(';') {
            connection.execute(stmt).await?;
        }
        Ok(Self { pool })
    }
}

#[derive(Clone)]
pub struct KyberPreKeyDb {
    pool: SqlitePool,
}

impl KyberPreKeyDb {
    pub async fn new(pool: SqlitePool) -> anyhow::Result<Self> {
        let sql = "CREATE TABLE IF NOT EXISTS kyber_pre_keys (\n    id INTEGER PRIMARY KEY,\n    record BLOB NOT NULL\n)";
        let mut connection = pool.acquire().await?;
        for stmt in sql.split(';') {
            connection.execute(stmt).await?;
        }
        Ok(Self { pool })
    }

    pub async fn get_kyber_pre_key(
        &self,
        kyber_prekey_id: KyberPreKeyId,
    ) -> Result<KyberPreKeyRecord, SignalProtocolError> {
        let result = sqlx::query("SELECT record FROM kyber_pre_keys WHERE id = ?")
            .bind(u32::from(kyber_prekey_id))
            .fetch_one(&self.pool)
            .await
            .map_err(|err| SignalProtocolError::FfiBindingError(err.to_string()))?;
        let record: &[u8] = result
            .try_get(0)
            .map_err(|err| SignalProtocolError::FfiBindingError(err.to_string()))?;
        Ok(KyberPreKeyRecord::deserialize(record)?)
    }

    pub async fn save_kyber_pre_key(
        &mut self,
        kyber_prekey_id: KyberPreKeyId,
        record: &KyberPreKeyRecord,
    ) -> Result<(), SignalProtocolError> {
        log::info!(
            "store insert: kyber_pre_key id={}",
            u32::from(kyber_prekey_id)
        );
        sqlx::query("INSERT OR REPLACE INTO kyber_pre_keys (id, record) VALUES (?, ?)")
            .bind(u32::from(kyber_prekey_id))
            .bind(record.serialize()?)
            .execute(&self.pool)
            .await
            .map_err(|err| SignalProtocolError::FfiBindingError(err.to_string()))?;
        Ok(())
    }

    pub async fn mark_kyber_pre_key_used(
        &mut self,
        _kyber_prekey_id: KyberPreKeyId,
        _ec_prekey_id: SignedPreKeyId,
        _base_key: &PublicKey,
    ) -> Result<(), SignalProtocolError> {
        // log::info!(
        //     "store delete: kyber_pre_key id={}",
        //     u32::from(kyber_prekey_id)
        // );
        // sqlx::query("DELETE FROM kyber_pre_keys WHERE id = ?")
        //     .bind(u32::from(kyber_prekey_id))
        //     .execute(&self.pool)
        //     .await
        //     .map_err(|err| SignalProtocolError::FfiBindingError(err.to_string()))?;
        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl KyberPreKeyStore for KyberPreKeyDb {
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
        self.mark_kyber_pre_key_used(kyber_prekey_id, ec_prekey_id, base_key)
            .await
    }
}

#[derive(Clone)]
pub struct KeyStores {
    pub identity_store: IdentityDb,
    pub session_store: SessionDb,
    pub signed_prekey_store: SignedPreKeyDb,
    pub prekey_store: PreKeyDb,
    pub kyber_key_store: KyberPreKeyDb,
    pub address_store: AddressStore,
    pub conversation_store: ConversationStore,
}

impl KeyStores {
    pub async fn new(pool: SqlitePool) -> anyhow::Result<Self> {
        let identity_store = IdentityDb::new(pool.clone()).await?;
        let session_store = SessionDb::new(pool.clone()).await?;
        let signed_prekey_store = SignedPreKeyDb::new(pool.clone()).await?;
        let prekey_store = PreKeyDb::new(pool.clone()).await?;
        let kyber_key_store = KyberPreKeyDb::new(pool.clone()).await?;
        let address_store = AddressStore::new(pool.clone()).await?;
        let conversation_store = ConversationStore::new(pool.clone()).await?;

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

                return Ok(decrypted);
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
                    &mut self.signed_prekey_store,
                    &mut self.kyber_key_store,
                    &mut rng,
                )
                .await?;

                return Ok(decrypted);
            }

            _ => return Err(anyhow::anyhow!("Invalid message type")),
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
            SystemTime::now(),
            &mut rng,
        )
        .await?;

        log::info!(
            "Encrypted message type: {} for {}",
            encrypted.message_type() as u8,
            remote_address
        );

        return Ok(EncryptedMessage {
            cipher_text: encrypted.serialize().to_vec(),
            ty: encrypted.message_type() as u8,
        });
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
            SystemTime::now(),
            &mut utils::rng(),
        )
        .await?;

        Ok(())
    }

    pub async fn generate_prekey_bundle(&mut self) -> anyhow::Result<FfiPreKeyBundle> {
        let mut rng = utils::rng();

        let full_identity_key_pair = self.identity_store.get_full_identity_key_pair().await?;
        let device_id = full_identity_key_pair.device_id.try_into()?;
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
            registration_id: registration_id,
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

#[cfg(test)]
mod tests {
    use crate::{db::setup_pool, utils::get_current_timestamp_millis_since_epoch};

    use super::*;

    // const DB_URI: &str ="sqlite:file:/tmp/stores.db?mode=rwc";
    const DB_URI: &str = ":memory:";

    #[tokio::test]
    async fn test_prekey_store() {
        let pool = setup_pool(DB_URI, 1).await.unwrap();
        let mut store = PreKeyDb::new(pool).await.unwrap();
        let key_pair = KeyPair::generate(&mut utils::rng());
        let record = PreKeyRecord::new(PreKeyId::from(1), &key_pair);
        store
            .save_pre_key(PreKeyId::from(1), &record)
            .await
            .unwrap();
        let retrieved = store.get_pre_key(PreKeyId::from(1)).await.unwrap();
        assert_eq!(record.serialize().unwrap(), retrieved.serialize().unwrap());
        store.remove_pre_key(PreKeyId::from(1)).await.unwrap();
    }

    #[tokio::test]
    async fn test_signed_prekey_store() {
        let pool = setup_pool(DB_URI, 1).await.unwrap();
        let mut store = SignedPreKeyDb::new(pool).await.unwrap();
        let key_pair = KeyPair::generate(&mut utils::rng());
        let signature = vec![0u8; 64];
        let record = SignedPreKeyRecord::new(
            SignedPreKeyId::from(1),
            Timestamp::from_epoch_millis(get_current_timestamp_millis_since_epoch()),
            &key_pair,
            &signature,
        );
        store
            .save_signed_pre_key(SignedPreKeyId::from(1), &record)
            .await
            .unwrap();
        let retrieved = store
            .get_signed_pre_key(SignedPreKeyId::from(1))
            .await
            .unwrap();
        assert_eq!(record.serialize().unwrap(), retrieved.serialize().unwrap());
    }

    #[tokio::test]
    async fn test_session_store() {
        let pool = setup_pool(DB_URI, 1).await.unwrap();
        let mut store = SessionDb::new(pool).await.unwrap();
        let address = ProtocolAddress::new("+14155551234".to_string(), DeviceId::new(1).unwrap());
        let record = SessionRecord::new_fresh();
        store.store_session(&address, &record).await.unwrap();
        let retrieved = store.load_session(&address).await.unwrap();
        assert!(retrieved.is_some());
    }

    #[tokio::test]
    async fn test_identity_store() {
        let pool = setup_pool(DB_URI, 1).await.unwrap();
        let store = IdentityDb::new(pool.clone()).await.unwrap();

        let _ = store.get_full_identity_key_pair().await.unwrap();
    }

    #[tokio::test]
    async fn test_kyber_prekey_store() {
        let pool = setup_pool(DB_URI, 1).await.unwrap();
        let mut store = KyberPreKeyDb::new(pool).await.unwrap();
        let key_pair = kem::KeyPair::generate(kem::KeyType::Kyber1024, &mut utils::rng());
        let signature = vec![0u8; 64];
        let record = KyberPreKeyRecord::new(
            KyberPreKeyId::from(1),
            Timestamp::from_epoch_millis(get_current_timestamp_millis_since_epoch()),
            &key_pair,
            &signature,
        );
        store
            .save_kyber_pre_key(KyberPreKeyId::from(1), &record)
            .await
            .unwrap();
        let retrieved = store
            .get_kyber_pre_key(KyberPreKeyId::from(1))
            .await
            .unwrap();
        assert_eq!(record.serialize().unwrap(), retrieved.serialize().unwrap());
        let key = IdentityKeyPair::generate(&mut utils::rng());
        let public_key = key.public_key();
        store
            .mark_kyber_pre_key_used(KyberPreKeyId::from(1), SignedPreKeyId::from(1), public_key)
            .await
            .unwrap();
    }

    async fn test_encryption(
        user1: &mut KeyStores,
        user1_name: &str,
        user2: &mut KeyStores,
        user2_name: &str,
        user2_pre_key_bundle: FfiPreKeyBundle,
    ) {
        let alice = user1;
        let bob = user2;
        let bob_device_id = bob
            .identity_store
            .get_full_identity_key_pair()
            .await
            .unwrap()
            .device_id;
        let alice_device_id = alice
            .identity_store
            .get_full_identity_key_pair()
            .await
            .unwrap()
            .device_id;

        let bob_address =
            ProtocolAddress::new(user2_name.to_string(), bob_device_id.try_into().unwrap());
        let alice_address =
            ProtocolAddress::new(user1_name.to_string(), alice_device_id.try_into().unwrap());

        let bob_bundle = user2_pre_key_bundle;
        alice
            .process_pre_key_bundle(user2_name.into(), bob_bundle)
            .await
            .unwrap();

        let msg1 = alice
            .encrypt(bob_address.clone(), b"Hello Bob".to_vec())
            .await
            .unwrap();
        println!("msg1: len={}, ty={}", msg1.cipher_text.len(), msg1.ty);
        let decrypted1 = bob
            .decrypt(alice_address.clone(), msg1.cipher_text, msg1.ty)
            .await
            .unwrap();
        assert_eq!(decrypted1, b"Hello Bob");

        let msg2 = bob
            .encrypt(alice_address.clone(), b"Hi Alice".to_vec())
            .await
            .unwrap();
        println!("msg2: len={}, ty={}", msg2.cipher_text.len(), msg2.ty);
        let decrypted2 = alice
            .decrypt(bob_address.clone(), msg2.cipher_text, msg2.ty)
            .await
            .unwrap();
        assert_eq!(decrypted2, b"Hi Alice");

        let msg3 = alice
            .encrypt(bob_address.clone(), b"How are you?".to_vec())
            .await
            .unwrap();
        println!("msg3: len={}, ty={}", msg3.cipher_text.len(), msg3.ty);
        let decrypted3 = bob
            .decrypt(alice_address.clone(), msg3.cipher_text, msg3.ty)
            .await
            .unwrap();
        assert_eq!(decrypted3, b"How are you?");
    }

    #[tokio::test]
    async fn test_alice_bob_encryption() {
        let _ = env_logger::Builder::from_default_env()
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
            .try_init();

        let alice_pool = setup_pool(DB_URI, 1).await.unwrap();
        let bob_pool = setup_pool(DB_URI, 1).await.unwrap();
        let charles_pool = setup_pool(DB_URI, 1).await.unwrap();

        let mut alice = KeyStores::new(alice_pool).await.unwrap();
        let mut bob = KeyStores::new(bob_pool).await.unwrap();
        let mut charles = KeyStores::new(charles_pool).await.unwrap();
        let bob_bundle = bob.generate_prekey_bundle().await.unwrap();
        let bob_bundle2 = bob.generate_prekey_bundle().await.unwrap();

        test_encryption(&mut charles, "charles", &mut bob, "bob", bob_bundle.clone()).await;
        test_encryption(&mut alice, "alice", &mut bob, "bob", bob_bundle2.clone()).await;
    }
}
