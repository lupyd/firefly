//! Password-encrypted, account-wide direct chat archives (protobuf -> zstd -> AES-GCM).
//! Passwords never leave the client. Local receipt IDs are not content identities.
use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit, Payload},
};
use argon2::{Algorithm, Argon2, Params, Version};
use firefly_protos::{
    deserialize_proto,
    firefly::{self, mod_DirectBackupRequest::Action},
    serialize_proto,
};
use rand::RngCore;
use sha2::{Digest, Sha256};
use sqlx::{Row, SqlitePool};
use std::{
    collections::{HashMap, HashSet},
    io::Read,
};
use zeroize::Zeroizing;
pub type SecretText = Zeroizing<String>;
use crate::db::messages::UserMessage;

const MAX_PLAIN: usize = 32 * 1024 * 1024;
const MAX_ARCHIVE: usize = 256 * 1024 * 1024;
const CHECK: &[u8; 20] = b"firefly-dm-backup-v1";
type Manifest = firefly::DirectBackupManifest<'static>;
type Chunk = firefly::DirectBackupChunk<'static>;

#[derive(Clone)]
pub struct Unlock {
    pub key: Zeroizing<[u8; 32]>,
    pub epoch: String,
}
pub struct BackupState {
    pub revision: i64,
    pub updated_at: i64,
    pub manifest: Option<Manifest>,
}
pub struct Remote<'a> {
    pub url: &'a str,
    pub cdn_url: &'a str,
    pub token: &'a str,
    pub username: &'a str,
}
fn random_id() -> String {
    let mut b = [0; 16];
    rand::rng().fill_bytes(&mut b);
    hex::encode(b)
}
pub fn derive(password: &str, salt: &[u8]) -> anyhow::Result<Zeroizing<[u8; 32]>> {
    anyhow::ensure!(
        salt.len() == 16 && !password.is_empty() && password.len() <= 1024,
        "Invalid backup password or salt"
    );
    let params = Params::new(65536, 3, 1, Some(32))
        .map_err(|_| anyhow::anyhow!("Invalid KDF parameters"))?;
    let mut key = Zeroizing::new([0; 32]);
    Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
        .hash_password_into(password.as_bytes(), salt, key.as_mut())
        .map_err(|_| anyhow::anyhow!("Password derivation failed"))?;
    Ok(key)
}
fn binding(user: &str, epoch: &str, chunk: Option<&Chunk>) -> anyhow::Result<Vec<u8>> {
    Ok(serialize_proto(&firefly::DirectBackupBinding {
        version: 1,
        username: user.into(),
        epoch: epoch.into(),
        chunk_id: chunk.map(|c| c.id.as_ref()).unwrap_or("key-check").into(),
        start: chunk.map(|c| c.start).unwrap_or(0),
        count: chunk.map(|c| c.count).unwrap_or(0),
    })?
    .to_vec())
}
fn seal(key: &[u8], plain: &[u8], aad: &[u8]) -> anyhow::Result<Vec<u8>> {
    let cipher = Aes256Gcm::new_from_slice(key)?;
    let mut nonce = [0; 12];
    rand::rng().fill_bytes(&mut nonce);
    let encrypted = cipher
        .encrypt(Nonce::from_slice(&nonce), Payload { msg: plain, aad })
        .map_err(|_| anyhow::anyhow!("Backup encryption failed"))?;
    Ok([nonce.as_slice(), encrypted.as_slice()].concat())
}
fn open(key: &[u8], blob: &[u8], aad: &[u8]) -> anyhow::Result<Vec<u8>> {
    anyhow::ensure!(
        (28..=MAX_PLAIN + 65536).contains(&blob.len()),
        "Invalid backup size"
    );
    Aes256Gcm::new_from_slice(key)?
        .decrypt(
            Nonce::from_slice(&blob[..12]),
            Payload {
                msg: &blob[12..],
                aad,
            },
        )
        .map_err(|_| anyhow::anyhow!("Incorrect backup password or damaged backup"))
}
fn record(m: &UserMessage) -> firefly::DirectBackupRecord<'_> {
    firefly::DirectBackupRecord {
        source_id: m.id,
        other: m.other.as_str().into(),
        sent_by_other: m.sent_by_other,
        message: m.message.as_slice().into(),
        message_type: m.message_type,
    }
}
pub fn fingerprint(m: &UserMessage) -> anyhow::Result<[u8; 32]> {
    let mut r = record(m);
    r.source_id = 0;
    r.message_type = 0;
    // Canonical protobuf removes field-order differences; retain send nonce.
    if let Ok(mut inner) = deserialize_proto::<firefly::UserMessageInner>(&m.message) {
        if matches!(
            inner.message,
            firefly::mod_UserMessageInner::OneOfmessage::None
        ) {
            return Ok(Sha256::digest(serialize_proto(&r)?).into());
        }
        inner.message_type = 0;
        if let firefly::mod_UserMessageInner::OneOfmessage::messagePayload(ref mut p) =
            inner.message
        {
            p.message_type = 0;
        }
        r.message = serialize_proto(&inner)?.to_vec().into();
    }
    Ok(Sha256::digest(serialize_proto(&r)?).into())
}
pub fn pack(
    user: &str,
    epoch: &str,
    chunk: &Chunk,
    key: &[u8],
    messages: &[UserMessage],
) -> anyhow::Result<Vec<u8>> {
    anyhow::ensure!(
        !messages.is_empty() && messages.len() <= 1000 && messages.len() == chunk.count as usize,
        "Invalid chunk count"
    );
    let estimated = messages.iter().try_fold(0usize, |n, m| {
        n.checked_add(m.message.len())?
            .checked_add(m.other.len())?
            .checked_add(64)
    });
    anyhow::ensure!(
        estimated.is_some_and(|n| n <= MAX_PLAIN),
        "Backup chunk exceeds plaintext limit"
    );
    let plain = serialize_proto(&firefly::DirectBackupRecords {
        version: 1,
        records: messages.iter().map(record).collect(),
    })?;
    anyhow::ensure!(plain.len() <= MAX_PLAIN, "Backup plaintext too large");
    seal(
        key,
        &zstd::stream::encode_all(plain.as_ref(), 3)?,
        &binding(user, epoch, Some(chunk))?,
    )
}
pub fn unpack(
    user: &str,
    epoch: &str,
    chunk: &Chunk,
    key: &[u8],
    blob: &[u8],
) -> anyhow::Result<Vec<UserMessage>> {
    anyhow::ensure!((1..=1000).contains(&chunk.count), "Invalid chunk count");
    let compressed = open(key, blob, &binding(user, epoch, Some(chunk))?)?;
    let mut plain = Vec::new();
    zstd::stream::read::Decoder::new(compressed.as_slice())?
        .take(MAX_PLAIN as u64 + 1)
        .read_to_end(&mut plain)?;
    anyhow::ensure!(
        plain.len() <= MAX_PLAIN,
        "Backup decompression limit exceeded"
    );
    let records = deserialize_proto::<firefly::DirectBackupRecords>(&plain)?;
    anyhow::ensure!(
        records.version == 1 && records.records.len() == chunk.count as usize,
        "Invalid backup records"
    );
    let mut seen = HashSet::new();
    records
        .records
        .into_iter()
        .map(|r| {
            anyhow::ensure!(
                !r.other.is_empty() && r.other.len() <= 256 && r.source_id <= i64::MAX as u64,
                "Invalid backup record"
            );
            let m = UserMessage {
                id: r.source_id,
                other: r.other.into_owned(),
                sent_by_other: r.sent_by_other,
                message: r.message.into_owned(),
                message_type: r.message_type,
            };
            anyhow::ensure!(seen.insert(fingerprint(&m)?), "Duplicate backup content");
            Ok(m)
        })
        .collect()
}
fn own(m: firefly::DirectBackupManifest<'_>) -> Manifest {
    Manifest {
        version: m.version,
        epoch: m.epoch.into_owned().into(),
        salt: m.salt.into_owned().into(),
        verifier: m.verifier.into_owned().into(),
        schedule_days: m.schedule_days,
        authentication: m.authentication.into_owned().into(),
        chunks: m
            .chunks
            .into_iter()
            .map(|c| Chunk {
                id: c.id.into_owned().into(),
                start: c.start,
                count: c.count,
                blob_path: c.blob_path.into_owned().into(),
                byte_size: c.byte_size,
                ciphertext_hash: c.ciphertext_hash.into_owned().into(),
            })
            .collect(),
    }
}
fn validate(m: &Manifest) -> anyhow::Result<()> {
    anyhow::ensure!(
        [1, 2].contains(&m.version)
            && m.epoch.len() == 32
            && m.salt.len() == 16
            && m.verifier.len() == 48
            && m.authentication.len() == 60
            && [7, 30].contains(&m.schedule_days)
            && m.chunks.len() <= 10000,
        "Unsupported backup format"
    );
    let mut start = 0;
    let mut ids = HashSet::new();
    for (i, c) in m.chunks.iter().enumerate() {
        anyhow::ensure!(
            c.id.len() == 32
                && c.id.bytes().all(|b| b.is_ascii_hexdigit())
                && ids.insert(&c.id)
                && c.start == start
                && (1..=1000).contains(&c.count)
                && (i + 1 == m.chunks.len() || c.count == 1000),
            "Invalid backup manifest ranges"
        );
        start += u64::from(c.count);
    }
    Ok(())
}
fn manifest_digest(m: &Manifest) -> anyhow::Result<Vec<u8>> {
    let mut bare = m.clone();
    bare.authentication = Vec::new().into();
    Ok(Sha256::digest(serialize_proto(&bare)?).to_vec())
}
fn manifest_binding(user: &str, m: &Manifest) -> anyhow::Result<Vec<u8>> {
    binding(
        user,
        &m.epoch,
        Some(&Chunk {
            id: "manifest".into(),
            start: 0,
            count: 0,
            blob_path: String::new().into(),
            byte_size: 0,
            ciphertext_hash: Vec::new().into(),
        }),
    )
}
fn authenticate(user: &str, m: &mut Manifest, key: &[u8]) -> anyhow::Result<()> {
    m.authentication = seal(key, &manifest_digest(m)?, &manifest_binding(user, m)?)?.into();
    Ok(())
}
fn verify_manifest(user: &str, m: &Manifest, key: &[u8]) -> anyhow::Result<()> {
    anyhow::ensure!(
        open(key, &m.authentication, &manifest_binding(user, m)?)? == manifest_digest(m)?,
        "Backup manifest authentication failed"
    );
    Ok(())
}
impl Remote<'_> {
    async fn request(
        &self,
        op: Option<&firefly::DirectBackupRequest<'_>>,
        id: Option<&str>,
    ) -> anyhow::Result<Vec<u8>> {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(90))
            .build()?;
        let url = format!("{}/backups/direct", self.url.trim_end_matches('/'));
        let req = if let Some(op) = op {
            http.post(url)
                .header("Content-Type", "application/x-protobuf")
                .body(serialize_proto(op)?.to_vec())
        } else {
            let req = http.get(url);
            if let Some(id) = id {
                req.query(&[("id", id)])
            } else {
                req
            }
        };
        let mut response = req.bearer_auth(self.token).send().await?;
        anyhow::ensure!(
            response.status().is_success(),
            "Backup server returned {}",
            response.status()
        );
        let limit = if id.is_some() {
            MAX_PLAIN + 65536
        } else {
            2 * 1024 * 1024
        };
        let mut bytes = Vec::new();
        while let Some(part) = response.chunk().await? {
            anyhow::ensure!(
                bytes.len() + part.len() <= limit,
                "Backup response too large"
            );
            bytes.extend_from_slice(&part);
        }
        Ok(bytes)
    }
    async fn upload_cdn(
        &self,
        blob_path: &str,
        ciphertext: &[u8],
        hash: &[u8],
    ) -> anyhow::Result<()> {
        if self.cdn_url.is_empty() {
            return Ok(());
        }
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(90))
            .build()?;
        let url = format!("{}{}", self.cdn_url.trim_end_matches('/'), blob_path);
        let hex_hash: String = hash.iter().map(|b| format!("{b:02x}")).collect();
        let res = http
            .put(url)
            .bearer_auth(self.token)
            .header("Content-Type", "application/octet-stream")
            .header("Content-Length", ciphertext.len())
            .header("X-Ciphertext-Sha256", hex_hash)
            .body(ciphertext.to_vec())
            .send()
            .await?;
        anyhow::ensure!(
            res.status().is_success(),
            "CDN backup upload failed with status {}",
            res.status()
        );
        Ok(())
    }
    async fn download_cdn(
        &self,
        c: &Chunk,
    ) -> anyhow::Result<Vec<u8>> {
        if !self.cdn_url.is_empty() && !c.blob_path.is_empty() {
            let http = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(90))
                .build()?;
            let url = format!("{}{}", self.cdn_url.trim_end_matches('/'), c.blob_path);
            let res = http
                .get(url)
                .bearer_auth(self.token)
                .send()
                .await?;
            if res.status().is_success() {
                let limit = MAX_PLAIN + 65536;
                let mut bytes = Vec::new();
                let mut res = res;
                while let Some(part) = res.chunk().await? {
                    anyhow::ensure!(bytes.len() + part.len() <= limit, "Backup chunk too large");
                    bytes.extend_from_slice(&part);
                }
                if !c.ciphertext_hash.is_empty() {
                    let hash = Sha256::digest(&bytes);
                    anyhow::ensure!(hash.as_slice() == c.ciphertext_hash.as_ref(), "CDN chunk hash mismatch");
                }
                return Ok(bytes);
            }
        }
        self.request(None, Some(&c.id)).await
    }
    pub async fn state(&self) -> anyhow::Result<BackupState> {
        let bytes = self.request(None, None).await?;
        let state = deserialize_proto::<firefly::DirectBackupState>(&bytes)?;
        let manifest = state.manifest.map(own);
        if let Some(m) = &manifest {
            validate(m)?;
        }
        Ok(BackupState {
            revision: state.revision,
            updated_at: state.updated_at,
            manifest,
        })
    }
    pub fn unlock_with_key(&self, key: [u8; 32], m: &Manifest) -> anyhow::Result<Unlock> {
        validate(m)?;
        let key = Zeroizing::new(key);
        anyhow::ensure!(
            open(
                key.as_ref(),
                &m.verifier,
                &binding(self.username, &m.epoch, None)?
            )? == CHECK,
            "Invalid backup key"
        );
        verify_manifest(self.username, m, key.as_ref())?;
        Ok(Unlock {
            key,
            epoch: m.epoch.to_string(),
        })
    }
    pub fn unlock(&self, password: &str, m: &Manifest) -> anyhow::Result<Unlock> {
        let key = derive(password, &m.salt)?;
        self.unlock_with_key(*key, m)
    }
    pub fn create(&self, password: &str, days: u32) -> anyhow::Result<(Manifest, Unlock)> {
        anyhow::ensure!(
            password.chars().count() >= 12,
            "Use a backup password of at least 12 characters"
        );
        anyhow::ensure!([7, 30].contains(&days), "Invalid backup schedule");
        let mut salt = vec![0; 16];
        rand::rng().fill_bytes(&mut salt);
        let key = derive(password, &salt)?;
        let epoch = random_id();
        let verifier = seal(key.as_ref(), CHECK, &binding(self.username, &epoch, None)?)?;
        let mut manifest = Manifest {
            version: 2,
            epoch: epoch.clone().into(),
            salt: salt.into(),
            verifier: verifier.into(),
            schedule_days: days,
            authentication: Vec::new().into(),
            chunks: Vec::new(),
        };
        authenticate(self.username, &mut manifest, key.as_ref())?;
        Ok((manifest, Unlock { key, epoch }))
    }
    async fn control(&self, action: Action, revision: i64, lease: &str) -> anyhow::Result<()> {
        self.request(
            Some(&firefly::DirectBackupRequest {
                action,
                revision,
                lease: lease.into(),
                ..Default::default()
            }),
            None,
        )
        .await?;
        Ok(())
    }
    async fn download(
        &self,
        m: &Manifest,
        key: &Unlock,
        lease: Option<(i64, &str)>,
    ) -> anyhow::Result<Vec<UserMessage>> {
        anyhow::ensure!(
            key.epoch == m.epoch,
            "Backup password changed; unlock again"
        );
        verify_manifest(self.username, m, key.key.as_ref())?;
        let mut records = Vec::new();
        let mut budget = 0;
        let mut seen = HashSet::new();
        for c in &m.chunks {
            if let Some((revision, lease)) = lease {
                self.control(Action::lease, revision, lease).await?;
            }
            let bytes = self.download_cdn(c).await?;
            let chunk = unpack(self.username, &m.epoch, c, key.key.as_ref(), &bytes)?;
            for r in chunk {
                budget += r.message.len() + r.other.len() + 64;
                anyhow::ensure!(
                    budget <= MAX_ARCHIVE,
                    "Archive exceeds this client's memory limit"
                );
                anyhow::ensure!(seen.insert(fingerprint(&r)?), "Duplicate archive content");
                records.push(r);
            }
        }
        Ok(records)
    }
    pub async fn restore(&self, pool: &SqlitePool, key: &Unlock) -> anyhow::Result<usize> {
        let state = self.state().await?;
        let m = state
            .manifest
            .ok_or_else(|| anyhow::anyhow!("No backup exists"))?;
        let records = self.download(&m, key, None).await?;
        // A concurrent password rotation invalidates the downloaded snapshot; retry rather than mix generations.
        anyhow::ensure!(
            self.state().await?.revision == state.revision,
            "Backup changed during restore; retry"
        );
        import(pool, &records).await
    }
    pub async fn sync(
        &self,
        pool: &SqlitePool,
        key: &Unlock,
        initial: Option<Manifest>,
        rotation: Option<(Manifest, Unlock)>,
        days: u32,
    ) -> anyhow::Result<Unlock> {
        anyhow::ensure!([7, 30].contains(&days), "Invalid backup schedule");
        let state = self.state().await?;
        let lease = random_id();
        self.control(Action::lease, state.revision, &lease).await?;
        let result = self
            .publish(pool, key, initial, rotation, days, &state, &lease)
            .await;
        if result.is_err() {
            let _ = self.control(Action::release, state.revision, &lease).await;
        }
        result
    }
    async fn publish(
        &self,
        pool: &SqlitePool,
        key: &Unlock,
        initial: Option<Manifest>,
        rotation: Option<(Manifest, Unlock)>,
        days: u32,
        state: &BackupState,
        lease: &str,
    ) -> anyhow::Result<Unlock> {
        let mut records = if let Some(m) = &state.manifest {
            self.download(m, key, Some((state.revision, lease))).await?
        } else {
            Vec::new()
        };
        let original_len = records.len();
        let rotating = rotation.is_some();
        let (mut m, new_key) = if let Some(pair) = rotation {
            pair
        } else {
            (
                state
                    .manifest
                    .clone()
                    .or(initial)
                    .ok_or_else(|| anyhow::anyhow!("Backup not configured"))?,
                Unlock {
                    key: Zeroizing::new(*key.key),
                    epoch: key.epoch.clone(),
                },
            )
        };
        anyhow::ensure!(
            m.epoch == new_key.epoch,
            "Backup password changed; unlock again"
        );
        m.schedule_days = days;
        let mut seen: HashSet<[u8; 32]> = records
            .iter()
            .map(fingerprint)
            .collect::<anyhow::Result<_>>()?;
        for r in local(pool).await? {
            if seen.insert(fingerprint(&r)?) {
                records.push(r);
            }
        }
        let total = records.iter().try_fold(0usize, |n, m| {
            n.checked_add(m.message.len())?
                .checked_add(m.other.len())?
                .checked_add(64)
        });
        anyhow::ensure!(
            total.is_some_and(|n| n <= MAX_ARCHIVE),
            "Merged backup exceeds the restore memory limit"
        );
        let start = if rotating {
            m.chunks.clear();
            0
        } else if records.len() > original_len && original_len % 1000 != 0 {
            m.chunks.pop();
            original_len / 1000
        } else {
            original_len / 1000
        };
        // Full chunks stay immutable; only replace the partial tail and append missing content.
        for (i, chunk) in records.chunks(1000).enumerate().skip(start) {
            if !rotating && records.len() == original_len {
                break;
            }
            self.control(Action::lease, state.revision, lease).await?;
            let id = random_id();
            let mut meta = Chunk {
                id: id.clone().into(),
                start: (i * 1000) as u64,
                count: chunk.len() as u32,
                blob_path: format!("/direct_backups/{id}").into(),
                byte_size: 0,
                ciphertext_hash: Vec::new().into(),
            };
            let bytes = pack(self.username, &m.epoch, &meta, new_key.key.as_ref(), chunk)?;
            let hash = Sha256::digest(&bytes);
            meta.byte_size = bytes.len() as u64;
            meta.ciphertext_hash = hash.to_vec().into();
            self.request(
                Some(&firefly::DirectBackupRequest {
                    action: Action::put,
                    revision: state.revision,
                    lease: lease.into(),
                    id: meta.id.clone(),
                    object: Some(meta.clone()),
                    ..Default::default()
                }),
                None,
            )
            .await?;
            self.upload_cdn(&meta.blob_path, &bytes, &hash).await?;
            m.chunks.push(meta);
        }
        authenticate(self.username, &mut m, new_key.key.as_ref())?;
        validate(&m)?;
        self.request(
            Some(&firefly::DirectBackupRequest {
                action: Action::commit,
                revision: state.revision,
                lease: lease.into(),
                manifest: Some(m),
                ..Default::default()
            }),
            None,
        )
        .await?;
        Ok(new_key)
    }
}
async fn local(pool: &SqlitePool) -> anyhow::Result<Vec<UserMessage>> {
    let mut result = Vec::new();
    let mut cursor = -1i64;
    let mut budget = 0;
    loop {
        let rows=sqlx::query("SELECT id,other,message,sent_by_other,message_type FROM user_messages WHERE id>? ORDER BY id LIMIT 500").bind(cursor).fetch_all(pool).await?;
        if rows.is_empty() {
            break;
        }
        for r in rows {
            cursor = r.try_get("id")?;
            let m = UserMessage {
                id: cursor as u64,
                other: r.try_get("other")?,
                message: r.try_get("message")?,
                sent_by_other: r.try_get("sent_by_other")?,
                message_type: r.try_get::<i64, _>("message_type")? as u32,
            };
            budget += m.message.len() + m.other.len() + 64;
            anyhow::ensure!(budget <= MAX_ARCHIVE, "Local backup exceeds memory limit");
            result.push(m);
        }
    }
    Ok(result)
}
fn reference_id(m: &UserMessage) -> Option<u64> {
    let inner = deserialize_proto::<firefly::UserMessageInner>(&m.message).ok()?;
    match inner.message {
        firefly::mod_UserMessageInner::OneOfmessage::reaction(r) => Some(r.reacting_to),
        firefly::mod_UserMessageInner::OneOfmessage::messagePayload(p) => match p.ext {
            firefly::mod_MessagePayload::OneOfext::editedOf(id)
            | firefly::mod_MessagePayload::OneOfext::replyingTo(id)
            | firefly::mod_MessagePayload::OneOfext::deleted(id) => Some(id),
            _ => None,
        },
        _ => None,
    }
}
pub async fn import(pool: &SqlitePool, records: &[UserMessage]) -> anyhow::Result<usize> {
    // BEGIN IMMEDIATE prevents a concurrent incoming message from racing ID allocation.
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
    let total: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(length(message)+length(other)+64),0) FROM user_messages",
    )
    .fetch_one(&mut *tx)
    .await?;
    anyhow::ensure!(
        total >= 0 && total as usize <= MAX_ARCHIVE,
        "Local restore exceeds memory limit"
    );
    let rows = sqlx::query("SELECT id,other,message,sent_by_other,message_type FROM user_messages")
        .fetch_all(&mut *tx)
        .await?;
    let mut ids = HashSet::new();
    let mut hashes = HashMap::new();
    for r in rows {
        let m = UserMessage {
            id: r.try_get::<i64, _>("id")? as u64,
            other: r.try_get("other")?,
            message: r.try_get("message")?,
            sent_by_other: r.try_get("sent_by_other")?,
            message_type: r.try_get::<i64, _>("message_type")? as u32,
        };
        ids.insert(m.id);
        hashes.insert(fingerprint(&m)?, m.id);
    }
    let mut source_ids = HashMap::new();
    for m in records {
        let local_id = hashes.get(&fingerprint(m)?).copied().unwrap_or(m.id);
        if let Some(previous) = source_ids.insert((m.other.as_str(), m.id), local_id) {
            anyhow::ensure!(
                previous == local_id,
                "Ambiguous backup IDs; reference-safe remapping required"
            );
        }
    }
    for m in records {
        if let Some(target) = reference_id(m) {
            anyhow::ensure!(
                source_ids
                    .get(&(m.other.as_str(), target))
                    .is_none_or(|local_id| *local_id == target),
                "Restore reference IDs differ; no messages were imported. Reference-safe remapping required"
            );
        }
    }
    let mut inserted = 0;
    for m in records {
        let hash = fingerprint(m)?;
        if hashes.contains_key(&hash) {
            continue;
        }
        // Never overwrite an unrelated live message. ID collisions require explicit remapping of reply/edit references.
        anyhow::ensure!(
            !ids.contains(&m.id),
            "Restore ID collision; no messages were imported. Reference-safe remapping required"
        );
        sqlx::query("INSERT INTO user_messages(id,other,message,sent_by_other,message_type,text) VALUES(?,?,?,?,?,?)")
            .bind(m.id as i64).bind(&m.other).bind(&m.message).bind(m.sent_by_other).bind(i64::from(m.message_type)).bind(crate::db::messages::extract_user_message_text(&m.message)).execute(&mut *tx).await?;
        ids.insert(m.id);
        hashes.insert(hash, m.id);
        inserted += 1;
    }
    tx.commit().await?;
    Ok(inserted)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn message(id: u64, text: &str) -> UserMessage {
        UserMessage {
            id,
            other: "bob".into(),
            message: serialize_proto(&firefly::UserMessageInner {
                message: firefly::mod_UserMessageInner::OneOfmessage::plainText(
                    text.as_bytes().into(),
                ),
                nonce: 42,
                ..Default::default()
            })
            .unwrap()
            .to_vec(),
            sent_by_other: false,
            message_type: 0,
        }
    }
    #[test]
    fn dedup_ignores_local_id_but_not_sender_or_content() {
        let a = message(1, "hi");
        let mut b = message(20, "hi");
        assert_eq!(fingerprint(&a).unwrap(), fingerprint(&b).unwrap());
        b.sent_by_other = true;
        assert_ne!(fingerprint(&a).unwrap(), fingerprint(&b).unwrap());
        assert_ne!(
            fingerprint(&a).unwrap(),
            fingerprint(&message(1, "hello")).unwrap()
        );
    }
    #[test]
    fn protobuf_zstd_encrypted_roundtrip_and_binding() {
        let c = Chunk {
            id: "a".repeat(32).into(),
            start: 0,
            count: 1,
            blob_path: format!("/direct_backups/{}", "a".repeat(32)).into(),
            byte_size: 0,
            ciphertext_hash: Vec::new().into(),
        };
        let key = [9; 32];
        let blob = pack("alice", "epoch", &c, &key, &[message(1, "hello")]).unwrap();
        assert_eq!(unpack("alice", "epoch", &c, &key, &blob).unwrap()[0].id, 1);
        assert!(unpack("bob", "epoch", &c, &key, &blob).is_err());
        assert!(unpack("alice", "different", &c, &key, &blob).is_err());
        assert!(unpack("alice", "epoch", &c, &[8; 32], &blob).is_err());
        let mut corrupt = blob;
        corrupt[13] ^= 1;
        assert!(unpack("alice", "epoch", &c, &key, &corrupt).is_err());
    }
    #[test]
    fn chunk_limit_and_duplicate_validation() {
        let c = Chunk {
            id: "a".repeat(32).into(),
            start: 0,
            count: 1001,
            blob_path: format!("/direct_backups/{}", "a".repeat(32)).into(),
            byte_size: 0,
            ciphertext_hash: Vec::new().into(),
        };
        assert!(pack("a", "b", &c, &[0; 32], &vec![message(1, "x"); 1001]).is_err());
        let c = Chunk { count: 2, ..c };
        let blob = pack("a", "b", &c, &[0; 32], &[message(1, "x"), message(2, "x")]).unwrap();
        assert!(unpack("a", "b", &c, &[0; 32], &blob).is_err());
    }
    #[test]
    fn full_chunk_roundtrip_preserves_one_thousand_distinct_sends() {
        let c = Chunk {
            id: "a".repeat(32).into(),
            start: 0,
            count: 1000,
            blob_path: format!("/direct_backups/{}", "a".repeat(32)).into(),
            byte_size: 0,
            ciphertext_hash: Vec::new().into(),
        };
        let messages: Vec<_> = (0..1000)
            .map(|i| message(i, &format!("message {i}")))
            .collect();
        let bytes = pack("alice", "epoch", &c, &[9; 32], &messages).unwrap();
        assert_eq!(
            unpack("alice", "epoch", &c, &[9; 32], &bytes)
                .unwrap()
                .len(),
            1000
        );
    }
    #[test]
    fn password_and_manifest_authentication_reject_wrong_password_and_truncation() {
        let api = Remote {
            url: "http://unused",
            cdn_url: "http://unused",
            token: "unused",
            username: "alice",
        };
        let (mut m, key) = api.create("correct horse battery staple", 7).unwrap();
        assert!(api.unlock("wrong password", &m).is_err());
        assert!(api.unlock("correct horse battery staple", &m).is_ok());
        m.chunks.push(Chunk {
            id: "a".repeat(32).into(),
            start: 0,
            count: 1,
            blob_path: format!("/direct_backups/{}", "a".repeat(32)).into(),
            byte_size: 100,
            ciphertext_hash: vec![0; 32].into(),
        });
        authenticate("alice", &mut m, key.key.as_ref()).unwrap();
        assert!(verify_manifest("alice", &m, key.key.as_ref()).is_ok());
        m.chunks.clear();
        assert!(verify_manifest("alice", &m, key.key.as_ref()).is_err());
    }
    async fn test_pool() -> anyhow::Result<SqlitePool> {
        let pool = crate::db::setup_pool("sqlite::memory:", 1).await?;
        crate::db::migrations::run_migrations(&pool).await?;
        Ok(pool)
    }
    #[tokio::test]
    async fn restore_deduplicates_existing_content_with_different_id() -> anyhow::Result<()> {
        let pool = test_pool().await?;
        assert_eq!(import(&pool, &[message(99, "hello")]).await?, 1);
        assert_eq!(
            import(&pool, &[message(12, "hello"), message(7, "new")]).await?,
            1
        );
        assert_eq!(
            import(&pool, &[message(12, "hello"), message(7, "new")]).await?,
            0
        );
        let ids: Vec<i64> = sqlx::query_scalar("SELECT id FROM user_messages ORDER BY id")
            .fetch_all(&pool)
            .await?;
        assert_eq!(ids, vec![7, 99]);
        Ok(())
    }
    #[tokio::test]
    async fn restore_collision_rolls_back_without_overwriting_live_content() -> anyhow::Result<()> {
        let pool = test_pool().await?;
        import(&pool, &[message(9, "live")]).await?;
        assert!(
            import(&pool, &[message(3, "new"), message(9, "different")])
                .await
                .is_err()
        );
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM user_messages")
            .fetch_one(&pool)
            .await?;
        assert_eq!(count, 1);
        Ok(())
    }
    #[tokio::test]
    async fn restore_reference_mismatch_rolls_back_instead_of_breaking_replies()
    -> anyhow::Result<()> {
        let pool = test_pool().await?;
        import(&pool, &[message(99, "target")]).await?;
        let mut reply = message(7, "reply");
        reply.message = serialize_proto(&firefly::UserMessageInner {
            nonce: 43,
            message: firefly::mod_UserMessageInner::OneOfmessage::messagePayload(
                firefly::MessagePayload {
                    text: "reply".into(),
                    ext: firefly::mod_MessagePayload::OneOfext::replyingTo(12),
                    ..Default::default()
                },
            ),
            ..Default::default()
        })?
        .to_vec();
        assert!(
            import(&pool, &[message(12, "target"), reply])
                .await
                .is_err()
        );
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM user_messages")
            .fetch_one(&pool)
            .await?;
        assert_eq!(count, 1);
        Ok(())
    }
}
