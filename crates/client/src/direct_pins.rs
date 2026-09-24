//! Pairwise pin snapshot codec. Snapshot bytes are opaque to the server.
use std::io::Read;
use aes_gcm::{aead::{Aead, KeyInit, Payload}, Aes256Gcm, Nonce};
use rand::RngCore;
use sha2::{Digest, Sha256};
use crate::{
    db::messages::{MessagesStore, UserMessage},
    utils::{deserialize_proto, serialize_proto},
};
use firefly_protos::firefly;

const LIMIT: usize = 64 * 1024;

fn peer(other: &str) -> anyhow::Result<&str> {
    anyhow::ensure!(
        !other.is_empty()
            && other.len() <= 256
            && other.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-' || b == b'.'),
        "Invalid direct conversation"
    );
    Ok(other)
}

fn id() -> String {
    let mut bytes = [0; 16];
    rand::rng().fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn pin_content_fingerprint(msg_bytes: &[u8]) -> [u8; 32] {
    if let Ok(mut inner) = deserialize_proto::<firefly::UserMessageInner>(msg_bytes) {
        inner.message_type = 0;
        if let firefly::mod_UserMessageInner::OneOfmessage::messagePayload(ref mut p) = inner.message {
            p.message_type = 0;
        }
        if let Ok(ser) = serialize_proto(&inner) {
            return Sha256::digest(&ser).into();
        }
    }
    Sha256::digest(msg_bytes).into()
}

fn aad(other: &str, id: &str, count: u32) -> anyhow::Result<Vec<u8>> {
    anyhow::ensure!(
        !other.is_empty() && other.len() <= 256 && id.len() == 32 && count <= 50,
        "Invalid pin capability"
    );
    Ok(serialize_proto(&firefly::DirectPinSnapshotSecret {
        version: 1,
        other: other.into(),
        snapshot_id: id.into(),
        key: Vec::new().into(),
        ciphertext_hash: Vec::new().into(),
        message_count: count,
    })?
    .to_vec())
}

pub fn seal(other: &str, id: &str, key: &[u8], messages: &[UserMessage]) -> anyhow::Result<Vec<u8>> {
    anyhow::ensure!(key.len() == 32 && messages.len() <= 50, "Invalid pin snapshot");
    let plain = serialize_proto(&firefly::DirectBackupRecords {
        version: 1,
        records: messages
            .iter()
            .map(|m| firefly::DirectBackupRecord {
                source_id: m.id,
                other: m.other.as_str().into(),
                sent_by_other: m.sent_by_other,
                message: m.message.as_slice().into(),
                message_type: m.message_type,
            })
            .collect(),
    })?;
    anyhow::ensure!(plain.len() <= LIMIT, "Pinned snapshot too large");
    let packed = zstd::stream::encode_all(plain.as_ref(), 3)?;
    let mut nonce = [0; 12];
    rand::rng().fill_bytes(&mut nonce);
    let encrypted = Aes256Gcm::new_from_slice(key)?
        .encrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: &packed,
                aad: &aad(other, id, messages.len() as u32)?,
            },
        )
        .map_err(|_| anyhow::anyhow!("Pin encryption failed"))?;
    Ok([nonce.as_slice(), encrypted.as_slice()].concat())
}

pub fn open(
    secret: &firefly::DirectPinSnapshotSecret<'_>,
    blob: &[u8],
) -> anyhow::Result<Vec<UserMessage>> {
    anyhow::ensure!(
        secret.version == 1
            && secret.key.len() == 32
            && secret.ciphertext_hash.len() == 32
            && secret.message_count <= 50
            && Sha256::digest(blob).as_slice() == secret.ciphertext_hash.as_ref(),
        "Invalid pin snapshot"
    );
    let compressed = Aes256Gcm::new_from_slice(&secret.key)?
        .decrypt(
            Nonce::from_slice(blob.get(..12).ok_or_else(|| anyhow::anyhow!("Invalid pinned snapshot"))?),
            Payload {
                msg: blob.get(12..).ok_or_else(|| anyhow::anyhow!("Invalid pinned snapshot"))?,
                aad: &aad(&secret.other, &secret.snapshot_id, secret.message_count)?,
            },
        )
        .map_err(|_| anyhow::anyhow!("Pinned snapshot cannot be decrypted"))?;
    let mut plain = Vec::new();
    zstd::stream::read::Decoder::new(compressed.as_slice())?
        .take(LIMIT as u64 + 1)
        .read_to_end(&mut plain)?;
    anyhow::ensure!(plain.len() <= LIMIT, "Pinned snapshot too large");
    let records = deserialize_proto::<firefly::DirectBackupRecords>(&plain)?;
    anyhow::ensure!(
        records.version == 1 && records.records.len() == secret.message_count as usize,
        "Invalid pinned records"
    );
    records
        .records
        .into_iter()
        .map(|r| {
            Ok(UserMessage {
                id: r.source_id,
                other: r.other.into_owned(),
                message: r.message.into_owned(),
                sent_by_other: r.sent_by_other,
                message_type: r.message_type | firefly_protos::MESSAGE_TYPE_PINNED,
            })
        })
        .collect()
}

async fn body(response: reqwest::Response) -> anyhow::Result<Vec<u8>> {
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        if !body.trim().is_empty() {
            anyhow::bail!("{body}");
        }
        anyhow::bail!("Pinned snapshot server returned {status}");
    }
    anyhow::ensure!(
        response.content_length().is_none_or(|n| n <= LIMIT as u64),
        "Pinned snapshot too large"
    );
    let mut out = Vec::new();
    let mut response = response;
    while let Some(part) = response.chunk().await? {
        anyhow::ensure!(out.len() + part.len() <= LIMIT, "Pinned snapshot too large");
        out.extend_from_slice(&part);
    }
    Ok(out)
}

pub async fn publish(
    base: &str,
    token: &str,
    other: &str,
    messages: &[UserMessage],
) -> anyhow::Result<firefly::DirectPinSnapshotSecret<'static>> {
    anyhow::ensure!(
        messages.iter().all(|m| m.other == other),
        "Pinned message conversation mismatch"
    );
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let url = format!("{}/direct-pins/{}", base.trim_end_matches('/'), peer(other)?);
    let expected = match http.get(&url).bearer_auth(token).send().await {
        Ok(r) if r.status() == reqwest::StatusCode::NOT_FOUND => String::new(),
        Ok(r) => {
            let id = r
                .headers()
                .get("X-Snapshot-Id")
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| anyhow::anyhow!("Missing pin head"))?
                .to_owned();
            let _ = body(r).await?;
            id
        }
        Err(e) => return Err(e.into()),
    };
    let snapshot = id();
    let mut key = [0; 32];
    rand::rng().fill_bytes(&mut key);
    let blob = seal(other, &snapshot, &key, messages)?;
    let hash = Sha256::digest(&blob).to_vec();
    let response = http
        .post(&url)
        .bearer_auth(token)
        .header("X-Snapshot-Id", &snapshot)
        .header("X-Expected-Snapshot", expected)
        .header("X-Message-Count", messages.len())
        .header(
            "X-Ciphertext-Sha256",
            hash.iter().map(|b| format!("{b:02x}")).collect::<String>(),
        )
        .body(blob)
        .send()
        .await?;
    let _ = body(response).await?;
    Ok(firefly::DirectPinSnapshotSecret {
        version: 1,
        other: other.to_owned().into(),
        snapshot_id: snapshot.into(),
        key: key.to_vec().into(),
        ciphertext_hash: hash.into(),
        message_count: messages.len() as u32,
    })
}

pub async fn apply_records(
    store: &MessagesStore,
    peer_username: &str,
    records: &[UserMessage],
) -> anyhow::Result<()> {
    let local = store
        .get_last_messages_of(peer_username, i64::MAX, 100_000)
        .await?;
    let mut hashes = std::collections::HashSet::new();
    for r in records {
        hashes.insert(pin_content_fingerprint(&r.message));
    }
    for message in local {
        let mut ty = message.message_type & !firefly_protos::MESSAGE_TYPE_PINNED;
        if hashes.contains(&pin_content_fingerprint(&message.message)) {
            ty |= firefly_protos::MESSAGE_TYPE_PINNED;
        }
        store
            .update_message_type(peer_username, message.id, ty)
            .await?;
    }
    Ok(())
}

pub async fn apply(
    base: &str,
    token: &str,
    store: &MessagesStore,
    peer_username: &str,
    secret: firefly::DirectPinSnapshotSecret<'static>,
) -> anyhow::Result<()> {
    let url = format!(
        "{}/direct-pins/{}",
        base.trim_end_matches('/'),
        peer(peer_username)?
    );
    let response = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?
        .get(url)
        .bearer_auth(token)
        .send()
        .await?;
    let id = response
        .headers()
        .get("X-Snapshot-Id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    anyhow::ensure!(id == secret.snapshot_id, "Stale pinned snapshot key");
    let records = open(&secret, &body(response).await?)?;
    apply_records(store, peer_username, &records).await
}
