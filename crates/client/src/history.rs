use std::io::Read;
use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce,
};
use rand::RngCore;
use sha2::{Digest, Sha256};

use crate::db::group_messages::GroupMessage;
use crate::utils::{deserialize_proto, serialize_proto};

pub const DEFAULT_CHUNK_SIZE: usize = 1000;
pub const MAX_CHUNK_PLAINTEXT: usize = 32 * 1024 * 1024;
pub const MAX_CHUNK_BYTES: usize = MAX_CHUNK_PLAINTEXT + 65536;


#[derive(Clone, Debug)]
pub struct PackedChunk {
    pub start_msg_id: u64,
    pub end_msg_id: u64,
    pub msg_count: u32,
    pub unencrypted_hash: Vec<u8>,
    pub key: Vec<u8>,
    pub nonce: Vec<u8>,
    pub blob: Vec<u8>,
}

fn check_plaintext_budget(messages: &[GroupMessage]) -> anyhow::Result<()> {
    let size=messages.iter().try_fold(0usize, |size,m|size.checked_add(m.message.len())?.checked_add(m.by.len())?.checked_add(64));
    anyhow::ensure!(size.is_some_and(|n|n<=MAX_CHUNK_PLAINTEXT), "History plaintext too large");
    Ok(())
}

pub fn compute_unencrypted_hash(messages: &[GroupMessage]) -> anyhow::Result<Vec<u8>> {
    check_plaintext_budget(messages)?;
    let pb_messages: Vec<_> = messages
        .iter()
        .map(|m| firefly_protos::firefly::GroupHistoryRecord {
            id: m.id,
            group_id: m.group_id,
            sender: std::borrow::Cow::Borrowed(&m.by),
            message: std::borrow::Cow::Borrowed(&m.message),
            // Plaintext history needs no MLS epoch. Receipt-time local epochs
            // can differ across peers and must not affect content consensus.
            epoch: 0,
        })
        .collect();

    let group_messages = firefly_protos::firefly::GroupHistoryRecords {
        format_version: 1,
        messages: pb_messages,
    };
    let unencrypted_bytes = serialize_proto(&group_messages)?;
    anyhow::ensure!(unencrypted_bytes.len() <= MAX_CHUNK_PLAINTEXT, "History plaintext too large");

    let mut hasher = Sha256::new();
    hasher.update(&unencrypted_bytes);
    Ok(hasher.finalize().to_vec())
}

pub fn pack_messages_into_chunk(messages: &[GroupMessage]) -> anyhow::Result<PackedChunk> {
    if messages.is_empty() || messages.len() > DEFAULT_CHUNK_SIZE {
        return Err(anyhow::anyhow!("Cannot pack empty messages into a chunk"));
    }

    check_plaintext_budget(messages)?;
    let start_msg_id = messages.first().unwrap().id;
    let end_msg_id = messages.last().unwrap().id;
    let msg_count = messages.len() as u32;

    let pb_messages: Vec<_> = messages
        .iter()
        .map(|m| firefly_protos::firefly::GroupHistoryRecord {
            id: m.id,
            group_id: m.group_id,
            sender: std::borrow::Cow::Borrowed(&m.by),
            message: std::borrow::Cow::Borrowed(&m.message),
            // Plaintext history needs no MLS epoch. Receipt-time local epochs
            // can differ across peers and must not affect content consensus.
            epoch: 0,
        })
        .collect();

    let group_messages = firefly_protos::firefly::GroupHistoryRecords {
        format_version: 1,
        messages: pb_messages,
    };
    let unencrypted_bytes = serialize_proto(&group_messages)?;
    anyhow::ensure!(unencrypted_bytes.len() <= MAX_CHUNK_PLAINTEXT, "History plaintext too large");

    let mut hasher = Sha256::new();
    hasher.update(&unencrypted_bytes);
    let unencrypted_hash = hasher.finalize().to_vec();

    // 1. Compress
    let compressed_bytes = zstd::stream::encode_all(unencrypted_bytes.as_ref(), 3)?;

    // 2. Symmetric key generation (AES-256-GCM)
    let mut key = vec![0u8; 32];
    rand::rng().fill_bytes(&mut key);

    let mut nonce_bytes = vec![0u8; 12];
    rand::rng().fill_bytes(&mut nonce_bytes);

    // 3. Encrypt
    let cipher = Aes256Gcm::new_from_slice(&key)?;
    let nonce = Nonce::from_slice(&nonce_bytes);
    let ciphertext = cipher
        .encrypt(nonce, compressed_bytes.as_ref())
        .map_err(|e| anyhow::anyhow!("Encryption failed: {e}"))?;

    // Blob format: [12 bytes nonce][ciphertext]
    let mut blob = Vec::with_capacity(12 + ciphertext.len());
    blob.extend_from_slice(&nonce_bytes);
    blob.extend_from_slice(&ciphertext);

    Ok(PackedChunk {
        start_msg_id,
        end_msg_id,
        msg_count,
        unencrypted_hash,
        key,
        nonce: nonce_bytes,
        blob,
    })
}

pub fn decrypt_and_unpack_chunk(
    blob: &[u8],
    key: &[u8],
    expected_hash: &[u8],
) -> anyhow::Result<Vec<GroupMessage>> {
    if blob.len() < 12 || blob.len() > MAX_CHUNK_BYTES {
        return Err(anyhow::anyhow!("Blob too small to contain 12-byte nonce"));
    }

    let (nonce_bytes, ciphertext) = blob.split_at(12);
    let cipher = Aes256Gcm::new_from_slice(key)?;
    let nonce = Nonce::from_slice(nonce_bytes);

    let compressed_bytes = cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| anyhow::anyhow!("Decryption failed: {e}"))?;

    let mut decoder = zstd::stream::read::Decoder::new(&compressed_bytes[..])?;
    // Cap both the decoder window and expanded plaintext, independently.
    decoder.window_log_max(25)?;
    let mut unencrypted_bytes = Vec::new();
    decoder.take((MAX_CHUNK_PLAINTEXT + 1) as u64).read_to_end(&mut unencrypted_bytes)?;
    anyhow::ensure!(unencrypted_bytes.len() <= MAX_CHUNK_PLAINTEXT, "History expansion limit exceeded");

    let mut hasher = Sha256::new();
    hasher.update(&unencrypted_bytes);
    let computed_hash = hasher.finalize();

    if computed_hash.as_slice() != expected_hash {
        return Err(anyhow::anyhow!(
            "Unencrypted hash verification failed! Expected {:?}, got {:?}",
            expected_hash,
            computed_hash.as_slice()
        ));
    }

    let pb_messages =
        deserialize_proto::<firefly_protos::firefly::GroupHistoryRecords>(&unencrypted_bytes)?;
    anyhow::ensure!(pb_messages.format_version == 1, "Unsupported history format");
    anyhow::ensure!(!pb_messages.messages.is_empty() && pb_messages.messages.len() <= DEFAULT_CHUNK_SIZE, "Invalid history count");

    let mut result = Vec::with_capacity(pb_messages.messages.len());
    for m in pb_messages.messages {
        let inner =
            deserialize_proto::<firefly_protos::firefly::GroupMessageInner>(&m.message).ok();
        let channel_id = inner.as_ref().map(|i| i.channelId).unwrap_or(0);
        let message_type = inner
            .as_ref()
            .map(|i| {
                i.message_type
                    | match &i.message {
                        firefly_protos::firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(
                            p,
                        ) => p.message_type,
                        _ => 0,
                    }
            })
            .unwrap_or(0);

        result.push(GroupMessage {
            id: m.id,
            group_id: m.group_id,
            by: m.sender.into_owned(),
            message: m.message.to_vec(),
            channel_id,
            epoch: m.epoch,
            message_type,
        });
    }

    Ok(result)
}


/// Validate the entire chunk before any database writes. Hash integrity alone
/// does not bind publisher-controlled records to the requested group or range.
pub fn validate_chunk_records(records: &[GroupMessage], group_id: u64, start: u64, end: u64, count: u32) -> anyhow::Result<()> {
    anyhow::ensure!(group_id > 0 && group_id <= i64::MAX as u64 && start > 0 && end >= start && end <= i64::MAX as u64, "Invalid history range");
    anyhow::ensure!(records.len() == count as usize && count > 0 && count as usize <= DEFAULT_CHUNK_SIZE, "History count mismatch");
    anyhow::ensure!(records.first().map(|r| r.id) == Some(start) && records.last().map(|r| r.id) == Some(end), "History boundary mismatch");
    let mut previous = 0;
    for record in records {
        anyhow::ensure!(record.group_id == group_id && record.id > previous && record.id >= start && record.id <= end, "Foreign, duplicate or unordered history record");
        anyhow::ensure!(!record.by.is_empty() && record.by.len() <= 256, "Missing history sender");
        let inner = deserialize_proto::<firefly_protos::firefly::GroupMessageInner>(&record.message)?;
        anyhow::ensure!(inner.channelId == record.channel_id && record.message_type & firefly_protos::MESSAGE_TYPE_HIDDEN == 0, "Invalid history channel/type");
        previous = record.id;
    }
    Ok(())
}

/// Download only from the configured CDN, with no credentials or key in URL.
pub fn history_chunk_url(base: &str, value: &str, group_id: u64) -> anyhow::Result<reqwest::Url> {
    let base = reqwest::Url::parse(base)?;
    let url = base.join(value)?;
    let prefix = format!("{}/group_chunks/{}/", base.path().trim_end_matches('/'), group_id);
    let id = url.path().strip_prefix(&prefix).unwrap_or_default();
    let dev_http = base.scheme() == "http" && (base.host_str() == Some("localhost") || base.host_str().and_then(|h| h.parse::<std::net::IpAddr>().ok()).is_some_and(|ip| ip.is_loopback() || matches!(ip, std::net::IpAddr::V4(v) if v.is_private())));
    anyhow::ensure!(url.origin() == base.origin() && (url.scheme() == "https" || dev_http) && url.username().is_empty() && url.password().is_none() && url.query().is_none() && url.fragment().is_none() && id.len() == 32 && id.bytes().all(|b| b.is_ascii_hexdigit()), "Untrusted history URL");
    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pack_and_unpack_chunk() {
        let messages = vec![
            GroupMessage {
                id: 100,
                group_id: 42,
                by: "alice".into(),
                message: b"hello world from epoch 1".to_vec(),
                channel_id: 1,
                epoch: 1,
                message_type: 0,
            },
            GroupMessage {
                id: 101,
                group_id: 42,
                by: "bob".into(),
                message: b"hello alice from epoch 1".to_vec(),
                channel_id: 1,
                epoch: 1,
                message_type: 0,
            },
        ];

        let packed = pack_messages_into_chunk(&messages).expect("packing chunk should succeed");
        assert_eq!(packed.start_msg_id, 100);
        assert_eq!(packed.end_msg_id, 101);
        assert_eq!(packed.msg_count, 2);
        assert!(!packed.unencrypted_hash.is_empty());

        let unpacked = decrypt_and_unpack_chunk(&packed.blob, &packed.key, &packed.unencrypted_hash)
            .expect("unpacking should succeed");

        assert_eq!(unpacked.len(), 2);
        assert_eq!(unpacked[0].id, 100);
        assert_eq!(unpacked[0].group_id, 42);
        assert_eq!(unpacked[0].message, b"hello world from epoch 1".to_vec());
        assert_eq!(unpacked[1].id, 101);
        assert_eq!(unpacked[1].group_id, 42);
        assert_eq!(unpacked[1].message, b"hello alice from epoch 1".to_vec());
    }

    #[test]
    fn test_tampered_hash_fails_verification() {
        let messages = vec![GroupMessage {
            id: 200,
            group_id: 42,
            by: "alice".into(),
            message: b"valid message".to_vec(),
            channel_id: 0,
            epoch: 1,
            message_type: 0,
        }];

        let packed = pack_messages_into_chunk(&messages).expect("pack");
        let mut corrupted_hash = packed.unencrypted_hash.clone();
        corrupted_hash[0] ^= 0xff;

        let result = decrypt_and_unpack_chunk(&packed.blob, &packed.key, &corrupted_hash);
        assert!(result.is_err(), "tampered hash must fail verification");
    }

    #[test]
    fn test_tampered_ciphertext_fails_decryption() {
        let messages = vec![GroupMessage {
            id: 300,
            group_id: 42,
            by: "alice".into(),
            message: b"secret message".to_vec(),
            channel_id: 0,
            epoch: 1,
            message_type: 0,
        }];

        let packed = pack_messages_into_chunk(&messages).expect("pack");
        let mut corrupted_blob = packed.blob.clone();
        corrupted_blob[15] ^= 0x55;

        let result = decrypt_and_unpack_chunk(&corrupted_blob, &packed.key, &packed.unencrypted_hash);
        assert!(result.is_err(), "corrupted ciphertext must fail decryption");
    }
}

/// Credential-free, bounded fetch shared by runtime and security tests.
pub async fn download_history_blob(base: &str, group_id: u64, url_or_path: &str) -> anyhow::Result<Vec<u8>> {
        let url = history_chunk_url(base, url_or_path, group_id)?;
        // Encrypted chunks are public opaque blobs. Never send an account bearer
        // token or decryption key to the CDN, including on redirects.
        let client = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).timeout(std::time::Duration::from_secs(30)).build()?;
        let mut response = client.get(url).send().await?;
        anyhow::ensure!(response.status().is_success(), "History download failed: {}", response.status());
        anyhow::ensure!(response.content_length().unwrap_or(0) <= MAX_CHUNK_BYTES as u64, "History blob too large");
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            anyhow::ensure!(bytes.len() + chunk.len() <= MAX_CHUNK_BYTES, "History blob exceeds limit");
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }
