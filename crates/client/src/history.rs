use std::io::{Read, Write};
use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce,
};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use rand::RngCore;
use sha2::{Digest, Sha256};

use crate::db::group_messages::GroupMessage;
use crate::utils::{deserialize_proto, serialize_proto};

pub const DEFAULT_CHUNK_SIZE: usize = 100;

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

pub fn compute_unencrypted_hash(messages: &[GroupMessage]) -> anyhow::Result<Vec<u8>> {
    let pb_messages: Vec<_> = messages
        .iter()
        .map(|m| firefly_protos::firefly::GroupMessage {
            id: m.id,
            groupId: m.group_id,
            message: std::borrow::Cow::Borrowed(&m.message),
            epoch: m.epoch,
        })
        .collect();

    let group_messages = firefly_protos::firefly::GroupMessages {
        messages: pb_messages,
    };
    let unencrypted_bytes = serialize_proto(&group_messages)?;

    let mut hasher = Sha256::new();
    hasher.update(&unencrypted_bytes);
    Ok(hasher.finalize().to_vec())
}

pub fn pack_messages_into_chunk(messages: &[GroupMessage]) -> anyhow::Result<PackedChunk> {
    if messages.is_empty() {
        return Err(anyhow::anyhow!("Cannot pack empty messages into a chunk"));
    }

    let start_msg_id = messages.first().unwrap().id;
    let end_msg_id = messages.last().unwrap().id;
    let msg_count = messages.len() as u32;

    let pb_messages: Vec<_> = messages
        .iter()
        .map(|m| firefly_protos::firefly::GroupMessage {
            id: m.id,
            groupId: m.group_id,
            message: std::borrow::Cow::Borrowed(&m.message),
            epoch: m.epoch,
        })
        .collect();

    let group_messages = firefly_protos::firefly::GroupMessages {
        messages: pb_messages,
    };
    let unencrypted_bytes = serialize_proto(&group_messages)?;

    let mut hasher = Sha256::new();
    hasher.update(&unencrypted_bytes);
    let unencrypted_hash = hasher.finalize().to_vec();

    // 1. Compress
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&unencrypted_bytes)?;
    let compressed_bytes = encoder.finish()?;

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
    if blob.len() < 12 {
        return Err(anyhow::anyhow!("Blob too small to contain 12-byte nonce"));
    }

    let (nonce_bytes, ciphertext) = blob.split_at(12);
    let cipher = Aes256Gcm::new_from_slice(key)?;
    let nonce = Nonce::from_slice(nonce_bytes);

    let compressed_bytes = cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| anyhow::anyhow!("Decryption failed: {e}"))?;

    let mut decoder = GzDecoder::new(&compressed_bytes[..]);
    let mut unencrypted_bytes = Vec::new();
    decoder.read_to_end(&mut unencrypted_bytes)?;

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
        deserialize_proto::<firefly_protos::firefly::GroupMessages>(&unencrypted_bytes)?;

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
            group_id: m.groupId,
            by: String::new(),
            message: m.message.to_vec(),
            channel_id,
            epoch: m.epoch,
            message_type,
        });
    }

    Ok(result)
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
