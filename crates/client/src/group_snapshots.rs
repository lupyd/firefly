//! Snapshot capabilities and durable, retryable publication. Never log this state.
use super::*;
use crate::db::group_messages::GroupMessage;
use crate::history::{decrypt_snapshot_records, pack_snapshot_records};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const PIN_BYTES: usize = 8 * 1024 * 1024;
#[derive(Serialize, Deserialize)]
struct PendingSnapshot {
    secret: String, // serialized protobuf, hex; secrets remain in the account database
    blob: String,
    expected: String,
    mutation: Option<(u64, bool)>,
}
fn secret_key(g: u64, kind: u32, id: &str) -> String {
    format!("snapshot-secret:{g}:{kind}:{id}")
}
fn pending_key(g: u64, kind: u32) -> String {
    format!("snapshot-outbox:{g}:{kind}")
}
fn valid_id(id: &str) -> bool {
    id.len() == 32
        && id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn encode_secret(secret: &firefly::GroupSnapshotSecret) -> anyhow::Result<String> {
    Ok(hex::encode(serialize_proto(secret)?))
}
fn decode_secret(encoded: &str) -> anyhow::Result<firefly::GroupSnapshotSecret<'static>> {
    let bytes = hex::decode(encoded)?;
    let s = deserialize_proto::<firefly::GroupSnapshotSecret>(&bytes)?;
    Ok(firefly::GroupSnapshotSecret {
        kind: s.kind,
        snapshot_id: s.snapshot_id.into_owned().into(),
        key: s.key.into_owned().into(),
        plaintext_hash: s.plaintext_hash.into_owned().into(),
        ciphertext_hash: s.ciphertext_hash.into_owned().into(),
        message_count: s.message_count,
        start_id: s.start_id,
        end_id: s.end_id,
    })
}
fn validate_secret(s: &firefly::GroupSnapshotSecret) -> anyhow::Result<()> {
    anyhow::ensure!(
        valid_id(&s.snapshot_id)
            && s.key.len() == 32
            && s.plaintext_hash.len() == 32
            && s.ciphertext_hash.len() == 32,
        "Invalid snapshot capability"
    );
    anyhow::ensure!(
        (s.kind == 1 && s.message_count <= 50)
            || (s.kind == 2 && (1..=999).contains(&s.message_count)),
        "Invalid snapshot count"
    );
    anyhow::ensure!(
        (s.message_count == 0 && s.start_id == 0 && s.end_id == 0)
            || (s.message_count > 0
                && s.start_id > 0
                && s.start_id <= s.end_id
                && s.end_id <= i64::MAX as u64),
        "Invalid snapshot range"
    );
    Ok(())
}
fn pack(
    group: u64,
    kind: u32,
    records: &[GroupMessage],
) -> anyhow::Result<(firefly::GroupSnapshotSecret<'static>, Vec<u8>)> {
    if let (Some(first), Some(last)) = (records.first(), records.last()) {
        validate_chunk_records(records, group, first.id, last.id, records.len() as u32)?;
    }
    let packed = pack_snapshot_records(records)?;
    let secret = firefly::GroupSnapshotSecret {
        kind,
        snapshot_id: format!(
            "{:016x}{:016x}",
            rand::random::<u64>(),
            rand::random::<u64>()
        )
        .into(),
        key: packed.key.into(),
        plaintext_hash: packed.unencrypted_hash.into(),
        ciphertext_hash: Sha256::digest(&packed.blob).to_vec().into(),
        message_count: packed.msg_count,
        start_id: packed.start_msg_id,
        end_id: packed.end_msg_id,
    };
    validate_secret(&secret)?;
    anyhow::ensure!(
        kind != 1 || packed.blob.len() <= PIN_BYTES,
        "Pin snapshot too large"
    );
    Ok((secret, packed.blob))
}
fn unpack(
    group: u64,
    s: &firefly::GroupSnapshotSecret,
    blob: &[u8],
) -> anyhow::Result<Vec<GroupMessage>> {
    validate_secret(s)?;
    anyhow::ensure!(
        s.kind != 1 || blob.len() <= PIN_BYTES,
        "Pin snapshot too large"
    );
    anyhow::ensure!(
        Sha256::digest(blob).as_slice() == s.ciphertext_hash.as_ref(),
        "Snapshot ciphertext mismatch"
    );
    let records = decrypt_snapshot_records(blob, &s.key, &s.plaintext_hash)?;
    anyhow::ensure!(
        records.len() == s.message_count as usize,
        "Snapshot count mismatch"
    );
    if !records.is_empty() {
        validate_chunk_records(&records, group, s.start_id, s.end_id, s.message_count)?;
    }
    Ok(records)
}

// Called before advancing the received-message cursor. Persist keys first so a
// restart after receipt but before download can safely resume without the adder.
pub(super) async fn accept_bundle(
    kv: &KeyValueStore,
    keys: &HistoryKeysStore,
    group: u64,
    b: &firefly::GroupSyncBundle<'_>,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        b.format_version == 1
            && b.group_id == group
            && valid_id(&b.bundle_id)
            && b.keys.len() <= 32
            && b.chunks.len() <= 32,
        "Invalid sync bundle"
    );
    for (secret, kind) in [(b.pinned.as_ref(), 1), (b.recent.as_ref(), 2)] {
        if let Some(s) = secret {
            validate_secret(s)?;
            anyhow::ensure!(s.kind == kind, "Snapshot kind mismatch");
        }
    }
    for key in &b.keys {
        anyhow::ensure!(
            key.group_id == group
                && key.key.len() == 32
                && key.nonce.len() == 12
                && key.unencrypted_hash.len() == 32,
            "Invalid archived capability"
        );
    }
    for key in &b.keys {
        keys.save_verified_key(
            group,
            key.start_msg_id,
            key.end_msg_id,
            &key.unencrypted_hash,
            &key.key,
            &key.nonce,
        )
        .await?;
    }
    for s in [b.pinned.as_ref(), b.recent.as_ref()].into_iter().flatten() {
        kv.set(
            &format!("snapshot-candidate:{group}:{}:{}", s.kind, s.snapshot_id),
            &encode_secret(s)?,
        )
        .await?;
    }
    if let Some(s) = &b.recent {
        kv.enqueue_snapshot_tail(group, &s.snapshot_id).await?;
    }
    // Keep each page, not just the last page. The normal history importer still
    // consults verified server records before trusting any archive URL.
    for chunk in &b.chunks {
        anyhow::ensure!(chunk.group_id == group, "Archive group mismatch");
        kv.set(
            &format!("snapshot-archive:{group}:{}", chunk.id),
            &hex::encode(serialize_proto(chunk)?),
        )
        .await?;
    }
    Ok(())
}

impl FireflyWsClient {
    fn snapshot_url(&self, g: u64, kind: u32, id: &str) -> String {
        if kind == 1 {
            format!(
                "{}/group/pinned/{g}",
                self.firefly_base_url.trim_end_matches('/')
            )
        } else {
            format!(
                "{}/group/messages_chunk/{g}/{id}",
                self.firefly_base_url.trim_end_matches('/')
            )
        }
    }
    async fn snapshot_get(
        &self,
        g: u64,
        kind: u32,
        id: &str,
    ) -> anyhow::Result<Option<(String, Vec<u8>)>> {
        // No bearer/cookie/key in this download request, even for the API origin.
        let mut response = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(30))
            .build()?
            .get(self.snapshot_url(g, kind, id))
            .send()
            .await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        anyhow::ensure!(
            response.status().is_success(),
            "Snapshot download unavailable"
        );
        let current = response
            .headers()
            .get("X-Snapshot-Id")
            .context("Missing snapshot identity")?
            .to_str()?
            .to_owned();
        anyhow::ensure!(
            valid_id(&current) && (kind == 1 || current == id),
            "Unexpected snapshot identity"
        );
        let max = if kind == 1 {
            PIN_BYTES
        } else {
            MAX_CHUNK_BYTES
        };
        anyhow::ensure!(
            response.content_length().is_none_or(|n| n <= max as u64),
            "Snapshot exceeds limit"
        );
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            anyhow::ensure!(
                bytes.len().saturating_add(chunk.len()) <= max,
                "Snapshot exceeds limit"
            );
            bytes.extend_from_slice(&chunk);
        }
        Ok(Some((current, bytes)))
    }
    async fn validated_snapshot(
        &self,
        g: u64,
        kind: u32,
        id: &str,
        blob: &[u8],
    ) -> anyhow::Result<(firefly::GroupSnapshotSecret<'static>, Vec<GroupMessage>)> {
        for key in [
            secret_key(g, kind, id),
            format!("snapshot-candidate:{g}:{kind}:{id}"),
        ] {
            if let Ok(encoded) = self.key_value_store.get(&key).await {
                if let Ok(secret) = decode_secret(&encoded) {
                    if secret.kind == kind && secret.snapshot_id.as_ref() == id {
                        if let Ok(records) = unpack(g, &secret, blob) {
                            self.key_value_store
                                .set(&secret_key(g, kind, id), &encoded)
                                .await?;
                            return Ok((secret, records));
                        }
                    }
                }
            }
        }
        anyhow::bail!("Snapshot key missing or invalid; waiting for authenticated capability")
    }
    async fn current_pins(
        &self,
        g: u64,
    ) -> anyhow::Result<(
        String,
        Vec<GroupMessage>,
        Option<firefly::GroupSnapshotSecret<'static>>,
    )> {
        let Some((id, blob)) = self.snapshot_get(g, 1, "").await? else {
            return Ok((String::new(), vec![], None));
        };
        let (secret, records) = self.validated_snapshot(g, 1, &id, &blob).await?;
        Ok((id, records, Some(secret)))
    }
    async fn prepare_pin(
        &self,
        g: u64,
        mutation: Option<(u64, bool)>,
    ) -> anyhow::Result<PendingSnapshot> {
        let (expected, mut records, _) = self.current_pins(g).await?;
        if let Some((id, pinned)) = mutation {
            records.retain(|r| r.id != id);
            if pinned {
                records.push(
                    self.group_message_store()
                        .get_message(g, id)
                        .await?
                        .context("Pin target not visible")?,
                );
            }
        }
        records.sort_by_key(|r| r.id);
        let (secret, blob) = pack(g, 1, &records)?;
        Ok(PendingSnapshot {
            secret: encode_secret(&secret)?,
            blob: base64::engine::general_purpose::STANDARD.encode(blob),
            expected,
            mutation,
        })
    }
    async fn publish_pending(
        &self,
        g: u64,
        kind: u32,
    ) -> anyhow::Result<Option<firefly::GroupSnapshotSecret<'static>>> {
        let key = pending_key(g, kind);
        let Some(encoded) = self
            .key_value_store
            .get(&key)
            .await
            .ok()
            .filter(|s| !s.is_empty())
        else {
            return Ok(None);
        };
        let mut pending: PendingSnapshot = serde_json::from_str(&encoded)?;
        // Limit per-turn CAS retries. Persist the newly merged attempt BEFORE PUT.
        for _ in 0..3 {
            let secret = decode_secret(&pending.secret)?;
            validate_secret(&secret)?;
            let blob = base64::engine::general_purpose::STANDARD.decode(&pending.blob)?;
            unpack(g, &secret, &blob)?;
            self.key_value_store
                .set(
                    &secret_key(g, secret.kind, &secret.snapshot_id),
                    &pending.secret,
                )
                .await?;
            let token = self
                .callbacks
                .get_access_token()
                .await
                .context("Not authenticated")?;
            let response = reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(30))
                .build()?
                .post(self.snapshot_url(g, kind, &secret.snapshot_id))
                .bearer_auth(token)
                .header("X-Snapshot-Id", secret.snapshot_id.as_ref())
                .header("X-Expected-Snapshot", &pending.expected)
                .header("X-Message-Count", secret.message_count.to_string())
                .header("X-Start-Id", secret.start_id.to_string())
                .header("X-End-Id", secret.end_id.to_string())
                .body(blob)
                .send()
                .await?;
            if response.status() == reqwest::StatusCode::CONFLICT && kind == 1 {
                pending = self.prepare_pin(g, pending.mutation).await?;
                self.key_value_store
                    .set(&key, &serde_json::to_string(&pending)?)
                    .await?;
                continue;
            }
            anyhow::ensure!(
                response.status().is_success(),
                "Snapshot publication deferred ({})",
                response.status()
            );
            return Ok(Some(secret)); // caller clears durable outbox only after MLS send
        }
        anyhow::bail!("Pin snapshot contention; retry persisted operation")
    }
    async fn send_bundle(
        &self,
        g: u64,
        pinned: Option<firefly::GroupSnapshotSecret<'static>>,
        recent: Option<firefly::GroupSnapshotSecret<'static>>,
        include_history: bool,
    ) -> anyhow::Result<()> {
        let bundle_id = format!(
            "{:016x}{:016x}",
            rand::random::<u64>(),
            rand::random::<u64>()
        );
        let mut before = 0;
        let mut page = 0;
        loop {
            let chunks = if include_history {
                self.get_group_history_chunks(g, 0, before).await?
            } else {
                vec![]
            };
            let next = chunks.last().map(|c| c.start_msg_id).unwrap_or(0);
            let last = chunks.len() < 32;
            let mut keys = Vec::new();
            for c in &chunks {
                let (key, nonce) = self
                    .history_keys_store
                    .verified_material(g, c.start_msg_id, c.end_msg_id, &c.unencrypted_hash)
                    .await?
                    .context("Missing archived key; join manifest remains pending")?;
                keys.push(firefly::GroupHistoryChunkKey {
                    group_id: g,
                    start_msg_id: c.start_msg_id,
                    end_msg_id: c.end_msg_id,
                    key: key.into(),
                    nonce: nonce.into(),
                    unencrypted_hash: c.unencrypted_hash.clone(),
                });
            }
            let bundle = firefly::GroupSyncBundle {
                format_version: 1,
                group_id: g,
                pinned: pinned.clone(),
                recent: recent.clone(),
                keys,
                chunks,
                bundle_id: bundle_id.clone().into(),
                page,
                last_page: last,
            };
            self.upload_group_message(
                g,
                firefly::GroupMessageInner {
                    channelId: 0,
                    message_type: firefly_protos::MESSAGE_TYPE_HIDDEN,
                    message: firefly::mod_GroupMessageInner::OneOfmessage::syncBundle(bundle),
                },
                0,
            )
            .await?;
            if last {
                break;
            }
            anyhow::ensure!(
                next > 0 && (before == 0 || next < before),
                "Manifest cursor stalled"
            );
            before = next;
            page += 1;
        }
        Ok(())
    }
    pub(super) async fn set_snapshot_pin(
        &self,
        g: u64,
        id: u64,
        pinned: bool,
    ) -> anyhow::Result<()> {
        let _lock = self.snapshot_work.lock().await;
        // Complete any interrupted mutation before accepting a new one.
        self.flush_pin(g).await?;
        let pending = self.prepare_pin(g, Some((id, pinned))).await?;
        self.key_value_store
            .set(&pending_key(g, 1), &serde_json::to_string(&pending)?)
            .await?;
        self.flush_pin(g).await
    }
    async fn flush_pin(&self, g: u64) -> anyhow::Result<()> {
        if let Some(secret) = self.publish_pending(g, 1).await? {
            self.send_bundle(g, Some(secret), None, false).await?;
            self.refresh_pins(g).await?;
            self.key_value_store.set(&pending_key(g, 1), "").await?;
        }
        Ok(())
    }
    async fn refresh_pins(&self, g: u64) -> anyhow::Result<()> {
        let (_, records, secret) = self.current_pins(g).await?;
        if let Some(secret) = secret {
            let previous = self.group_message_store().get_pinned_messages(g).await?;
            self.group_messages_store
                .apply_pin_snapshot(g, &records, &secret.plaintext_hash)
                .await?;
            for r in previous.into_iter().chain(records) {
                if let Some(m) = self.group_message_store().get_message(g, r.id).await? {
                    self.callbacks.on_group_message_updated(m).await;
                }
            }
        }
        Ok(())
    }
    pub(super) async fn share_join_snapshots(&self, g: u64) -> anyhow::Result<()> {
        self.key_value_store
            .set(
                &format!("snapshot-join-pending:{g}"),
                &format!("{:016x}", rand::random::<u64>()),
            )
            .await?;
        let result = self.resume_snapshots(g).await;
        if result.is_err() {
            self.callbacks
                .on_group_history_signal(crate::callbacks::GroupHistorySignal {
                    group_id: g,
                    signal_type: 8,
                    request_id: 0,
                    chunk_id: 0,
                    username: self.callbacks.name().to_string(),
                })
                .await;
        }
        result
    }
    async fn import_snapshot_tail(&self, g: u64, id: &str) -> anyhow::Result<()> {
        let Some((_, blob)) = self.snapshot_get(g, 2, id).await? else {
            let encoded = self
                .key_value_store
                .get(&secret_key(g, 2, id))
                .await
                .or(self
                    .key_value_store
                    .get(&format!("snapshot-candidate:{g}:2:{id}"))
                    .await)?;
            let old = decode_secret(&encoded)?;
            validate_secret(&old)?;
            let local = self
                .group_message_store()
                .snapshot_history_range(g, old.start_id, old.end_id)
                .await?;
            let already_recovered = local.len() == old.message_count as usize
                && compute_unencrypted_hash(&local)?.as_slice() == old.plaintext_hash.as_ref();
            let archive = self
                .get_group_history_chunks(g, old.start_id, old.end_id.saturating_add(1))
                .await?;
            let covered = archive.iter().any(|c| {
                c.verified && c.start_msg_id <= old.start_id && c.end_msg_id >= old.end_id
            });
            let recovered = if !already_recovered && covered {
                !self
                    .import_group_history_page(g, old.start_id, old.end_id.saturating_add(1))
                    .await?
                    .pending
            } else {
                already_recovered
            };
            if recovered {
                return Ok(());
            }
            self.request_group_history(g, old.start_id, old.end_id)
                .await?;
            anyhow::bail!("Recent snapshot unavailable; fresh handoff requested");
        };
        let (secret, records) = self.validated_snapshot(g, 2, id, &blob).await?;
        // Tail snapshots are unverified imports, never independent evidence.
        let ids = records.iter().map(|r| r.id).collect::<Vec<_>>();
        let evidence = self
            .group_messages_store
            .authenticated_history_ids(g, &ids)
            .await?;
        if evidence.len() == records.len() {
            anyhow::ensure!(
                compute_unencrypted_hash(&evidence)?.as_slice() == secret.plaintext_hash.as_ref(),
                "Recent snapshot disagrees with authenticated originals"
            );
        }
        // A reserved positive provenance ID allows later approved archive
        // imports to replace this adder-trusted (not peer-approved) tail.
        self.group_messages_store
            .import_verified_history(g, i64::MAX as u64, &secret.plaintext_hash, &records)
            .await?;
        self.callbacks
            .on_group_history_signal(crate::callbacks::GroupHistorySignal {
                group_id: g,
                signal_type: 4,
                request_id: 0,
                chunk_id: 0,
                username: self.callbacks.name().to_string(),
            })
            .await;
        Ok(())
    }
    pub async fn resume_snapshots(&self, g: u64) -> anyhow::Result<()> {
        let _lock = self.snapshot_work.lock().await;
        // A pin key gap must not prevent independent tail capabilities progressing.
        let pins_result = async {
            self.flush_pin(g).await?;
            self.refresh_pins(g).await
        }
        .await;
        let history = self.history_enabled(g).await?;
        if history {
            self.key_value_store
                .drain_snapshot_tails(
                    g,
                    crate::utils::get_current_timestamp_millis_since_epoch() as i64,
                    |id| async move { self.import_snapshot_tail(g, &id).await },
                )
                .await?;
        }
        let tails_pending = history && self.key_value_store.has_pending_snapshot_tails(g).await?;
        let pending = self
            .key_value_store
            .get(&format!("snapshot-join-pending:{g}"))
            .await
            .unwrap_or_default();
        if pending.is_empty()
            || pending
                == self
                    .key_value_store
                    .get(&format!("snapshot-join-completed:{g}"))
                    .await
                    .unwrap_or_default()
        {
            pins_result?;
            anyhow::ensure!(!tails_pending, "Snapshot tails deferred for retry");
            return Ok(());
        }
        let (_, _, pinned) = self.current_pins(g).await?;
        let mut recent = None;
        if history {
            anyhow::ensure!(
                self.get_mls_group(g)
                    .await?
                    .has_full_channel_access()
                    .await?,
                "Full access required to share recent history"
            );
            self.archive_ready_history(g).await?;
            if self
                .key_value_store
                .get(&pending_key(g, 2))
                .await
                .ok()
                .filter(|s| !s.is_empty())
                .is_none()
            {
                let chunks = self.get_group_history_chunks(g, 0, 0).await?;
                let last = chunks.iter().map(|c| c.end_msg_id).max().unwrap_or(0);
                let records = self
                    .group_messages_store
                    .snapshot_history_range(g, last.saturating_add(1), i64::MAX as u64)
                    .await?;
                if records.len() >= 1000 && pinned.is_some() {
                    // Degraded handoff: pins need not wait for peer archive approval.
                    self.send_bundle(g, pinned.clone(), None, false).await?;
                }
                anyhow::ensure!(
                    records.len() < 1000,
                    "Waiting for full chunks to be verified before tail handoff"
                );
                if !records.is_empty() {
                    let (secret, blob) = pack(g, 2, &records)?;
                    let p = PendingSnapshot {
                        secret: encode_secret(&secret)?,
                        blob: base64::engine::general_purpose::STANDARD.encode(blob),
                        expected: String::new(),
                        mutation: None,
                    };
                    self.key_value_store
                        .set(&pending_key(g, 2), &serde_json::to_string(&p)?)
                        .await?;
                }
            }
            recent = self.publish_pending(g, 2).await?;
        }
        self.send_bundle(g, pinned, recent, history).await?;
        self.key_value_store.set(&pending_key(g, 2), "").await?;
        self.key_value_store
            .set(&format!("snapshot-join-completed:{g}"), &pending)
            .await?;
        pins_result?;
        anyhow::ensure!(!tails_pending, "Snapshot tails deferred for retry");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn record(id: u64) -> GroupMessage {
        let inner = firefly::GroupMessageInner {
            channelId: 1,
            message_type: 0,
            message: firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(
                firefly::MessagePayload {
                    text: format!("original {id}").into(),
                    ..Default::default()
                },
            ),
        };
        GroupMessage {
            id,
            group_id: 42,
            by: "alice".into(),
            message: serialize_proto(&inner).unwrap().to_vec(),
            channel_id: 1,
            epoch: 5,
            message_type: 0,
        }
    }
    #[test]
    fn pin_snapshots_roundtrip_zero_one_fifty_and_reject_fifty_one() {
        for count in [0, 1, 50] {
            let records = (1..=count).map(record).collect::<Vec<_>>();
            let (secret, blob) = pack(42, 1, &records).unwrap();
            let decoded = unpack(42, &secret, &blob).unwrap();
            assert_eq!(decoded.len(), records.len());
            for (a, b) in records.iter().zip(decoded) {
                assert_eq!(a.id, b.id);
                assert_eq!(a.by, b.by);
                assert_eq!(a.message, b.message);
                assert_eq!(a.channel_id, b.channel_id);
            }
        }
        assert!(pack(42, 1, &(1..=51).map(record).collect::<Vec<_>>()).is_err());
    }
    #[test]
    fn recent_snapshots_require_partial_batches_and_bound_capabilities() {
        assert!(pack(42, 2, &[]).is_err());
        assert!(pack(42, 2, &(1..=1000).map(record).collect::<Vec<_>>()).is_err());
        let (secret, blob) = pack(42, 2, &(1..=999).map(record).collect::<Vec<_>>()).unwrap();
        assert_eq!(unpack(42, &secret, &blob).unwrap().len(), 999);
        assert!(unpack(43, &secret, &blob).is_err());
        for field in 0..6 {
            let mut bad = secret.clone();
            match field {
                0 => bad.key = vec![0; 32].into(),
                1 => bad.plaintext_hash = vec![0; 32].into(),
                2 => bad.ciphertext_hash = vec![0; 32].into(),
                3 => bad.message_count = 998,
                4 => bad.start_id = 2,
                5 => bad.snapshot_id = "../../token".into(),
                _ => unreachable!(),
            }
            assert!(unpack(42, &bad, &blob).is_err(), "field {field}");
        }
        let mut corrupt = blob.clone();
        corrupt[20] ^= 1;
        assert!(unpack(42, &secret, &corrupt).is_err());
        assert!(unpack(42, &secret, &blob[..20]).is_err());
    }
    #[tokio::test]
    async fn durable_bundle_receipt_deduplicates_and_survives_store_recreation()
    -> anyhow::Result<()> {
        let pool = crate::db::setup_pool("sqlite::memory:", 1).await?;
        let kv = KeyValueStore::new(pool.clone()).await?;
        let keys = HistoryKeysStore::new(pool.clone()).await?;
        let (pin, _) = pack(42, 1, &[])?;
        let (tail, blob) = pack(42, 2, &[record(10)])?;
        let bundle = firefly::GroupSyncBundle {
            format_version: 1,
            group_id: 42,
            pinned: Some(pin),
            recent: Some(tail.clone()),
            bundle_id: "a".repeat(32).into(),
            last_page: true,
            ..Default::default()
        };
        accept_bundle(&kv, &keys, 42, &bundle).await?;
        accept_bundle(&kv, &keys, 42, &bundle).await?;
        assert!(kv.has_pending_snapshot_tails(42).await?);
        drop(kv);
        let kv = KeyValueStore::new(pool).await?;
        let recovered = decode_secret(
            &kv.get(&format!("snapshot-candidate:42:2:{}", tail.snapshot_id))
                .await?,
        )?;
        assert_eq!(unpack(42, &recovered, &blob)?.len(), 1);
        assert!(accept_bundle(&kv, &keys, 43, &bundle).await.is_err());
        assert_eq!(
            kv.drain_snapshot_tails(42, 0, |_| async { Ok(()) }).await?,
            1
        );
        accept_bundle(&kv, &keys, 42, &bundle).await?;
        assert!(!kv.has_pending_snapshot_tails(42).await?);
        Ok(())
    }
    #[tokio::test]
    async fn pin_snapshot_unpins_originals_without_overwriting_content() -> anyhow::Result<()> {
        let store = crate::db::group_messages::GroupMessagesStore::new(
            crate::db::setup_pool("sqlite::memory:", 1).await?,
        )
        .await?;
        let original = record(10);
        store
            .add(10, 42, 1, 5, "alice", &original.message, 0)
            .await?;
        let mut forged = original.clone();
        forged.by = "forged".into();
        store
            .apply_pin_snapshot(42, &[forged, record(20)], &[1; 32])
            .await?;
        assert_eq!(store.get_message(42, 10).await?.unwrap().by, "alice");
        assert_eq!(store.get_pinned_messages(42).await?.len(), 2);
        store
            .add(10, 42, 1, 6, "alice", &original.message, 0)
            .await?;
        assert_eq!(
            store.get_pinned_messages(42).await?.len(),
            2,
            "Original receipt must not erase snapshot pins"
        );
        assert_eq!(
            store.authenticated_history_ids(42, &[10, 20]).await?.len(),
            1
        );
        store.apply_pin_snapshot(42, &[], &[2; 32]).await?;
        assert!(store.get_pinned_messages(42).await?.is_empty());
        assert!(store.get_message(42, 10).await?.is_some());
        let mut foreign = record(30);
        foreign.group_id = 43;
        assert!(
            store
                .apply_pin_snapshot(42, &[foreign], &[3; 32])
                .await
                .is_err()
        );
        Ok(())
    }
}

#[cfg(test)]
mod disk_recovery_tests {
    use super::*;
    #[tokio::test]
    async fn unpublished_snapshot_reopens_with_identical_ciphertext_and_key() -> anyhow::Result<()>
    {
        struct Scratch(std::path::PathBuf);
        impl Drop for Scratch {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let dir = Scratch(std::env::temp_dir().join(format!(
            "firefly-snapshot-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        )));
        std::fs::create_dir_all(&dir.0)?;
        let path = dir.0.join("account.sqlite");
        let path = path.to_str().context("temp path UTF8")?;
        let (secret, blob) = pack(42, 1, &[])?;
        let pending = PendingSnapshot {
            secret: encode_secret(&secret)?,
            blob: base64::engine::general_purpose::STANDARD.encode(&blob),
            expected: "a".repeat(32),
            mutation: Some((99, false)),
        };
        let pool = crate::db::setup_pool_from_path(path, 1).await?;
        let kv = KeyValueStore::new(pool.clone()).await?;
        kv.set(&pending_key(42, 1), &serde_json::to_string(&pending)?)
            .await?;
        drop(kv);
        pool.close().await;
        drop(pool);
        let pool = crate::db::setup_pool_from_path(path, 1).await?;
        let kv = KeyValueStore::new(pool.clone()).await?;
        let reopened: PendingSnapshot = serde_json::from_str(&kv.get(&pending_key(42, 1)).await?)?;
        assert_eq!(reopened.expected, pending.expected);
        assert_eq!(reopened.mutation, pending.mutation);
        let recovered = decode_secret(&reopened.secret)?;
        let bytes = base64::engine::general_purpose::STANDARD.decode(&reopened.blob)?;
        assert_eq!(recovered.snapshot_id, secret.snapshot_id);
        assert_eq!(recovered.key, secret.key);
        assert_eq!(bytes, blob);
        assert!(unpack(42, &recovered, &bytes)?.is_empty());
        drop(kv);
        pool.close().await;
        Ok(())
    }
}
