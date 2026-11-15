//! Approving this account's other devices before they get its history (see
//! `crate::db::own_devices`).
//!
//! A device of the account that the user has not approved:
//! - gets no answer to direct history requests (they wait here until the user decides),
//! - is never sent the direct-backup key,
//! - is not believed: its `selfMessage` copies, direct pin snapshots and history pages are
//!   dropped,
//! - makes the account's other devices ask the user (`on_own_device_trust_request`).
//!
//! Approving or rejecting on one trusted device tells the account's other trusted devices
//! (an `ownDeviceTrust` message inside a Signal-encrypted `selfMessage`), so the user decides
//! once. Older app versions ignore that message and keep answering as before.
use super::*;
use crate::callbacks::{DirectHistoryRequest, OwnDeviceTrustRequest};
use crate::db::own_devices::{KEY_SERVER_BOOTSTRAPPED, OwnDeviceTrust, OwnDeviceTrustEntry, OwnDeviceTrustStore};

/// Set once this device announced itself to the account's other devices.
const KEY_ANNOUNCED: &str = "own_device_trust_announced";
/// Set when this device registers for the first time (a fresh install), until its bootstrap ran.
pub(super) const KEY_FRESH_DEVICE: &str = "own_device_trust_fresh_device";
/// The account's devices this device trusts only provisionally (JSON list of address ids): a
/// fresh install takes the server's word for which devices are the user's, so their messages are
/// believed (the device that approves it sends its history and decisions), but they get nothing
/// from it (history, the backup key) and vouch for no one until the user approves them here, or
/// a device trusted here in full vouches for them. Otherwise a device the server (or a holder of
/// the account's token) listed at install time would be trusted in full, get the history the
/// approved install receives, and pass its trust on to the account's other devices.
const KEY_PROVISIONAL: &str = "own_device_trust_provisional";
/// How often the user is asked again about the same device while it keeps asking.
const PROMPT_INTERVAL_MS: u64 = 60_000;
/// History requests kept per untrusted device until the user decides.
const MAX_HELD_REQUESTS: usize = 8;

/// Keyed by (this device's address, the other device's address): several clients of one
/// account can run in one process (tests, the MCP).
#[derive(Default)]
struct Pending {
    prompted_at: HashMap<(u64, u64), u64>,
    held: HashMap<(u64, u64), Vec<DirectHistoryRequest>>,
}

static PENDING: std::sync::LazyLock<std::sync::Mutex<Pending>> = std::sync::LazyLock::new(Default::default);

fn pending() -> std::sync::MutexGuard<'static, Pending> {
    PENDING.lock().unwrap_or_else(|e| e.into_inner())
}

/// How this device trusts the account's device `address_id`/`device_id`. Unknown on errors.
pub(super) async fn own_trust(kv: &KeyValueStore, me: &str, address_id: u64, device_id: u32) -> OwnDeviceTrust {
    match OwnDeviceTrustStore::new(kv.pool()).trust_of(me, address_id, device_id).await {
        Ok(trust) => trust,
        Err(err) => {
            log::warn!("could not read trust of own device {address_id}: {err:#}");
            OwnDeviceTrust::Unknown
        }
    }
}

async fn provisional(kv: &KeyValueStore) -> std::collections::BTreeSet<u64> {
    kv.get(KEY_PROVISIONAL)
        .await
        .ok()
        .and_then(|v| serde_json::from_str(&v).ok())
        .unwrap_or_default()
}

async fn update_provisional(kv: &KeyValueStore, f: impl FnOnce(&mut std::collections::BTreeSet<u64>)) -> anyhow::Result<()> {
    let mut set = provisional(kv).await;
    let before = set.clone();
    f(&mut set);
    if set != before {
        kv.set(KEY_PROVISIONAL, &serde_json::to_string(&set)?).await?;
    }
    Ok(())
}

/// Whether this device may give the account's device `address_id` its history or the backup
/// key, or vouch for it: trusted, and not just provisionally (see `KEY_PROVISIONAL`).
pub(super) async fn own_trust_to_share(kv: &KeyValueStore, me: &str, address_id: u64, device_id: u32) -> OwnDeviceTrust {
    match own_trust(kv, me, address_id, device_id).await {
        OwnDeviceTrust::Trusted if provisional(kv).await.contains(&address_id) => OwnDeviceTrust::Unknown,
        trust => trust,
    }
}

/// Records the account's devices the server listed (`bootstrap_own_device_trust`). On a fresh
/// install they are trusted only provisionally.
pub(super) async fn trust_listed_devices(
    kv: &KeyValueStore,
    me: &str,
    listed: &[(u64, u32)],
    fresh: bool,
) -> anyhow::Result<usize> {
    let store = OwnDeviceTrustStore::new(kv.pool());
    let mut others = 0;
    for &(address_id, device_id) in listed {
        let identity = store.current_identity(me, device_id).await?;
        if fresh && store.trust_of(me, address_id, device_id).await? == OwnDeviceTrust::Unknown {
            update_provisional(kv, |p| {
                p.insert(address_id);
            })
            .await?;
        }
        store.trust_if_undecided(address_id, device_id, identity.as_deref()).await?;
        others += 1;
    }
    Ok(others)
}

/// Asks the user about an unapproved device, at most once a minute per device.
pub(super) async fn prompt(callbacks: &Arc<dyn FireflyWsClientCallback>, my_address: u64, address_id: u64, device_id: u32) {
    let now = get_current_timestamp_millis_since_epoch();
    {
        let mut p = pending();
        let last = p.prompted_at.get(&(my_address, address_id)).copied().unwrap_or(0);
        if last != 0 && now.saturating_sub(last) < PROMPT_INTERVAL_MS {
            return;
        }
        p.prompted_at.insert((my_address, address_id), now);
    }
    log::warn!("own device {address_id} (device {device_id}) is not trusted: asking the user");
    callbacks.on_own_device_trust_request(OwnDeviceTrustRequest { address_id, device_id }).await;
}

/// Keeps a history request of an unapproved device until the user decides.
pub(super) fn hold_history_request(my_address: u64, request: DirectHistoryRequest) {
    let mut p = pending();
    let held = p.held.entry((my_address, request.requester_address)).or_default();
    held.retain(|r| r.request_id != request.request_id);
    held.push(request);
    if held.len() > MAX_HELD_REQUESTS {
        held.remove(0);
    }
}

fn take_held(my_address: u64, address_id: u64) -> Vec<DirectHistoryRequest> {
    let mut p = pending();
    p.prompted_at.remove(&(my_address, address_id));
    p.held.remove(&(my_address, address_id)).unwrap_or_default()
}

/// Applies the decisions another trusted device of this account sent (`ownDeviceTrust`).
///
/// `sender_address` is the trusted device that sent them. A device trusted only provisionally
/// (`KEY_PROVISIONAL`) can't make another device more than provisionally trusted, and can't
/// reject one: it may be the one the server slipped in.
pub(super) async fn apply_trust_update(
    kv: &KeyValueStore,
    callbacks: &Arc<dyn FireflyWsClientCallback>,
    my_address: u64,
    sender_address: u64,
    update: &firefly::OwnDeviceTrust<'_>,
) {
    if update.entries.is_empty() {
        return;
    }
    apply_trust_entries(kv, my_address, sender_address, update).await;
    callbacks.on_own_device_trust_changed().await;
}

async fn apply_trust_entries(kv: &KeyValueStore, my_address: u64, sender_address: u64, update: &firefly::OwnDeviceTrust<'_>) {
    let store = OwnDeviceTrustStore::new(kv.pool());
    let sender_provisional = provisional(kv).await.contains(&sender_address);
    for entry in &update.entries {
        if entry.address_id == my_address || entry.address_id == 0 || entry.address_id == sender_address {
            continue;
        }
        let identity = (!entry.identity_key.is_empty()).then_some(entry.identity_key.as_ref());
        if sender_provisional {
            if !entry.trusted {
                log::warn!("ignoring a rejection of own device {} from provisionally trusted device {sender_address}", entry.address_id);
                continue;
            }
            // Only a device this one never decided about: a decision made here stands, and a
            // device already trusted stays as fully trusted as it was.
            let decided = match store.list().await {
                Ok(list) => list.iter().any(|e| e.address_id == entry.address_id),
                Err(_) => true,
            };
            if decided {
                continue;
            }
            if let Err(err) = update_provisional(kv, |p| {
                p.insert(entry.address_id);
            })
            .await
            {
                log::warn!("could not record provisional trust of own device {}: {err:#}", entry.address_id);
                continue;
            }
        } else if let Err(err) = update_provisional(kv, |p| {
            p.remove(&entry.address_id);
        })
        .await
        {
            log::warn!("could not record trust of own device {}: {err:#}", entry.address_id);
            continue;
        }
        if let Err(err) = store.set(entry.address_id, entry.device_id, entry.trusted, identity).await {
            log::warn!("could not record trust of own device {}: {err:#}", entry.address_id);
        }
        if !entry.trusted {
            take_held(my_address, entry.address_id);
        } else {
            pending().prompted_at.remove(&(my_address, entry.address_id));
        }
    }
}

impl FireflyWsClient {
    fn own_trust_store(&self) -> OwnDeviceTrustStore {
        OwnDeviceTrustStore::new(self.pool.clone())
    }

    /// How this device trusts the account's other devices it decided about.
    pub async fn own_device_trust_list(&self) -> anyhow::Result<Vec<OwnDeviceTrustEntry>> {
        self.own_trust_store().list().await
    }

    /// Whether this device gives `address_id` the account's history and backup key. A device
    /// trusted only provisionally (a fresh install's view of the account's other devices) is
    /// `Unknown` here until the user approves it on this device.
    pub async fn own_device_trust(&self, address_id: u64, device_id: u32) -> OwnDeviceTrust {
        own_trust_to_share(&self.key_value_store, self.callbacks.name(), address_id, device_id).await
    }

    /// The user approves another device of this account: it gets the account's history from
    /// now on, its held history requests are answered (`on_direct_history_request`), and the
    /// account's other trusted devices learn the decision.
    pub async fn trust_own_device(&self, address_id: u64, device_id: u32) -> anyhow::Result<()> {
        self.decide_own_device(address_id, device_id, true).await?;
        let my_address = self.address_id();
        for request in take_held(my_address, address_id) {
            if request.requester_device_id == device_id {
                self.callbacks.on_direct_history_request(request).await;
            }
        }
        Ok(())
    }

    /// The user rejects another device of this account: it is never answered, and the account's
    /// other trusted devices stop asking about it. Revoking an earlier approval is the same.
    pub async fn reject_own_device(&self, address_id: u64, device_id: u32) -> anyhow::Result<()> {
        self.decide_own_device(address_id, device_id, false).await?;
        take_held(self.address_id(), address_id);
        Ok(())
    }

    async fn decide_own_device(&self, address_id: u64, device_id: u32, trusted: bool) -> anyhow::Result<()> {
        let me = self.callbacks.name().to_string();
        anyhow::ensure!(address_id != 0 && address_id != self.address_id(), "not another device of this account");
        let store = self.own_trust_store();
        let mut identity = None;
        if trusted {
            // Pin the identity key the device has now: a different key later is another device.
            if self.key_stores.store().address_store.get_by_id(address_id).await?.is_none() {
                let token = self.callbacks.get_access_token().await.context("token not found")?;
                let allowed = [me.as_str()];
                self.get_and_process_pre_key_bundles_per_ids(&[address_id], Some(&allowed[..]), &token).await?;
            }
            let address = self
                .key_stores
                .store()
                .address_store
                .get_by_id(address_id)
                .await?
                .context("that device is no longer registered")?;
            anyhow::ensure!(
                address.username == me && address.device_id as u32 == device_id,
                "that device is not this account's device {device_id}"
            );
            identity = store.current_identity(&me, device_id).await?;
        }
        store.set(address_id, device_id, trusted, identity.as_deref()).await?;
        update_provisional(&self.key_value_store, |p| {
            p.remove(&address_id);
        })
        .await?;
        log::info!("own device {address_id} (device {device_id}) {}", if trusted { "trusted" } else { "rejected" });

        // Tell the other trusted devices (and the newly trusted one) so they don't ask again.
        // Devices trusted here only provisionally are not vouched for.
        let provisional = provisional(&self.key_value_store).await;
        let entries = store
            .list()
            .await?
            .into_iter()
            .filter(|e| !provisional.contains(&e.address_id))
            .map(|e| firefly::OwnDeviceTrustEntry {
                address_id: e.address_id,
                device_id: e.device_id,
                identity_key: e.identity_key.into(),
                trusted: e.trusted,
            })
            .collect();
        let update = firefly::OwnDeviceTrust { version: 1, request: false, entries };
        let inner = serialize_proto(&firefly::UserMessageInner {
            message: firefly::mod_UserMessageInner::OneOfmessage::ownDeviceTrust(update),
            nonce: rng().next_u32(),
            message_type: firefly_protos::MESSAGE_TYPE_HIDDEN,
        })?
        .to_vec();
        if let Err(err) = self.send_to_trusted_own_devices(&inner, true).await {
            log::warn!("could not tell the account's other devices about device {address_id}: {err:#}");
        }
        self.callbacks.on_own_device_trust_changed().await;
        Ok(())
    }

    /// Sends `inner` (a serialized `UserMessageInner`) inside a `selfMessage` to the account's
    /// other devices this device trusts, and to no other device. Returns how many it reached.
    /// Devices trusted only provisionally get it only with `include_provisional` (never secrets).
    pub(super) async fn send_to_trusted_own_devices(&self, inner: &[u8], include_provisional: bool) -> anyhow::Result<usize> {
        let me = self.callbacks.name().to_string();
        let my_address = self.address_id();
        let token = self.callbacks.get_access_token().await.context("token not found")?;
        let address_store = &self.key_stores.store().address_store;
        let store = self.own_trust_store();

        let trust = |address_id: u64, device_id: u32| {
            let kv = &self.key_value_store;
            let me = me.clone();
            async move {
                if include_provisional {
                    own_trust(kv, &me, address_id, device_id).await
                } else {
                    own_trust_to_share(kv, &me, address_id, device_id).await
                }
            }
        };
        let trusted = store.list().await?.into_iter().filter(|e| e.trusted).collect::<Vec<_>>();
        let known = address_store.get(&me).await?;
        let missing = trusted
            .iter()
            .filter(|e| !known.iter().any(|a| a.address_id == e.address_id))
            .map(|e| e.address_id)
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            let allowed = [me.as_str()];
            if let Err(err) = self.get_and_process_pre_key_bundles_per_ids(&missing, Some(&allowed[..]), &token).await {
                log::warn!("could not fetch keys of own devices {missing:?}: {err:#}");
            }
        }

        let payload = serialize_proto(&firefly::UserMessageInner {
            message: firefly::mod_UserMessageInner::OneOfmessage::selfMessage(firefly::SelfUserMessage {
                to: me.clone().into(),
                inner: inner.to_vec().into(),
            }),
            nonce: rng().next_u32(),
            message_type: firefly_protos::MESSAGE_TYPE_HIDDEN,
        })?
        .to_vec();

        let mut upload = firefly::UploadUserMessage::default();
        for address in address_store.get(&me).await? {
            if address.address_id == my_address {
                continue;
            }
            if trust(address.address_id, address.device_id as u32).await != OwnDeviceTrust::Trusted {
                continue;
            }
            match self
                .create_encrypted_message_refreshing(
                    ProtocolAddress::new(me.clone(), DeviceId::new(address.device_id)?),
                    address.address_id,
                    1,
                    payload.clone(),
                    &me,
                    &token,
                )
                .await
            {
                // Refreshing may have replaced the device's keys: only a still trusted identity
                // gets the message.
                Ok(message) if trust(address.address_id, address.device_id as u32).await == OwnDeviceTrust::Trusted =>
                {
                    upload.messages.push(message)
                }
                Ok(_) => log::warn!("own device {} changed its keys: not sending", address.address_id),
                Err(err) => log::warn!("not encrypting for own device {}: {err:#}", address.address_id),
            }
        }
        let count = upload.messages.len();
        if count == 0 {
            return Ok(0);
        }
        let bytes = self
            .request(firefly::Request { payload: firefly::mod_Request::OneOfpayload::uploadUserMessage(upload), id: 0 })
            .await?;
        let response = deserialize_proto::<firefly::Response<'_>>(&bytes)?;
        if let Some(error) = response.error {
            return Err(anyhow::anyhow!("[{}], {}", error.errorCode, error.error));
        }
        Ok(count)
    }

    /// Once per database, before this device reads any message: trusts the account's devices
    /// the server lists now (devices that existed before this device, or before approval
    /// shipped, are trusted without asking).
    pub(super) async fn bootstrap_own_device_trust(&self, token: &str) -> anyhow::Result<()> {
        let me = self.callbacks.name().to_string();
        let my_address = self.address_id();
        if my_address == 0 || me.is_empty() {
            return Ok(());
        }
        if !self.key_value_store.get(KEY_SERVER_BOOTSTRAPPED).await.unwrap_or_default().is_empty() {
            return Ok(());
        }
        let response = HTTP_CLIENT
            .get(format!("{}/user/devices", self.firefly_base_url))
            .bearer_auth(token)
            .send()
            .await?;
        anyhow::ensure!(response.status().is_success(), "listing devices failed: {}", response.status());
        let body = response.bytes().await?;
        let addresses = deserialize_proto::<firefly::Addresses>(&body)?;
        let listed = addresses
            .addresses
            .iter()
            .filter(|a| a.id != my_address && a.id != 0 && (a.username.is_empty() || a.username == me))
            .map(|a| (a.id, a.deviceId))
            .collect::<Vec<_>>();
        let fresh = !self.key_value_store.get(KEY_FRESH_DEVICE).await.unwrap_or_default().is_empty();
        let others = trust_listed_devices(&self.key_value_store, &me, &listed, fresh).await?;
        self.key_value_store.set(KEY_SERVER_BOOTSTRAPPED, "1").await?;
        log::info!(
            "trusting the account's {others} existing devices{}",
            if fresh { " provisionally (fresh install)" } else { "" }
        );
        Ok(())
    }

    /// Once per database, once connected: tells the account's other devices about this one, so
    /// a new device is offered for approval right away (older app versions ignore it).
    pub(super) async fn announce_own_device(&self) -> anyhow::Result<()> {
        if !self.key_value_store.get(KEY_ANNOUNCED).await.unwrap_or_default().is_empty() {
            return Ok(());
        }
        if !self.own_trust_store().list().await?.iter().any(|e| e.trusted) {
            // The account's first device: nobody to ask. A later device announces itself.
            self.key_value_store.set(KEY_ANNOUNCED, "1").await?;
            return Ok(());
        }
        let inner = serialize_proto(&firefly::UserMessageInner {
            message: firefly::mod_UserMessageInner::OneOfmessage::ownDeviceTrust(firefly::OwnDeviceTrust {
                version: 1,
                request: true,
                entries: vec![],
            }),
            nonce: rng().next_u32(),
            message_type: firefly_protos::MESSAGE_TYPE_HIDDEN,
        })?
        .to_vec();
        self.send_to_trusted_own_devices(&inner, true).await?;
        self.key_value_store.set(KEY_ANNOUNCED, "1").await?;
        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    async fn kv() -> anyhow::Result<KeyValueStore> {
        let pool = crate::db::setup_pool("sqlite::memory:", 1).await?;
        crate::db::migrations::run_migrations(&pool).await?;
        crate::db::stores::IdentityDb::new(pool.clone()).await?;
        KeyValueStore::new(pool).await
    }

    fn update(entries: &[(u64, u32, bool)]) -> firefly::OwnDeviceTrust<'static> {
        firefly::OwnDeviceTrust {
            version: 1,
            request: false,
            entries: entries
                .iter()
                .map(|&(address_id, device_id, trusted)| firefly::OwnDeviceTrustEntry {
                    address_id,
                    device_id,
                    identity_key: Default::default(),
                    trusted,
                })
                .collect(),
        }
    }

    /// R3-FF-C1. A fresh install (address 30) is told by the server that the account has the
    /// devices 10 (the user's phone) and 66 (a ghost the server, or whoever holds the account's
    /// token, registered). It used to trust both in full: once the user approved the install on
    /// the phone and the phone sent it the history, the ghost could ask the install for that
    /// history and the backup key, and any decision the user made on the install vouched for
    /// the ghost to the phone.
    #[tokio::test]
    async fn fresh_install_trusts_listed_devices_only_provisionally() -> anyhow::Result<()> {
        let kv = kv().await?;
        trust_listed_devices(&kv, "bob", &[(10, 1), (66, 7)], true).await?;
        // Their messages are believed (the phone's history and decisions must get through)...
        assert_eq!(own_trust(&kv, "bob", 10, 1).await, OwnDeviceTrust::Trusted);
        assert_eq!(own_trust(&kv, "bob", 66, 7).await, OwnDeviceTrust::Trusted);
        // ...but neither gets history or the backup key without the user's approval here.
        assert_eq!(own_trust_to_share(&kv, "bob", 10, 1).await, OwnDeviceTrust::Unknown);
        assert_eq!(own_trust_to_share(&kv, "bob", 66, 7).await, OwnDeviceTrust::Unknown);

        // The ghost (provisional) can't reject the phone, nor make another ghost more than
        // provisional, nor touch its own entry.
        apply_trust_entries(&kv, 30, 66, &update(&[(10, 1, false), (67, 8, true), (66, 7, true)])).await;
        assert_eq!(own_trust(&kv, "bob", 10, 1).await, OwnDeviceTrust::Trusted);
        assert_eq!(own_trust(&kv, "bob", 67, 8).await, OwnDeviceTrust::Trusted);
        assert_eq!(own_trust_to_share(&kv, "bob", 67, 8).await, OwnDeviceTrust::Unknown);

        // The user approves the phone here: from then on the phone gets history, and its own
        // decisions (it rejected the ghosts) count in full.
        update_provisional(&kv, |p| {
            p.remove(&10);
        })
        .await?;
        assert_eq!(own_trust_to_share(&kv, "bob", 10, 1).await, OwnDeviceTrust::Trusted);
        apply_trust_entries(&kv, 30, 10, &update(&[(66, 7, false), (67, 8, false), (30, 2, true)])).await;
        assert_eq!(own_trust(&kv, "bob", 66, 7).await, OwnDeviceTrust::Rejected);
        assert_eq!(own_trust(&kv, "bob", 67, 8).await, OwnDeviceTrust::Rejected);
        Ok(())
    }

    /// An existing device upgrading (not a fresh install) keeps trusting the devices it lists.
    #[tokio::test]
    async fn upgrade_bootstrap_trusts_in_full() -> anyhow::Result<()> {
        let kv = kv().await?;
        trust_listed_devices(&kv, "bob", &[(10, 1)], false).await?;
        assert_eq!(own_trust_to_share(&kv, "bob", 10, 1).await, OwnDeviceTrust::Trusted);
        // A fully trusted device vouches in full.
        apply_trust_entries(&kv, 30, 10, &update(&[(11, 3, true)])).await;
        assert_eq!(own_trust_to_share(&kv, "bob", 11, 3).await, OwnDeviceTrust::Trusted);
        Ok(())
    }
}
