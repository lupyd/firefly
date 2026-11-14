//! Which of this account's other devices this device trusts with the account's history.
//!
//! Any device registered under the account (by whoever holds the account's token, or by the
//! server itself) receives new messages, but only a device the user approved on another device
//! gets the account's direct-message history, the direct-backup key, and is believed when it
//! sends `selfMessage` copies, direct pin snapshots or history pages.
//!
//! A trusted device is pinned to its Signal identity key once one is known: a device that later
//! shows up under the same address with another identity key is a new, untrusted device.
//!
//! Devices that existed when this shipped are trusted without asking (migration v9 trusts the
//! own devices this device already had sessions with; the first connect after that trusts the
//! devices the server lists, see `FireflyWsClient::bootstrap_own_device_trust`).

use crate::sql::{SqlitePool, prelude::*};

pub const STATE_TRUSTED: i64 = 1;
pub const STATE_REJECTED: i64 = 2;

/// Set once this device trusted the account's devices the server listed on its first connect.
pub const KEY_SERVER_BOOTSTRAPPED: &str = "own_device_trust_server_bootstrapped";

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum OwnDeviceTrust {
    Trusted,
    Rejected,
    /// Never approved (or its identity key changed since): ask the user.
    Unknown,
}

/// One of this account's other devices as this device sees it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OwnDeviceTrustEntry {
    pub address_id: u64,
    pub device_id: u32,
    pub trusted: bool,
    /// The pinned Signal identity key (empty until one was seen).
    pub identity_key: Vec<u8>,
}

#[derive(Clone)]
pub struct OwnDeviceTrustStore {
    pool: SqlitePool,
}

impl OwnDeviceTrustStore {
    /// The table comes from migration v9 (`run_migrations`), which every pool runs first.
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// The identity key this device holds for `username`'s device `device_id`, if any.
    pub async fn current_identity(&self, username: &str, device_id: u32) -> anyhow::Result<Option<Vec<u8>>> {
        let row = crate::sql::query("SELECT identity_key FROM identities WHERE address = ?")
            .bind(format!("{username}.{device_id}"))
            .fetch_optional(&self.pool)
            .await?;
        Ok(match row {
            Some(row) => Some(row.try_get::<Vec<u8>, _>(0)?),
            None => None,
        })
    }

    /// Whether `address_id`/`device_id` is a trusted device of `username` (this account). A
    /// trusted row without a pinned identity pins the one this device holds now.
    pub async fn trust_of(&self, username: &str, address_id: u64, device_id: u32) -> anyhow::Result<OwnDeviceTrust> {
        let row = crate::sql::query("SELECT device_id, identity_key, state FROM own_device_trust WHERE address_id = ?")
            .bind(address_id as i64)
            .fetch_optional(&self.pool)
            .await?;
        let Some(row) = row else { return Ok(OwnDeviceTrust::Unknown) };
        let row_device: i64 = row.try_get(0)?;
        let pinned: Option<Vec<u8>> = row.try_get(1)?;
        let state: i64 = row.try_get(2)?;
        if row_device != device_id as i64 {
            return Ok(OwnDeviceTrust::Unknown);
        }
        if state == STATE_REJECTED {
            return Ok(OwnDeviceTrust::Rejected);
        }
        if state != STATE_TRUSTED {
            return Ok(OwnDeviceTrust::Unknown);
        }
        let current = self.current_identity(username, device_id).await?;
        match (pinned.filter(|k| !k.is_empty()), current) {
            (Some(pinned), Some(current)) if pinned != current => {
                log::warn!("own device {address_id} (device {device_id}) changed its identity key: no longer trusted");
                Ok(OwnDeviceTrust::Unknown)
            }
            (None, Some(current)) => {
                crate::sql::query("UPDATE own_device_trust SET identity_key = ? WHERE address_id = ? AND identity_key IS NULL")
                    .bind(current)
                    .bind(address_id as i64)
                    .execute(&self.pool)
                    .await?;
                Ok(OwnDeviceTrust::Trusted)
            }
            _ => Ok(OwnDeviceTrust::Trusted),
        }
    }

    /// Records the user's decision. `identity_key` pins the device (None keeps or leaves it
    /// unpinned; it is pinned on first use).
    pub async fn set(&self, address_id: u64, device_id: u32, trusted: bool, identity_key: Option<&[u8]>) -> anyhow::Result<()> {
        let state = if trusted { STATE_TRUSTED } else { STATE_REJECTED };
        let now = crate::utils::get_current_timestamp_millis_since_epoch() as i64;
        let identity = identity_key.filter(|k| !k.is_empty()).map(|k| k.to_vec());
        crate::sql::query(
            "INSERT INTO own_device_trust (address_id, device_id, identity_key, state, updated_at) VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(address_id) DO UPDATE SET
               identity_key = CASE WHEN excluded.device_id <> own_device_trust.device_id THEN excluded.identity_key
                                   ELSE COALESCE(excluded.identity_key, own_device_trust.identity_key) END,
               device_id = excluded.device_id, state = excluded.state, updated_at = excluded.updated_at",
        )
        .bind(address_id as i64)
        .bind(device_id as i64)
        .bind(identity)
        .bind(state)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Trusts a device unless the user already decided about it (bootstrap must not undo a
    /// rejection).
    pub async fn trust_if_undecided(&self, address_id: u64, device_id: u32, identity_key: Option<&[u8]>) -> anyhow::Result<()> {
        let now = crate::utils::get_current_timestamp_millis_since_epoch() as i64;
        crate::sql::query(
            "INSERT OR IGNORE INTO own_device_trust (address_id, device_id, identity_key, state, updated_at) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(address_id as i64)
        .bind(device_id as i64)
        .bind(identity_key.filter(|k| !k.is_empty()).map(|k| k.to_vec()))
        .bind(STATE_TRUSTED)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn forget(&self, address_id: u64) -> anyhow::Result<()> {
        crate::sql::query("DELETE FROM own_device_trust WHERE address_id = ?")
            .bind(address_id as i64)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn list(&self) -> anyhow::Result<Vec<OwnDeviceTrustEntry>> {
        let rows = crate::sql::query("SELECT address_id, device_id, identity_key, state FROM own_device_trust ORDER BY address_id")
            .fetch_all(&self.pool)
            .await?;
        rows.iter()
            .map(|row| {
                Ok(OwnDeviceTrustEntry {
                    address_id: row.try_get::<i64, _>(0)? as u64,
                    device_id: row.try_get::<i64, _>(1)? as u32,
                    identity_key: row.try_get::<Option<Vec<u8>>, _>(2)?.unwrap_or_default(),
                    trusted: row.try_get::<i64, _>(3)? == STATE_TRUSTED,
                })
            })
            .collect()
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    async fn store() -> anyhow::Result<OwnDeviceTrustStore> {
        let pool = crate::db::setup_pool("sqlite::memory:", 1).await?;
        crate::db::migrations::run_migrations(&pool).await?;
        crate::db::stores::IdentityDb::new(pool.clone()).await?;
        Ok(OwnDeviceTrustStore::new(pool))
    }

    async fn set_identity(store: &OwnDeviceTrustStore, address: &str, key: &[u8]) -> anyhow::Result<()> {
        crate::sql::query("INSERT OR REPLACE INTO identities (address, identity_key) VALUES (?, ?)")
            .bind(address)
            .bind(key)
            .execute(&store.pool)
            .await?;
        Ok(())
    }

    /// A database from before v9: the account's devices it has sessions with become trusted,
    /// pinned to the identity key it holds; other users and this device itself do not.
    #[tokio::test]
    async fn migration_trusts_own_devices_known_before_it() -> anyhow::Result<()> {
        let pool = crate::db::setup_pool("sqlite::memory:", 1).await?;
        crate::db::migrations::run_migrations(&pool).await?;
        crate::db::stores::IdentityDb::new(pool.clone()).await?;
        crate::db::address::AddressStore::new(pool.clone()).await?;
        crate::sql::query("UPDATE identity_keypair SET id = 10, username = 'bob', device_id = 1").execute(&pool).await?;
        for (id, user, device) in [(10, "bob", 1), (11, "bob", 3), (12, "bob", 4), (13, "alice", 3)] {
            crate::sql::query("INSERT INTO addresses (id, username, device_id) VALUES (?, ?, ?)")
                .bind(id)
                .bind(user)
                .bind(device)
                .execute(&pool)
                .await?;
        }
        crate::sql::query("INSERT INTO identities (address, identity_key) VALUES ('bob.3', ?), ('alice.3', ?)")
            .bind(vec![3u8; 33])
            .bind(vec![9u8; 33])
            .execute(&pool)
            .await?;
        // Pretend v9 never ran.
        crate::sql::query("DROP TABLE own_device_trust").execute(&pool).await?;
        crate::sql::query("DELETE FROM _schema_migrations WHERE version = 9").execute(&pool).await?;
        crate::db::migrations::run_migrations(&pool).await?;

        let s = OwnDeviceTrustStore::new(pool.clone());
        let list = s.list().await?;
        assert_eq!(list.iter().map(|e| e.address_id).collect::<Vec<_>>(), vec![11, 12]);
        assert_eq!(list[0].identity_key, vec![3; 33]);
        assert!(list[1].identity_key.is_empty());
        assert_eq!(s.trust_of("bob", 11, 3).await?, OwnDeviceTrust::Trusted);
        assert_eq!(s.trust_of("bob", 13, 3).await?, OwnDeviceTrust::Unknown);
        // Running again changes nothing (fast path), and a fresh database trusts nobody.
        crate::db::migrations::run_migrations(&pool).await?;
        assert_eq!(s.list().await?.len(), 2);
        assert!(store().await?.list().await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn unknown_until_trusted_and_pinned_to_identity() -> anyhow::Result<()> {
        let s = store().await?;
        assert_eq!(s.trust_of("bob", 7, 3).await?, OwnDeviceTrust::Unknown);
        s.set(7, 3, true, None).await?;
        // Unpinned: the identity held at first use is pinned.
        set_identity(&s, "bob.3", &[1; 33]).await?;
        assert_eq!(s.trust_of("bob", 7, 3).await?, OwnDeviceTrust::Trusted);
        assert_eq!(s.list().await?[0].identity_key, vec![1; 33]);
        // Another identity under the same address is another device.
        set_identity(&s, "bob.3", &[2; 33]).await?;
        assert_eq!(s.trust_of("bob", 7, 3).await?, OwnDeviceTrust::Unknown);
        // So is another device id under the same address.
        set_identity(&s, "bob.3", &[1; 33]).await?;
        assert_eq!(s.trust_of("bob", 7, 4).await?, OwnDeviceTrust::Unknown);
        assert_eq!(s.trust_of("bob", 7, 3).await?, OwnDeviceTrust::Trusted);
        s.set(7, 3, false, None).await?;
        assert_eq!(s.trust_of("bob", 7, 3).await?, OwnDeviceTrust::Rejected);
        // Bootstrap never overrides a decision.
        s.trust_if_undecided(7, 3, None).await?;
        assert_eq!(s.trust_of("bob", 7, 3).await?, OwnDeviceTrust::Rejected);
        s.trust_if_undecided(8, 5, None).await?;
        assert_eq!(s.trust_of("bob", 8, 5).await?, OwnDeviceTrust::Trusted);
        Ok(())
    }
}
