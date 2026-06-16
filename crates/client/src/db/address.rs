use sqlx::{Pool, Sqlite};
use sqlx::{SqlitePool, prelude::*};

#[derive(Clone)]
pub struct AddressStore {
    pool: SqlitePool,
}

impl AddressStore {
    pub async fn new(pool: Pool<Sqlite>) -> anyhow::Result<Self> {
        let mut db = pool.acquire().await?;

        db.execute(
            "CREATE TABLE IF NOT EXISTS addresses (id INTEGER, username TEXT, device_id INTEGER)",
        )
        .await?;

        db.execute("CREATE INDEX IF NOT EXISTS addresses_by_username ON addresses (username)")
            .await?;

        db.execute("CREATE INDEX IF NOT EXISTS addresses_by_id ON addresses (id)")
            .await?;

        Ok(Self { pool })
    }

    pub async fn add(&self, id: u64, username: &str, device_id: u8) -> anyhow::Result<()> {
        log::info!("store insert: address id={} username={} device_id={}", id, username, device_id);
        let q = "INSERT INTO addresses (id, username, device_id) VALUES (?, ?, ?)";

        sqlx::query(q)
            .bind(id as i64)
            .bind(username)
            .bind(device_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn get(&self, username: &str) -> anyhow::Result<Vec<AddressIdAndDeviceId>> {
        let q = "SELECT id, device_id FROM addresses WHERE username = ?";

        let rows = sqlx::query(q).bind(username).fetch_all(&self.pool).await?;

        let mut addresses = Vec::with_capacity(rows.len());

        for row in rows {
            let id: i64 = row.try_get(0)?;
            let device_id: u8 = row.try_get(1)?;

            addresses.push(AddressIdAndDeviceId {
                address_id: id as u64,
                device_id,
                username: username.to_string(),
            });
        }

        Ok(addresses)
    }

    pub async fn delete_by_device_id(&self, username: &str, device_id: u8) -> anyhow::Result<()> {
        log::info!("store delete: address username={} device_id={}", username, device_id);
        let q = "DELETE FROM addresses WHERE username = ? AND device_id = ?";

        sqlx::query(q)
            .bind(username)
            .bind(device_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn delete_by_id(&self, id: u64) -> anyhow::Result<()> {
        log::info!("store delete: address id={}", id);
        let q = "DELETE FROM addresses WHERE id = ?";

        sqlx::query(q).bind(id as i64).execute(&self.pool).await?;

        Ok(())
    }

    pub async fn get_by_id(&self, id: u64) -> anyhow::Result<Option<AddressIdAndDeviceId>> {
        let q = "SELECT username, device_id FROM addresses WHERE id = ?";

        let row = sqlx::query(q)
            .bind(id as i64)
            .fetch_optional(&self.pool)
            .await?;

        if let Some(row) = row {
            let username: String = row.try_get(0)?;
            let device_id: u8 = row.try_get(1)?;

            Ok(Some(AddressIdAndDeviceId {
                address_id: id,
                device_id,
                username,
            }))
        } else {
            Ok(None)
        }
    }
}

#[derive(Clone)]
pub struct AddressIdAndDeviceId {
    pub address_id: u64,
    pub device_id: u8,
    pub username: String,
}
