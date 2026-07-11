use std::collections::HashMap;
use zeroize::Zeroizing;

use firefly_core::storage_provider::MlsGroupStateStorage;
use firefly_core::storage_provider::MlsKeyPackageStorage;
use firefly_core::storage_provider::MlsPreSharedKeyStorage;
use sqlx::Executor;
use sqlx::SqlitePool;
use sqlx::prelude::*;

pub struct GroupStateStore {
    pool: SqlitePool,
}

impl GroupStateStore {
    pub async fn new(pool: SqlitePool) -> anyhow::Result<Self> {
        pool.execute(
            r#"
        CREATE TABLE IF NOT EXISTS group_states (
            id BLOB PRIMARY KEY,
            state BLOB NOT NULL
        )"#,
        )
        .await?;

        pool.execute(
            r#"
        CREATE TABLE IF NOT EXISTS group_epoch_states (
            id BLOB NOT NULL,
            epoch INTEGER NOT NULL,
            state BLOB NOT NULL,
            PRIMARY KEY (id, epoch),
            FOREIGN KEY (id) REFERENCES group_states(id) ON DELETE CASCADE
        )"#,
        )
        .await?;

        Ok(Self { pool })
    }

    pub async fn get_state(&self, id: &[u8]) -> anyhow::Result<Vec<u8>> {
        log::info!("store select: group_state id={:?}", id);
        let row = sqlx::query("SELECT state FROM group_states WHERE id = ?")
            .bind(id)
            .fetch_one(&self.pool)
            .await?;
        let state = row.try_get(0)?;
        Ok(state)
    }

    pub async fn set_state(
        &self,
        group_id: &[u8],
        state_data: &[u8],
        epoch_inserts: HashMap<u64, Vec<u8>>,
        epoch_updates: HashMap<u64, Vec<u8>>,
    ) -> anyhow::Result<()> {
        log::info!("store insert: group_state id={:?}", group_id);
        let mut tx = self.pool.begin().await?;

        sqlx::query("INSERT OR REPLACE INTO group_states (id, state) VALUES (?, ?)")
            .bind(&group_id)
            .bind(&state_data)
            .execute(&mut *tx)
            .await?;

        for (epoch_id, state) in epoch_inserts {
            log::info!(
                "store insert: group_epoch_state id={:?} epoch={}",
                group_id,
                epoch_id
            );
            sqlx::query(
                "INSERT OR REPLACE INTO group_epoch_states (id, epoch, state) VALUES (?, ?, ?)",
            )
            .bind(&group_id)
            .bind(epoch_id as i64)
            .bind(state)
            .execute(&mut *tx)
            .await?;
        }
        for (epoch_id, state) in epoch_updates {
            log::info!(
                "store update: group_epoch_state id={:?} epoch={}",
                group_id,
                epoch_id
            );
            sqlx::query(
                "INSERT OR REPLACE INTO group_epoch_states (id, epoch, state) VALUES (?, ?, ?)",
            )
            .bind(&group_id)
            .bind(epoch_id as i64)
            .bind(state)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;

        Ok(())
    }

    pub async fn get_epoch_state(&self, id: &[u8], epoch_id: u64) -> anyhow::Result<Vec<u8>> {
        log::info!(
            "store select: group_epoch_state id={:?} epoch={}",
            id,
            epoch_id
        );
        let row = sqlx::query("SELECT state FROM group_epoch_states WHERE id = ? AND epoch = ?")
            .bind(id)
            .bind(epoch_id as i64)
            .fetch_one(&self.pool)
            .await?;

        Ok(row.try_get(0)?)
    }

    pub async fn get_max_epoch_id(&self, group_id: &[u8]) -> anyhow::Result<Option<u64>> {
        log::info!("store select: group_max_epoch id={:?}", group_id);
        let row = sqlx::query("SELECT MAX(epoch) FROM group_epoch_states WHERE id = ?")
            .bind(&group_id)
            .fetch_one(&self.pool)
            .await?;

        let id: Option<i64> = row.try_get(0)?;

        Ok(id.map(|val| val as u64))
    }
}

#[async_trait::async_trait]
impl MlsGroupStateStorage for GroupStateStore {
    async fn state(&self, group_id: Vec<u8>) -> Option<Zeroizing<Vec<u8>>> {
        self.get_state(&group_id).await.ok().map(Zeroizing::new)
    }
    async fn epoch(&self, group_id: Vec<u8>, epoch_id: u64) -> Option<Zeroizing<Vec<u8>>> {
        self.get_epoch_state(&group_id, epoch_id)
            .await
            .ok()
            .map(Zeroizing::new)
    }
    async fn write(
        &self,
        group_id: Vec<u8>,
        state_data: Zeroizing<Vec<u8>>,
        epoch_inserts: HashMap<u64, Zeroizing<Vec<u8>>>,
        epoch_updates: HashMap<u64, Zeroizing<Vec<u8>>>,
    ) -> bool {
        self.set_state(
            &group_id,
            &state_data,
            epoch_inserts
                .into_iter()
                .map(|(k, v)| (k, v.to_vec()))
                .collect(),
            epoch_updates
                .into_iter()
                .map(|(k, v)| (k, v.to_vec()))
                .collect(),
        )
        .await
        .is_ok()
    }
    async fn max_epoch_id(&self, group_id: Vec<u8>) -> Option<u64> {
        self.get_max_epoch_id(&group_id).await.ok().flatten()
    }
}

pub struct GroupPskStore {}

#[async_trait::async_trait]
impl MlsPreSharedKeyStorage for GroupPskStore {
    async fn get(&self, _id: Vec<u8>) -> Option<Vec<u8>> {
        None
    }
}

pub struct SelfGroupKeyPackageStore {
    pool: SqlitePool,
}

impl SelfGroupKeyPackageStore {
    pub async fn new(pool: SqlitePool) -> anyhow::Result<Self> {
        pool.execute(
            r#"
            CREATE TABLE IF NOT EXISTS self_group_key_packages(
                id INTEGER PRIMARY KEY NOT NULL,
                key_package BLOB NOT NULL
            )
            "#,
        )
        .await?;

        Ok(Self { pool })
    }

    pub async fn set(&self, id: i32, key_package_data: &[u8]) -> anyhow::Result<()> {
        log::info!("store insert: self_group_key_package id={}", id);
        sqlx::query(
            r#"
                INSERT OR REPLACE INTO self_group_key_packages (id, key_package)
                VALUES (?, ?)
                "#,
        )
        .bind(id)
        .bind(key_package_data)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn get(&self, id: i32) -> anyhow::Result<Vec<u8>> {
        log::info!("store select: self_group_key_package id={}", id);
        let row = sqlx::query(
            r#"
                SELECT key_package FROM self_group_key_packages
                WHERE id = ?
                "#,
        )
        .bind(id)
        .fetch_one(&self.pool)
        .await?;

        let key_package: Vec<u8> = row.try_get(0)?;

        Ok(key_package)
    }
}

pub struct GroupKeyPackageStore {
    pool: SqlitePool,
}

impl GroupKeyPackageStore {
    pub async fn new(pool: SqlitePool) -> anyhow::Result<Self> {
        pool.execute(
            r#"
            CREATE TABLE IF NOT EXISTS group_key_packages(
                id BLOB PRIMARY KEY NOT NULL,
                key_package BLOB NOT NULL
            )
            "#,
        )
        .await?;

        Ok(Self { pool })
    }
}

#[async_trait::async_trait]
impl MlsKeyPackageStorage for GroupKeyPackageStore {
    async fn insert(&self, id: Vec<u8>, key_package_data: Vec<u8>) -> bool {
        log::info!("store insert: group_key_package id={:?}", id);
        sqlx::query(
            r#"
                INSERT INTO group_key_packages (id, key_package)
                VALUES (?, ?)
                "#,
        )
        .bind(id)
        .bind(key_package_data)
        .execute(&self.pool)
        .await
        .is_ok()
    }

    async fn delete(&self, id: Vec<u8>) -> bool {
        log::info!("store delete: group_key_package id={:?}", id);
        sqlx::query(
            r#"
                DELETE FROM group_key_packages
                WHERE id = ?
                "#,
        )
        .bind(id)
        .execute(&self.pool)
        .await
        .is_ok()
    }

    async fn get(&self, id: Vec<u8>) -> Option<Vec<u8>> {
        log::info!("store select: group_key_package id={:?}", id);
        let row = sqlx::query(
            r#"
                SELECT key_package FROM group_key_packages
                WHERE id = ?
                "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .ok()??;

        let key_package: Vec<u8> = row.try_get(0).ok()?;

        Some(key_package)
    }
}

pub struct GroupInfo {
    pub id: u64,
    pub identifier: Vec<u8>,
    pub name: String,
    pub description: String,
}

#[derive(Clone)]
pub struct GroupInfoStore {
    pool: SqlitePool,
}

impl GroupInfoStore {
    pub async fn new(pool: SqlitePool) -> anyhow::Result<Self> {
        pool.execute(
            r#"
            CREATE TABLE IF NOT EXISTS group_infos(
                id INTEGER PRIMARY KEY NOT NULL,
                group_state_id BLOB NOT NULL,
                name TEXT NOT NULL,
                description TEXT NOT NULL
            )
            "#,
        )
        .await?;

        Ok(Self { pool })
    }

    pub async fn get_all(&self) -> anyhow::Result<Vec<GroupInfo>> {
        log::info!("store select: group_infos all");
        let rows = sqlx::query(
            r#"
                SELECT id, group_state_id, name, description FROM group_infos
                "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let mut infos = Vec::new();
        for row in rows {
            let id: i64 = row.try_get(0)?;
            let identifier: Vec<u8> = row.try_get(1)?;
            let name: String = row.try_get(2)?;
            let description: String = row.try_get(3)?;

            let group = GroupInfo {
                id: id as u64,
                name,
                description,
                identifier,
            };

            infos.push(group);
        }

        Ok(infos)
    }

    pub async fn get(&self, id: u64) -> anyhow::Result<GroupInfo> {
        log::info!("store select: group_info id={}", id);
        let row = sqlx::query(
            r#"
                SELECT id, group_state_id, name, description FROM group_infos WHERE id = ?
                "#,
        )
        .bind(id as i64)
        .fetch_one(&self.pool)
        .await?;

        let id: i64 = row.try_get(0)?;
        let identifier: Vec<u8> = row.try_get(1)?;
        let name: String = row.try_get(2)?;
        let description: String = row.try_get(3)?;

        let group = GroupInfo {
            id: id as u64,
            name,
            description,
            identifier,
        };

        Ok(group)
    }

    pub async fn set(
        &self,
        id: u64,
        name: String,
        description: String,
        group_state_id: Vec<u8>,
    ) -> anyhow::Result<()> {
        log::info!("store insert: group_info id={} name={}", id, name);
        sqlx::query(
            r#"
                INSERT OR REPLACE INTO group_infos (id, name, description, group_state_id) VALUES (?, ?, ?, ?)
                "#,
        )
        .bind(id as i64)
        .bind(name)
        .bind(description)
        .bind(group_state_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn delete(&self, id: u64) -> anyhow::Result<()> {
        log::info!("store delete: group_info id={}", id);
        sqlx::query("DELETE FROM group_infos WHERE id = ?")
            .bind(id as i64)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::SqlitePool;
    use std::collections::HashMap;

    async fn setup_test_db() -> SqlitePool {
        SqlitePool::connect(":memory:").await.unwrap()
    }

    #[tokio::test]
    async fn test_group_state_store() {
        let pool = setup_test_db().await;
        let store = GroupStateStore::new(pool).await.unwrap();

        let group_id = vec![1, 2, 3];
        let state_data = vec![4, 5, 6];
        let mut epoch_inserts = HashMap::new();
        epoch_inserts.insert(1, vec![7, 8, 9]);
        let epoch_updates = HashMap::new();

        store
            .set_state(&group_id, &state_data, epoch_inserts, epoch_updates)
            .await
            .unwrap();

        let retrieved_state = store.get_state(&group_id).await.unwrap();
        assert_eq!(retrieved_state, state_data);

        let epoch_state = store.get_epoch_state(&group_id, 1).await.unwrap();
        assert_eq!(epoch_state, vec![7, 8, 9]);

        let max_epoch = store.get_max_epoch_id(&group_id).await.unwrap().unwrap();
        assert_eq!(max_epoch, 1);
    }

    #[tokio::test]
    async fn test_group_key_package_store() {
        let pool = setup_test_db().await;
        let store = GroupKeyPackageStore::new(pool).await.unwrap();

        let id = vec![1, 2, 3];
        let key_package = vec![4, 5, 6];

        assert!(store.insert(id.clone(), key_package.clone()).await);

        let retrieved = store.get(id.clone()).await.unwrap();
        assert_eq!(retrieved, key_package);

        assert!(store.delete(id.clone()).await);
        assert!(store.get(id).await.is_none());
    }

    #[tokio::test]
    async fn test_group_info_store() {
        let pool = setup_test_db().await;
        let store = GroupInfoStore::new(pool).await.unwrap();

        let id = 1;
        let name = "Test Group".to_string();
        let description = "Test Description".to_string();
        let group_state_id = vec![1, 2, 3];

        store
            .set(
                id,
                name.clone(),
                description.clone(),
                group_state_id.clone(),
            )
            .await
            .unwrap();

        let groups = store.get_all().await.unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].name, name);
        assert_eq!(groups[0].description, description);
    }

    #[tokio::test]
    async fn test_group_info_delete() {
        let pool = setup_test_db().await;
        let store = GroupInfoStore::new(pool).await.unwrap();

        let id = 1;
        store
            .set(
                id,
                "Test Group".to_string(),
                "Test Description".to_string(),
                vec![1, 2, 3],
            )
            .await
            .unwrap();

        assert_eq!(store.get_all().await.unwrap().len(), 1);

        store.delete(id).await.unwrap();

        assert_eq!(store.get_all().await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_group_psk_store() {
        let store = GroupPskStore {};
        let result = store.get(vec![1, 2, 3]).await;
        assert!(result.is_none());
    }
}
