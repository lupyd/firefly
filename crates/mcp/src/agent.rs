//! The signed-in account's Firefly client: a device of its own, with its keys and messages in
//! the data directory, kept connected while the MCP server runs.

use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use anyhow::Context;
use firefly_client::callbacks::FireflyWsClientCallback;
use firefly_client::db::messages::MessagesStore;
use firefly_client::storage::{GroupMessage, UserMessage};
use firefly_client::websocket::{FfiFireflyWsClient, RegisteredDevice};

use crate::auth::Auth;
use crate::content::{decode_direct, decode_group};
use crate::events::{DeviceView, EventContent, EventKind, EventLog};

#[derive(Debug, Clone)]
pub struct Endpoints {
    /// Firefly's HTTP API, e.g. `https://firefly.example.com`.
    pub api_url: String,
    /// Firefly's WebSocket, e.g. `wss://firefly.example.com/`.
    pub ws_url: String,
    /// Firefly's WebTransport endpoint, tried before the WebSocket.
    pub wt_url: Option<String>,
}

/// How many of a conversation's latest messages a delivery is compared with to spot a resend.
const RESEND_WINDOW: i64 = 100;

struct Callback {
    username: String,
    auth: Arc<Auth>,
    events: Arc<EventLog>,
    store: Arc<OnceLock<MessagesStore>>,
    /// The devices last reported by `on_device_limit_reached`.
    limit_reported: std::sync::Mutex<Vec<u64>>,
}

impl Callback {
    /// Stores a delivered direct message; `false` when it is already here (this device sent
    /// it) or a resend of one already here (a sender's outbox retrying under a new id).
    async fn store(&self, message: &UserMessage) -> bool {
        let Some(store) = self.store.get() else { return true };
        if store.has_user_message(message.id).await.unwrap_or(false) {
            return false;
        }
        if let Ok(recent) = store.get_last_messages_of(&message.other, message.id as i64, RESEND_WINDOW).await
            && recent.iter().any(|m| m.sent_by_other == message.sent_by_other && m.message == message.message)
        {
            return false;
        }
        if let Err(e) = store.insert_user_message(message.clone()).await {
            log::error!("failed to store a message from {}: {e:#}", message.other);
        }
        true
    }
}

#[async_trait::async_trait]
impl FireflyWsClientCallback for Callback {
    fn name(&self) -> &str {
        &self.username
    }

    async fn get_access_token(&self) -> Option<String> {
        match self.auth.access_token().await {
            Ok(token) => Some(token),
            Err(e) => {
                log::error!("can't get an access token: {e:#}");
                if e.downcast_ref::<crate::auth::SignInRequired>().is_some() {
                    self.events.push(EventKind::SignedOut { reason: e.to_string() });
                }
                None
            }
        }
    }

    async fn on_message(&self, message: UserMessage) {
        if message.message_type & firefly_protos::MESSAGE_TYPE_HIDDEN != 0 || !self.store(&message).await {
            return;
        }
        // Messages this account sent from another device are stored but aren't news.
        if !message.sent_by_other {
            return;
        }
        if let Some(content) = EventContent::from_content(decode_direct(&message.message)) {
            self.events.push(EventKind::Direct { from: message.other, message_id: message.id, content });
        }
    }

    async fn on_group_message(&self, message: GroupMessage) {
        // The client stores group messages itself.
        if message.by == self.username || message.message_type & firefly_protos::MESSAGE_TYPE_HIDDEN != 0 {
            return;
        }
        let (channel_id, content) = decode_group(&message.message);
        if let Some(content) = EventContent::from_content(content) {
            self.events.push(EventKind::Group {
                group_id: message.group_id,
                channel_id,
                from: message.by,
                message_id: message.id,
                content,
            });
        }
    }

    // Pin changes and history imports; not news.
    async fn on_group_message_updated(&self, _message: GroupMessage) {}

    async fn on_group_joined(&self, group_id: u64) {
        self.events.push(EventKind::AddedToGroup { group_id });
    }

    async fn on_group_updated(&self, group_id: u64) {
        self.events.push(EventKind::GroupUpdated { group_id });
    }

    async fn on_group_members_changed(&self, group_id: u64, added: Vec<String>, removed: Vec<String>) {
        self.events.push(EventKind::MembersChanged { group_id, joined: added, left: removed });
    }

    async fn on_unauthenticated(&self) {
        log::warn!("the server rejected this account's token");
    }

    async fn on_device_revoked(&self) {
        self.events.push(EventKind::SignedOut {
            reason: "this device was removed from the account; run login again".into(),
        });
    }

    async fn on_device_limit_reached(&self, devices: Vec<RegisteredDevice>) {
        // Repeated while it waits: report each new list once.
        let ids: Vec<u64> = devices.iter().map(|d| d.address_id).collect();
        if std::mem::replace(&mut *self.limit_reported.lock().unwrap(), ids.clone()) == ids {
            return;
        }
        if devices.is_empty() {
            log::info!("a device was removed; this device is registered now");
            return;
        }
        log::warn!(
            "{} already has {} devices, so this one can't be added yet. Remove one in the app (Settings, Devices); this device then connects by itself.",
            self.username,
            devices.len()
        );
        self.events.push(EventKind::DeviceLimitReached { devices: devices.iter().map(DeviceView::from).collect() });
    }
}

pub struct Agent {
    pub client: FfiFireflyWsClient,
    pub username: String,
    pub events: Arc<EventLog>,
    pub store: MessagesStore,
    _lock: std::fs::File,
}

/// Two processes on one key store would break its encryption sessions.
fn lock_data_dir(dir: &Path) -> anyhow::Result<std::fs::File> {
    let path = dir.join("firefly-mcp.lock");
    let file = std::fs::OpenOptions::new().create(true).truncate(false).write(true).open(&path)?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(std::fs::TryLockError::WouldBlock) => anyhow::bail!(
            "another firefly-mcp is already running on {} (one process per account and data directory)",
            dir.display()
        ),
        Err(std::fs::TryLockError::Error(e)) => Err(e.into()),
    }
}

impl Agent {
    /// Opens the account's device in `data_dir` and starts connecting in the background.
    /// `device_name` is what the account's device list calls it.
    pub async fn start(auth: Arc<Auth>, endpoints: &Endpoints, data_dir: PathBuf, device_name: &str) -> anyhow::Result<Arc<Self>> {
        std::fs::create_dir_all(&data_dir)?;
        let lock = lock_data_dir(&data_dir)?;
        let username = auth.username().await;
        let events = Arc::new(EventLog::default());
        let store_slot = Arc::new(OnceLock::new());
        let callback = Callback {
            username: username.clone(),
            auth,
            events: events.clone(),
            store: store_slot.clone(),
            limit_reported: Default::default(),
        };
        let client = FfiFireflyWsClient::create_with_endpoints(
            endpoints.api_url.clone(),
            endpoints.ws_url.clone(),
            endpoints.wt_url.clone(),
            1000,
            Box::new(callback),
            data_dir.join("firefly.db").to_string_lossy().into_owned(),
            15_000,
        )
        .await
        .context("can't open the Firefly client")?;
        client.set_device_name(device_name);
        let store = client.messages_store();
        let _ = store_slot.set(store.clone());
        if let Err(e) = client.load_all_groups().await {
            log::warn!("failed to load groups: {e:#}");
        }
        let connecting = client.clone();
        tokio::spawn(async move {
            if let Err(e) = connecting.initialize_with_retrying().await {
                log::error!("firefly connection stopped: {e:#}");
            }
        });
        Ok(Arc::new(Self { client, username, events, store, _lock: lock }))
    }

    pub async fn dispose(&self) {
        self.client.dispose().await;
    }
}
