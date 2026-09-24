use crate::callbacks::{CallSignal, GroupMeetingSignal, ReadUserMessagesUpto};
use crate::storage::GroupMessage;
#[path = "group_snapshots.rs"]
mod group_snapshots;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, AtomicU64},
    },
    time::Duration,
};

use anyhow::Context;
use base64::Engine;
use bytes::Bytes;
use firefly_protos::firefly::{self};
use futures::{SinkExt, StreamExt};
use libsignal_protocol::{DeviceId, PreKeyId, ProtocolAddress};
use mls_rs::MlsMessage;
use rand::RngCore;
use sqlx::SqlitePool;
use tokio::{
    net::TcpStream,
    sync::{RwLock, mpsc::Sender, oneshot},
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

use crate::{
    callbacks::FireflyWsClientCallback,
    db::{
        auth::get_claims_from_token,
        conversations::ConversationSettings,
        favourites::FavouriteMessagesStore,
        ffi_stores::FfiKeyStores,
        group_messages::GroupMessagesStore,
        group_stores::{GroupInfo, GroupInfoStore, GroupKeyPackageStore, SelfGroupKeyPackageStore},
        history_keys::HistoryKeysStore,
        keyvalue::{KEY_FCM_TOKEN, KEY_LAST_RECEIVED_MESSAGE_ID, KeyValueStore},
        messages::{MessagesStore, UserMessage},
        search::{SearchEngine, SearchResultItem, SearchScope},
        setup_pool_from_path,
    },
    group::{FfiMlsClient, FfiMlsGroup},
    history::{
        compute_unencrypted_hash, decrypt_and_unpack_chunk, pack_messages_into_chunk,
        DEFAULT_CHUNK_SIZE, MAX_CHUNK_BYTES, history_chunk_url, validate_chunk_records,
    },
    storage::{FavouriteMessage, FavouriteMessageStorage},
    logger::CURRENT_CLIENT,
    utils::{
        HTTP_CLIENT, deserialize_proto, get_current_timestamp_microseconds_since_epoch,
        get_current_timestamp_millis_since_epoch, get_current_timestamp_seconds_since_epoch, rng,
        serialize_proto, write_url_comma_seperated,
    },
};


// Trait removed, imported from callbacks module

struct InternalSignalCallback {
    inner: Arc<dyn FireflyWsClientCallback>,
    signal_tx: tokio::sync::mpsc::Sender<crate::callbacks::GroupHistorySignal>,
}

#[async_trait::async_trait]
impl FireflyWsClientCallback for InternalSignalCallback {
    fn name(&self) -> &str { self.inner.name() }
    async fn get_access_token(&self) -> Option<String> { self.inner.get_access_token().await }
    async fn on_message(&self, message: UserMessage) { self.inner.on_message(message).await; }
    async fn on_group_message(&self, group_message: GroupMessage) { self.inner.on_group_message(group_message).await; }
    async fn on_group_message_updated(&self, message: GroupMessage) { self.inner.on_group_message_updated(message).await; }
    async fn on_group_joined(&self, group_id: u64) { self.inner.on_group_joined(group_id).await; }
    async fn on_call_signal(&self, signal: CallSignal) { self.inner.on_call_signal(signal).await; }
    async fn on_group_meeting_signal(&self, signal: GroupMeetingSignal) { self.inner.on_group_meeting_signal(signal).await; }
    async fn on_read_user_messages_upto(&self, read: ReadUserMessagesUpto) { self.inner.on_read_user_messages_upto(read).await; }
    async fn on_group_history_signal(&self, signal: crate::callbacks::GroupHistorySignal) {
        let _ = self.signal_tx.send(signal.clone()).await;
        self.inner.on_group_history_signal(signal).await;
    }
    async fn on_group_updated(&self, group_id: u64) {
        self.inner.on_group_updated(group_id).await;
    }
    async fn on_unauthenticated(&self) {
        self.inner.on_unauthenticated().await;
    }
}

pub struct Connection {
    sender_task: tokio::task::JoinHandle<()>,
    receiver_task: tokio::task::JoinHandle<()>,
    sender: Sender<Bytes>,
}

impl Connection {
    pub fn new(
        callbacks: Arc<dyn FireflyWsClientCallback>,
        key_stores: Arc<FfiKeyStores>,
        pending_requests: PendingRequests,
        stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
        on_connection_closed: oneshot::Sender<()>,
        key_value_store: KeyValueStore,
        firefly_mls_client: Arc<FfiMlsClient>,
        group_info_store: GroupInfoStore,
        group_messages_store: GroupMessagesStore,
        history_keys_store: HistoryKeysStore,
        address_id: u64,
        device_id: u8,
        firefly_base_url: String,
        signal_tx: tokio::sync::mpsc::Sender<crate::callbacks::GroupHistorySignal>,
    ) -> Self {
        let callbacks: Arc<dyn FireflyWsClientCallback> = Arc::new(InternalSignalCallback {
            inner: callbacks,
            signal_tx,
        });
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<Bytes>(100);
        let (mut ws_sender, mut ws_receiver) = stream.split();
        let sender2 = sender.clone();
        let id = callbacks.name().to_string();
        let receiver_task = tokio::spawn(CURRENT_CLIENT.scope(id.clone(), async move {
            while let Some(Ok(msg)) = ws_receiver.next().await {
                match msg {
                    Message::Binary(bytes) => {
                        let payload = bytes;
                        match deserialize_proto::<firefly::ServerMessage<'_>>(&payload) {
                            Ok(server_message) => {
                                if let Err(err) = on_server_message(
                                    server_message,
                                    &pending_requests,
                                    &key_stores,
                                    &callbacks,
                                    &key_value_store,
                                    &firefly_mls_client,
                                    &group_info_store,
                                    &group_messages_store,
                                    &history_keys_store,
                                    sender2.clone(),
                                    address_id,
                                    device_id,
                                    &firefly_base_url,
                                )
                                .await
                                {
                                    log::error!("failed to handle server message: {}", err);
                                }

                            }
                            Err(err) => log::error!("failed to deserialize message: {}", err),
                        }
                    }

                    Message::Close(close_frame) => {
                        log::info!("ws closed: {:?}", close_frame);
                        break;
                    }

                    _ => {
                        log::warn!("unhandled ws message type {:?}", msg);
                    }
                };
            }
            log::info!("ws receiver task finished");

            if on_connection_closed.send(()).is_err() {
                log::error!("unable to send on_connection_closed signal");
            }
        }));

        let last_message_sent_ts_secs =
            Arc::new(AtomicU64::new(get_current_timestamp_seconds_since_epoch()));

        let sender_task = {
            let last_message_sent_ts_secs = last_message_sent_ts_secs.clone();
            let id = id.clone();
            tokio::spawn(CURRENT_CLIENT.scope(id.clone(), async move {
                while let Some(msg) = receiver.recv().await {
                    if let Err(err) = ws_sender.send(Message::Binary(msg)).await {
                        log::error!("failed to send message: {}", err);
                        break;
                    }
                    last_message_sent_ts_secs.store(
                        get_current_timestamp_seconds_since_epoch(),
                        std::sync::atomic::Ordering::Relaxed,
                    );
                }
                log::info!("ws sender task finished");
            }))
        };
        {
            let ping_sender = sender.clone();

            let id = id.clone();
            tokio::spawn(CURRENT_CLIENT.scope(id, async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(10)).await;

                    let now_secs = get_current_timestamp_seconds_since_epoch();
                    let last_sent_secs =
                        last_message_sent_ts_secs.load(std::sync::atomic::Ordering::Relaxed);

                    if now_secs - last_sent_secs < 30 {
                        continue;
                    }

                    let ping = vec![0u8; 64];
                    let ping = serialize_proto(&firefly::ClientMessage {
                        message: firefly::mod_ClientMessage::OneOfmessage::ping(ping.into()),
                    })
                    .unwrap();

                    if ping_sender.send(ping).await.is_err() {
                        break;
                    }
                }
            }));
        }
        Self {
            sender_task,
            receiver_task,
            sender,
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.sender_task.abort();
        self.receiver_task.abort();
        log::info!("dropping connection");
    }
}

type PendingRequests = Arc<std::sync::Mutex<HashMap<u32, oneshot::Sender<Bytes>>>>;

#[derive(Default, Clone, Debug)]
pub enum ConnectionState {
    #[default]
    Disconnected,
    Initializing,
    Retrying,
    Connected,
    CheckingSetup,
}

#[derive(Default, Debug)]
pub struct HistoryImportPage {
    pub imported_count: usize,
    pub next_before: Option<u64>,
    pub has_more: bool,
    pub pending: bool,
}

pub struct FireflyWsClient {
    callbacks: Arc<dyn FireflyWsClientCallback>,
    retry_interval: Duration,
    firefly_base_url: String,
    firefly_base_ws_url: String,
    cdn_base_url: std::sync::RwLock<Option<String>>,
    history_work: tokio::sync::Mutex<()>,
    snapshot_work: tokio::sync::Mutex<()>,
    key_stores: Arc<FfiKeyStores>,

    key_value_store: KeyValueStore,

    connection: Arc<RwLock<Option<Connection>>>,
    last_connection_tried_timestamp: AtomicU64,

    pending_requests: PendingRequests,

    request_timeout: Duration,
    stop_reconnecting: AtomicBool,

    state: Arc<std::sync::RwLock<ConnectionState>>,

    addressId: AtomicU64,
    group_messages_store: GroupMessagesStore,
    messages_store: MessagesStore,
    favourite_messages_store: FavouriteMessagesStore,
    pub history_keys_store: HistoryKeysStore,
    firefly_mls_client: Arc<tokio::sync::OnceCell<Arc<FfiMlsClient>>>,
    group_info_store: GroupInfoStore,
    self_group_key_packages_store: SelfGroupKeyPackageStore,
    group_key_packages_store: GroupKeyPackageStore,
    fully_initialized: AtomicBool,
    pool: SqlitePool,
    next_request_id: AtomicU32,
    last_connection_error: std::sync::Mutex<Option<String>>,
}

impl FireflyWsClient {
    pub async fn create(
        firefly_base_url: String,
        firefly_base_ws_url: String,
        retry_interval_in_ms: u64,
        callbacks: Box<dyn FireflyWsClientCallback>,
        key_stores_pathname: String,
        request_timeout_in_ms: u64,
    ) -> anyhow::Result<Self> {
        let pool = setup_pool_from_path(&key_stores_pathname, 5).await?;
        let key_stores = Arc::new(FfiKeyStores::new(pool.clone()).await?);
        let key_value_store = KeyValueStore::new(pool.clone()).await?;

        let groups_store = GroupMessagesStore::new(pool.clone()).await?;
        let messages_store = MessagesStore::new(pool.clone()).await?;
        let favourite_messages_store = FavouriteMessagesStore::new(pool.clone()).await?;
        let self_group_key_packages_store = SelfGroupKeyPackageStore::new(pool.clone()).await?;
        let group_key_packages_store = GroupKeyPackageStore::new(pool.clone()).await?;
        let history_keys_store = HistoryKeysStore::new(pool.clone()).await?;

        let last_connection_established_timestamp = get_current_timestamp_millis_since_epoch();

        let group_info_store = GroupInfoStore::new(pool.clone()).await?;

        Ok(Self {
            pool,
            callbacks: callbacks.into(),
            retry_interval: Duration::from_millis(retry_interval_in_ms),
            firefly_base_url,
            firefly_base_ws_url,
            cdn_base_url: std::sync::RwLock::new(
                std::env::var("FIREFLY_CDN_URL")
                    .or_else(|_| std::env::var("CDN_URL"))
                    .ok(),
            ),
            key_stores,
            last_connection_tried_timestamp: last_connection_established_timestamp.into(),

            pending_requests: Default::default(),
            request_timeout: Duration::from_millis(request_timeout_in_ms),
            connection: Default::default(),
            stop_reconnecting: AtomicBool::new(false),
            key_value_store,
            state: Default::default(),
            addressId: Default::default(),
            group_messages_store: groups_store,
            messages_store,
            favourite_messages_store,
            history_keys_store,
            history_work: tokio::sync::Mutex::new(()),
            snapshot_work: tokio::sync::Mutex::new(()),
            self_group_key_packages_store,
            group_key_packages_store,
            fully_initialized: AtomicBool::new(false),
            firefly_mls_client: Default::default(),

            group_info_store,
            next_request_id: Default::default(),
            last_connection_error: std::sync::Mutex::new(None),
        })
    }
    pub async fn initialize_with_retrying(&self) -> anyhow::Result<()> {
        {
            let mut state = self.state.write().unwrap();
            if !matches!(*state, ConnectionState::Disconnected) {
                log::info!(
                    "initialize_with_retrying: already in state {:?}, skipping duplicate initialization",
                    *state
                );
                return Ok(());
            }
            *state = ConnectionState::CheckingSetup;
        }

        let id = self.callbacks.name().to_string();
        while !self
            .stop_reconnecting
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            CURRENT_CLIENT
                .scope(id.clone(), async {
                    log::info!("checking setup");
                    match self.check_setup().await {
                        Ok(_) => {
                            log::info!("setup check passed");
                            // break is tricky inside async block for loop
                        }
                        Err(err) => {
                            log::error!("failed to check setup: {:?}", err);
                        }
                    }
                })
                .await;

            if self.addressId.load(std::sync::atomic::Ordering::Relaxed) != 0 {
                break;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }

        {
            *self.state.write().unwrap() = ConnectionState::Initializing;
        }
        while !self
            .stop_reconnecting
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            CURRENT_CLIENT
                .scope(id.clone(), async {
                    {
                        *self.state.write().unwrap() = ConnectionState::Retrying;
                    }

                    self.last_connection_tried_timestamp.store(
                        get_current_timestamp_millis_since_epoch(),
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    let _ = self.connect().await;

                    if !self
                        .stop_reconnecting
                        .load(std::sync::atomic::Ordering::Relaxed)
                    {
                        tokio::time::sleep(self.retry_interval).await;
                    }
                })
                .await;
        }

        {
            *self.state.write().unwrap() = ConnectionState::Disconnected;
        }

        Ok(())
    }

    async fn connect(&self) -> anyhow::Result<()> {
        let token = self
            .callbacks
            .get_access_token()
            .await
            .context("token not found")?;

        let addressId = self.addressId.load(std::sync::atomic::Ordering::Relaxed);

        if addressId == 0 {
            return Err(anyhow::anyhow!("addressId is not set"));
        }

        let identity = self
            .key_stores
            .store()
            .identity_store
            .get_full_identity_key_pair()
            .await?;

        let device_id = identity.device_id;

        let last_synced_upto = self
            .key_value_store
            .get(KEY_LAST_RECEIVED_MESSAGE_ID)
            .await
            .unwrap_or_default()
            .parse::<u64>()
            .unwrap_or_default();

        let base_ws = self.firefly_base_ws_url.trim_end_matches('/');
        let url = format!(
            "{}/?uid={}&device_id={}&last_synced_upto={}&token={}",
            base_ws, addressId, device_id, last_synced_upto, token
        );

        let sanitized_url = format!(
            "{}/?uid={}&device_id={}&last_synced_upto={}&token=[REDACTED]",
            base_ws, addressId, device_id, last_synced_upto
        );

        let show_connecting = {
            let last_err = self.last_connection_error.lock().unwrap();
            last_err.is_none()
        };
        if show_connecting {
            log::info!("connecting to {}", sanitized_url);
        }

        let (stream, response) = match tokio_tungstenite::connect_async(&url).await {
            Ok(v) => v,
            Err(err) => {
                let err_str = format!("{:?}", err);
                if let tokio_tungstenite::tungstenite::error::Error::Http(ref resp) = err {
                    if resp.status() == 401 || resp.status() == 403 {
                        log::warn!("Connection rejected by server with HTTP status {}, invoking on_unauthenticated", resp.status());
                        self.callbacks.on_unauthenticated().await;
                    }
                }
                let mut last_err = self.last_connection_error.lock().unwrap();
                if last_err.as_ref() != Some(&err_str) {
                    log::error!("connection request failed {:?}", err);
                    log::info!("waiting {}ms to reconnect", self.retry_interval.as_millis());
                    *last_err = Some(err_str);
                }
                return Err(err.into());
            }
        };

        {
            let mut last_err = self.last_connection_error.lock().unwrap();
            *last_err = None;
        }

        {
            *self.state.write().unwrap() = ConnectionState::Connected;
        }

        log::info!(
            "connected successfully to {}, Headers: {:?} ",
            sanitized_url,
            response.headers()
        );

        let pending_requests = self.pending_requests.clone();
        let key_stores = self.key_stores.clone();
        let callbacks = self.callbacks.clone();

        {
            pending_requests.lock().unwrap().clear(); // cleanup for fresh connection
        }

        let (on_connection_closed_tx, on_connection_closed_rx) = oneshot::channel::<()>();

        let mut signal_rx = None;
        let firefly_mls_client = self
            .firefly_mls_client
            .get()
            .context("firefly_mls_client is uninitialized")?;

        {
            let mut g = self.connection.write().await;
            let (signal_tx, signal_rx_new) = tokio::sync::mpsc::channel(100);
            signal_rx = Some(signal_rx_new);
            *g = Some(Connection::new(
                callbacks,
                key_stores,
                pending_requests,
                stream,
                on_connection_closed_tx,
                self.key_value_store.clone(),
                firefly_mls_client.clone(),
                self.group_info_store.clone(),
                self.group_messages_store.clone(),
                self.history_keys_store.clone(),
                self.addressId.load(std::sync::atomic::Ordering::Relaxed),
                self.key_stores
                    .store()
                    .identity_store
                    .get_full_identity_key_pair()
                    .await
                    .map(|i| i.device_id)
                    .unwrap_or(0) as u8,
                self.firefly_base_url.clone(),
                signal_tx,
            ));
        }

        if let Err(err) = self.sync_all_group_messages().await {
            log::error!("sync group messages failed: {:?}", err);
        }

        if let Some(token) = self.callbacks.get_access_token().await {
            let address_id = self.addressId.load(std::sync::atomic::Ordering::Relaxed);
            let device_id = self
                .key_stores
                .store()
                .identity_store
                .get_full_identity_key_pair()
                .await
                .map(|i| i.device_id)
                .unwrap_or(0) as u8;

            let _ = self.join_groups(&token, address_id, device_id).await;
            let _ = self
                .add_requested_re_add_group_members(&token, address_id, device_id)
                .await;
        }

        if let Some(mut rx) = signal_rx {
            tokio::select! {
                _ = on_connection_closed_rx => {},
                _ = async {
                    while let Some(signal) = rx.recv().await {
                        if let Err(e) = self.handle_group_history_signal(signal).await {
                            log::error!("auto handle_group_history_signal error: {:?}", e);
                        }
                    }
                } => {},
            }
        } else {
            on_connection_closed_rx.await?;
        }
        Ok(())
    }

    pub async fn dispose(&self) {
        self.stop_reconnecting
            .store(true, std::sync::atomic::Ordering::Relaxed);
        self.connection.write().await.take();
    }

    pub async fn request(&self, request: firefly::Request<'_>) -> anyhow::Result<Bytes> {
        let (tx, rx) = oneshot::channel();
        let id = {
            self.next_request_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        };
        let mut request = request;
        request.id = id;

        {
            self.pending_requests.lock().unwrap().insert(id, tx);
        }

        self.send_request(request).await?;

        let bytes = rx.await?;
        Ok(bytes)
    }

    pub async fn get_online_status(&self, usernames: Vec<String>) -> anyhow::Result<Vec<String>> {
        if usernames.is_empty() {
            return Ok(vec![]);
        }

        let req = firefly::Request {
            payload: firefly::mod_Request::OneOfpayload::userOnlineStatus(
                firefly::UserOnlineStatusRequest {
                    usernames: usernames.iter().map(|x| x.as_str().into()).collect(),
                },
            ),
            ..Default::default()
        };

        let response_bytes = self.request(req).await?;
        let response = deserialize_proto::<firefly::Response>(&response_bytes)?;

        if let Some(err) = response.error {
            return Err(anyhow::anyhow!(
                "server error {}: {}",
                err.errorCode,
                err.error
            ));
        }

        match response.body {
            firefly::mod_Response::OneOfbody::userOnlineStatus(res) => {
                let mut online_users = Vec::new();
                for (i, username) in usernames.iter().enumerate() {
                    if i >= 32 {
                        break;
                    }
                    if (res.online_bits & (1 << i)) != 0 {
                        online_users.push(username.clone());
                    }
                }
                Ok(online_users)
            }
            _ => Err(anyhow::anyhow!("unexpected response body")),
        }
    }

    async fn send_request(&self, request: firefly::Request<'_>) -> anyhow::Result<()> {
        let client_message = firefly::ClientMessage {
            message: firefly::mod_ClientMessage::OneOfmessage::request(request),
        };

        let g = self.connection.read().await;
        if let Some(conn) = &*g {
            conn.sender.send(serialize_proto(&client_message)?).await?;
            Ok(())
        } else {
            Err(anyhow::anyhow!("not connected"))
        }
    }

    pub async fn get_device_id(&self) -> u32 {
        self.key_stores
            .store()
            .identity_store
            .get_full_identity_key_pair()
            .await
            .map(|i| i.device_id as u32)
            .unwrap_or(0)
    }

    pub async fn get_self_username(&self) -> anyhow::Result<String> {
        let token = self
            .callbacks
            .get_access_token()
            .await
            .context("token not found")?;
        let claims = get_claims_from_token(&token)?;
        Ok(claims.uname)
    }

    pub fn generate_call_id(&self) -> u64 {
        rand::random::<u64>()
    }

    pub async fn send_call_signal(
        &self,
        call_id: u64,
        receiver_username: String,
        signal_type: firefly_protos::firefly::CallSignalType,
        sdp: String,
        candidate: String,
        sdp_m_line_index: i32,
        sdp_mid: String,
    ) -> anyhow::Result<()> {
        let sender_username = self.get_self_username().await?;
        let sender_device_id = self.get_device_id().await;

        let call_signal = firefly_protos::firefly::CallSignal {
            call_id,
            sender_username: std::borrow::Cow::Owned(sender_username),
            receiver_username: std::borrow::Cow::Owned(receiver_username),
            type_pb: signal_type,
            sdp: std::borrow::Cow::Owned(sdp),
            candidate: std::borrow::Cow::Owned(candidate),
            sdp_m_line_index,
            sdp_mid: std::borrow::Cow::Owned(sdp_mid),
            sender_device_id,
        };

        let client_message = firefly_protos::firefly::ClientMessage {
            message: firefly_protos::firefly::mod_ClientMessage::OneOfmessage::callSignal(
                call_signal,
            ),
        };

        let g = self.connection.read().await;
        if let Some(conn) = &*g {
            conn.sender.send(serialize_proto(&client_message)?).await?;
            Ok(())
        } else {
            Err(anyhow::anyhow!("Websocket connection is offline"))
        }
    }

    pub async fn initiate_call(
        &self,
        call_id: u64,
        receiver_username: String,
        sdp_offer: String,
    ) -> anyhow::Result<()> {
        self.send_call_signal(
            call_id,
            receiver_username,
            firefly_protos::firefly::CallSignalType::CALL_REQUEST,
            sdp_offer,
            "".to_string(),
            0,
            "".to_string(),
        )
        .await
    }

    pub async fn accept_call(
        &self,
        call_id: u64,
        caller_username: String,
        sdp_answer: String,
    ) -> anyhow::Result<()> {
        self.send_call_signal(
            call_id,
            caller_username,
            firefly_protos::firefly::CallSignalType::CALL_ANSWER,
            sdp_answer,
            "".to_string(),
            0,
            "".to_string(),
        )
        .await
    }

    pub async fn reject_call(&self, call_id: u64, caller_username: String) -> anyhow::Result<()> {
        self.send_call_signal(
            call_id,
            caller_username,
            firefly_protos::firefly::CallSignalType::CALL_REJECT,
            "".to_string(),
            "".to_string(),
            0,
            "".to_string(),
        )
        .await
    }

    pub async fn cancel_call(&self, call_id: u64, receiver_username: String) -> anyhow::Result<()> {
        self.send_call_signal(
            call_id,
            receiver_username,
            firefly_protos::firefly::CallSignalType::CALL_CANCEL,
            "".to_string(),
            "".to_string(),
            0,
            "".to_string(),
        )
        .await
    }

    pub async fn hangup_call(&self, call_id: u64, other_username: String) -> anyhow::Result<()> {
        self.send_call_signal(
            call_id,
            other_username,
            firefly_protos::firefly::CallSignalType::CALL_HANGUP,
            "".to_string(),
            "".to_string(),
            0,
            "".to_string(),
        )
        .await
    }

    pub async fn send_ice_candidate(
        &self,
        call_id: u64,
        other_username: String,
        candidate: String,
        sdp_mid: String,
        sdp_m_line_index: i32,
    ) -> anyhow::Result<()> {
        self.send_call_signal(
            call_id,
            other_username,
            firefly_protos::firefly::CallSignalType::CALL_ICECANDIDATE,
            "".to_string(),
            candidate,
            sdp_m_line_index,
            sdp_mid,
        )
        .await
    }

    async fn create_encrypted_message(
        &self,
        address: ProtocolAddress,
        addressId: u64,
        settings: u32,
        payload: Vec<u8>,
    ) -> anyhow::Result<firefly::UserMessage<'static>> {
        let fromId = self.addressId.load(std::sync::atomic::Ordering::Relaxed);

        if fromId == 0 {
            return Err(anyhow::anyhow!("self.addressId not set"));
        }

        let hashValue = twox_hash::XxHash3_64::oneshot(&payload);
        let cipher = self
            .key_stores
            .encrypt(address, payload)
            .await
            .map_err(|err| anyhow::anyhow!(err))?;

        let message = firefly::UserMessage {
            id: get_current_timestamp_microseconds_since_epoch(),
            toId: addressId,
            fromId,
            text: cipher.cipher_text.into(),
            type_pb: cipher.ty as u32,
            settings,
            fromUsername: Default::default(),
            fromDeviceId: Default::default(),

            hashValue,
        };

        Ok(message)
    }

    async fn create_conversation(
        &self,
        to: &str,
        settings: u64,
        token: &str,
    ) -> anyhow::Result<ConversationSettings> {
        let url = format!(
            "{}/user/conversation?other={}&settings={}&merge=true",
            self.firefly_base_url, to, settings
        );

        let response = HTTP_CLIENT.post(url).bearer_auth(token).send().await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "unexpected status [{}]: {}",
                response.status(),
                response.text().await?
            ));
        }

        self.key_stores
            .store()
            .conversation_store
            .set_conversation(to, ConversationSettings::new(settings))
            .await?;

        Ok(ConversationSettings::new(settings))
    }

    pub async fn read_user_messages_upto(
        &self,
        other: String,
        upto_message_id: u64,
    ) -> anyhow::Result<()> {
        let read_msg = firefly_protos::firefly::ReadUserMessagesUpto {
            other: std::borrow::Cow::Owned(other),
            uptoMessageId: upto_message_id,
        };

        let client_message = firefly_protos::firefly::ClientMessage {
            message: firefly_protos::firefly::mod_ClientMessage::OneOfmessage::readUserMessagesUpto(
                read_msg,
            ),
        };

        let g = self.connection.read().await;
        if let Some(conn) = &*g {
            conn.sender.send(serialize_proto(&client_message)?).await?;
            Ok(())
        } else {
            Err(anyhow::anyhow!("Websocket connection is offline"))
        }
    }

    pub async fn encrypt_and_send(
        &self,
        to: String,
        payload: Vec<u8>,
    ) -> anyhow::Result<UserMessage> {
        self.encrypt_and_send_with_type(to, payload, 0).await
    }

    pub async fn encrypt_and_send_with_type(
        &self,
        to: String,
        payload: Vec<u8>,
        message_type: u32,
    ) -> anyhow::Result<UserMessage> {
        let result = self.encrypt_and_send_internal(&to, &payload, message_type).await;
        match result {
            Ok(msg) => Ok(msg),
            Err(err) => {
                let err_str = err.to_string();
                if err_str.contains("invalid from_address")
                    || err_str.contains("Sender address is invalid or rotated")
                    || err_str.contains("400")
                {
                    log::warn!("encrypt_and_send failed with sender address error, re-registering device address: {err}");
                    self.fully_initialized
                        .store(false, std::sync::atomic::Ordering::Relaxed);
                    if let Err(re_reg_err) = self.check_setup().await {
                        log::error!("failed to re-register device address during recovery: {re_reg_err}");
                        return Err(err);
                    }
                    log::info!("re-registered device address successfully, retrying encrypt_and_send");
                    return self.encrypt_and_send_internal(&to, &payload, message_type).await;
                }
                Err(err)
            }
        }
    }

    pub async fn encrypt_and_send_pinned(
        &self,
        to: String,
        payload: Vec<u8>,
    ) -> anyhow::Result<UserMessage> {
        self.encrypt_and_send_with_type(to, payload, firefly_protos::MESSAGE_TYPE_PINNED).await
    }

    async fn encrypt_and_send_internal(
        &self,
        to: &str,
        payload: &[u8],
        message_type: u32,
    ) -> anyhow::Result<UserMessage> {
        let token = self
            .callbacks
            .get_access_token()
            .await
            .context("token not found")?;

        let store = self.key_stores.store();

        let _settings =
            if let Some(settings) = store.conversation_store.get_conversation(to).await? {
                settings
            } else {
                self.create_conversation(to, 1, &token).await?
            };
        let address_store = &self.key_stores.store().address_store;

        let other_addresses = address_store.get(to).await?;

        if other_addresses.is_empty() {
            self.get_and_process_all_pre_key_bundles_of_user(to, &token)
                .await?;
        }

        let other_addresses = self.key_stores.store().address_store.get(to).await?;
        if other_addresses.is_empty() {
            return Err(anyhow::anyhow!("no addresses found for user {}", to));
        }

        let claims = get_claims_from_token(&token)?;
        let self_username = claims.uname;

        let self_addresses = address_store.get(&self_username).await?;

        let mut message_entries = firefly::UploadUserMessage::default();

        let message_settings = 0;
        let self_message_settings = 1;

        let mut effective_payload = payload.to_vec();
        let mut effective_type = message_type;

        if let Ok(mut inner) = deserialize_proto::<firefly::UserMessageInner>(&effective_payload) {
            let inner_type = inner.message_type | match &inner.message {
                firefly::mod_UserMessageInner::OneOfmessage::messagePayload(p) => p.message_type,
                _ => 0,
            };
            effective_type |= inner_type;
            if message_type != 0 {
                inner.message_type |= message_type;
                if let firefly::mod_UserMessageInner::OneOfmessage::messagePayload(ref mut p) = inner.message {
                    p.message_type |= message_type;
                }
                if let Ok(ser) = serialize_proto(&inner) {
                    effective_payload = ser.to_vec();
                }
            }
        } else if message_type != 0 {
            let inner = firefly::UserMessageInner {
                message: firefly::mod_UserMessageInner::OneOfmessage::messagePayload(
                    firefly::MessagePayload {
                        text: String::from_utf8_lossy(payload).into_owned().into(),
                        files: None,
                        ext: firefly::mod_MessagePayload::OneOfext::None,
                        message_type,
                    },
                ),
                nonce: rng().next_u32(),
                message_type,
            };
            if let Ok(ser) = serialize_proto(&inner) {
                effective_payload = ser.to_vec();
            }
        }

        for address in other_addresses.iter() {
            let message = self
                .create_encrypted_message(
                    ProtocolAddress::new(to.to_string(), DeviceId::new(address.device_id)?),
                    address.address_id,
                    message_settings,
                    effective_payload.clone(),
                )
                .await?;
            message_entries.messages.push(message);
        }
        let self_message_payload = serialize_proto(&firefly::UserMessageInner {
            message: firefly::mod_UserMessageInner::OneOfmessage::selfMessage(
                firefly::SelfUserMessage {
                    to: to.to_string().into(),
                    inner: effective_payload.clone().into(),
                },
            ),
            nonce: rng().next_u32(),
            message_type: effective_type,
        })?
        .to_vec();
        {
            for address in self_addresses.iter() {
                let message = self.create_encrypted_message(
                    ProtocolAddress::new(self_username.clone(), DeviceId::new(address.device_id)?),
                    address.address_id,
                    self_message_settings,
                    self_message_payload.clone(),
                );
                match message.await {
                    Ok(m) => message_entries.messages.push(m),
                    Err(e) => {
                        log::warn!(
                            "Failed to encrypt self-sync message for device {}: {}",
                            address.device_id,
                            e
                        );
                    }
                }
            }
        }
        let bytes = self
            .request(firefly::Request {
                payload: firefly::mod_Request::OneOfpayload::uploadUserMessage(message_entries),
                id: 0,
            })
            .await?;
        let response = deserialize_proto::<firefly::Response<'_>>(&bytes)?;

        if let Some(error) = response.error {
            return Err(anyhow::anyhow!("[{}], {}", error.errorCode, error.error));
        }

        let mut more_addresses_to_send_to = Vec::new();

        let mut addresses_to_not_send_to = self_addresses.clone();
        addresses_to_not_send_to.extend_from_slice(&other_addresses);

        if let firefly::mod_Response::OneOfbody::userMessageUploaded(body) = response.body {
            log::info!("uploaded messages: {:?}", body);
            // If body.messageIds is empty (all target addresses were stale/rotated), refresh pre_key_bundles
            if body.messageIds.is_empty() {
                log::warn!("received empty messageIds from server, purging cached addresses for {} and fetching fresh pre_key_bundles", to);
                for addr in &other_addresses {
                    let _ = address_store.delete_by_id(addr.address_id).await;
                }
                self.get_and_process_all_pre_key_bundles_of_user(to, &token).await?;
                let fresh_addresses = address_store.get(to).await?;
                for addr in fresh_addresses {
                    if addr.username != self_username {
                        more_addresses_to_send_to.push(addr.address_id);
                    }
                }
            } else {
                for ids in body.messageIds {
                    if ids.id == 0 && ids.to != 0 {
                        more_addresses_to_send_to.push(ids.to);
                    } else {
                        if let Some(index) = addresses_to_not_send_to
                            .iter()
                            .position(|x| x.address_id == ids.to)
                        {
                            addresses_to_not_send_to.swap_remove(index);
                        }
                    }
                }
            }
        } else {
            return Err(anyhow::anyhow!("unexpected or empty body returned"));
        }

        for id in addresses_to_not_send_to {
            let _ = address_store.delete_by_id(id.address_id).await;
        }

        if more_addresses_to_send_to.is_empty() {
            return Ok(UserMessage {
                id: get_current_timestamp_microseconds_since_epoch(),
                other: to.to_string(),
                message: effective_payload,
                sent_by_other: false,
                message_type: effective_type,
            });
        }

        self.get_and_process_pre_key_bundles_per_ids(&more_addresses_to_send_to, &token)
            .await?;

        let mut upload_request = firefly::UploadUserMessage::default();
        for addressId in more_addresses_to_send_to {
            let address = store.address_store.get_by_id(addressId).await?;
            if let Some(address) = address {
                let is_self = address.username == self_username;
                let protocol_address =
                    ProtocolAddress::new(address.username, DeviceId::new(address.device_id)?);
                let message = if is_self {
                    self.create_encrypted_message(
                        protocol_address,
                        address.address_id,
                        self_message_settings,
                        self_message_payload.clone(),
                    )
                } else {
                    self.create_encrypted_message(
                        protocol_address,
                        address.address_id,
                        message_settings,
                        effective_payload.clone(),
                    )
                };

                match message.await {
                    Ok(m) => upload_request.messages.push(m),
                    Err(e) => {
                        if !is_self {
                            return Err(e);
                        } else {
                            log::warn!("failed to encrypt message for self device: {}", e);
                        }
                    }
                }
            } else {
                continue;
            }
        }

        let bytes = self
            .request(firefly::Request {
                id: 0,
                payload: firefly::mod_Request::OneOfpayload::uploadUserMessage(upload_request),
            })
            .await?;
        let response = deserialize_proto::<firefly::Response<'_>>(&bytes)?;

        if let Some(error) = response.error {
            return Err(anyhow::anyhow!("[{}], {}", error.errorCode, error.error));
        }

        if let firefly::mod_Response::OneOfbody::userMessageUploaded(uploaded) = response.body {
            log::info!("uploaded messages: {:?}", uploaded);
        }

        Ok(UserMessage {
            id: get_current_timestamp_microseconds_since_epoch(),
            other: to.to_string(),
            message: effective_payload,
            sent_by_other: false,
            message_type: effective_type,
        })
    }

    async fn get_and_process_all_pre_key_bundles_of_user(
        &self,
        to: &str,
        token: &str,
    ) -> anyhow::Result<()> {
        let url = format!("{}/user/preKeyBundles?other={}", self.firefly_base_url, to);

        let response = HTTP_CLIENT.get(url).bearer_auth(token).send().await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "unexpected status [{}] {}",
                response.status(),
                response.text().await?
            ));
        }

        let body = response.bytes().await?;

        let entries = deserialize_proto::<firefly::PreKeyBundleEntries>(&body)?.entries;

        for entry in entries {
            let Some(bundle) = entry.bundle else {
                log::warn!(
                    "failed to process key_bundle {} {} {} {}: no bundle",
                    entry.id,
                    entry.address,
                    entry.username,
                    entry.device_id
                );

                continue;
            };

            if let Err(err) = self
                .key_stores
                .process_pre_key_bundle(entry.username.to_string(), bundle.into())
                .await
            {
                log::warn!(
                    "failed to process key_bundle {} {} {} {}: {err}",
                    entry.id,
                    entry.address,
                    entry.username,
                    entry.device_id
                );
            }

            self.key_stores
                .store()
                .address_store
                .add(entry.address, &entry.username, entry.device_id as u8)
                .await?;
        }

        Ok(())
    }

    async fn sync_all_group_messages(&self) -> anyhow::Result<()> {
        const LIMIT: usize = 100;
        let addressId = self.addressId.load(std::sync::atomic::Ordering::Relaxed);
        if addressId == 0 {
            return Err(anyhow::anyhow!("addressId is not set"));
        }

        let firefly_mls_client = self
            .firefly_mls_client
            .get()
            .context("firefly_mls_client is not initialized")?;

        loop {
            let token = self
                .callbacks
                .get_access_token()
                .await
                .context("token not found")?;
            let last_messages = self.group_messages_store.get_all_last_messages().await?;

            if last_messages.is_empty() {
                break;
            }

            let mut groupRequests = firefly::GroupSyncRequests::default();

            for last_message in &last_messages {
                let mut request = firefly::GroupSyncRequest::default();
                request.group_id = last_message.group_id;
                request.start_after = last_message.id;
                groupRequests.requests.push(request);
            }

            let body = serialize_proto(&groupRequests)?;

            let url = format!(
                "{}/group/sync?address={}&limit={}",
                self.firefly_base_url, addressId, LIMIT
            );
            let response = HTTP_CLIENT
                .post(url)
                .bearer_auth(&token)
                .body(body)
                .send()
                .await?;

            if !response.status().is_success() {
                return Err(anyhow::anyhow!(
                    "unexpected status [{}]: {}",
                    response.status(),
                    response.text().await?
                ));
            }

            let body = response.bytes().await?;
            let messages = deserialize_proto::<firefly::GroupMessages>(&body)?;
            let messages_len = messages.messages.len();
            for message in messages.messages {
                if let Err(err) = on_group_message(
                    &message,
                    firefly_mls_client,
                    &self.group_info_store,
                    &self.group_messages_store,
                    &self.history_keys_store,
                    &self.key_value_store,
                    &self.callbacks,
                    false,
                )
                .await
                {
                    log::error!("failed to process group message {:?}", err);
                }
            }
            if messages_len < LIMIT {
                break;
            }
        }

        if let Some(token) = self.callbacks.get_access_token().await {
            if let Err(err) = self.update_group_commits(&token, addressId).await {
                log::error!(
                    "[sync_group_messages] Failed to update group commits to server: {:?}",
                    err
                );
            } else {
                log::info!("[sync_group_messages] Successfully synced group cursors to server");
            }
        } else {
            log::warn!(
                "[sync_group_messages] Skipping update_group_commits, access token unavailable"
            );
        }

        Ok(())
    }

    pub async fn get_and_process_pre_key_bundles_per_ids(
        &self,
        ids: &[u64],
        token: &str,
    ) -> anyhow::Result<()> {
        let mut url = String::with_capacity(256);

        url.push_str(&self.firefly_base_url);
        url.push_str("/user/preKeyBundles?ids=");

        write_url_comma_seperated(&mut url, ids.iter())?;

        let response = HTTP_CLIENT.get(url).bearer_auth(token).send().await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "unexpected status [{}] {}",
                response.status(),
                response.text().await?
            ));
        }

        let body = response.bytes().await?;

        let entries = deserialize_proto::<firefly::PreKeyBundleEntries>(&body)?.entries;

        for entry in entries {
            let Some(bundle) = entry.bundle else {
                log::warn!(
                    "failed to process key_bundle {} {} {} {}: no bundle",
                    entry.id,
                    entry.address,
                    entry.username,
                    entry.device_id
                );

                continue;
            };

            if let Err(err) = self
                .key_stores
                .process_pre_key_bundle(entry.username.to_string(), bundle.into())
                .await
            {
                log::warn!(
                    "failed to process key_bundle {} {} {} {}: {err}",
                    entry.id,
                    entry.address,
                    entry.username,
                    entry.device_id
                );
            }
            self.key_stores
                .store()
                .address_store
                .add(entry.address, &entry.username, entry.device_id as u8)
                .await?;
        }

        Ok(())
    }

    async fn check_key_packages(
        &self,
        token: &str,
        addressId: u64,
        device_id: u8,
    ) -> anyhow::Result<()> {
        let firefly_mls_client = self
            .firefly_mls_client
            .get()
            .context("firefly_mls_client is not initialized")?;
        let url = format!(
            "{}/group/keyPackages?address_id={}&device_id={}",
            self.firefly_base_url, addressId, device_id
        );
        let response = HTTP_CLIENT.get(url).bearer_auth(token).send().await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "unexpected status [{}] {}",
                response.status(),
                response.text().await?
            ));
        }

        let bytes = response.bytes().await?;
        let key_packages = deserialize_proto::<firefly::GroupKeyPackages<'_>>(&bytes)?;

        const MAX_KEY_PACKAGES_LIMIT: usize = 32;

        let received_key_packages_len = key_packages.packages.len();
        let mut ids_to_delete = Vec::with_capacity(received_key_packages_len);
        let current_signing_identity = firefly_mls_client.signing_identity();
        for package in key_packages.packages {
            let id = package.id;

            if let Ok(package_data) = self.self_group_key_packages_store.get(id).await {
                if package_data != package.package.as_ref() {
                    ids_to_delete.push(id);
                    continue;
                } else {
                    let message = MlsMessage::from_bytes(&package.package).ok();
                    let valid_identity = message
                        .as_ref()
                        .and_then(|x| {
                            x.as_key_package().map(|x| x.signing_identity() == &current_signing_identity)
                        })
                        .unwrap_or_default();
                    if !valid_identity {
                        ids_to_delete.push(id);
                        continue;
                    }

                    let Ok(kp_ref) = firefly_core::FireflyMlsClient::key_package_reference(&package.package).await else {
                        log::warn!(
                            "Key package id={} could not be parsed for reference, marking for deletion",
                            id
                        );
                        ids_to_delete.push(id);
                        continue;
                    };
                    if !self.group_key_packages_store.contains(&kp_ref).await {
                        log::warn!(
                            "Key package id={} is missing private key in local storage, marking for deletion",
                            id
                        );
                        ids_to_delete.push(id);
                        continue;
                    }
                }
            } else {
                ids_to_delete.push(id);
                continue;
            }
        }

        if !ids_to_delete.is_empty() {
            let mut url = format!(
                "{}/group/keyPackages?address={}&device_id={}&ids=",
                self.firefly_base_url, addressId, device_id
            );
            write_url_comma_seperated(&mut url, ids_to_delete.iter())?;

            let response = HTTP_CLIENT.delete(url).bearer_auth(token).send().await?;
            if !response.status().is_success() {
                return Err(anyhow::anyhow!(
                    "unexpected status [{}] {}",
                    response.status(),
                    response.text().await?
                ));
            }

            self.self_group_key_packages_store.delete_many(&ids_to_delete).await?;
            log::info!("deleted key packages: {:?}", ids_to_delete);
        }

        log::info!(
            "MLS: received: {}, ids_to_delete: {}, max: {}",
            received_key_packages_len,
            ids_to_delete.len(),
            MAX_KEY_PACKAGES_LIMIT
        );
        let keys_remained = received_key_packages_len - ids_to_delete.len();
        let keys_to_generate = MAX_KEY_PACKAGES_LIMIT.saturating_sub(keys_remained);

        if keys_to_generate > 0 {
            let mut key_packages = firefly::GroupKeyPackages::default();
            for _ in 0..keys_to_generate {
                let id = (rng().next_u32() % 32000) as i32;
                let key_package = firefly_mls_client
                    .generate_key_package()
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?;
                self.self_group_key_packages_store
                    .set(id, &key_package)
                    .await?;
                key_packages.packages.push(firefly::GroupKeyPackage {
                    id,
                    package: key_package.into(),
                    address: addressId,
                    username: Default::default(),
                });
            }

            let body = serialize_proto(&key_packages)?;

            let url = format!(
                "{}/group/keyPackages?address={}&device_id={}",
                self.firefly_base_url, addressId, device_id
            );
            let response = HTTP_CLIENT
                .post(url)
                .bearer_auth(token)
                .body(body)
                .send()
                .await?;

            if !response.status().is_success() {
                return Err(anyhow::anyhow!(
                    "unexpected status [{}] {}",
                    response.status(),
                    response.text().await?
                ));
            }

            log::info!(
                "group key packages uploaded: {}",
                key_packages.packages.len()
            );
        }
        Ok(())
    }

    pub async fn replenish_key_packages(&self, count: usize) -> anyhow::Result<()> {
        if count == 0 {
            return Ok(());
        }
        let Some(token) = self.callbacks.get_access_token().await else {
            return Ok(());
        };
        let address_id = self.addressId.load(std::sync::atomic::Ordering::Relaxed);
        let device_id = self.get_device_id().await as u8;
        let firefly_mls_client = self
            .firefly_mls_client
            .get()
            .context("firefly_mls_client is not initialized")?;

        replenish_key_packages_standalone(
            &self.firefly_base_url,
            &token,
            address_id,
            device_id,
            count,
            firefly_mls_client,
            &self.self_group_key_packages_store,
        ).await
    }

    async fn ensure_mls_client_initialized(&self) -> anyhow::Result<Arc<FfiMlsClient>> {
        let address_id = self.addressId.load(std::sync::atomic::Ordering::Relaxed);
        let address_id = if address_id == 0 {
            let identity = self
                .key_stores
                .store()
                .identity_store
                .get_full_identity_key_pair()
                .await?;

            if identity.id != 0 {
                self.addressId
                    .store(identity.id as u64, std::sync::atomic::Ordering::Relaxed);
                identity.id as u64
            } else {
                return Err(anyhow::anyhow!("addressId is not set"));
            }
        } else {
            address_id
        };

        let device_id = self
            .key_stores
            .store()
            .identity_store
            .get_full_identity_key_pair()
            .await?
            .device_id;

        let callbacks = self.callbacks.clone();
        let key_value_store = self.key_value_store.clone();
        let firefly_base_url = self.firefly_base_url.clone();
        let pool = self.pool.clone();

        self.firefly_mls_client
            .get_or_try_init(|| {
                Box::pin(async move {
                    Ok::<_, anyhow::Error>(Arc::new(
                        FfiMlsClient::initialize(
                            device_id,
                            address_id,
                            callbacks,
                            key_value_store,
                            firefly_base_url,
                            pool,
                        )
                        .await?,
                    ))
                })
            })
            .await
            .cloned()
    }

    async fn check_mls_setup(&self) -> anyhow::Result<()> {
        let token = self
            .callbacks
            .get_access_token()
            .await
            .context("token not found")?;

        let address_id = self.addressId.load(std::sync::atomic::Ordering::Relaxed);
        if address_id == 0 {
            return Err(anyhow::anyhow!("address_id is not set"));
        }

        let device_id = self
            .key_stores
            .store()
            .identity_store
            .get_full_identity_key_pair()
            .await?
            .device_id;

        self.ensure_mls_client_initialized().await?;

        self.check_key_packages(&token, address_id, device_id)
            .await?;

        let _ = self.join_groups(&token, address_id, device_id).await;
        let _ = self
            .request_group_re_adds(&token, address_id, device_id)
            .await;
        let _ = self
            .add_requested_re_add_group_members(&token, address_id, device_id)
            .await;

        let _ = self.update_group_commits(&token, address_id).await;

        self.fully_initialized
            .store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
    async fn request_group_re_adds(
        &self,
        token: &str,
        addressId: u64,
        device_id: u8,
    ) -> anyhow::Result<()> {
        let url = format!("{}/groups", self.firefly_base_url);

        let response = HTTP_CLIENT.get(url).bearer_auth(token).send().await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "unexpected status [{}] {}",
                response.status(),
                response.text().await?
            ));
        }

        let bytes = response.bytes().await?;
        let groups = deserialize_proto::<firefly::Groups<'_>>(&bytes)?;

        let mut groupIds_to_be_requested_to_add = Vec::new();

        for group in groups.groups {
            if self.group_info_store.get(group.id).await.is_err() {
                groupIds_to_be_requested_to_add.push(group.id);
            }
        }

        if !groupIds_to_be_requested_to_add.is_empty() {
            let mut url = format!(
                "{}/group/reAdd?address={}&device_id={}&groupIds=",
                self.firefly_base_url, addressId, device_id
            );
            write_url_comma_seperated(&mut url, groupIds_to_be_requested_to_add.iter())?;

            let response = HTTP_CLIENT.post(url).bearer_auth(token).send().await?;
            if !response.status().is_success() {
                return Err(anyhow::anyhow!(
                    "unexpected status [{}] {}",
                    response.status(),
                    response.text().await?
                ));
            }
        }
        Ok(())
    }

    async fn add_requested_re_add_group_members(
        &self,
        token: &str,
        addressId: u64,
        _device_id: u8,
    ) -> anyhow::Result<()> {
        let groups = self.group_info_store.get_all().await?;

        if groups.is_empty() {
            return Ok(());
        }

        let mut url = format!(
            "{}/group/reAdds?address={}&groupIds=",
            self.firefly_base_url, addressId
        );

        write_url_comma_seperated(&mut url, groups.iter().map(|x| x.id))?;

        log::info!(
            "[add_requested_re_add_group_members] Querying reAdds: url={}, group_count={}",
            url,
            groups.len()
        );

        let response = HTTP_CLIENT.get(&url).bearer_auth(token).send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            log::error!("[add_requested_re_add_group_members] Failed status [{}] {}", status, body);
            return Err(anyhow::anyhow!(
                "unexpected status [{}] {}",
                status,
                body
            ));
        }

        let bytes = response.bytes().await?;
        let requests = deserialize_proto::<firefly::GroupReAddRequests<'_>>(&bytes)?;

        log::info!(
            "[add_requested_re_add_group_members] Received {} pending re-add/join requests",
            requests.requests.len()
        );

        let firefly_mls_client = self
            .firefly_mls_client
            .get()
            .context("firefly_mls_client is not initialized")?;

        for request in requests.requests {
            log::info!(
                "[add_requested_re_add_group_members] Processing request: group={}, user={}, address={}",
                request.group_id,
                request.username,
                request.address_id
            );
            match self
                .re_add_member(token, firefly_mls_client, &request, addressId)
                .await
            {
                Ok(_) => {
                    log::info!("[add_requested_re_add_group_members] Successfully processed readd/join member: {:?}", request);
                }
                Err(err) => {
                    log::error!("[add_requested_re_add_group_members] Failed to process readd/join member {:?}: {:?}", request, err);
                }
            }
        }

        Ok(())
    }

    async fn re_add_member(
        &self,
        token: &str,
        firefly_mls_client: &FfiMlsClient,
        request: &firefly::GroupReAddRequest<'_>,
        addressId: u64,
    ) -> anyhow::Result<()> {
        let groupId = request.group_id;

        // If request is for our current device address, delete redundant reAdd request from server
        if request.address_id == addressId {
            log::info!(
                "[re_add_member] Request is for our own current address {}, deleting redundant request",
                addressId
            );
            let _ = HTTP_CLIENT
                .delete(format!(
                    "{}/group/reAdd?groupId={}&address={}&myAddress={}",
                    self.firefly_base_url, groupId, request.address_id, addressId,
                ))
                .bearer_auth(token)
                .send()
                .await;
            return Ok(());
        }

        log::info!("[re_add_member] Loading group {} for re-adding user {}", groupId, request.username);
        let group_info = self.group_info_store.get(groupId).await?;

        let group = firefly_mls_client
            .load_group(groupId, group_info.identifier.clone())
            .await?;

        let is_owner = group.is_owner().await.unwrap_or(false);
        if !is_owner {
            // Non-owners wait 1.5s to let the group owner prioritize handling the re-add
            tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        }

        let claim_res = HTTP_CLIENT
            .post(format!(
                "{}/group/reAdd/claim?groupId={}&address={}&myAddress={}",
                self.firefly_base_url, groupId, request.address_id, addressId,
            ))
            .bearer_auth(token)
            .send()
            .await?;

        if claim_res.status() == reqwest::StatusCode::CONFLICT {
            log::info!(
                "[re_add_member] Contention lost: request for {} (address {}) in group {} already claimed by another member",
                request.username, request.address_id, groupId
            );
            return Ok(());
        }

        log::info!("[re_add_member] Calling group.re_add_member for user {} (address {}) in group {}", request.username, request.address_id, groupId);
        self.key_value_store.set(&format!("snapshot-join-pending:{groupId}"),&format!("{:016x}",rand::random::<u64>())).await?;
        let res = group
            .re_add_member(request.username.to_string(), request.address_id)
            .await;

        let id = match res {
            Ok(id) => id,
            Err(err) => {
                let err_str = err.to_string();
                if err_str.contains("don't have permission")
                    || err_str.contains("Committer can not remove themselves")
                {
                    log::warn!(
                        "[re_add_member] Skipping and deleting invalid/unauthorized reAdd request: {:?}",
                        err_str
                    );
                    let _ = HTTP_CLIENT
                        .delete(format!(
                            "{}/group/reAdd?groupId={}&address={}&myAddress={}",
                            self.firefly_base_url, groupId, request.address_id, addressId,
                        ))
                        .bearer_auth(token)
                        .send()
                        .await;
                    return Ok(());
                }
                return Err(err);
            }
        };

        self.group_messages_store
            .update_cursor(id, groupId, group.epoch().await as u32)
            .await?;

        if let Err(err) = self.re_encrypt_and_send_pinned_messages(groupId).await {
            log::warn!("Failed to re-encrypt pinned messages after re_add_member: {:?}", err);
        }

        log::info!("[re_add_member] Successfully committed re-add. Notifying server to delete reAdd request...");
        let response = HTTP_CLIENT
            .delete(format!(
                "{}/group/reAdd?groupId={}&address={}&myAddress={}",
                self.firefly_base_url, groupId, request.address_id, addressId,
            ))
            .bearer_auth(token)
            .send()
            .await?;

        log::info!(
            "[re_add_member] delete reAdd result: [{}] {}",
            response.status(),
            response.text().await.unwrap_or_default()
        );

        Ok(())
    }

    async fn join_groups(&self, token: &str, addressId: u64, device_id: u8) -> anyhow::Result<()> {
        let url = format!(
            "{}/group/invites?address={}&device_id={}",
            self.firefly_base_url, addressId, device_id
        );

        let response = HTTP_CLIENT.get(&url).bearer_auth(token).send().await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "unexpected status [{}] {}",
                response.status(),
                response.text().await?
            ));
        }

        let bytes = response.bytes().await?;
        let invites = deserialize_proto::<firefly::GroupInvites<'_>>(&bytes)?;

        for invite in invites.invites.iter() {
            match self.join_group(invite, token, addressId, device_id).await {
                Ok(group) => {
                    log::info!("joined group via invite: {:?}", invite);

                    self.group_messages_store
                        .update_cursor(invite.commitId, invite.groupId, group.epoch().await as u32)
                        .await?;
                }
                Err(err) => {
                    log::error!("failed to join group via invite: {:?}: {:?}", invite, err);
                }
            };
        }
        let joined_count = invites.invites.len();
        if joined_count > 0 {
            let _ = self.replenish_key_packages(joined_count).await;
        }

        {
            if !invites.invites.is_empty() {
                let mut url = format!(
                    "{}/group/invites?address={}&groupIds=",
                    self.firefly_base_url, addressId
                );
                write_url_comma_seperated(&mut url, invites.invites.iter().map(|x| x.groupId))?;
                let response = HTTP_CLIENT.delete(&url).bearer_auth(token).send().await?;
                if !response.status().is_success() {
                    return Err(anyhow::anyhow!(
                        "unexpected status [{}] {}",
                        response.status(),
                        response.text().await?
                    ));
                }

                log::info!("deleted invites: {}", url);
            }
        }

        Ok(())
    }

    pub async fn create_group(
        &self,
        name: String,
        description: String,
        settings: u32,
    ) -> anyhow::Result<GroupInfo> {
        let client = self
            .firefly_mls_client
            .get()
            .context("firefly_mls_client is not initialized")?;

        let group = client
            .create_group_with_settings(name.clone(),description.clone(),settings)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        self.group_info_store
            .set(
                group.group_id(),
                name.clone(),
                description,
                group
                    .group_identifier()
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?,
            )
            .await?;

        return self.group_info_store.get(group.group_id()).await;
    }

    pub fn is_initialized(&self) -> bool {
        self.fully_initialized
            .load(std::sync::atomic::Ordering::Relaxed)
            && self
                .connection
                .try_read()
                .map(|g| g.is_some())
                .unwrap_or(false)
    }

    pub async fn upload_group_message(
        &self,
        groupId: u64,
        message: firefly::GroupMessageInner<'_>,
        _epoch: u32,
    ) -> anyhow::Result<u64> {
        let firefly_mls_client = self
            .firefly_mls_client
            .get()
            .context("firefly_mls_client is not initialized")?;

        let group_info = self.group_info_store.get(groupId).await?;

        let group = firefly_mls_client
            .load_group(groupId, group_info.identifier)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        let payload = serialize_proto(&message)?;
        let encrypted = group
            .encrypt(payload.to_vec())
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        if let Err(err) = group.save().await {
            log::warn!("Failed to save group state after encrypt: {:?}", err);
        }

        let current_epoch = group.epoch().await as u32;
        let group_message = firefly::GroupMessage {
            id: 0,
            groupId,
            message: encrypted.into(),
            epoch: current_epoch,
        };

        let bytes = self
            .request(firefly::Request {
                id: 0,
                payload: firefly::mod_Request::OneOfpayload::uploadGroupMessage(group_message),
            })
            .await?;
        let response = deserialize_proto::<firefly::Response<'_>>(&bytes)?;

        if let Some(err) = response.error {
            return Err(anyhow::anyhow!(
                "unexpected response: [{}] {:?}",
                err.errorCode,
                err.error
            ));
        }

        let firefly::mod_Response::OneOfbody::groupMessageUploaded(uploaded_group_message) =
            response.body
        else {
            return Err(anyhow::anyhow!("unexpected response: {:?}", response));
        };

        let claims = get_claims_from_token(
            &self
                .callbacks
                .get_access_token()
                .await
                .context("token not found")?,
        )?;

        let effective_type = message.message_type
            | match &message.message {
                firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(p) => p.message_type,
                _ => 0,
            };

        self.group_messages_store
            .add(
                uploaded_group_message.id,
                groupId,
                message.channelId,
                uploaded_group_message.epoch,
                &claims.uname,
                &payload,
                effective_type,
            )
            .await?;

        if effective_type & firefly_protos::MESSAGE_TYPE_HIDDEN == 0 {
            self.callbacks.on_group_history_signal(crate::callbacks::GroupHistorySignal{group_id:groupId,signal_type:6,request_id:0,chunk_id:0,username:self.callbacks.name().to_string()}).await;
        }
        Ok(uploaded_group_message.id)
    }

    pub async fn set_group_message_pin(&self, group_id:u64, message_id:u64, pinned:bool)->anyhow::Result<()> {
        let result=self.set_snapshot_pin(group_id,message_id,pinned).await;
        if result.is_err() {self.callbacks.on_group_history_signal(crate::callbacks::GroupHistorySignal{group_id,signal_type:8,request_id:0,chunk_id:0,username:self.callbacks.name().to_string()}).await;}
        result
    }

    // Retained FFI name for existing callers; no original messages are resent.
    pub async fn re_encrypt_and_send_pinned_messages(&self, group_id:u64)->anyhow::Result<()> {
        self.share_join_snapshots(group_id).await
    }

    pub async fn encrypt_and_send_group_with_type(
        &self,
        group_id: u64,
        payload: Vec<u8>,
        message_type: u32,
    ) -> anyhow::Result<u64> {
        let mut message = deserialize_proto::<firefly::GroupMessageInner<'_>>(&payload)?;
        let pin=(message.message_type | message_type) & firefly_protos::MESSAGE_TYPE_PINNED != 0;
        message.message_type=(message.message_type | message_type) & !firefly_protos::MESSAGE_TYPE_PINNED;
        if let firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(ref mut p) = message.message {
            p.message_type=(p.message_type | message_type) & !firefly_protos::MESSAGE_TYPE_PINNED;
        }
        let id=self.upload_group_message(group_id, message, 0).await?;
        if pin {self.set_group_message_pin(group_id,id,true).await?;}
        Ok(id)
    }

    pub async fn encrypt_and_send_group_pinned(
        &self,
        group_id: u64,
        payload: Vec<u8>,
    ) -> anyhow::Result<u64> {
        self.encrypt_and_send_group_with_type(group_id, payload, firefly_protos::MESSAGE_TYPE_PINNED).await
    }

    pub fn group_message_store(&self) -> GroupMessagesStore {
        self.group_messages_store.with_read_access(self.firefly_mls_client.clone(), self.group_info_store.clone())
    }

    pub fn messages_store(&self) -> MessagesStore {
        self.messages_store.clone()
    }

    pub async fn search_messages(
        &self,
        query: &str,
        scope: SearchScope,
        limit: u32,
        offset: u32,
    ) -> anyhow::Result<Vec<SearchResultItem>> {
        let engine = SearchEngine::with_group_messages_store(
            self.pool.clone(),
            self.group_message_store(),
        );
        engine.search(query, &scope, limit, offset).await
    }

    pub async fn search_user_messages(
        &self,
        query: &str,
        other: Option<&str>,
        limit: u32,
        offset: u32,
    ) -> anyhow::Result<Vec<SearchResultItem>> {
        self.search_messages(
            query,
            SearchScope::User {
                other: other.map(|s| s.to_string()),
            },
            limit,
            offset,
        )
        .await
    }

    pub async fn search_group_messages(
        &self,
        query: &str,
        group_id: Option<u64>,
        channel_id: Option<u32>,
        limit: u32,
        offset: u32,
    ) -> anyhow::Result<Vec<SearchResultItem>> {
        self.search_messages(
            query,
            SearchScope::Group {
                group_id,
                channel_id,
            },
            limit,
            offset,
        )
        .await
    }

    pub fn favourite_messages_store(&self) -> FavouriteMessagesStore {
        self.favourite_messages_store
            .with_read_access(self.firefly_mls_client.clone(), self.group_info_store.clone())
    }

    pub fn favorite_messages_store(&self) -> FavouriteMessagesStore {
        self.favourite_messages_store()
    }

    pub async fn add_favourite(&self, favourite: FavouriteMessage) -> anyhow::Result<u64> {
        self.favourite_messages_store().add(favourite).await
    }

    pub async fn favourite_user_message(
        &self,
        message: &UserMessage,
        custom_text: Option<String>,
    ) -> anyhow::Result<u64> {
        self.favourite_messages_store()
            .add_user_message(message, custom_text)
            .await
    }

    pub async fn favourite_group_message(
        &self,
        message: &crate::db::group_messages::GroupMessage,
        custom_text: Option<String>,
    ) -> anyhow::Result<u64> {
        self.favourite_messages_store()
            .add_group_message(message, custom_text)
            .await
    }

    pub async fn remove_user_favourite(
        &self,
        other: &str,
        message_id: u64,
    ) -> anyhow::Result<bool> {
        self.favourite_messages_store()
            .remove_user_favourite(other, message_id)
            .await
    }

    pub async fn remove_group_favourite(
        &self,
        group_id: u64,
        message_id: u64,
    ) -> anyhow::Result<bool> {
        self.favourite_messages_store()
            .remove_group_favourite(group_id, message_id)
            .await
    }

    pub async fn remove_favourite_by_id(&self, favourite_id: u64) -> anyhow::Result<bool> {
        self.favourite_messages_store()
            .remove_by_id(favourite_id)
            .await
    }

    pub async fn is_user_favourite(
        &self,
        other: &str,
        message_id: u64,
    ) -> anyhow::Result<bool> {
        self.favourite_messages_store()
            .is_user_favourite(other, message_id)
            .await
    }

    pub async fn is_group_favourite(
        &self,
        group_id: u64,
        message_id: u64,
    ) -> anyhow::Result<bool> {
        self.favourite_messages_store()
            .is_group_favourite(group_id, message_id)
            .await
    }

    pub async fn get_favourites(
        &self,
        limit: u32,
        offset: u32,
    ) -> anyhow::Result<Vec<FavouriteMessage>> {
        self.favourite_messages_store().get_all(limit, offset).await
    }

    pub async fn get_user_favourites(
        &self,
        other: Option<&str>,
        limit: u32,
        offset: u32,
    ) -> anyhow::Result<Vec<FavouriteMessage>> {
        self.favourite_messages_store()
            .get_user_favourites(other, limit, offset)
            .await
    }

    pub async fn get_group_favourites(
        &self,
        group_id: Option<u64>,
        channel_id: Option<u32>,
        limit: u32,
        offset: u32,
    ) -> anyhow::Result<Vec<FavouriteMessage>> {
        self.favourite_messages_store()
            .get_group_favourites(group_id, channel_id, limit, offset)
            .await
    }

    async fn join_group(
        &self,
        invite: &firefly::GroupInvite<'_>,
        token: &str,
        addressId: u64,
        _device_id: u8,
    ) -> anyhow::Result<Arc<FfiMlsGroup>> {
        let groupId = invite.groupId;

        let firefly_mls_client = self
            .firefly_mls_client
            .get()
            .context("firefly_mls_client is not initialized")?;
        let group = firefly_mls_client
            .join_group(groupId, invite.welcomeMessage.to_vec())
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        log::info!("joined group: {}", invite.groupId);
        group.save().await.map_err(|e| anyhow::anyhow!(e))?;

        let url = format!("{}/group?id={}", self.firefly_base_url, groupId);
        let response = HTTP_CLIENT.get(url).bearer_auth(token).send().await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "unexpected status [{}] {}",
                response.status(),
                response.text().await?
            ));
        }

        let bytes = response.bytes().await?;
        let group_info = deserialize_proto::<firefly::Group<'_>>(&bytes)?;

        self.group_info_store
            .set(
                groupId,
                group_info.name.to_string(),
                group_info.description.to_string(),
                group
                    .group_identifier()
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?,
            )
            .await?;

        let url = format!(
            "{}/group/member?groupId={}&address={}",
            self.firefly_base_url, groupId, addressId
        );

        let mut last_message_seen = self
            .group_messages_store
            .get_last_message_of_group(groupId)
            .await
            .map(|last_message| last_message.id)
            .unwrap_or(0);
        if last_message_seen == 0 {
            last_message_seen = invite.commitId;
        }
        let update = firefly::GroupMemberUpdate {
            group_id: groupId,
            last_epoch: group.epoch().await as u32,
            last_message_seen,
        };
        let response = HTTP_CLIENT
            .post(url)
            .bearer_auth(token)
            .body(serialize_proto(&update)?)
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "unexpected status [{}] {}",
                response.status(),
                response.text().await?
            ));
        }

        self.callbacks.on_group_joined(groupId).await;

        Ok(group)
    }

    pub async fn check_setup(&self) -> anyhow::Result<()> {
        if self
            .fully_initialized
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            log::info!("check_setup: already fully initialized, skipping setup requests");
            return Ok(());
        }

        let token = self
            .callbacks
            .get_access_token()
            .await
            .context("token not found")?;

        {
            let fcm_token = self
                .key_value_store
                .get(KEY_FCM_TOKEN)
                .await
                .unwrap_or_default();
            log::info!("fcm token: {}", KEY_FCM_TOKEN);

            let identity = self
                .key_stores
                .store()
                .identity_store
                .get_full_identity_key_pair()
                .await?;
            let address = firefly::Address {
                id: identity.id as u64,
                username: get_claims_from_token(&token)?.uname.into(),
                deviceId: if identity.id == 0 {
                    0
                } else {
                    identity.device_id as u32
                },
                fcmToken: fcm_token.into(),
            };

            log::info!("address to upload {:?}", address);

            self.addressId
                .store(identity.id as u64, std::sync::atomic::Ordering::Relaxed);

            let response = HTTP_CLIENT
                .post(format!("{}/user/device", self.firefly_base_url))
                .body(serialize_proto(&address)?)
                .bearer_auth(&token)
                .send()
                .await?;

            if !response.status().is_success() {
                return Err(anyhow::anyhow!(
                    "unexpected status [{}]: {}",
                    response.status(),
                    response.text().await?
                ));
            }

            let body = response.bytes().await?;
            let address = deserialize_proto::<firefly::Address>(&body)?;

            self.addressId
                .store(address.id, std::sync::atomic::Ordering::Relaxed);

            let assigned_device_id = u8::try_from(address.deviceId)
                .context("server returned a device ID outside the supported range")?;
            DeviceId::new(assigned_device_id)?;

            self.key_stores
                .store()
                .identity_store
                .update_registration_for_keypair(
                    address.id as i64,
                    &address.username,
                    assigned_device_id,
                )
                .await?;

            self.update_pre_key_bundles(&token).await?;
        }

        {
            self.check_mls_setup().await?;
        }

        Ok(())
    }

    async fn update_pre_key_bundles(&self, token: &str) -> anyhow::Result<()> {
        let _registration_id = self
            .key_stores
            .store()
            .identity_store
            .get_local_registration_id()
            .await?;
        let claims = get_claims_from_token(token)?;
        let username = claims.uname.clone();

        let addressId = self.addressId.load(std::sync::atomic::Ordering::Relaxed);
        if addressId == 0 {
            return Err(anyhow::anyhow!("addressId is 0"));
        }
        let url = format!(
            "{}/user/preKeyBundles?id={}&onlyIds=true",
            self.firefly_base_url, addressId
        );
        let response = HTTP_CLIENT.get(url).bearer_auth(token).send().await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "failed to get preKeyBundles: [{}] {}",
                response.status().as_u16(),
                response.text().await?
            ));
        }
        let bytes = response.bytes().await?;
        let bundles = deserialize_proto::<firefly::PreKeyBundleEntries<'_>>(&bytes)?;

        let mut key_ids_to_delete = Vec::<u32>::new();

        let bundles_length = bundles.entries.len();

        log::info!("received {} key bundles", bundles_length);
        for bundle in bundles.entries {
            let bundle_id = bundle.id;
            if self
                .key_stores
                .store()
                .prekey_store
                .get_pre_key(PreKeyId::from(bundle_id))
                .await
                .is_err()
            {
                key_ids_to_delete.push(bundle_id);
            }
        }

        if !key_ids_to_delete.is_empty() {
            let mut url = String::with_capacity(256);

            use std::fmt::Write;
            write!(
                &mut url,
                "{}/user/preKeyBundles?addressId={}&ids=",
                self.firefly_base_url, addressId
            )
            .unwrap();

            write_url_comma_seperated(&mut url, key_ids_to_delete.iter())?;

            log::info!("Deleting preKeyBundles via {}", url);
            let response = HTTP_CLIENT.delete(&url).bearer_auth(token).send().await?;

            if response.status().is_success() {
                log::info!("deleted preKeyBundles via {}", url);
            } else {
                return Err(anyhow::anyhow!(
                    "failed to delete preKeyBundles: [{}] {}",
                    response.status().as_u16(),
                    response.text().await?
                ));
            }
        }

        const MAX_KEYS_LIMIT: usize = 32;

        let keys_remaining = bundles_length - key_ids_to_delete.len();

        if keys_remaining < MAX_KEYS_LIMIT {
            let keys_to_create = MAX_KEYS_LIMIT - keys_remaining;

            log::info!("creating {} number of key bundles", keys_to_create);

            let mut bundles = firefly::PreKeyBundleEntries::default();
            for _ in 0..keys_to_create {
                let pre_key_bundle = self
                    .key_stores
                    .generate_prekey_bundle()
                    .await
                    .map_err(|err| anyhow::anyhow!(err))?;
                let device_id = pre_key_bundle.device_id;
                let pre_key: firefly::PreKeyBundle = pre_key_bundle.into();

                bundles.entries.push(firefly::PreKeyBundleEntry {
                    id: pre_key.preKeyId,
                    address: addressId,
                    bundle: Some(pre_key),
                    username: username.to_string().into(),
                    device_id: device_id as u32,
                });
            }
            let url = format!("{}/user/preKeyBundles", self.firefly_base_url);
            let response = HTTP_CLIENT
                .post(url)
                .bearer_auth(token)
                .body(serialize_proto(&bundles)?)
                .send()
                .await?;

            if response.status().is_success() {
                log::info!("created and uploaded preKeyBundles {} keys", keys_to_create);
            } else {
                return Err(anyhow::anyhow!(
                    "failed to create preKeyBundle: [{}] {}",
                    response.status().as_u16(),
                    response.text().await?
                ));
            }
        }

        Ok(())
    }

    pub async fn update_group_commits(&self, token: &str, addressId: u64) -> anyhow::Result<()> {
        let firefly_mls_client = self.firefly_mls_client.get().context("mls client uninit")?;
        let mut group_commit_syncs = firefly::GroupMemberUpdates::default();

        for info in self.group_info_store.get_all().await? {
            let group = firefly_mls_client
                .load_group(info.id, info.identifier)
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let epoch = group.epoch().await;

            let last_message_seen = match self
                .group_messages_store
                .get_last_message_of_group(info.id)
                .await
            {
                Ok(message) => message.id,
                Err(_) => 0,
            };

            group_commit_syncs.updates.push(firefly::GroupMemberUpdate {
                group_id: info.id,
                last_message_seen,
                last_epoch: epoch as u32,
            });
        }

        if group_commit_syncs.updates.is_empty() {
            return Ok(());
        }

        let response = HTTP_CLIENT
            .post(format!(
                "{}/group/syncUpdate?address={}",
                self.firefly_base_url, addressId
            ))
            .bearer_auth(token)
            .body(serialize_proto(&group_commit_syncs)?)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "failed to sync group updates: [{}] {}",
                response.status().as_u16(),
                response.text().await?
            ));
        }
        log::info!("update group members sync");

        Ok(())
    }

    pub async fn upload_fcm_token(&self, token: Option<String>) -> anyhow::Result<()> {
        let _token = match token {
            Some(val) => {
                let current_token = self
                    .key_value_store
                    .get(KEY_FCM_TOKEN)
                    .await
                    .unwrap_or_default();
                if current_token != val {
                    log::info!(
                        "FCM token changed, resetting fully_initialized to false to force setup update"
                    );
                    self.fully_initialized
                        .store(false, std::sync::atomic::Ordering::Relaxed);
                }
                self.key_value_store.set(KEY_FCM_TOKEN, &val).await?;

                val
            }
            None => {
                
                self.key_value_store.get(KEY_FCM_TOKEN).await?
            }
        };

        Ok(())
    }

    pub async fn get_conversations(&self, token: &str) -> anyhow::Result<Vec<FfiConversation>> {
        let url = format!("{}/user/conversations", self.firefly_base_url);

        let claims = get_claims_from_token(token)?;

        let response = HTTP_CLIENT.get(url).bearer_auth(token).send().await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "unexpected status: [{}] {}",
                response.status(),
                response.text().await?
            ));
        }

        let body = response.bytes().await?;

        let conversations = deserialize_proto::<firefly::Conversations>(&body)?;

        let mut records = Vec::new();

        for conversation in conversations.conversations {
            let other = if conversation.user1 == claims.uname {
                conversation.user2
            } else {
                conversation.user1
            };

            let settings = conversation.settings;

            self.key_stores
                .store()
                .conversation_store
                .set_conversation(&other, ConversationSettings::new(settings))
                .await?;

            records.push(FfiConversation {
                other: other.to_string(),
                settings,
            });
        }

        Ok(records)
    }

    pub async fn get_mls_group(&self, group_id: u64) -> anyhow::Result<Arc<FfiMlsGroup>> {
        let client = self.ensure_mls_client_initialized().await?;
        let group_info = self.group_info_store.get(group_id).await?;
        client
            .load_group(group_id, group_info.identifier)
            .await
            .map_err(|e| anyhow::anyhow!(e))
    }

    pub async fn get_group_extension(&self, groupId: u64) -> anyhow::Result<Vec<u8>> {
        let client = self.ensure_mls_client_initialized().await?;

        let group_info = self.group_info_store.get(groupId).await?;

        let group = client
            .load_group(groupId, group_info.identifier)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        group.extension().await.map_err(|e| anyhow::anyhow!(e))
    }

    pub async fn export_group_meeting_key(&self, groupId: u64) -> anyhow::Result<Vec<u8>> {
        let client = self.ensure_mls_client_initialized().await?;

        let group_info = self.group_info_store.get(groupId).await?;

        let group = client
            .load_group(groupId, group_info.identifier)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        group.export_secret("meeting-e2ee-v1", &[], 32).await
    }

    pub async fn update_group_users(
        &self,
        groupId: u64,
        users: Vec<crate::group::UpdateUserProposalFfi>,
    ) -> anyhow::Result<u64> {
        let client = self
            .firefly_mls_client
            .get()
            .context("firefly_mls_client is unitialized")?;

        let group_info = self.group_info_store.get(groupId).await?;
        let group = client
            .load_group(groupId, group_info.identifier)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        let id = group
            .update_users(users)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        self.group_messages_store
            .update_cursor(id, groupId, group.epoch().await as u32)
            .await?;

        Ok(id)
    }

    pub async fn update_group_channel(
        &self,
        groupId: u64,
        id: u32,
        delete: bool,
        name: String,
        channel_ty: u8,
        default_permissions: u32,
    ) -> anyhow::Result<u64> {
        let client = self
            .firefly_mls_client
            .get()
            .context("firefly_mls_client is unitialized")?;

        let group_info = self.group_info_store.get(groupId).await?;
        let group = client
            .load_group(groupId, group_info.identifier)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        let old_epoch = group.epoch().await;

        log::info!(
            "updating channel groupId: {}, old_group_epoch: {}",
            group.group_id(),
            old_epoch,
        );

        let commit_id = match group
            .update_channel(id, delete, name, channel_ty, default_permissions)
            .await
        {
            Ok(commit_id) => commit_id,
            Err(err) => {
                log::error!(
                    "failed to update channel {:?}, epoch: {}",
                    err,
                    group.epoch().await
                );
                return Err(anyhow::anyhow!(err));
            }
        };

        log::info!("channel updated, commit_id: {}", commit_id);

        self.group_messages_store
            .update_cursor(commit_id, groupId, group.epoch().await as u32)
            .await?;
        Ok(commit_id)
    }

    pub async fn update_group_roles(
        &self,
        groupId: u64,
        roles: Vec<crate::group::UpdateRoleProposalFfi>,
    ) -> anyhow::Result<u64> {
        let client = self
            .firefly_mls_client
            .get()
            .context("firefly_mls_client is unitialized")?;

        let group_info = self.group_info_store.get(groupId).await?;
        let group = client
            .load_group(groupId, group_info.identifier)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        let id = group
            .update_roles(roles)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        self.group_messages_store
            .update_cursor(id, groupId, group.epoch().await as u32)
            .await?;
        Ok(id)
    }

    pub async fn update_group_roles_in_channel(
        &self,
        groupId: u64,
        channel_id: u32,
        roles: Vec<crate::group::UpdateRoleProposalFfi>,
    ) -> anyhow::Result<u64> {
        let client = self
            .firefly_mls_client
            .get()
            .context("firefly_mls_client is unitialized")?;

        let group_info = self.group_info_store.get(groupId).await?;
        let group = client
            .load_group(groupId, group_info.identifier)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        let id = group
            .update_roles_in_channel(channel_id, roles)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        self.group_messages_store
            .update_cursor(id, groupId, group.epoch().await as u32)
            .await?;
        Ok(id)
    }

    pub async fn add_group_member(
        &self,
        group_id: u64,
        username: String,
        role_id: u32,
    ) -> anyhow::Result<()> {
        let client = self
            .firefly_mls_client
            .get()
            .context("firefly_mls_client is unitialized")?;

        let group_info = self.group_info_store.get(group_id).await?;
        let group = client
            .load_group(group_id, group_info.identifier)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        self.key_value_store.set(&format!("snapshot-join-pending:{group_id}"),&format!("{:016x}",rand::random::<u64>())).await?;
        let id = group.add_member(username, role_id).await?;

        self.group_messages_store
            .update_cursor(id, group_id, group.epoch().await as u32)
            .await?;

        if let Err(err) = self.re_encrypt_and_send_pinned_messages(group_id).await {
            log::warn!("Failed to re-encrypt pinned messages after add_group_member: {:?}", err);
        }

        Ok(())
    }

    pub async fn request_re_add(&self, group_ids: Vec<u64>) -> anyhow::Result<()> {
        let token = self
            .callbacks
            .get_access_token()
            .await
            .context("token not found")?;

        let address_id = self.addressId.load(std::sync::atomic::Ordering::Relaxed);
        let url = format!(
            "{}/group/reAdd?address_id={}",
            self.firefly_base_url, address_id
        );

        let response = HTTP_CLIENT
            .post(url)
            .bearer_auth(&token)
            .body(serde_json::to_vec(&group_ids)?)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "unexpected status code: [{}]: {}",
                response.status(),
                response.text().await?
            ));
        }

        Ok(())
    }

    pub async fn kick_group_member(&self, groupId: u64, username: String) -> anyhow::Result<()> {
        let client = self
            .firefly_mls_client
            .get()
            .context("firefly_mls_client is unitialized")?;

        let group_info = self.group_info_store.get(groupId).await?;
        let group = client
            .load_group(groupId, group_info.identifier)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        let id = group.kick_member(username).await?;

        self.group_messages_store
            .update_cursor(id, groupId, group.epoch().await as u32)
            .await?;

        Ok(())
    }

    pub async fn delete_group(&self, groupId: u64) -> anyhow::Result<()> {
        let url = format!("{}/group?id={}", self.firefly_base_url, groupId);
        let token = self
            .callbacks
            .get_access_token()
            .await
            .context("token not found")?;
        let resp = HTTP_CLIENT.delete(url).bearer_auth(token).send().await?;

        log::info!(
            "delete group, response: [{}]: {}",
            resp.status(),
            resp.text().await?
        );

        self.group_info_store.delete(groupId).await?;
        self.group_messages_store
            .delete_by_group_id(groupId)
            .await?;

        Ok(())
    }

    pub async fn create_join_link(
        &self,
        group_id: u64,
        expires_in_seconds: u64,
        max_uses: u32,
    ) -> anyhow::Result<String> {
        let req = firefly::Request {
            id: 0,
            payload: firefly::mod_Request::OneOfpayload::createJoinLink(
                firefly::CreateJoinLinkRequest {
                    group_id,
                    expires_in_seconds,
                    max_uses,
                },
            ),
        };

        let bytes = self.request(req).await?;
        let response = deserialize_proto::<firefly::Response<'_>>(&bytes)?;

        if let Some(error) = response.error {
            return Err(anyhow::anyhow!(
                "Server error: {} ({})",
                error.error,
                error.errorCode
            ));
        }

        match response.body {
            firefly::mod_Response::OneOfbody::createJoinLink(res) => Ok(res.token.to_string()),
            _ => Err(anyhow::anyhow!("Unexpected response from server")),
        }
    }

    pub async fn join_via_link(&self, token: &str) -> anyhow::Result<()> {
        let req = firefly::Request {
            id: 0,
            payload: firefly::mod_Request::OneOfpayload::joinViaLink(firefly::JoinViaLinkRequest {
                token: token.into(),
            }),
        };

        let bytes = self.request(req).await?;
        let response = deserialize_proto::<firefly::Response<'_>>(&bytes)?;

        if let Some(error) = response.error {
            return Err(anyhow::anyhow!(
                "Server error: {} ({})",
                error.error,
                error.errorCode
            ));
        }

        match response.body {
            firefly::mod_Response::OneOfbody::joinViaLinkSuccess(_) => Ok(()),
            _ => Err(anyhow::anyhow!("Unexpected response from server")),
        }
    }

    pub async fn sync_group_joins_and_readds(&self) -> anyhow::Result<()> {
        let token = self
            .callbacks
            .get_access_token()
            .await
            .context("token not found")?;
        let address_id = self.addressId.load(std::sync::atomic::Ordering::Relaxed);
        let device_id = self
            .key_stores
            .store()
            .identity_store
            .get_full_identity_key_pair()
            .await
            .map(|i| i.device_id)
            .unwrap_or(0) as u8;

        let _ = self.join_groups(&token, address_id, device_id).await;
        let _ = self
            .add_requested_re_add_group_members(&token, address_id, device_id)
            .await;

        Ok(())
    }

    pub fn address_id(&self) -> u64 {
        self.addressId.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn key_stores(&self) -> &Arc<crate::db::ffi_stores::FfiKeyStores> {
        &self.key_stores
    }

    pub fn history_keys_store(&self) -> &HistoryKeysStore {
        &self.history_keys_store
    }

    pub async fn request_group_history(
        &self,
        group_id: u64,
        start_msg_id: u64,
        end_msg_id: u64,
    ) -> anyhow::Result<()> {
        let req = firefly::Request {
            payload: firefly::mod_Request::OneOfpayload::createHistoryRequest(
                firefly::CreateHistoryRequest {
                    group_id,
                    start_msg_id,
                    end_msg_id,
                },
            ),
            ..Default::default()
        };
        let response_bytes = self.request(req).await?;
        let response = deserialize_proto::<firefly::Response>(&response_bytes)?;
        if let Some(err) = response.error {
            return Err(anyhow::anyhow!("server error {}: {}", err.errorCode, err.error));
        }
        Ok(())
    }

    pub async fn claim_group_history_request(
        &self,
        group_id: u64,
        request_id: u64,
        start_msg_id: u64,
        end_msg_id: u64,
    ) -> anyhow::Result<bool> {
        let req = firefly::Request {
            payload: firefly::mod_Request::OneOfpayload::claimHistoryRequest(
                firefly::ClaimHistoryRequest {
                    group_id,
                    request_id,
                    start_msg_id,
                    end_msg_id,
                },
            ),
            ..Default::default()
        };
        let response_bytes = self.request(req).await?;
        let response = deserialize_proto::<firefly::Response>(&response_bytes)?;
        if let Some(err) = response.error {
            return Err(anyhow::anyhow!("server error {}: {}", err.errorCode, err.error));
        }
        match response.body {
            firefly::mod_Response::OneOfbody::claimHistoryResponse(res) => Ok(res.granted),
            _ => Err(anyhow::anyhow!("unexpected response body for claimHistoryRequest")),
        }
    }

    pub async fn publish_group_history_chunk(
        &self,
        group_id: u64,
        start_msg_id: u64,
        end_msg_id: u64,
        msg_count: u32,
        unencrypted_hash: Vec<u8>,
        chunk_url: String,
    ) -> anyhow::Result<()> {
        let req = firefly::Request {
            payload: firefly::mod_Request::OneOfpayload::publishHistoryChunk(
                firefly::PublishHistoryChunkRequest {
                    reserve_only: false,
                    group_id,
                    start_msg_id,
                    end_msg_id,
                    msg_count,
                    unencrypted_hash: unencrypted_hash.into(),
                    chunk_url: chunk_url.into(),
                },
            ),
            ..Default::default()
        };
        let response_bytes = self.request(req).await?;
        let response = deserialize_proto::<firefly::Response>(&response_bytes)?;
        if let Some(err) = response.error {
            return Err(anyhow::anyhow!("server error {}: {}", err.errorCode, err.error));
        }
        Ok(())
    }

    pub async fn disapprove_group_history_chunk(
        &self,
        group_id: u64,
        chunk_id: u64,
        reason: String,
    ) -> anyhow::Result<()> {
        let _ = self.history_keys_store.record_disapproval(group_id, chunk_id, &reason).await;

        let req = firefly::Request {
            payload: firefly::mod_Request::OneOfpayload::disapproveHistoryChunk(
                firefly::DisapproveHistoryChunkRequest {
                    group_id,
                    chunk_id,
                    reason: reason.into(),
                },
            ),
            ..Default::default()
        };
        let response_bytes = self.request(req).await?;
        let response = deserialize_proto::<firefly::Response>(&response_bytes)?;
        if let Some(err) = response.error {
            return Err(anyhow::anyhow!("server error {}: {}", err.errorCode, err.error));
        }
        Ok(())
    }

    pub async fn get_group_history_chunks(
        &self,
        group_id: u64,
        since_msg_id: u64,
        until_msg_id: u64,
    ) -> anyhow::Result<Vec<firefly::GroupHistoryChunkItem<'static>>> {
        self.list_group_history_chunks(group_id, since_msg_id, until_msg_id, false).await
    }

    pub async fn list_group_history_chunks(&self, group_id: u64, since_msg_id: u64, until_msg_id: u64, include_unverified: bool) -> anyhow::Result<Vec<firefly::GroupHistoryChunkItem<'static>>> {
        let req = firefly::Request {
            payload: firefly::mod_Request::OneOfpayload::getHistoryChunks(
                firefly::GetHistoryChunksRequest {
                    group_id,
                    since_msg_id,
                    until_msg_id,
                    include_unverified,
                },
            ),
            ..Default::default()
        };
        let response_bytes = self.request(req).await?;
        let response = deserialize_proto::<firefly::Response>(&response_bytes)?;
        if let Some(err) = response.error {
            return Err(anyhow::anyhow!("server error {}: {}", err.errorCode, err.error));
        }
        match response.body {
            firefly::mod_Response::OneOfbody::getHistoryChunksResponse(res) => {
                let chunks = res.chunks.into_iter().map(|c| firefly::GroupHistoryChunkItem {
                    id: c.id,
                    group_id: c.group_id,
                    start_msg_id: c.start_msg_id,
                    end_msg_id: c.end_msg_id,
                    msg_count: c.msg_count,
                    unencrypted_hash: c.unencrypted_hash.to_vec().into(),
                    chunk_url: c.chunk_url.to_string().into(),
                    created_at: c.created_at,
                    uploaded_by: c.uploaded_by.to_string().into(),
                    status: c.status,
                    verified: c.verified,
                    disapproved_by: c.disapproved_by.to_string().into(),
                    disapproved_reason: c.disapproved_reason.to_string().into(),
                }).collect();
                Ok(chunks)
            }
            _ => Err(anyhow::anyhow!("unexpected response body for getHistoryChunks")),
        }
    }

    pub async fn get_pending_group_history_requests(
        &self,
        group_ids: Vec<u64>,
    ) -> anyhow::Result<Vec<firefly::GroupHistoryRequestItem<'static>>> {
        let req = firefly::Request {
            payload: firefly::mod_Request::OneOfpayload::getPendingHistoryRequests(
                firefly::GetPendingHistoryRequests {
                    group_ids,
                },
            ),
            ..Default::default()
        };
        let response_bytes = self.request(req).await?;
        let response = deserialize_proto::<firefly::Response>(&response_bytes)?;
        if let Some(err) = response.error {
            return Err(anyhow::anyhow!("server error {}: {}", err.errorCode, err.error));
        }
        match response.body {
            firefly::mod_Response::OneOfbody::getPendingHistoryRequestsResponse(res) => {
                let requests = res.requests.into_iter().map(|r| firefly::GroupHistoryRequestItem {
                    id: r.id,
                    group_id: r.group_id,
                    requester_address: r.requester_address,
                    requester_username: r.requester_username.to_string().into(),
                    start_msg_id: r.start_msg_id,
                    end_msg_id: r.end_msg_id,
                    status: r.status,
                    claimed_by: r.claimed_by,
                    created_at: r.created_at,
                }).collect();
                Ok(requests)
            }
            _ => Err(anyhow::anyhow!("unexpected response body for getPendingHistoryRequests")),
        }
    }

    pub async fn close_group_history_request(
        &self,
        group_id: u64,
        request_id: u64,
    ) -> anyhow::Result<()> {
        let req = firefly::Request {
            payload: firefly::mod_Request::OneOfpayload::closeHistoryRequest(
                firefly::CloseHistoryRequest {
                    group_id,
                    request_id,
                },
            ),
            ..Default::default()
        };
        let response_bytes = self.request(req).await?;
        let response = deserialize_proto::<firefly::Response>(&response_bytes)?;
        if let Some(err) = response.error {
            return Err(anyhow::anyhow!("server error {}: {}", err.errorCode, err.error));
        }
        Ok(())
    }

    pub async fn handle_group_history_signal(&self, signal: crate::callbacks::GroupHistorySignal) -> anyhow::Result<()> {
        let name = self.callbacks.name().to_string();
        CURRENT_CLIENT.scope(name, async {
            match signal.signal_type {
                0 => {
                    for request in self.get_pending_group_history_requests(vec![signal.group_id]).await?.into_iter() {
                        self.fulfill_group_history(signal.group_id, request.id, request.start_msg_id, request.end_msg_id).await?;
                    }
                }
                1 | 5 => { self.verify_published_chunks(signal.group_id).await?; }
                2 => {
                    if signal.chunk_id != 0 {
                        let _ = self.history_keys_store.record_disapproval(signal.group_id, signal.chunk_id, "Disapproved by peer").await;
                        let _ = self.group_messages_store.purge_imported_chunk(signal.group_id, signal.chunk_id).await;
                    }
                    self.key_value_store.set(&format!("history_export_cursor:{}", signal.group_id), "0").await?;
                    self.archive_ready_history(signal.group_id).await?;
                }
                6 => { self.archive_ready_history(signal.group_id).await?; }
                8 => { self.resume_snapshots(signal.group_id).await?; }
                7 => {
                    self.re_encrypt_and_send_pinned_messages(signal.group_id).await?;
                }
                _ => {}
            }
            Ok(())
        }).await
    }

    pub async fn history_enabled(&self, group_id: u64) -> anyhow::Result<bool> {
        #[derive(serde::Deserialize)]
        struct Policy { group_id: u64, is_member: bool, settings: u32 }
        let token=self.callbacks.get_access_token().await.context("Authentication required")?;
        let response=reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).timeout(Duration::from_secs(10)).build()?
            .get(format!("{}/group/policy?id={}",self.firefly_base_url.trim_end_matches('/'),group_id)).bearer_auth(token).send().await?.error_for_status()?;
        let policy: Policy=serde_json::from_slice(&response.bytes().await?)?;
        Ok(policy.group_id==group_id && policy.is_member && policy.settings & 2 != 0)
    }

    pub async fn share_group_history_keys(
        &self,
        group_id: u64,
        keys: Vec<firefly::GroupHistoryChunkKey<'static>>,
    ) -> anyhow::Result<u64> {
        anyhow::ensure!(self.history_enabled(group_id).await?, "Group history disabled");
        self.share_history_payload(group_id, firefly::GroupHistoryKeysPayload {keys,chunks:vec![]}).await
    }

    async fn share_history_payload(&self, group_id:u64, payload:firefly::GroupHistoryKeysPayload<'static>)->anyhow::Result<u64> {
        anyhow::ensure!(self.history_enabled(group_id).await?, "Group history disabled");
        let serialized = serialize_proto(&payload)?;
        let text = hex::encode(&serialized);
        let message_type = firefly_protos::MESSAGE_TYPE_HIDDEN | firefly_protos::MESSAGE_TYPE_HISTORY_KEYS;

        let inner = firefly::GroupMessageInner {
            channelId: 0,
            message: firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(
                firefly::MessagePayload {
                    text: text.into(),
                    files: None,
                    ext: firefly::mod_MessagePayload::OneOfext::None,
                    message_type,
                },
            ),
            message_type,
        };

        self.upload_group_message(group_id, inner, 0).await
    }

    /// Send only metadata and per-chunk keys through the authenticated hidden stream.
    /// Recipients still consult the authoritative server before lazy imports.
    pub async fn share_history_manifest(&self, group_id:u64)->anyhow::Result<()> {
        if !self.history_enabled(group_id).await? { return Ok(()); }
        let mut before=0;
        loop {
            let chunks=self.get_group_history_chunks(group_id,0,before).await?;
            if chunks.is_empty() {break;}
            let next=chunks.last().map(|c|c.start_msg_id).unwrap_or(0);
            anyhow::ensure!(next>0 && (before==0 || next<before),"History manifest cursor did not advance");
            let mut keys=Vec::new();
            for chunk in &chunks {
                if let Some((key,nonce))=self.history_keys_store.verified_material(group_id,chunk.start_msg_id,chunk.end_msg_id,&chunk.unencrypted_hash).await? {
                    keys.push(firefly::GroupHistoryChunkKey {group_id,start_msg_id:chunk.start_msg_id,end_msg_id:chunk.end_msg_id,key:key.into(),nonce:nonce.into(),unencrypted_hash:chunk.unencrypted_hash.clone()});
                }
            }
            let complete=chunks.len()<32;
            self.share_history_payload(group_id,firefly::GroupHistoryKeysPayload{keys,chunks}).await?;
            if complete {break;} before=next;
        }
        Ok(())
    }

    /// Automatic archival is bounded and only publishes full 5000-visible-record batches.
    pub async fn archive_ready_history(&self, group_id:u64)->anyhow::Result<()> {
        // Wait instead of dropping another group's archival wake-up.
        let _work=self.history_work.lock().await;
        let progress_key=format!("history_export_cursor:{group_id}");
        let mut cursor=self.key_value_store.get(&progress_key).await.ok().and_then(|s|s.parse::<u64>().ok()).unwrap_or(0);
        for _ in 0..8 {
            let mut scan=cursor.saturating_add(1);
            let mut messages=Vec::new();
            // Skip unreadable channels without treating them as visible records.
            loop {
                let raw=self.group_messages_store.authenticated_history_range(group_id,scan,i64::MAX as u64,DEFAULT_CHUNK_SIZE as u32).await?;
                if raw.is_empty() {break;}
                let end=raw.last().map(|m|m.id).unwrap_or(scan);
                let exhausted=raw.len()<DEFAULT_CHUNK_SIZE;
                messages.extend(self.group_message_store().visible_messages(raw).await?);
                if messages.len()>=DEFAULT_CHUNK_SIZE {messages.truncate(DEFAULT_CHUNK_SIZE);break;}
                if exhausted {break;}
                scan=end.saturating_add(1);
                tokio::task::yield_now().await;
            }
            if messages.len()<DEFAULT_CHUNK_SIZE {return Ok(());}
            if !self.history_enabled(group_id).await? {return Ok(());}
            let mls_group = match self.get_mls_group(group_id).await {
                Ok(g) => g,
                Err(err) => {
                    log::warn!("Could not load MLS group for history archive: {:?}", err);
                    return Ok(());
                }
            };
            if !mls_group.has_full_channel_access().await.unwrap_or(false) {
                log::info!("Skipping history archive for group {}: member lacks full channel access", group_id);
                return Ok(());
            }
            let is_owner = mls_group.is_owner().await.unwrap_or(false);
            let start=messages[0].id;
            let end=messages[messages.len()-1].id;
            // Reuse a peer's active record instead of continually uploading it.
            let existing=self.list_group_history_chunks(group_id,start,end.saturating_add(1),true).await?;
            if let Some(chunk)=existing.iter().filter(|c|c.status==0 && c.start_msg_id<=start && c.end_msg_id>=start).max_by_key(|c|c.end_msg_id) {
                if !chunk.verified {
                    if let Some((key,nonce))=self.history_keys_store.verified_material(group_id,chunk.start_msg_id,chunk.end_msg_id,&chunk.unencrypted_hash).await? {
                        self.share_group_history_keys(group_id,vec![firefly::GroupHistoryChunkKey{group_id,start_msg_id:chunk.start_msg_id,end_msg_id:chunk.end_msg_id,key:key.into(),nonce:nonce.into(),unencrypted_hash:chunk.unencrypted_hash.clone()}]).await?;
                    }
                }
                cursor=chunk.end_msg_id;
            } else {
                if !is_owner {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                if !self.claim_group_history_request(group_id,0,start,end).await? {
                    let cb = self.callbacks.clone();
                    let u = self.callbacks.name().to_string();
                    tokio::spawn(async move {
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        cb.on_group_history_signal(crate::callbacks::GroupHistorySignal {
                            group_id, signal_type: 6, request_id: 0, chunk_id: 0, username: u,
                        }).await;
                    });
                    return Ok(());
                }
                let packed=pack_messages_into_chunk(&messages)?;
                validate_chunk_records(&messages,group_id,start,end,packed.msg_count)?;
                let url=self.upload_chunk_blob(group_id,packed.blob,start,end,&packed.unencrypted_hash).await?;
                self.history_keys_store.save_verified_key(group_id,start,end,&packed.unencrypted_hash,&packed.key,&packed.nonce).await?;
                self.publish_group_history_chunk(group_id,start,end,packed.msg_count,packed.unencrypted_hash.clone(),url).await?;
                self.share_group_history_keys(group_id,vec![firefly::GroupHistoryChunkKey{group_id,start_msg_id:start,end_msg_id:end,key:packed.key.into(),nonce:packed.nonce.into(),unencrypted_hash:packed.unencrypted_hash.into()}]).await?;
                cursor=end;
            }
            self.key_value_store.set(&progress_key,&cursor.to_string()).await?;
        }
        // Yield after eight batches, then resume through the coalescing worker.
        self.callbacks.on_group_history_signal(crate::callbacks::GroupHistorySignal {
            group_id, signal_type: 6, request_id: 0, chunk_id: 0,
            username: self.callbacks.name().to_string(),
        }).await;
        Ok(())
    }

    pub fn set_cdn_url(&self, url: String) {
        if let Ok(mut cdn) = self.cdn_base_url.write() {
            *cdn = Some(url);
        }
    }

    pub fn get_cdn_url(&self) -> String {
        if let Ok(cdn) = self.cdn_base_url.read() {
            if let Some(ref url) = *cdn {
                return url.trim_end_matches('/').to_string();
            }
        }
        if let Ok(env_url) = std::env::var("FIREFLY_CDN_URL").or_else(|_| std::env::var("CDN_URL")) {
            return env_url.trim_end_matches('/').to_string();
        }
        format!("{}/cdn", self.firefly_base_url.trim_end_matches('/'))
    }

    pub async fn upload_chunk_blob(
        &self,
        group_id: u64,
        blob_bytes: Vec<u8>,
        start: u64,
        end: u64,
        hash: &[u8],
    ) -> anyhow::Result<String> {
        let token = self
            .callbacks
            .get_access_token()
            .await
            .context("token not found")?;
        let cdn_base = self.get_cdn_url();
        let cdn_base = cdn_base.trim_end_matches('/');
        // Generate random ID for CDN chunk
        let random_id = format!("{:016x}{:016x}", rand::random::<u64>(), rand::random::<u64>());
        let upload_url = format!("{}/group_chunks/{}/{}", cdn_base, group_id, random_id);

        anyhow::ensure!(blob_bytes.len() <= MAX_CHUNK_BYTES, "History upload exceeds limit");
        let upload_url = history_chunk_url(cdn_base, &upload_url, group_id)?;
        // Reserve cleanup evidence before any object can reach storage. A crash
        // between PUT and metadata publication must not create an untracked blob.
        let reservation=firefly::Request {payload:firefly::mod_Request::OneOfpayload::publishHistoryChunk(firefly::PublishHistoryChunkRequest{group_id,start_msg_id:start,end_msg_id:end,msg_count:DEFAULT_CHUNK_SIZE as u32,unencrypted_hash:hash.to_vec().into(),chunk_url:upload_url.to_string().into(),reserve_only:true}),..Default::default()};
        let bytes=self.request(reservation).await?;
        let reply=deserialize_proto::<firefly::Response>(&bytes)?;
        if let Some(error)=reply.error {anyhow::bail!("History upload reservation denied: {}",error.error);}
        let response = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).timeout(Duration::from_secs(30)).build()?
            .put(upload_url.clone())
            .bearer_auth(token)
            .header("Content-Type", "application/octet-stream")
            .body(blob_bytes)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "upload chunk blob to cdn failed [{}]: {}",
                response.status(),
                response.text().await?
            ));
        }

        Ok(upload_url.to_string())
    }

    pub async fn download_chunk_blob(&self, group_id: u64, url_or_path: &str) -> anyhow::Result<Vec<u8>> {
        crate::history::download_history_blob(&self.get_cdn_url(), group_id, url_or_path).await
    }

    pub async fn approve_group_history_chunk(&self, group_id: u64, chunk_id: u64, hash: Vec<u8>) -> anyhow::Result<()> {
        let req = firefly::Request { payload: firefly::mod_Request::OneOfpayload::approveHistoryChunk(firefly::ApproveHistoryChunkRequest { group_id, chunk_id, unencrypted_hash: hash.into() }), ..Default::default() };
        let bytes = self.request(req).await?;
        let response = deserialize_proto::<firefly::Response>(&bytes)?;
        if let Some(error) = response.error { anyhow::bail!("History approval rejected: {}", error.error); }
        Ok(())
    }

    pub async fn set_direct_message_pin(&self, other: String, message_id: u64, pinned: bool) -> anyhow::Result<()> {
        let token=self.callbacks.get_access_token().await.context("token not found")?;
        let before=self.messages_store.get_last_messages_of(&other,i64::MAX,100_000).await?;
        let old=before.iter().find(|m|m.id==message_id).ok_or_else(||anyhow::anyhow!("Message unavailable for pinning"))?.message_type;
        let next=if pinned {old|firefly_protos::MESSAGE_TYPE_PINNED}else{old&!firefly_protos::MESSAGE_TYPE_PINNED};
        self.messages_store.update_message_type(&other,message_id,next).await?;
        let pins=self.messages_store.get_pinned_messages_of(&other).await?;
        match crate::direct_pins::publish(&self.firefly_base_url,&token,&other,&pins).await {
            Ok(secret)=>{if let Err(e)=self.send_direct_pin_snapshot(&other,secret).await {self.messages_store.update_message_type(&other,message_id,old).await?;return Err(e)}Ok(())},
            Err(e)=>{self.messages_store.update_message_type(&other,message_id,old).await?;Err(e)}
        }
    }
    async fn send_direct_pin_snapshot(&self, other: &str, secret: firefly::DirectPinSnapshotSecret<'static>) -> anyhow::Result<()> {
        let payload = serialize_proto(&firefly::UserMessageInner {
            message: firefly::mod_UserMessageInner::OneOfmessage::directPinSnapshot(secret),
            nonce: rng().next_u32(),
            message_type: firefly_protos::MESSAGE_TYPE_HIDDEN,
        })?
        .to_vec();
        // 1. Sync encryption key capability to the chat participant (other user)
        self.encrypt_and_send_internal(other, &payload, firefly_protos::MESSAGE_TYPE_HIDDEN).await?;
        // 2. Sync encryption key capability between own sibling devices
        let my_username = self.callbacks.name();
        if !my_username.is_empty() && my_username != other {
            if let Err(e) = self.encrypt_and_send_internal(my_username, &payload, firefly_protos::MESSAGE_TYPE_HIDDEN).await {
                log::warn!("Failed to sync direct pin snapshot to sibling devices: {e}");
            }
        }
        Ok(())
    }

    pub async fn send_backup_key_sync(
        &self,
        sync: firefly_protos::firefly::DirectBackupKeySync<'static>,
    ) -> anyhow::Result<()> {
        let payload = firefly_protos::serialize_proto(&firefly_protos::firefly::UserMessageInner {
            message: firefly_protos::firefly::mod_UserMessageInner::OneOfmessage::backupKeySync(sync),
            nonce: rand::rng().next_u32(),
            message_type: 0,
        })?
        .to_vec();
        let username = self.callbacks.name().to_string();
        self.encrypt_and_send_internal(&username, &payload, 0).await.map(|_| ())
    }

    pub async fn fulfill_group_history(&self, group_id:u64, request_id:u64, start_msg_id:u64, end_msg_id:u64)->anyhow::Result<bool> {
        if !self.history_enabled(group_id).await? {return Ok(false);}
        if !self.claim_group_history_request(group_id,request_id,start_msg_id,end_msg_id).await? {return Ok(false);}
        // Only complete thousand-message batches are archived. Requests also
        // repair missed join-time key delivery without republishing old blobs.
        self.archive_ready_history(group_id).await?;
        self.share_join_snapshots(group_id).await?;
        self.close_group_history_request(group_id,request_id).await?;
        Ok(true)
    }

    pub async fn fetch_and_verify_group_history(&self, group_id: u64, since: u64, until: u64) -> anyhow::Result<usize> {
        Ok(self.import_group_history_page(group_id, since, until).await?.imported_count)
    }

    pub async fn import_group_history_page(&self, group_id: u64, since: u64, until: u64) -> anyhow::Result<HistoryImportPage> {
        if !self.history_enabled(group_id).await? { return Ok(HistoryImportPage::default()); }
        let chunks = self.get_group_history_chunks(group_id, since, until).await?;
        let mut page = HistoryImportPage::default();
        for chunk in chunks.iter().take(8) {
            page.next_before = Some(chunk.start_msg_id);
            if !chunk.verified || chunk.status != 0 || self.history_keys_store.is_chunk_disapproved(group_id, chunk.id).await? { continue; }
            if self.history_keys_store.is_imported(group_id, chunk.id, &chunk.unencrypted_hash).await? { continue; }
            let Some(key) = self.history_keys_store.verified_key(group_id, chunk.start_msg_id, chunk.end_msg_id, &chunk.unencrypted_hash).await? else { page.pending = true; continue; };
            let blob = self.download_chunk_blob(group_id, &chunk.chunk_url).await?;
            let records = decrypt_and_unpack_chunk(&blob, &key, &chunk.unencrypted_hash)?;
            validate_chunk_records(&records, group_id, chunk.start_msg_id, chunk.end_msg_id, chunk.msg_count)?;
            // Read policy is checked again when returning rows to callers. Imports
            // never overwrite live messages and never become evidence for a vote.
            page.imported_count += self.group_messages_store.import_verified_history(group_id, chunk.id, &chunk.unencrypted_hash, &records).await?;
        }
        page.has_more = chunks.len() > 8;
        Ok(page)
    }

    pub async fn verify_published_chunks(&self, group_id: u64) -> anyhow::Result<()> {
        let mls_group = match self.get_mls_group(group_id).await {
            Ok(g) => g,
            Err(err) => {
                log::warn!("Could not load MLS group for history verification: {:?}", err);
                return Ok(());
            }
        };
        if !mls_group.has_full_channel_access().await.unwrap_or(false) {
            log::info!("Skipping history chunk verification for group {}: member lacks full channel access", group_id);
            return Ok(());
        }
        let cursor_key=format!("history_verify_cursor:{group_id}");
        let mut before=self.key_value_store.get(&cursor_key).await.ok().and_then(|s|s.parse::<u64>().ok()).unwrap_or(0);
        for _ in 0..8 {
        let chunks = self.list_group_history_chunks(group_id, 0, before, true).await?;
        let complete=chunks.len()<32;
        let next=chunks.last().map(|c|c.start_msg_id).unwrap_or(0);
        for chunk in chunks {
            if chunk.verified || chunk.uploaded_by == self.callbacks.name() || self.history_keys_store.is_chunk_disapproved(group_id, chunk.id).await? { continue; }
            let Some(key) = self.history_keys_store.verified_key(group_id, chunk.start_msg_id, chunk.end_msg_id, &chunk.unencrypted_hash).await? else { continue; };
            let blob = match self.download_chunk_blob(group_id,&chunk.chunk_url).await {
                Ok(blob)=>blob,
                Err(err)=>{log::debug!("Peer chunk unavailable for verification: {}",err);continue;}
            };
            let records = match decrypt_and_unpack_chunk(&blob, &key, &chunk.unencrypted_hash)
                .and_then(|records| {
                    validate_chunk_records(&records, group_id, chunk.start_msg_id, chunk.end_msg_id, chunk.msg_count)?;
                    Ok(records)
                }) {
                Ok(records) => records,
                Err(_) => {
                    self.disapprove_group_history_chunk(group_id, chunk.id, "Invalid history chunk".into()).await?;
                    continue;
                }
            };
            let ids: Vec<u64> = records.iter().map(|r| r.id).collect();
            let local = self.group_messages_store.authenticated_history_ids(group_id, &ids).await?;
            // Sparse channels are legitimate. Missing independent evidence means
            // abstention, never an approval based only on the publisher's hash.
            if local.len() != records.len() { continue; }
            if compute_unencrypted_hash(&local)? == chunk.unencrypted_hash.as_ref() {
                self.approve_group_history_chunk(group_id, chunk.id, chunk.unencrypted_hash.into_owned()).await?;
            } else {
                self.disapprove_group_history_chunk(group_id, chunk.id, "History differs from authenticated local messages".into()).await?;
            }
        }
        if complete || next==0 || (before!=0 && next>=before) {
            self.key_value_store.set(&cursor_key,"0").await?;
            return Ok(());
        }
        before=next;
        self.key_value_store.set(&cursor_key,&before.to_string()).await?;
        }
        self.callbacks.on_group_history_signal(crate::callbacks::GroupHistorySignal {
            group_id, signal_type: 5, request_id: 0, chunk_id: 0,
            username: self.callbacks.name().to_string(),
        }).await;
        Ok(())
    }

}

async fn on_group_message(
    msg: &firefly::GroupMessage<'_>,
    firefly_mls_client: &FfiMlsClient,
    group_info_store: &GroupInfoStore,
    group_message_store: &GroupMessagesStore,
    history_keys_store: &HistoryKeysStore,
    key_value_store: &KeyValueStore,
    callbacks: &Arc<dyn FireflyWsClientCallback>,
    is_commit: bool,
) -> anyhow::Result<()> {
    let _ = key_value_store
        .update_last_received_message_id(msg.id)
        .await;

    let groupId = msg.groupId;
    let group = match group_info_store.get(groupId).await {
        Ok(g) => g,
        Err(_) => {
            log::warn!(
                "received group message for unknown group {}, skipping until sync",
                groupId
            );
            return Ok(());
        }
    };

    let group = firefly_mls_client
        .load_group(groupId, group.identifier)
        .await
        .map_err(|err| anyhow::anyhow!(err))?;

    let current_epoch = group.epoch().await;
    log::info!(
        "Processing group message: group {} id {}, epoch {}, local epoch {}",
        groupId,
        msg.id,
        msg.epoch,
        current_epoch
    );

    if msg.epoch < current_epoch as u32 {
        log::info!("Skipping old message (group {} id {})", groupId, msg.id);
        return Ok(());
    }

    if is_commit && msg.epoch == current_epoch as u32 {
        log::info!(
            "Skipping redundant commit message (group {} id {})",
            groupId,
            msg.id
        );
        return Ok(());
    }

    let message = match group.process(msg.message.to_vec()).await {
        Ok(message) => message,
        Err(err) if err.downcast_ref::<firefly_core::rules::MessagePermissionDenied>().is_some() => {
            // Consume rejected messages without storing plaintext or emitting callbacks.
            // Advancing the cursor also prevents an offline-sync retry loop.
            group_message_store.update_cursor(msg.id, groupId, group.epoch().await as u32).await?;
            return Ok(());
        }
        Err(err) => return Err(err),
    };

    let epoch = group.epoch().await as u32;
    match message {
        crate::group::FireflyMlsReceivedMessage::Message(encrypted_group_message) => {
            let inner = deserialize_proto::<firefly::GroupMessageInner>(&encrypted_group_message.message).ok();
            // Pin/history features were not released. Legacy per-message pin
            // controls must not bypass the authoritative 50-record snapshot cap.
            if inner.as_ref().is_some_and(|i|matches!(&i.message,firefly::mod_GroupMessageInner::OneOfmessage::pinUpdate(_) | firefly::mod_GroupMessageInner::OneOfmessage::pinSnapshot(_))) {
                group_message_store.update_cursor(msg.id,groupId,epoch).await?;
                return Ok(());
            }

            if let Some(firefly::GroupMessageInner{message:firefly::mod_GroupMessageInner::OneOfmessage::syncBundle(bundle),..})=inner.as_ref() {
                group_snapshots::accept_bundle(key_value_store,history_keys_store,groupId,bundle).await?;
                callbacks.on_group_history_signal(crate::callbacks::GroupHistorySignal{group_id:groupId,signal_type:8,request_id:0,chunk_id:0,username:encrypted_group_message.sender.clone()}).await;
            }
            let channelId = inner.as_ref().map(|i| i.channelId).unwrap_or(0);
            let message_type = inner.as_ref().map(|i| {
                i.message_type
                    | match &i.message {
                        firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(p) => p.message_type,
                        _ => 0,
                    }
            }).unwrap_or(0);

            if message_type & firefly_protos::MESSAGE_TYPE_HISTORY_KEYS != 0 {
                let raw_keys_bytes = if let Some(inner_msg) = inner.as_ref() {
                    match &inner_msg.message {
                        firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(p) => {
                            if let Ok(raw) = hex::decode(p.text.as_bytes()) {
                                Some(raw)
                            } else if let Ok(raw) = base64::engine::general_purpose::STANDARD.decode(p.text.as_bytes()) {
                                Some(raw)
                            } else {
                                None
                            }
                        }
                        _ => None,
                    }
                } else {
                    None
                };

                let keys_payload = raw_keys_bytes
                    .as_deref()
                    .and_then(|raw| deserialize_proto::<firefly::GroupHistoryKeysPayload>(raw).ok())
                    .or_else(|| {
                        deserialize_proto::<firefly::GroupHistoryKeysPayload>(&encrypted_group_message.message).ok()
                    });

                if let Some(keys_payload) = keys_payload {
                    for key in keys_payload.keys {
                        if key.group_id != groupId { continue; }
                        if let Err(err) = history_keys_store
                            .save_verified_key(
                                groupId,
                                key.start_msg_id,
                                key.end_msg_id,
                                &key.unencrypted_hash,
                                &key.key,
                                &key.nonce,
                            )
                            .await
                        {
                            log::error!("failed to save history chunk key: {:?}", err);
                        } else {
                            log::info!(
                                "saved history chunk key: group={}, range=[{}-{}]",
                                groupId,
                                key.start_msg_id,
                                key.end_msg_id
                            );
                        }
                    }
                }
            }

            if message_type & firefly_protos::MESSAGE_TYPE_HISTORY_KEYS != 0 {
                callbacks.on_group_history_signal(crate::callbacks::GroupHistorySignal { group_id: groupId, signal_type: 5, request_id: 0, chunk_id: 0, username: encrypted_group_message.sender.clone() }).await;
            }
            group_message_store
                .add(
                    msg.id,
                    msg.groupId,
                    channelId,
                    epoch,
                    &encrypted_group_message.sender,
                    &encrypted_group_message.message,
                    message_type,
                )
                .await?;
            let pin_target = inner.as_ref().and_then(|i| match &i.message { firefly::mod_GroupMessageInner::OneOfmessage::pinUpdate(update) => Some(update.message_id), _ => None });
            let message = crate::db::group_messages::GroupMessage {
                id: msg.id,
                group_id: groupId,
                by: encrypted_group_message.sender,
                message: encrypted_group_message.message,
                channel_id: channelId,
                epoch,
                message_type,
            };

            log::info!(
                "processed group message: id: {}, by: {}, len: {}, message_epoch: {}, group_epoch: {}, message_type: {}",
                message.id,
                message.by,
                message.message.len(),
                msg.epoch,
                epoch,
                message_type,
            );
            if let Some(target_id) = pin_target {
                if let Some(target)=group_message_store.get_message(groupId,target_id).await? {
                    if group.can_see_message(target.channel_id).await? { callbacks.on_group_message_updated(target).await; }
                }
            }
            if message_type & firefly_protos::MESSAGE_TYPE_HIDDEN == 0 {
                let message=group_message_store.get_message(groupId,message.id).await?.unwrap_or(message);
                callbacks.on_group_message(message).await;
                callbacks.on_group_history_signal(crate::callbacks::GroupHistorySignal{group_id:groupId,signal_type:6,request_id:0,chunk_id:0,username:callbacks.name().to_string()}).await;
            }
        }
        crate::group::FireflyMlsReceivedMessage::Commit => {
            group_message_store
                .update_cursor(msg.id, msg.groupId, epoch)
                .await?;
            callbacks.on_group_updated(groupId).await;
        }
        _ => {
            group_message_store
                .update_cursor(msg.id, msg.groupId, epoch)
                .await?;
        }
    }

    Ok(())
}

async fn on_user_message(
    msg: &firefly::UserMessage<'_>,
    callbacks: &Arc<dyn FireflyWsClientCallback>,
    key_stores: &Arc<FfiKeyStores>,
    key_value_store: &KeyValueStore,
    sender: Sender<Bytes>,
) -> anyhow::Result<()> {
    if let Err(err) = key_value_store
        .update_last_received_message_id(msg.id)
        .await
    {
        log::error!("failed to update last received message id: {}", err);
    }

    let from = msg.fromUsername.clone();
    let fromDeviceId = msg.fromDeviceId as u8;
    let decrypted = match key_stores
        .decrypt(
            ProtocolAddress::new(from.clone().into_owned(), fromDeviceId.try_into()?),
            msg.text.clone().into_owned(),
            msg.type_pb as u8,
        )
        .await
    {
        Ok(d) => d,
        Err(err) => {
            log::error!("failed to decrypt message: {}", err);
            return Err(anyhow::anyhow!(err));
        }
    };

    let hash_value = twox_hash::XxHash3_64::oneshot(&decrypted);

    if hash_value != msg.hashValue {
        log::warn!(
            "hash value mismatch: expected: {}, got: {}",
            msg.hashValue,
            hash_value
        );
    }

    let my_username = callbacks.name();
    let is_self_msg = msg.settings == 1 || from == my_username;

    let mut is_dummy = false;
    let mut other_username = from.into_owned();
    let mut final_message = decrypted;
    let mut sent_by_other = true;
    let mut message_type: u32 = 0;

    if let Ok(inner) = deserialize_proto::<firefly::UserMessageInner>(&final_message) {
        let inner_type = inner.message_type | match &inner.message {
            firefly::mod_UserMessageInner::OneOfmessage::messagePayload(p) => p.message_type,
            _ => 0,
        };
        message_type |= inner_type;
        match inner.message {
            firefly::mod_UserMessageInner::OneOfmessage::None => {
                is_dummy = true;
            }
            firefly::mod_UserMessageInner::OneOfmessage::selfMessage(self_msg) => {
                if is_self_msg {
                    other_username = self_msg.to.into_owned();
                    final_message = self_msg.inner.into_owned();
                    sent_by_other = false;

                    if let Ok(inner_inner) =
                        deserialize_proto::<firefly::UserMessageInner>(&final_message)
                    {
                        let inner_inner_type = inner_inner.message_type | match &inner_inner.message {
                            firefly::mod_UserMessageInner::OneOfmessage::messagePayload(p) => p.message_type,
                            _ => 0,
                        };
                        message_type |= inner_inner_type;
                        if let firefly::mod_UserMessageInner::OneOfmessage::None =
                            inner_inner.message
                        {
                            is_dummy = true;
                        }
                        if let firefly::mod_UserMessageInner::OneOfmessage::backupKeySync(sync) = inner_inner.message {
                            is_dummy = true;
                            let owned_sync = firefly_protos::firefly::DirectBackupKeySync {
                                version: sync.version,
                                epoch: sync.epoch.into_owned().into(),
                                key: sync.key.into_owned().into(),
                                request_key: sync.request_key,
                                request_id: sync.request_id.into_owned().into(),
                            };
                            callbacks.on_direct_backup_key_sync(owned_sync).await;
                        }
                    }
                }
            }
            _ => {
                if is_self_msg {
                    is_dummy = true;
                }
            }
        }
    } else if is_self_msg {
        is_dummy = true;
    }

    if let Ok(inner) = deserialize_proto::<firefly::UserMessageInner>(&final_message) {
        if let firefly::mod_UserMessageInner::OneOfmessage::directPinSnapshot(secret) = inner.message {
            is_dummy = true;
            let conversation_peer = if sent_by_other {
                other_username.clone()
            } else {
                secret.other.to_string()
            };
            callbacks
                .on_direct_pin_snapshot(
                    conversation_peer,
                    firefly::DirectPinSnapshotSecret {
                        version: secret.version,
                        other: secret.other.into_owned().into(),
                        snapshot_id: secret.snapshot_id.into_owned().into(),
                        key: secret.key.into_owned().into(),
                        ciphertext_hash: secret.ciphertext_hash.into_owned().into(),
                        message_count: secret.message_count,
                    },
                )
                .await;
        }
    }

    if !is_dummy {
        callbacks
            .on_message(crate::db::messages::UserMessage {
                id: msg.id,
                other: other_username,
                message: final_message,
                sent_by_other,
                message_type,
            })
            .await;
    }

    let ack = firefly::ClientMessage {
        message: firefly::mod_ClientMessage::OneOfmessage::verifiedUserMessage(
            firefly::UserMessage {
                id: msg.id,
                toId: msg.toId,
                fromId: msg.fromId,
                hashValue: hash_value,
                ..Default::default() // other fields are not important
            },
        ),
    };

    let payload = serialize_proto(&ack)?;
    sender.send(payload).await.ok();

    if msg.type_pb == 3 && !is_dummy {
        log::info!(
            "Received prekey message from {} device {}. Replying with a dummy message to accept it.",
            msg.fromUsername,
            msg.fromDeviceId
        );

        let dummy_inner = firefly::UserMessageInner {
            message: firefly::mod_UserMessageInner::OneOfmessage::None,
            nonce: rng().next_u32(),
            message_type: 0,
        };

        match serialize_proto(&dummy_inner) {
            Ok(dummy_bytes_raw) => {
                let dummy_bytes = dummy_bytes_raw.to_vec();
                let recipient_address = ProtocolAddress::new(
                    msg.fromUsername.clone().into_owned(),
                    fromDeviceId.try_into()?,
                );

                match key_stores
                    .encrypt(recipient_address, dummy_bytes.clone())
                    .await
                {
                    Ok(cipher) => {
                        let hash_value = twox_hash::XxHash3_64::oneshot(&dummy_bytes);

                        let dummy_msg = firefly::UserMessage {
                            id: get_current_timestamp_microseconds_since_epoch(),
                            toId: msg.fromId,
                            fromId: msg.toId,
                            text: cipher.cipher_text.into(),
                            type_pb: cipher.ty as u32,
                            settings: 0,
                            fromUsername: Default::default(),
                            fromDeviceId: Default::default(),
                            hashValue: hash_value,
                        };

                        let mut message_entries = firefly::UploadUserMessage::default();
                        message_entries.messages.push(dummy_msg);

                        let client_msg = firefly::ClientMessage {
                            message: firefly::mod_ClientMessage::OneOfmessage::request(
                                firefly::Request {
                                    id: 0,
                                    payload: firefly::mod_Request::OneOfpayload::uploadUserMessage(
                                        message_entries,
                                    ),
                                },
                            ),
                        };

                        match serialize_proto(&client_msg) {
                            Ok(payload) => {
                                if let Err(e) = sender.send(payload).await {
                                    log::error!("failed to send dummy accept message: {}", e);
                                } else {
                                    log::info!(
                                        "successfully sent dummy accept message to {}",
                                        msg.fromUsername
                                    );
                                }
                            }
                            Err(e) => {
                                log::error!(
                                    "failed to serialize client message for dummy reply: {}",
                                    e
                                );
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("failed to encrypt dummy accept message: {}", e);
                    }
                }
            }
            Err(e) => {
                log::error!("failed to serialize dummy accept message: {}", e);
            }
        }
    }

    Ok(())
}

async fn re_add_member_internal(
    request: &firefly::GroupReAddRequest<'_>,
    role_id: u32,
    firefly_mls_client: &FfiMlsClient,
    group_info_store: &GroupInfoStore,
    group_message_store: &GroupMessagesStore,
    _pending_requests: &PendingRequests,
    _sender: &Sender<Bytes>,
    callbacks: &Arc<dyn FireflyWsClientCallback>,
    firefly_base_url: &str,
    my_address_id: u64,
) -> anyhow::Result<()> {
    let group_id = request.group_id;
    if request.address_id == my_address_id {
        return Ok(());
    }
    let group_info = group_info_store.get(group_id).await?;
    let group = firefly_mls_client
        .load_group(group_id, group_info.identifier)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    let is_owner = group.is_owner().await.unwrap_or(false);
    if !is_owner {
        // Non-owners wait 1.5s to let the group owner prioritize handling the re-add
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    }

    if let Some(token) = callbacks.get_access_token().await {
        let claim_res = HTTP_CLIENT
            .post(format!(
                "{}/group/reAdd/claim?groupId={}&address={}&myAddress={}",
                firefly_base_url, group_id, request.address_id, my_address_id,
            ))
            .bearer_auth(&token)
            .send()
            .await?;

        if claim_res.status() == reqwest::StatusCode::CONFLICT {
            log::info!(
                "[re_add_member_internal] Contention lost: request for {} (address {}) in group {} already claimed by another member",
                request.username, request.address_id, group_id
            );
            return Ok(());
        }
    }

    group_message_store.mark_snapshot_join_pending(group_id).await?;
    let res = group
        .re_add_member(request.username.to_string(), request.address_id)
        .await;

    let id = match res {
        Ok(id) => id,
        Err(err) => {
            log::warn!(
                "[re_add_member_internal] group.re_add_member failed, trying group.add_member: {:?}",
                err
            );
            group.add_member(request.username.to_string(), role_id).await?
        }
    };

    group_message_store
        .update_cursor(id, group_id, group.epoch().await as u32)
        .await?;

    // Delete reAdd request from server
    if let Some(token) = callbacks.get_access_token().await {
        let _ = HTTP_CLIENT
            .delete(format!(
                "{}/group/reAdd?groupId={}&address={}&myAddress={}",
                firefly_base_url, group_id, request.address_id, my_address_id,
            ))
            .bearer_auth(token)
            .send()
            .await;
    }

    callbacks.on_group_history_signal(crate::callbacks::GroupHistorySignal{group_id,signal_type:7,request_id:0,chunk_id:0,username:callbacks.name().to_string()}).await;
    Ok(())
}

async fn add_member_internal(
    group_id: u64,
    username: String,
    role_id: u32,
    firefly_mls_client: &FfiMlsClient,
    group_info_store: &GroupInfoStore,
    group_message_store: &GroupMessagesStore,
    _pending_requests: &PendingRequests,
    _sender: &Sender<Bytes>,
    callbacks: &Arc<dyn FireflyWsClientCallback>,
) -> anyhow::Result<()> {
    let group_info = group_info_store.get(group_id).await?;
    let group = firefly_mls_client
        .load_group(group_id, group_info.identifier)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    group_message_store.mark_snapshot_join_pending(group_id).await?;
    let id = group.add_member(username, role_id).await?;

    group_message_store
        .update_cursor(id, group_id, group.epoch().await as u32)
        .await?;

    callbacks.on_group_history_signal(crate::callbacks::GroupHistorySignal{group_id,signal_type:7,request_id:0,chunk_id:0,username:callbacks.name().to_string()}).await;
    Ok(())
}

async fn replenish_key_packages_standalone(
    firefly_base_url: &str,
    token: &str,
    address_id: u64,
    device_id: u8,
    count: usize,
    firefly_mls_client: &FfiMlsClient,
    self_group_key_packages_store: &SelfGroupKeyPackageStore,
) -> anyhow::Result<()> {
    if count == 0 {
        return Ok(());
    }
    let mut key_packages = firefly::GroupKeyPackages::default();
    for _ in 0..count {
        let id = (rand::rng().next_u32() % 32000) as i32;
        let key_package = firefly_mls_client
            .generate_key_package()
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        self_group_key_packages_store
            .set(id, &key_package)
            .await?;
        key_packages.packages.push(firefly::GroupKeyPackage {
            id,
            package: key_package.into(),
            address: address_id,
            username: Default::default(),
        });
    }

    let body = serialize_proto(&key_packages)?;

    let url = format!(
        "{}/group/keyPackages?address={}&device_id={}",
        firefly_base_url, address_id, device_id
    );
    let response = HTTP_CLIENT
        .post(url)
        .bearer_auth(token)
        .body(body)
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(anyhow::anyhow!(
            "unexpected status [{}] {}",
            response.status(),
            response.text().await?
        ));
    }

    log::info!(
        "replenished {} group key packages for address {}",
        count,
        address_id
    );
    Ok(())
}

async fn join_group_internal(
    invite: &firefly::GroupInvite<'_>,
    token: &str,
    _address_id: u64,
    _device_id: u8,
    firefly_base_url: &str,
    firefly_mls_client: &FfiMlsClient,
    group_info_store: &GroupInfoStore,
    group_message_store: &GroupMessagesStore,
    callbacks: &Arc<dyn FireflyWsClientCallback>,
) -> anyhow::Result<()> {
    let group_id = invite.groupId;
    let group = firefly_mls_client
        .join_group(group_id, invite.welcomeMessage.to_vec())
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    log::info!("joined group from invite: {}", invite.groupId);
    group.save().await.map_err(|e| anyhow::anyhow!(e))?;

    let url = format!("{}/group?id={}", firefly_base_url, group_id);
    let response = HTTP_CLIENT.get(url).bearer_auth(token).send().await?;

    if response.status().is_success() {
        let body = response.bytes().await?;
        let info = deserialize_proto::<firefly::Group>(&body)?;
        group_info_store
            .set(
                group_id,
                info.name.to_string(),
                info.description.to_string(),
                group
                    .group_identifier()
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?,
            )
            .await?;

        group_message_store
            .update_cursor(invite.commitId, group_id, group.epoch().await as u32)
            .await?;
    }

    let member_url = format!(
        "{}/group/member?groupId={}&address={}",
        firefly_base_url, group_id, _address_id
    );
    let update = firefly::GroupMemberUpdate {
        group_id,
        last_epoch: group.epoch().await as u32,
        last_message_seen: invite.commitId,
    };
    if let Ok(body) = serialize_proto(&update) {
        let _ = HTTP_CLIENT
            .post(member_url)
            .bearer_auth(token)
            .body(body)
            .send()
            .await;
    }

    callbacks.on_group_joined(group_id).await;

    Ok(())
}

async fn on_server_message(
    msg: firefly::ServerMessage<'_>,
    pending_requests: &PendingRequests,
    key_stores: &Arc<FfiKeyStores>,
    callbacks: &Arc<dyn FireflyWsClientCallback>,
    key_value_store: &KeyValueStore,
    firefly_mls_client: &Arc<FfiMlsClient>,
    group_info_store: &GroupInfoStore,
    group_message_store: &GroupMessagesStore,
    history_keys_store: &HistoryKeysStore,
    sender: Sender<Bytes>,
    address_id: u64,
    device_id: u8,
    firefly_base_url: &str,
) -> anyhow::Result<()> {
    match msg.message {
        firefly::mod_ServerMessage::OneOfmessage::userMessage(user_message) => {
            log::info!(
                "from server user message: id: {}, from: {}, fromId: {}, fromDeviceId: {}, payload_ty: {}, payload_len: {}",
                user_message.id,
                user_message.fromUsername,
                user_message.fromId,
                user_message.fromDeviceId,
                user_message.text.len(),
                user_message.type_pb,
            );

            on_user_message(
                &user_message,
                callbacks,
                key_stores,
                key_value_store,
                sender,
            )
            .await?;
        }
        firefly::mod_ServerMessage::OneOfmessage::groupMessage(group_message) => {
            log::info!(
                "from server group message: id: {}, groupId: {}, payload_len: {}, epoch: {}",
                group_message.id,
                group_message.groupId,
                group_message.message.len(),
                group_message.epoch,
            );
            if let Err(err) = on_group_message(
                &group_message,
                firefly_mls_client,
                group_info_store,
                group_message_store,
                history_keys_store,
                key_value_store,
                callbacks,
                false,
            )
            .await
            {
                log::error!(
                    "failed to process group message {}: {:?}",
                    group_message.id,
                    err
                );
            }
            let _ = key_value_store
                .update_last_received_message_id(group_message.id)
                .await;
        }
        firefly::mod_ServerMessage::OneOfmessage::response(response) => {
            if let Some(tx) = pending_requests.lock().unwrap().remove(&response.id) {
                let bytes = serialize_proto(&response)?;
                if tx.send(bytes).is_err() {
                    log::warn!("failed to send response");
                }
            }
        }
        firefly::mod_ServerMessage::OneOfmessage::groupMessages(messages) => {
            for group_message in messages.messages {
                if let Err(err) = on_group_message(
                    &group_message,
                    firefly_mls_client,
                    group_info_store,
                    group_message_store,
                    history_keys_store,
                    key_value_store,
                    callbacks,
                    false,
                )
                .await
                {
                    log::error!(
                        "failed to process group message {}: {:?}",
                        group_message.id,
                        err
                    );
                }
                let _ = key_value_store
                    .update_last_received_message_id(group_message.id)
                    .await;
            }
        }
        firefly::mod_ServerMessage::OneOfmessage::groupInvite(invite) => {
            log::info!("received group invite for group {}", invite.groupId);

            let token = callbacks
                .get_access_token()
                .await
                .context("token not found")?;

            if let Err(err) = join_group_internal(
                &invite,
                &token,
                address_id,
                device_id,
                firefly_base_url,
                firefly_mls_client,
                group_info_store,
                group_message_store,
                callbacks,
            )
            .await
            {
                log::error!("failed to join group via invite: {:?}", err);
            }
        }
        firefly::mod_ServerMessage::OneOfmessage::groupCommits(commits) => {
            for commit in commits.commits {
                let msg = firefly::GroupMessage {
                    id: commit.id,
                    groupId: commit.groupId,
                    message: commit.commit,
                    epoch: commit.epoch,
                };
                if let Err(err) = on_group_message(
                    &msg,
                    firefly_mls_client,
                    group_info_store,
                    group_message_store,
                    history_keys_store,
                    key_value_store,
                    callbacks,
                    true,
                )
                .await
                {
                    log::error!("failed to process group commit {}: {:?}", commit.id, err);
                }
                let _ = key_value_store
                    .update_last_received_message_id(commit.id)
                    .await;
            }
        }
        firefly::mod_ServerMessage::OneOfmessage::groupReAddRequests(requests) => {
            for request in requests.requests {
                log::info!(
                    "received re-add request for group {} user {} address {}",
                    request.group_id,
                    request.username,
                    request.address_id
                );
                let req_owned = firefly::GroupReAddRequest {
                    group_id: request.group_id,
                    username: request.username.to_string().into(),
                    address_id: request.address_id,
                };
                let mls = firefly_mls_client.clone();
                let gis = group_info_store.clone();
                let gms = group_message_store.clone();
                let pr = pending_requests.clone();
                let snd = sender.clone();
                let cb = callbacks.clone();
                let base_url = firefly_base_url.to_string();
                tokio::spawn(async move {
                    if let Err(err) = re_add_member_internal(
                        &req_owned,
                        0,
                        &mls,
                        &gis,
                        &gms,
                        &pr,
                        &snd,
                        &cb,
                        &base_url,
                        address_id,
                    )
                    .await
                    {
                        log::error!("failed to re-add member: {:?}", err);
                    }
                });
            }
        }
        firefly::mod_ServerMessage::OneOfmessage::groupJoinRequests(requests) => {
            for request in requests.requests {
                log::info!(
                    "received join request for group {} user {}",
                    request.group_id,
                    request.username
                );
                let group_id = request.group_id;
                let username = request.username.to_string();
                let mls = firefly_mls_client.clone();
                let gis = group_info_store.clone();
                let gms = group_message_store.clone();
                let pr = pending_requests.clone();
                let snd = sender.clone();
                let cb = callbacks.clone();
                tokio::spawn(async move {
                    if let Err(err) = add_member_internal(
                        group_id,
                        username,
                        0,
                        &mls,
                        &gis,
                        &gms,
                        &pr,
                        &snd,
                        &cb,
                    )
                    .await
                    {
                        log::error!("failed to process join request: {:?}", err);
                    }
                });
            }
        }
        firefly::mod_ServerMessage::OneOfmessage::callSignal(signal) => {
            log::info!(
                "received call signal type {:?} from {} for {}",
                signal.type_pb,
                signal.sender_username,
                signal.receiver_username
            );
            let ffi_signal = crate::callbacks::CallSignal {
                call_id: signal.call_id,
                sender_username: signal.sender_username.to_string(),
                receiver_username: signal.receiver_username.to_string(),
                signal_type: signal.type_pb as i32,
                sdp: signal.sdp.to_string(),
                candidate: signal.candidate.to_string(),
                sdp_m_line_index: signal.sdp_m_line_index,
                sdp_mid: signal.sdp_mid.to_string(),
                sender_device_id: signal.sender_device_id,
            };
            callbacks.on_call_signal(ffi_signal).await;
        }
        firefly::mod_ServerMessage::OneOfmessage::groupMeetingSignal(signal) => {
            log::info!(
                "received group meeting signal type {:?} from {} for group {}",
                signal.type_pb,
                signal.username,
                signal.group_id
            );
            let ffi_signal = crate::callbacks::GroupMeetingSignal {
                group_id: signal.group_id,
                channel_id: signal.channel_id,
                session_id: signal.session_id,
                signal_type: signal.type_pb as i32,
                username: signal.username.to_string(),
                cf_meeting_id: signal.cf_meeting_id.to_string(),
            };
            callbacks.on_group_meeting_signal(ffi_signal).await;
        }
        firefly::mod_ServerMessage::OneOfmessage::readUserMessagesUpto(read_upto) => {
            log::info!(
                "received readUserMessagesUpto: other: {}, upto: {}",
                read_upto.other,
                read_upto.uptoMessageId
            );
            let ffi_read = crate::callbacks::ReadUserMessagesUpto {
                other: read_upto.other.to_string(),
                upto_message_id: read_upto.uptoMessageId,
            };
            callbacks.on_read_user_messages_upto(ffi_read).await;
        }
        firefly::mod_ServerMessage::OneOfmessage::groupHistorySignal(signal) => {
            log::info!(
                "received group history signal: group_id={}, signal_type={:?}, request_id={}, chunk_id={}, username={}",
                signal.group_id,
                signal.type_pb,
                signal.request_id,
                signal.chunk_id,
                signal.username,
            );
            let s = crate::callbacks::GroupHistorySignal {
                group_id: signal.group_id,
                signal_type: signal.type_pb as i32,
                request_id: signal.request_id,
                chunk_id: signal.chunk_id,
                username: signal.username.to_string(),
            };
            callbacks.on_group_history_signal(s).await;
        }
        firefly::mod_ServerMessage::OneOfmessage::pong(_pong_bytes) => {}
        firefly::mod_ServerMessage::OneOfmessage::ping(_ping_bytes) => {}
        _ => return Err(anyhow::anyhow!("unhandled server message")),
    };
    Ok(())
}

pub struct FfiConversation {
    pub other: String,
    pub settings: u64,
}

pub struct FfiGroupInfo {
    pub id: u64,
    pub name: String,
    pub description: String,
    pub pending: bool,
    pub owner: String,
    pub has_local_state: bool,
}

#[derive(Clone)]
pub struct FfiFireflyWsClient {
    inner: Arc<FireflyWsClient>,
}

impl FfiFireflyWsClient {
    pub fn set_cdn_url(&self, url: String) { self.inner.set_cdn_url(url); }
    pub async fn set_direct_message_pin(&self, other:String, message_id:u64, pinned:bool)->anyhow::Result<()> {let name=self.inner.callbacks.name().to_string();CURRENT_CLIENT.scope(name,self.inner.set_direct_message_pin(other,message_id,pinned)).await}

    pub async fn send_backup_key_sync(&self, sync: firefly_protos::firefly::DirectBackupKeySync<'static>) -> anyhow::Result<()> {
        let name = self.inner.callbacks.name().to_string();
        CURRENT_CLIENT.scope(name, self.inner.send_backup_key_sync(sync)).await
    }
    pub async fn request_group_history(&self, group: u64, start: u64, end: u64) -> anyhow::Result<()> { self.inner.request_group_history(group, start, end).await.map(|_| ()) }
    pub async fn import_group_history_page(&self, group: u64, since: u64, until: u64) -> anyhow::Result<HistoryImportPage> { self.inner.import_group_history_page(group,since,until).await }
    pub async fn handle_group_history_signal(&self, signal: crate::callbacks::GroupHistorySignal) -> anyhow::Result<()> {
        self.inner.handle_group_history_signal(signal).await
    }

    pub async fn replenish_key_packages(&self, count: usize) -> anyhow::Result<()> {
        let name = self.inner.callbacks.name().to_string();
        CURRENT_CLIENT.scope(name, self.inner.replenish_key_packages(count)).await
    }

    pub async fn create(
        firefly_base_url: String,
        firefly_base_ws_url: String,
        retry_interval_in_ms: u64,
        callbacks: Box<dyn FireflyWsClientCallback>,
        key_stores_pathname: String,
        request_timeout_in_ms: u64,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            inner: Arc::new(
                FireflyWsClient::create(
                    firefly_base_url,
                    firefly_base_ws_url,
                    retry_interval_in_ms,
                    callbacks,
                    key_stores_pathname,
                    request_timeout_in_ms,
                )
                .await?,
            ),
        })
    }

    pub async fn initialize_with_retrying(&self) -> anyhow::Result<()> {
        self.inner.initialize_with_retrying().await?;
        for group in self.inner.group_info_store.get_all().await? {
            for signal_type in [5,6,8] {
                self.inner.callbacks.on_group_history_signal(crate::callbacks::GroupHistorySignal{group_id:group.id,signal_type,request_id:0,chunk_id:0,username:self.inner.callbacks.name().to_string()}).await;
            }
        }
        Ok(())
    }
    pub async fn check_setup(&self) -> anyhow::Result<()> {
        self.inner.check_setup().await
    }

    pub async fn dispose(&self) {
        self.inner.dispose().await;
    }

    pub async fn read_user_messages_upto(
        &self,
        other: String,
        upto_message_id: u64,
    ) -> anyhow::Result<()> {
        let name = self.inner.callbacks.name().to_string();
        CURRENT_CLIENT
            .scope(name, async {
                self.inner.read_user_messages_upto(other, upto_message_id).await
            })
            .await
    }

    pub async fn encrypt_and_send(
        &self,
        to: String,
        payload: Vec<u8>,
    ) -> anyhow::Result<UserMessage> {
        let name = self.inner.callbacks.name().to_string();
        CURRENT_CLIENT
            .scope(name, async {
                self.inner.encrypt_and_send(to, payload).await
            })
            .await
    }

    pub async fn encrypt_and_send_with_type(
        &self,
        to: String,
        payload: Vec<u8>,
        message_type: u32,
    ) -> anyhow::Result<UserMessage> {
        let name = self.inner.callbacks.name().to_string();
        CURRENT_CLIENT
            .scope(name, async {
                self.inner.encrypt_and_send_with_type(to, payload, message_type).await
            })
            .await
    }

    pub async fn encrypt_and_send_pinned(
        &self,
        to: String,
        payload: Vec<u8>,
    ) -> anyhow::Result<UserMessage> {
        self.encrypt_and_send_with_type(to, payload, firefly_protos::MESSAGE_TYPE_PINNED).await
    }

    pub async fn encrypt_and_send_group(
        &self,
        groupId: u64,
        payload: Vec<u8>,
    ) -> anyhow::Result<u64> {
        let name = self.inner.callbacks.name().to_string();
        CURRENT_CLIENT
            .scope(name, async {
                let message = deserialize_proto::<firefly::GroupMessageInner<'_>>(&payload)?;
                self.inner.upload_group_message(groupId, message, 0).await
            })
            .await
    }

    pub async fn encrypt_and_send_group_with_type(
        &self,
        groupId: u64,
        payload: Vec<u8>,
        message_type: u32,
    ) -> anyhow::Result<u64> {
        let name = self.inner.callbacks.name().to_string();
        CURRENT_CLIENT
            .scope(name, async {
                let mut message = deserialize_proto::<firefly::GroupMessageInner<'_>>(&payload)?;
                message.message_type |= message_type;
                if let firefly::mod_GroupMessageInner::OneOfmessage::messagePayload(ref mut p) = message.message {
                    p.message_type |= message_type;
                }
                self.inner.upload_group_message(groupId, message, 0).await
            })
            .await
    }

    pub async fn encrypt_and_send_group_pinned(
        &self,
        groupId: u64,
        payload: Vec<u8>,
    ) -> anyhow::Result<u64> {
        self.encrypt_and_send_group_with_type(groupId, payload, firefly_protos::MESSAGE_TYPE_PINNED).await
    }

    pub async fn set_group_message_pin(&self, group_id: u64, message_id: u64, pinned: bool) -> anyhow::Result<()> {
        let name=self.inner.callbacks.name().to_string();
        CURRENT_CLIENT.scope(name,self.inner.set_group_message_pin(group_id,message_id,pinned)).await
    }

    pub async fn re_encrypt_and_send_pinned_messages(&self, group_id: u64) -> anyhow::Result<()> {
        let name = self.inner.callbacks.name().to_string();
        CURRENT_CLIENT
            .scope(name, async {
                self.inner.re_encrypt_and_send_pinned_messages(group_id).await
            })
            .await
    }

    pub async fn upload_fcm_token(&self, token: Option<String>) -> anyhow::Result<()> {
        let name = self.inner.callbacks.name().to_string();
        CURRENT_CLIENT
            .scope(name, async { self.inner.upload_fcm_token(token).await })
            .await
    }

    pub fn get_connection_state(&self) -> ConnectionState {
        let guard = self.inner.state.read().unwrap();
        guard.clone()
    }

    pub fn is_initialized(&self) -> bool {
        self.inner.is_initialized()
    }

    pub async fn get_conversations(&self, token: String) -> anyhow::Result<Vec<FfiConversation>> {
        let name = self.inner.callbacks.name().to_string();
        CURRENT_CLIENT
            .scope(name, async { self.inner.get_conversations(&token).await })
            .await
    }

    pub async fn create_group(
        &self,
        name: String,
        description: String,
        settings: u32,
    ) -> anyhow::Result<crate::db::group_stores::GroupInfo> {
        let id = self.inner.callbacks.name().to_string();
        CURRENT_CLIENT
            .scope(id, async {
                self.inner.create_group(name, description, settings).await
            })
            .await
    }

    pub async fn history_enabled(&self, group_id: u64) -> anyhow::Result<bool> { self.inner.history_enabled(group_id).await }

    pub fn group_message_store(&self) -> GroupMessagesStore {
        self.inner.group_message_store()
    }

    pub fn messages_store(&self) -> MessagesStore {
        self.inner.messages_store()
    }

    pub async fn search_messages(
        &self,
        query: String,
        scope: SearchScope,
        limit: u32,
        offset: u32,
    ) -> anyhow::Result<Vec<SearchResultItem>> {
        self.inner.search_messages(&query, scope, limit, offset).await
    }

    pub async fn search_user_messages(
        &self,
        query: String,
        other: Option<String>,
        limit: u32,
        offset: u32,
    ) -> anyhow::Result<Vec<SearchResultItem>> {
        self.inner
            .search_user_messages(&query, other.as_deref(), limit, offset)
            .await
    }

    pub async fn search_group_messages(
        &self,
        query: String,
        group_id: Option<u64>,
        channel_id: Option<u32>,
        limit: u32,
        offset: u32,
    ) -> anyhow::Result<Vec<SearchResultItem>> {
        self.inner
            .search_group_messages(&query, group_id, channel_id, limit, offset)
            .await
    }

    pub fn favourite_messages_store(&self) -> FavouriteMessagesStore {
        self.inner.favourite_messages_store()
    }

    pub fn favorite_messages_store(&self) -> FavouriteMessagesStore {
        self.inner.favorite_messages_store()
    }

    pub async fn add_favourite(&self, favourite: FavouriteMessage) -> anyhow::Result<u64> {
        self.inner.add_favourite(favourite).await
    }

    pub async fn remove_user_favourite(
        &self,
        other: String,
        message_id: u64,
    ) -> anyhow::Result<bool> {
        self.inner.remove_user_favourite(&other, message_id).await
    }

    pub async fn remove_group_favourite(
        &self,
        group_id: u64,
        message_id: u64,
    ) -> anyhow::Result<bool> {
        self.inner.remove_group_favourite(group_id, message_id).await
    }

    pub async fn remove_favourite_by_id(&self, favourite_id: u64) -> anyhow::Result<bool> {
        self.inner.remove_favourite_by_id(favourite_id).await
    }

    pub async fn is_user_favourite(
        &self,
        other: String,
        message_id: u64,
    ) -> anyhow::Result<bool> {
        self.inner.is_user_favourite(&other, message_id).await
    }

    pub async fn is_group_favourite(
        &self,
        group_id: u64,
        message_id: u64,
    ) -> anyhow::Result<bool> {
        self.inner.is_group_favourite(group_id, message_id).await
    }

    pub async fn get_favourites(
        &self,
        limit: u32,
        offset: u32,
    ) -> anyhow::Result<Vec<FavouriteMessage>> {
        self.inner.get_favourites(limit, offset).await
    }

    pub async fn get_user_favourites(
        &self,
        other: Option<String>,
        limit: u32,
        offset: u32,
    ) -> anyhow::Result<Vec<FavouriteMessage>> {
        self.inner
            .get_user_favourites(other.as_deref(), limit, offset)
            .await
    }

    pub async fn get_group_favourites(
        &self,
        group_id: Option<u64>,
        channel_id: Option<u32>,
        limit: u32,
        offset: u32,
    ) -> anyhow::Result<Vec<FavouriteMessage>> {
        self.inner
            .get_group_favourites(group_id, channel_id, limit, offset)
            .await
    }

    pub fn group_info_store(&self) -> GroupInfoStore {
        self.inner.group_info_store.clone()
    }

    pub async fn get_group_extension(&self, groupId: u64) -> anyhow::Result<Vec<u8>> {
        let name = self.inner.callbacks.name().to_string();
        CURRENT_CLIENT
            .scope(name, async {
                self.inner.get_group_extension(groupId).await
            })
            .await
    }

    pub async fn export_group_meeting_key(&self, groupId: u64) -> anyhow::Result<Vec<u8>> {
        let name = self.inner.callbacks.name().to_string();
        CURRENT_CLIENT
            .scope(name, async {
                self.inner.export_group_meeting_key(groupId).await
            })
            .await
    }

    pub async fn process_group_message(
        &self,
        groupId: u64,
        message: Vec<u8>,
    ) -> anyhow::Result<crate::group::FireflyMlsReceivedMessage> {
        let name = self.inner.callbacks.name().to_string();
        CURRENT_CLIENT
            .scope(name, async {
                let client = self.inner.ensure_mls_client_initialized().await?;
                let group_info = self.inner.group_info_store.get(groupId).await?;

                let group = client.load_group(groupId, group_info.identifier).await?;

                group.process(message).await
            })
            .await
    }

    pub async fn load_all_groups(&self) -> anyhow::Result<()> {
        let name = self.inner.callbacks.name().to_string();
        CURRENT_CLIENT
            .scope(name, async {
                let client = self.inner.ensure_mls_client_initialized().await?;
                client.load_all_groups().await?;
                Ok(())
            })
            .await
    }

    pub async fn update_group_users(
        &self,
        groupId: u64,
        users: Vec<crate::group::UpdateUserProposalFfi>,
    ) -> anyhow::Result<u64> {
        self.inner.update_group_users(groupId, users).await
    }

    pub async fn update_group_channel(
        &self,
        groupId: u64,
        id: u32,
        delete: bool,
        name: String,
        channel_ty: u8,
        default_permissions: u32,
    ) -> anyhow::Result<u64> {
        self.inner
            .update_group_channel(groupId, id, delete, name, channel_ty, default_permissions)
            .await
    }

    pub async fn update_group_roles(
        &self,
        groupId: u64,
        roles: Vec<crate::group::UpdateRoleProposalFfi>,
    ) -> anyhow::Result<u64> {
        self.inner.update_group_roles(groupId, roles).await
    }

    pub async fn update_group_roles_in_channel(
        &self,
        groupId: u64,
        channel_id: u32,
        roles: Vec<crate::group::UpdateRoleProposalFfi>,
    ) -> anyhow::Result<u64> {
        self.inner
            .update_group_roles_in_channel(groupId, channel_id, roles)
            .await
    }

    pub async fn add_group_member(
        &self,
        groupId: u64,
        username: String,
        role_id: u32,
    ) -> anyhow::Result<()> {
        self.inner
            .add_group_member(groupId, username, role_id)
            .await
    }

    pub async fn request_re_add(&self, group_ids: Vec<u64>) -> anyhow::Result<()> {
        self.inner.request_re_add(group_ids).await
    }

    pub async fn kick_group_member(&self, groupId: u64, username: String) -> anyhow::Result<()> {
        self.inner.kick_group_member(groupId, username).await
    }

    pub async fn delete_group(&self, groupId: u64) -> anyhow::Result<()> {
        self.inner.delete_group(groupId).await
    }

    pub async fn create_join_link(
        &self,
        group_id: u64,
        expires_in_seconds: u64,
        max_uses: u32,
    ) -> anyhow::Result<String> {
        self.inner
            .create_join_link(group_id, expires_in_seconds, max_uses)
            .await
    }

    pub async fn join_via_link(&self, token: &str) -> anyhow::Result<()> {
        self.inner.join_via_link(token).await
    }

    pub fn generate_call_id(&self) -> u64 {
        self.inner.generate_call_id()
    }

    pub async fn initiate_call(
        &self,
        call_id: u64,
        receiver_username: String,
        sdp_offer: String,
    ) -> anyhow::Result<()> {
        let name = self.inner.callbacks.name().to_string();
        CURRENT_CLIENT
            .scope(name, async {
                self.inner
                    .initiate_call(call_id, receiver_username, sdp_offer)
                    .await
            })
            .await
    }

    pub async fn accept_call(
        &self,
        call_id: u64,
        caller_username: String,
        sdp_answer: String,
    ) -> anyhow::Result<()> {
        let name = self.inner.callbacks.name().to_string();
        CURRENT_CLIENT
            .scope(name, async {
                self.inner
                    .accept_call(call_id, caller_username, sdp_answer)
                    .await
            })
            .await
    }

    pub async fn reject_call(&self, call_id: u64, caller_username: String) -> anyhow::Result<()> {
        let name = self.inner.callbacks.name().to_string();
        CURRENT_CLIENT
            .scope(name, async {
                self.inner.reject_call(call_id, caller_username).await
            })
            .await
    }

    pub async fn cancel_call(&self, call_id: u64, receiver_username: String) -> anyhow::Result<()> {
        let name = self.inner.callbacks.name().to_string();
        CURRENT_CLIENT
            .scope(name, async {
                self.inner.cancel_call(call_id, receiver_username).await
            })
            .await
    }

    pub async fn hangup_call(&self, call_id: u64, other_username: String) -> anyhow::Result<()> {
        let name = self.inner.callbacks.name().to_string();
        CURRENT_CLIENT
            .scope(name, async {
                self.inner.hangup_call(call_id, other_username).await
            })
            .await
    }

    pub async fn send_ice_candidate(
        &self,
        call_id: u64,
        other_username: String,
        candidate: String,
        sdp_mid: String,
        sdp_m_line_index: i32,
    ) -> anyhow::Result<()> {
        let name = self.inner.callbacks.name().to_string();
        CURRENT_CLIENT
            .scope(name, async {
                self.inner
                    .send_ice_candidate(
                        call_id,
                        other_username,
                        candidate,
                        sdp_mid,
                        sdp_m_line_index,
                    )
                    .await
            })
            .await
    }

    pub async fn get_group_infos(&self) -> anyhow::Result<Vec<FfiGroupInfo>> {
        let name = self.inner.callbacks.name().to_string();
        CURRENT_CLIENT
            .scope(name, async {
                // 1. Fetch server groups
                let token = self.inner
                    .callbacks
                    .get_access_token()
                    .await
                    .context("token not found")?;

                let url = format!("{}/groups", self.inner.firefly_base_url);
                let response = HTTP_CLIENT.get(url).bearer_auth(&token).send().await?;

                if !response.status().is_success() {
                    return Err(anyhow::anyhow!(
                        "unexpected status [{}] {}",
                        response.status(),
                        response.text().await?
                    ));
                }

                let bytes = response.bytes().await?;
                let groups = deserialize_proto::<firefly::Groups<'_>>(&bytes)?;

                // 2. Fetch local group infos to check which ones we have keys for
                let local_groups = self.inner.group_info_store.get_all().await.unwrap_or_default();
                let local_ids: std::collections::HashSet<u64> = local_groups.iter().map(|g| g.id).collect();

                let claims = get_claims_from_token(&token).context("failed to parse claims")?;
                let username = claims.uname.to_string();

                let mut result = Vec::new();
                for group in groups.groups {
                    let has_local_keys = local_ids.contains(&group.id);
                    let is_pending = group.pending;
                    let is_owner = group.owner == username;

                    // We include the group if:
                    // - it is in local_groups (meaning we are a member and have keys)
                    // - OR it is pending (waiting for approval)
                    // - OR the current user is the owner (so they can see and delete it)
                    if has_local_keys || is_pending || is_owner {
                        result.push(FfiGroupInfo {
                            id: group.id,
                            name: group.name.into_owned(),
                            description: group.description.into_owned(),
                            pending: is_pending,
                            owner: group.owner.into_owned(),
                            has_local_state: has_local_keys,
                        });
                    }
                }

                Ok(result)
            })
            .await
    }

    pub async fn get_online_status(&self, usernames: Vec<String>) -> anyhow::Result<Vec<String>> {
        let name = self.inner.callbacks.name().to_string();
        CURRENT_CLIENT
            .scope(name, async {
                self.inner.get_online_status(usernames).await
            })
            .await
    }

    pub async fn sync_group_joins_and_readds(&self) -> anyhow::Result<()> {
        let name = self.inner.callbacks.name().to_string();
        CURRENT_CLIENT
            .scope(name, async {
                self.inner.sync_group_joins_and_readds().await
            })
            .await
    }

    pub fn address_id(&self) -> u64 {
        self.inner.address_id()
    }

    pub async fn get_device_id(&self) -> u32 {
        self.inner.get_device_id().await
    }

    pub async fn ping_device(
        &self,
        call_id: u64,
        target_username: String,
    ) -> anyhow::Result<()> {
        let name = self.inner.callbacks.name().to_string();
        CURRENT_CLIENT
            .scope(name, async {
                self.inner
                    .send_call_signal(
                        call_id,
                        target_username,
                        firefly_protos::firefly::CallSignalType::CALL_DISMISS,
                        "ping".to_string(),
                        "".to_string(),
                        0,
                        "".to_string(),
                    )
                    .await
            })
            .await
    }
}
