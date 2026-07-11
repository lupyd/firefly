use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, AtomicU64},
    },
    time::Duration,
};

use anyhow::Context;
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
        ffi_stores::FfiKeyStores,
        group_messages::GroupMessagesStore,
        group_stores::{GroupInfo, GroupInfoStore, SelfGroupKeyPackageStore},
        keyvalue::{KEY_FCM_TOKEN, KEY_LAST_RECEIVED_MESSAGE_ID, KeyValueStore},
        messages::UserMessage,
        setup_pool_from_path,
    },
    group::{FfiMlsClient, FfiMlsGroup},
    logger::CURRENT_CLIENT,
    utils::{
        HTTP_CLIENT, deserialize_proto, get_current_timestamp_microseconds_since_epoch,
        get_current_timestamp_millis_since_epoch, get_current_timestamp_seconds_since_epoch, rng,
        serialize_proto, write_url_comma_seperated,
    },
};

// Trait removed, imported from callbacks module

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
        address_id: u64,
        device_id: u8,
        firefly_base_url: String,
    ) -> Self {
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

#[derive(Default, Clone)]
pub enum ConnectionState {
    #[default]
    Disconnected,
    Initializing,
    Retrying,
    Connected,
    CheckingSetup,
}

pub struct FireflyWsClient {
    callbacks: Arc<dyn FireflyWsClientCallback>,
    retry_interval: Duration,
    firefly_base_url: String,
    firefly_base_ws_url: String,
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
    firefly_mls_client: tokio::sync::OnceCell<Arc<FfiMlsClient>>,
    group_info_store: GroupInfoStore,
    self_group_key_packages_store: SelfGroupKeyPackageStore,
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
        let self_group_key_packages_store = SelfGroupKeyPackageStore::new(pool.clone()).await?;

        let last_connection_established_timestamp = get_current_timestamp_millis_since_epoch();

        let group_info_store = GroupInfoStore::new(pool.clone()).await?;

        Ok(Self {
            pool,
            callbacks: callbacks.into(),
            retry_interval: Duration::from_millis(retry_interval_in_ms),
            firefly_base_url,
            firefly_base_ws_url,
            key_stores,
            last_connection_tried_timestamp: last_connection_established_timestamp.into(),

            pending_requests: Default::default(),
            request_timeout: Duration::from_millis(request_timeout_in_ms),
            connection: Default::default(),
            stop_reconnecting: AtomicBool::new(false),
            key_value_store: key_value_store,
            state: Default::default(),
            addressId: Default::default(),
            group_messages_store: groups_store,
            self_group_key_packages_store,
            fully_initialized: AtomicBool::new(false),
            firefly_mls_client: Default::default(),
            group_info_store,
            next_request_id: Default::default(),
            last_connection_error: std::sync::Mutex::new(None),
        })
    }
    pub async fn initialize_with_retrying(&self) -> anyhow::Result<()> {
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

        let url = format!(
            "{}?uid={}&device_id={}&last_synced_upto={}&token={}",
            self.firefly_base_ws_url, addressId, device_id, last_synced_upto, token
        );

        let show_connecting = {
            let last_err = self.last_connection_error.lock().unwrap();
            last_err.is_none()
        };
        if show_connecting {
            log::info!("connecting to {}", url);
        }

        let (stream, response) = match tokio_tungstenite::connect_async(&url).await {
            Ok(v) => v,
            Err(err) => {
                let err_str = format!("{:?}", err);
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
            url,
            response.headers()
        );

        let pending_requests = self.pending_requests.clone();
        let key_stores = self.key_stores.clone();
        let callbacks = self.callbacks.clone();

        {
            pending_requests.lock().unwrap().clear(); // cleanup for fresh connection
        }

        let (on_connection_closed_tx, on_connection_closed_rx) = oneshot::channel::<()>();

        let firefly_mls_client = self
            .firefly_mls_client
            .get()
            .context("firefly_mls_client is uninitialized")?;

        {
            let mut g = self.connection.write().await;
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
                self.addressId.load(std::sync::atomic::Ordering::Relaxed),
                self.key_stores
                    .store()
                    .identity_store
                    .get_full_identity_key_pair()
                    .await
                    .map(|i| i.device_id)
                    .unwrap_or(0) as u8,
                self.firefly_base_url.clone(),
            ));
        }

        if let Err(err) = self.sync_all_group_messages().await {
            log::error!("sync group messages failed: {:?}", err);
        }

        on_connection_closed_rx.await?;
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
            fromId: fromId,
            text: cipher.cipher_text.into(),
            type_pb: cipher.ty as u32,
            settings,
            fromUsername: Default::default(),
            fromDeviceId: Default::default(),

            hashValue: hashValue,
        };

        return Ok(message);
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

    pub async fn encrypt_and_send(
        &self,
        to: String,
        payload: Vec<u8>,
    ) -> anyhow::Result<UserMessage> {
        let token = self
            .callbacks
            .get_access_token()
            .await
            .context("token not found")?;

        let store = self.key_stores.store();

        let _settings =
            if let Some(settings) = store.conversation_store.get_conversation(&to).await? {
                settings
            } else {
                self.create_conversation(&to, 1, &token).await?
            };
        let address_store = &self.key_stores.store().address_store;

        let other_addresses = address_store.get(&to).await?;

        if other_addresses.is_empty() {
            self.get_and_process_all_pre_key_bundles_of_user(&to, &token)
                .await?;
        }

        let other_addresses = self.key_stores.store().address_store.get(&to).await?;
        if other_addresses.is_empty() {
            return Err(anyhow::anyhow!("no addresses found for user {}", to));
        }

        let claims = get_claims_from_token(&token)?;
        let self_username = claims.uname;

        let self_addresses = address_store.get(&self_username).await?;

        let mut message_entries = firefly::UploadUserMessage::default();

        let message_settings = 0;
        let self_message_settings = 1;

        for address in other_addresses.iter() {
            let message = self
                .create_encrypted_message(
                    ProtocolAddress::new(to.clone(), DeviceId::new(address.device_id)?),
                    address.address_id,
                    message_settings,
                    payload.clone(),
                )
                .await?;
            message_entries.messages.push(message);
        }
        let self_message_payload = serialize_proto(&firefly::UserMessageInner {
            message: firefly::mod_UserMessageInner::OneOfmessage::selfMessage(
                firefly::SelfUserMessage {
                    to: to.clone().into(),
                    inner: payload.clone().into(),
                },
            ),
            nonce: rng().next_u32(),
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
                let message = message.await?;
                message_entries.messages.push(message);
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
        } else {
            return Err(anyhow::anyhow!("unexpected or empty body returned"));
        }

        for id in addresses_to_not_send_to {
            let _ = address_store.delete_by_id(id.address_id).await;
        }

        if more_addresses_to_send_to.is_empty() {
            return Ok(UserMessage {
                id: get_current_timestamp_microseconds_since_epoch(),
                other: to.clone(),
                message: payload.clone(),
                sent_by_other: false,
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
                        payload.clone(),
                    )
                }
                .await?;

                upload_request.messages.push(message);
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

        return Ok(UserMessage {
            id: get_current_timestamp_microseconds_since_epoch(),
            other: to.clone(),
            message: payload.clone(),
            sent_by_other: false,
        });
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
                    &self.callbacks,
                    false,
                )
                .await
                {
                    log::error!("failed to process group message {:?}", err)
                }
            }
            if messages_len < LIMIT {
                break;
            }
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
        let response = HTTP_CLIENT.get(url).bearer_auth(&token).send().await?;

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
                    if !message
                        .and_then(|x| {
                            x.as_key_package().and_then(|x| {
                                Some(x.signing_identity() == &current_signing_identity)
                            })
                        })
                        .unwrap_or_default()
                    {
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

            let response = HTTP_CLIENT.delete(url).bearer_auth(&token).send().await?;
            if !response.status().is_success() {
                return Err(anyhow::anyhow!(
                    "unexpected status [{}] {}",
                    response.status(),
                    response.text().await?
                ));
            }

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
                .bearer_auth(&token)
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

            let response = HTTP_CLIENT.post(url).bearer_auth(&token).send().await?;
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
            addressId, self.firefly_base_url
        );

        write_url_comma_seperated(&mut url, groups.iter().map(|x| x.id))?;

        let response = HTTP_CLIENT.get(url).bearer_auth(&token).send().await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "unexpected status [{}] {}",
                response.status(),
                response.text().await?
            ));
        }

        let bytes = response.bytes().await?;
        let requests = deserialize_proto::<firefly::GroupReAddRequests<'_>>(&bytes)?;

        let firefly_mls_client = self
            .firefly_mls_client
            .get()
            .context("firefly_mls_client is not initialized")?;

        for request in requests.requests {
            match self
                .re_add_member(token, &firefly_mls_client, &request, addressId)
                .await
            {
                Ok(_) => {
                    log::info!("readded member: {:?}", request);
                }
                Err(err) => {
                    log::error!("failed to readd member {:?}", err);
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
        let group_info = self.group_info_store.get(groupId).await?;

        let group = firefly_mls_client
            .load_group(groupId, group_info.identifier.clone())
            .await?;

        let id = group
            .re_add_member(request.username.to_string(), request.address_id)
            .await?;

        self.group_messages_store
            .update_cursor(id, groupId, group.epoch().await as u32)
            .await?;

        let response = HTTP_CLIENT
            .post(format!(
                "{}/group/reAdd?groupId={}&other_address_id={}&address={}",
                self.firefly_base_url, groupId, request.address_id, addressId,
            ))
            .bearer_auth(token)
            .send()
            .await?;

        log::info!(
            "delete reAdd result: [{}] {}",
            response.status(),
            response.text().await?
        );

        Ok(())
    }

    async fn join_groups(&self, token: &str, addressId: u64, device_id: u8) -> anyhow::Result<()> {
        let url = format!(
            "{}/group/invites?address={}&device_id={}",
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

        {
            if !invites.invites.is_empty() {
                let mut url = format!(
                    "{}/group/invites?address={}&groupIds=",
                    addressId, self.firefly_base_url
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
    ) -> anyhow::Result<GroupInfo> {
        let client = self
            .firefly_mls_client
            .get()
            .context("firefly_mls_client is not initialized")?;

        let group = client
            .create_group(name.clone())
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

        return Ok(self.group_info_store.get(group.group_id()).await?);
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

        self.group_messages_store
            .add(
                uploaded_group_message.id,
                groupId,
                message.channelId,
                uploaded_group_message.epoch,
                &claims.uname,
                &payload,
            )
            .await?;

        Ok(uploaded_group_message.id)
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

        let last_message_seen = self
            .group_messages_store
            .get_last_message_of_group(groupId)
            .await
            .map(|last_message| last_message.id)
            .unwrap_or(0);
        let update = firefly::GroupMemberUpdate {
            group_id: groupId,
            last_epoch: group.epoch().await as u32,
            last_message_seen: last_message_seen,
        };
        let response = HTTP_CLIENT
            .post(url)
            .bearer_auth(&token)
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
                deviceId: identity.device_id as u32,
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

            self.key_stores
                .store()
                .identity_store
                .update_id_for_keypair(address.id as i64, &address.username)
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
        let response = HTTP_CLIENT.get(url).bearer_auth(&token).send().await?;

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
            let response = HTTP_CLIENT.delete(&url).bearer_auth(&token).send().await?;

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
                .bearer_auth(&token)
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
                last_message_seen: last_message_seen,
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
                let val = self.key_value_store.get(KEY_FCM_TOKEN).await?;
                val
            }
        };

        Ok(())
    }

    pub async fn get_conversations(&self, token: &str) -> anyhow::Result<Vec<FfiConversation>> {
        let url = format!("{}/user/conversations", self.firefly_base_url);

        let claims = get_claims_from_token(&token)?;

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

        let id = group.add_member(username, role_id).await?;

        self.group_messages_store
            .update_cursor(id, group_id, group.epoch().await as u32)
            .await?;

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
}

async fn on_group_message(
    msg: &firefly::GroupMessage<'_>,
    firefly_mls_client: &FfiMlsClient,
    group_info_store: &GroupInfoStore,
    group_message_store: &GroupMessagesStore,
    callbacks: &Arc<dyn FireflyWsClientCallback>,
    is_commit: bool,
) -> anyhow::Result<()> {
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

    let message = group
        .process(msg.message.to_vec())
        .await
        .map_err(|err| anyhow::anyhow!(err))?;

    let epoch = group.epoch().await as u32;
    match message {
        crate::group::FireflyMlsReceivedMessage::Message(encrypted_group_message) => {
            let channelId =
                deserialize_proto::<firefly::GroupMessageInner>(&encrypted_group_message.message)?
                    .channelId;

            group_message_store
                .add(
                    msg.id,
                    msg.groupId,
                    channelId,
                    epoch,
                    &encrypted_group_message.sender,
                    &encrypted_group_message.message,
                )
                .await?;
            let message = crate::db::group_messages::GroupMessage {
                id: msg.id,
                group_id: groupId,
                by: encrypted_group_message.sender,
                message: encrypted_group_message.message,
                channel_id: channelId,
                epoch,
            };

            log::info!(
                "processed group message: id: {}, by: {}, len: {}, message_epoch: {}, group_epoch: {}",
                message.id,
                message.by,
                message.message.len(),
                msg.epoch,
                epoch,
            );
            callbacks.on_group_message(message).await;
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

    let mut is_dummy = false;
    let mut other_username = from.into_owned();
    let mut final_message = decrypted;
    let mut sent_by_other = true;

    if let Ok(inner) = deserialize_proto::<firefly::UserMessageInner>(&final_message) {
        match inner.message {
            firefly::mod_UserMessageInner::OneOfmessage::None => {
                is_dummy = true;
            }
            firefly::mod_UserMessageInner::OneOfmessage::selfMessage(self_msg) => {
                if msg.settings == 1 {
                    other_username = self_msg.to.into_owned();
                    final_message = self_msg.inner.into_owned();
                    sent_by_other = false;

                    if let Ok(inner_inner) =
                        deserialize_proto::<firefly::UserMessageInner>(&final_message)
                    {
                        if let firefly::mod_UserMessageInner::OneOfmessage::None =
                            inner_inner.message
                        {
                            is_dummy = true;
                        }
                    }
                }
            }
            _ => {}
        }
    }

    if !is_dummy {
        callbacks
            .on_message(crate::db::messages::UserMessage {
                id: msg.id,
                other: other_username,
                message: final_message,
                sent_by_other,
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
                                if let Err(e) = sender.send(Bytes::from(payload)).await {
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
    group_id: u64,
    username: String,
    role_id: u32,
    firefly_mls_client: &FfiMlsClient,
    group_info_store: &GroupInfoStore,
    group_message_store: &GroupMessagesStore,
) -> anyhow::Result<()> {
    let group_info = group_info_store.get(group_id).await?;
    let group = firefly_mls_client
        .load_group(group_id, group_info.identifier)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    let id = group.add_member(username, role_id).await?;

    group_message_store
        .update_cursor(id, group_id, group.epoch().await as u32)
        .await?;

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

    callbacks.on_group_joined(group_id).await;

    Ok(())
}

async fn on_server_message(
    msg: firefly::ServerMessage<'_>,
    pending_requests: &PendingRequests,
    key_stores: &Arc<FfiKeyStores>,
    callbacks: &Arc<dyn FireflyWsClientCallback>,
    key_value_store: &KeyValueStore,
    firefly_mls_client: &FfiMlsClient,
    group_info_store: &GroupInfoStore,
    group_message_store: &GroupMessagesStore,
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
            on_group_message(
                &group_message,
                firefly_mls_client,
                group_info_store,
                group_message_store,
                callbacks,
                false,
            )
            .await?;
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
                on_group_message(
                    &group_message,
                    firefly_mls_client,
                    group_info_store,
                    group_message_store,
                    callbacks,
                    false,
                )
                .await?;
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
                on_group_message(
                    &msg,
                    firefly_mls_client,
                    group_info_store,
                    group_message_store,
                    callbacks,
                    true,
                )
                .await?;
            }
        }
        firefly::mod_ServerMessage::OneOfmessage::groupReAddRequests(requests) => {
            for request in requests.requests {
                log::info!(
                    "received re-add request for group {} user {}",
                    request.group_id,
                    request.username
                );
                if let Err(err) = re_add_member_internal(
                    request.group_id,
                    request.username.into(),
                    0,
                    firefly_mls_client,
                    group_info_store,
                    group_message_store,
                )
                .await
                {
                    log::error!("failed to re-add member: {:?}", err);
                }
            }
        }
        firefly::mod_ServerMessage::OneOfmessage::groupJoinRequests(requests) => {
            for request in requests.requests {
                log::info!(
                    "received join request for group {} user {}",
                    request.group_id,
                    request.username
                );
                if let Err(err) = re_add_member_internal(
                    request.group_id,
                    request.username.into(),
                    0,
                    firefly_mls_client,
                    group_info_store,
                    group_message_store,
                )
                .await
                {
                    log::error!("failed to process join request: {:?}", err);
                }
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

#[derive(Clone)]
pub struct FfiFireflyWsClient {
    inner: Arc<FireflyWsClient>,
}

impl FfiFireflyWsClient {
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
        self.inner.initialize_with_retrying().await
    }
    pub async fn check_setup(&self) -> anyhow::Result<()> {
        self.inner.check_setup().await
    }

    pub async fn dispose(&self) {
        self.inner.dispose().await;
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

    pub async fn upload_fcm_token(&self, token: Option<String>) -> anyhow::Result<()> {
        let name = self.inner.callbacks.name().to_string();
        CURRENT_CLIENT
            .scope(name, async { self.inner.upload_fcm_token(token).await })
            .await
    }

    pub fn get_connection_state(&self) -> ConnectionState {
        let guard = self.inner.state.read().unwrap();
        return guard.clone();
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
    ) -> anyhow::Result<crate::db::group_stores::GroupInfo> {
        let id = self.inner.callbacks.name().to_string();
        CURRENT_CLIENT
            .scope(id, async {
                self.inner.create_group(name, description).await
            })
            .await
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

    pub fn group_message_store(&self) -> GroupMessagesStore {
        self.inner.group_messages_store.clone()
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
}
