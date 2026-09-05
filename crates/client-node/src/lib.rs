use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
};
use std::collections::HashMap;

use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;
use web_sys::{BinaryType, MessageEvent, WebSocket};

use firefly_client::callbacks::{CallSignal, FireflyWsClientCallback, GroupMeetingSignal, ReadUserMessagesUpto};
use firefly_client::group::FfiMlsClient;
use firefly_client::libsignal_protocol::{DeviceId, ProtocolAddress};
use firefly_client::storage::{
    FireflyStorage, GenericGroupInfoStore, GenericGroupMessagesStore, GenericKeyStores,
    GenericKeyValueStore, GenericMessagesStore, GenericSelfGroupKeyPackageStore, GroupInfo, GroupMessage, MemoryStorage,
    UserMessage,
};
use firefly_client::utils::{deserialize_proto, serialize_proto, HTTP_CLIENT};
use firefly_protos::firefly::{self};

#[wasm_bindgen]
pub fn init_logger(_file_path: String) {
    std::panic::set_hook(Box::new(|info| {
        let msg = format!("Rust panic: {}", info);
        web_sys::console::error_1(&wasm_bindgen::JsValue::from_str(&msg));
    }));
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct JsUserMessage {
    pub id: f64,
    pub other: String,
    pub message: Vec<u8>,
    pub sent_by_other: bool,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct JsGroupMessage {
    pub id: f64,
    pub group_id: f64,
    pub by: String,
    pub message: Vec<u8>,
    pub channel_id: u32,
    pub epoch: u32,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct JsGroupInfo {
    pub id: f64,
    pub name: String,
    pub description: String,
    pub pending: bool,
    pub owner: String,
    pub has_local_state: bool,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct JsConversation {
    pub other: String,
    pub settings: f64,
}

struct WasmClientCallbacks {
    name: String,
    token: Arc<Mutex<Option<String>>>,
    on_message_fn: Option<js_sys::Function>,
    on_group_message_fn: Option<js_sys::Function>,
    on_group_joined_fn: Option<js_sys::Function>,
    on_call_signal_fn: Option<js_sys::Function>,
    on_group_meeting_signal_fn: Option<js_sys::Function>,
    on_read_user_messages_upto_fn: Option<js_sys::Function>,
    get_access_token_fn: Option<js_sys::Function>,
}

#[async_trait::async_trait]
impl FireflyWsClientCallback for WasmClientCallbacks {
    fn name(&self) -> &str {
        &self.name
    }

    async fn get_access_token(&self) -> Option<String> {
        if let Some(ref f) = self.get_access_token_fn {
            if let Ok(res) = f.call0(&JsValue::NULL) {
                if let Some(s) = res.as_string() {
                    *self.token.lock().unwrap() = Some(s.clone());
                    return Some(s);
                }
            }
        }
        self.token.lock().unwrap().clone()
    }

    async fn on_message(&self, message: UserMessage) {
        if let Some(ref f) = self.on_message_fn {
            let msg = JsUserMessage {
                id: message.id as f64,
                other: message.other,
                message: message.message,
                sent_by_other: message.sent_by_other,
            };
            if let Ok(json) = serde_json::to_string(&msg) {
                let _ = f.call2(&JsValue::NULL, &JsValue::NULL, &JsValue::from_str(&json));
            }
        }
    }

    async fn on_group_message(&self, group_message: GroupMessage) {
        if let Some(ref f) = self.on_group_message_fn {
            let msg = JsGroupMessage {
                id: group_message.id as f64,
                group_id: group_message.group_id as f64,
                by: group_message.by,
                message: group_message.message,
                channel_id: group_message.channel_id,
                epoch: group_message.epoch,
            };
            if let Ok(json) = serde_json::to_string(&msg) {
                let _ = f.call2(&JsValue::NULL, &JsValue::NULL, &JsValue::from_str(&json));
            }
        }
    }

    async fn on_group_joined(&self, group_id: u64) {
        if let Some(ref f) = self.on_group_joined_fn {
            let _ = f.call1(&JsValue::NULL, &JsValue::from_f64(group_id as f64));
        }
    }

    async fn on_call_signal(&self, signal: CallSignal) {
        if let Some(ref f) = self.on_call_signal_fn {
            let obj = js_sys::Object::new();
            let _ = js_sys::Reflect::set(&obj, &"callId".into(), &(signal.call_id as f64).into());
            let _ = js_sys::Reflect::set(&obj, &"senderUsername".into(), &signal.sender_username.into());
            let _ = js_sys::Reflect::set(&obj, &"receiverUsername".into(), &signal.receiver_username.into());
            let _ = js_sys::Reflect::set(&obj, &"signalType".into(), &signal.signal_type.into());
            let _ = js_sys::Reflect::set(&obj, &"sdp".into(), &signal.sdp.into());
            let _ = js_sys::Reflect::set(&obj, &"candidate".into(), &signal.candidate.into());
            let _ = js_sys::Reflect::set(&obj, &"sdpMLineIndex".into(), &signal.sdp_m_line_index.into());
            let _ = js_sys::Reflect::set(&obj, &"sdpMid".into(), &signal.sdp_mid.into());
            let _ = js_sys::Reflect::set(&obj, &"senderDeviceId".into(), &signal.sender_device_id.into());
            let _ = f.call1(&JsValue::NULL, &obj);
        }
    }

    async fn on_group_meeting_signal(&self, signal: GroupMeetingSignal) {
        if let Some(ref f) = self.on_group_meeting_signal_fn {
            let obj = js_sys::Object::new();
            let _ = js_sys::Reflect::set(&obj, &"groupId".into(), &(signal.group_id as f64).into());
            let _ = js_sys::Reflect::set(&obj, &"channelId".into(), &signal.channel_id.into());
            let _ = js_sys::Reflect::set(&obj, &"sessionId".into(), &(signal.session_id as f64).into());
            let _ = js_sys::Reflect::set(&obj, &"signalType".into(), &signal.signal_type.into());
            let _ = js_sys::Reflect::set(&obj, &"username".into(), &signal.username.into());
            let _ = js_sys::Reflect::set(&obj, &"cfMeetingId".into(), &signal.cf_meeting_id.into());
            let _ = f.call1(&JsValue::NULL, &obj);
        }
    }

    async fn on_read_user_messages_upto(&self, read: ReadUserMessagesUpto) {
        if let Some(ref f) = self.on_read_user_messages_upto_fn {
            let obj = js_sys::Object::new();
            let _ = js_sys::Reflect::set(&obj, &"other".into(), &read.other.into());
            let _ = js_sys::Reflect::set(&obj, &"uptoMessageId".into(), &(read.upto_message_id as f64).into());
            let _ = f.call1(&JsValue::NULL, &obj);
        }
    }
}

#[wasm_bindgen]
pub struct FireflyClientNode {
    storage: Arc<dyn FireflyStorage>,
    key_stores: Arc<tokio::sync::RwLock<GenericKeyStores>>,
    key_value_store: GenericKeyValueStore,
    group_messages_store: GenericGroupMessagesStore,
    user_messages_store: GenericMessagesStore,
    group_info_store: GenericGroupInfoStore,
    mls_client: Arc<tokio::sync::RwLock<Option<Arc<FfiMlsClient>>>>,
    callbacks: Arc<WasmClientCallbacks>,
    token: Arc<Mutex<Option<String>>>,
    firefly_base_url: String,
    firefly_base_ws_url: String,
    ws: Arc<tokio::sync::RwLock<Option<WebSocket>>>,
    connection_state: Arc<tokio::sync::RwLock<String>>,
    is_initialized: Arc<AtomicBool>,
    disposed: Arc<AtomicBool>,
    address_id: Arc<AtomicU64>,
    device_id: Arc<AtomicU32>,
    pending_requests: Arc<tokio::sync::RwLock<HashMap<u32, tokio::sync::oneshot::Sender<Vec<u8>>>>>,
    next_request_id: Arc<AtomicU32>,
}

async fn join_groups_helper(
    base_url: &str,
    token: &str,
    address_id: u64,
    device_id: u8,
    mls: &Arc<FfiMlsClient>,
    group_info_store: &GenericGroupInfoStore,
    group_messages_store: &GenericGroupMessagesStore,
    callbacks: &Arc<WasmClientCallbacks>,
) -> anyhow::Result<()> {
    let url = format!(
        "{}/group/invites?address={}&device_id={}",
        base_url, address_id, device_id
    );
    let response = HTTP_CLIENT.get(&url).bearer_auth(token).send().await?;
    if !response.status().is_success() {
        return Ok(());
    }
    let bytes = response.bytes().await?;
    let invites = deserialize_proto::<firefly::GroupInvites<'_>>(&bytes)?;

    for invite in invites.invites.iter() {
        let group_id = invite.groupId;
        match mls.join_group(group_id, invite.welcomeMessage.to_vec()).await {
            Ok(group) => {
                let _ = group.save().await;
                let grp_url = format!("{}/group?id={}", base_url, group_id);
                if let Ok(resp) = HTTP_CLIENT.get(&grp_url).bearer_auth(token).send().await {
                    if resp.status().is_success() {
                        if let Ok(b) = resp.bytes().await {
                            if let Ok(info) = deserialize_proto::<firefly::Group>(&b) {
                                let ident = group.group_identifier().await.unwrap_or_default();
                                let _ = group_info_store.set(group_id, info.name.to_string(), info.description.to_string(), ident).await;
                                let _ = group_messages_store.update_cursor(invite.commitId, group_id, group.epoch().await as u32).await;
                            }
                        }
                    }
                }
                let member_url = format!("{}/group/member?groupId={}&address={}", base_url, group_id, address_id);
                let update = firefly::GroupMemberUpdate {
                    group_id,
                    last_epoch: group.epoch().await as u32,
                    last_message_seen: invite.commitId,
                };
                if let Ok(body) = serialize_proto(&update) {
                    let _ = HTTP_CLIENT.post(member_url).bearer_auth(token).body(body.to_vec()).send().await;
                }
                callbacks.on_group_joined(group_id).await;
            }
            Err(e) => {
                web_sys::console::error_1(&JsValue::from_str(&format!("join_group error: {:?}", e)));
            }
        }
    }

    if !invites.invites.is_empty() {
        let mut del_url = format!("{}/group/invites?address={}&groupIds=", base_url, address_id);
        firefly_client::utils::write_url_comma_seperated(&mut del_url, invites.invites.iter().map(|x| x.groupId))?;
        let _ = HTTP_CLIENT.delete(&del_url).bearer_auth(token).send().await;
    }
    Ok(())
}

#[wasm_bindgen]
impl FireflyClientNode {
    #[wasm_bindgen]
    pub fn create(
        firefly_base_url: String,
        firefly_base_ws_url: String,
        _retry_interval_in_ms: f64,
        callbacks_obj: JsValue,
        _key_stores_pathname: String,
        _request_timeout_in_ms: f64,
    ) -> js_sys::Promise {
        future_to_promise(async move {
            let initial_token = js_sys::Reflect::get(&callbacks_obj, &"initialToken".into())
                .ok()
                .and_then(|v| v.as_string());
            let name = js_sys::Reflect::get(&callbacks_obj, &"name".into())
                .ok()
                .and_then(|v| v.as_string())
                .unwrap_or_default();

            let get_access_token_fn = js_sys::Reflect::get(&callbacks_obj, &"getAccessToken".into())
                .ok()
                .and_then(|v| v.dyn_into::<js_sys::Function>().ok());
            let on_message_fn = js_sys::Reflect::get(&callbacks_obj, &"onMessage".into())
                .ok()
                .and_then(|v| v.dyn_into::<js_sys::Function>().ok());
            let on_group_message_fn = js_sys::Reflect::get(&callbacks_obj, &"onGroupMessage".into())
                .ok()
                .and_then(|v| v.dyn_into::<js_sys::Function>().ok());
            let on_group_joined_fn = js_sys::Reflect::get(&callbacks_obj, &"onGroupJoined".into())
                .ok()
                .and_then(|v| v.dyn_into::<js_sys::Function>().ok());
            let on_call_signal_fn = js_sys::Reflect::get(&callbacks_obj, &"onCallSignal".into())
                .ok()
                .and_then(|v| v.dyn_into::<js_sys::Function>().ok());
            let on_group_meeting_signal_fn = js_sys::Reflect::get(&callbacks_obj, &"onGroupMeetingSignal".into())
                .ok()
                .and_then(|v| v.dyn_into::<js_sys::Function>().ok());
            let on_read_user_messages_upto_fn = js_sys::Reflect::get(&callbacks_obj, &"onReadUserMessagesUpto".into())
                .ok()
                .and_then(|v| v.dyn_into::<js_sys::Function>().ok());

            let token = Arc::new(Mutex::new(initial_token));
            let callbacks = Arc::new(WasmClientCallbacks {
                name,
                token: token.clone(),
                on_message_fn,
                on_group_message_fn,
                on_group_joined_fn,
                on_call_signal_fn,
                on_group_meeting_signal_fn,
                on_read_user_messages_upto_fn,
                get_access_token_fn,
            });

            let storage: Arc<dyn FireflyStorage> = Arc::new(MemoryStorage::new());
            let key_stores = Arc::new(tokio::sync::RwLock::new(
                GenericKeyStores::new(storage.clone())
                    .await
                    .map_err(|e| JsValue::from_str(&e.to_string()))?,
            ));
            let key_value_store = GenericKeyValueStore::new(storage.clone());
            let group_messages_store = GenericGroupMessagesStore::new(storage.clone());
            let user_messages_store = GenericMessagesStore::new(storage.clone());
            let group_info_store = GenericGroupInfoStore::new(storage.clone());

            let client = FireflyClientNode {
                storage,
                key_stores,
                key_value_store,
                group_messages_store,
                user_messages_store,
                group_info_store,
                mls_client: Arc::new(tokio::sync::RwLock::new(None)),
                callbacks,
                token,
                firefly_base_url,
                firefly_base_ws_url,
                ws: Arc::new(tokio::sync::RwLock::new(None)),
                connection_state: Arc::new(tokio::sync::RwLock::new("Disconnected".to_string())),
                is_initialized: Arc::new(AtomicBool::new(false)),
                disposed: Arc::new(AtomicBool::new(false)),
                address_id: Arc::new(AtomicU64::new(0)),
                device_id: Arc::new(AtomicU32::new(1)),
                pending_requests: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
                next_request_id: Arc::new(AtomicU32::new(1)),
            };

            Ok(JsValue::from(client))
        })
    }

    #[wasm_bindgen]
    pub fn set_access_token(&self, token: String) {
        *self.token.lock().unwrap() = Some(token);
    }

    #[wasm_bindgen]
    pub fn check_setup(&self) -> js_sys::Promise {
        let callbacks = self.callbacks.clone();
        let key_stores = self.key_stores.clone();
        let base_url = self.firefly_base_url.clone();
        let storage = self.storage.clone();
        let mls_client_holder = self.mls_client.clone();
        let address_id_atomic = self.address_id.clone();
        let device_id_atomic = self.device_id.clone();
        let group_info_store = self.group_info_store.clone();
        let group_messages_store = self.group_messages_store.clone();

        future_to_promise(async move {
            let token = callbacks
                .get_access_token()
                .await
                .ok_or_else(|| JsValue::from_str("Missing access token for check_setup"))?;

            // Register or fetch device address via /user/device
            let username = callbacks.name().to_string();
            let mut ks = key_stores.write().await;
            let identity = ks
                .identity_store
                .get_full_identity_key_pair()
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;

            let address = firefly::Address {
                id: identity.id as u64,
                username: username.clone().into(),
                deviceId: if identity.id == 0 { 0 } else { identity.device_id as u32 },
                fcmToken: "".into(),
            };

            let device_url = format!("{}/user/device", base_url);
            let proto_bytes = serialize_proto(&address)
                .map_err(|e| JsValue::from_str(&e.to_string()))?;

            let response = HTTP_CLIENT
                .post(&device_url)
                .bearer_auth(&token)
                .body(proto_bytes.to_vec())
                .send()
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;

            if response.status().is_success() {
                if let Ok(bytes) = response.bytes().await {
                    if let Ok(resp_address) = deserialize_proto::<firefly::Address>(&bytes) {
                        address_id_atomic.store(resp_address.id, Ordering::Relaxed);
                        device_id_atomic.store(resp_address.deviceId, Ordering::Relaxed);
                        let _ = ks
                            .identity_store
                            .update_registration_for_keypair(
                                resp_address.id as i64,
                                &resp_address.username,
                                resp_address.deviceId as u8,
                            )
                            .await;
                    }
                }
            }

            let address_id = address_id_atomic.load(Ordering::Relaxed);
            let device_id = device_id_atomic.load(Ordering::Relaxed) as u8;

            // Generate PreKeyBundle and upload if needed
            if let Ok(bundle) = ks.generate_prekey_bundle().await {
                let upload_url = format!("{}/user/preKeyBundles", base_url);
                let proto_bundle: firefly::PreKeyBundle<'static> = bundle.into();
                let entries = firefly::PreKeyBundleEntries {
                    entries: vec![firefly::PreKeyBundleEntry {
                        id: proto_bundle.preKeyId,
                        address: address_id,
                        bundle: Some(proto_bundle),
                        username: username.into(),
                        device_id: device_id as u32,
                    }],
                };
                if let Ok(proto_bytes) = serialize_proto(&entries) {
                    let _ = HTTP_CLIENT
                        .post(&upload_url)
                        .bearer_auth(&token)
                        .body(proto_bytes.to_vec())
                        .send()
                        .await;
                }
            }

            // Initialize MLS client
            let mls = FfiMlsClient::initialize_with_storage(
                device_id,
                address_id,
                callbacks.clone(),
                storage.clone(),
                base_url.clone(),
            )
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

            let mls = Arc::new(mls);

            // Generate and upload MLS key packages
            let self_kp_store = GenericSelfGroupKeyPackageStore::new(storage.clone());
            let mut key_packages = firefly::GroupKeyPackages::default();
            for _ in 0..16 {
                let id = (js_sys::Math::random() * 32000.0) as i32;
                if let Ok(key_package) = mls.generate_key_package().await {
                    let _ = self_kp_store.set(id, &key_package).await;
                    key_packages.packages.push(firefly::GroupKeyPackage {
                        id,
                        package: key_package.into(),
                        address: address_id,
                        username: Default::default(),
                    });
                }
            }
            if !key_packages.packages.is_empty() {
                if let Ok(body) = serialize_proto(&key_packages) {
                    let kp_url = format!(
                        "{}/group/keyPackages?address={}&device_id={}",
                        base_url, address_id, device_id
                    );
                    let _ = HTTP_CLIENT
                        .post(&kp_url)
                        .bearer_auth(&token)
                        .body(body.to_vec())
                        .send()
                        .await;
                }
            }

            // Sync pending group invites if any
            let _ = join_groups_helper(
                &base_url,
                &token,
                address_id,
                device_id,
                &mls,
                &group_info_store,
                &group_messages_store,
                &callbacks,
            ).await;

            *mls_client_holder.write().await = Some(mls);

            Ok(JsValue::NULL)
        })
    }

    #[wasm_bindgen]
    pub fn initialize_with_retrying(&self) -> js_sys::Promise {
        let callbacks = self.callbacks.clone();
        let base_ws_url = self.firefly_base_ws_url.clone();
        let base_url = self.firefly_base_url.clone();
        let address_id_atomic = self.address_id.clone();
        let device_id_atomic = self.device_id.clone();
        let ws_holder = self.ws.clone();
        let state_holder = self.connection_state.clone();
        let is_initialized_atomic = self.is_initialized.clone();
        let _disposed_atomic = self.disposed.clone();
        let key_stores = self.key_stores.clone();
        let user_messages_store = self.user_messages_store.clone();
        let group_messages_store = self.group_messages_store.clone();
        let group_info_store = self.group_info_store.clone();
        let pending_requests = self.pending_requests.clone();
        let mls_holder = self.mls_client.clone();

        let base_url_msg = base_url.clone();
        let token_holder = self.token.clone();

        future_to_promise(async move {
            *state_holder.write().await = "Initializing".to_string();

            let token = callbacks
                .get_access_token()
                .await
                .unwrap_or_default();
            let address_id = address_id_atomic.load(Ordering::Relaxed);
            let device_id = device_id_atomic.load(Ordering::Relaxed);

            let ws_url = format!(
                "{}?uid={}&device_id={}&last_synced_upto=0&token={}",
                base_ws_url, address_id, device_id, token
            );

            let ws = WebSocket::new(&ws_url)
                .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;
            ws.set_binary_type(BinaryType::Arraybuffer);

            // Wire onmessage
            let key_stores_clone = key_stores.clone();
            let user_messages_store_clone = user_messages_store.clone();
            let group_messages_store_clone = group_messages_store.clone();
            let group_info_store_clone = group_info_store.clone();
            let callbacks_clone = callbacks.clone();
            let pending_requests_clone = pending_requests.clone();
            let mls_holder_clone = mls_holder.clone();
            let b_url = base_url_msg.clone();
            let tok = token_holder.clone();
            let addr_id = address_id;

            let onmessage_callback = Closure::wrap(Box::new(move |e: MessageEvent| {
                if let Ok(ab) = e.data().dyn_into::<js_sys::ArrayBuffer>() {
                    let u8_array = js_sys::Uint8Array::new(&ab);
                    let bytes = u8_array.to_vec();

                    let ks = key_stores_clone.clone();
                    let ums = user_messages_store_clone.clone();
                    let gms = group_messages_store_clone.clone();
                    let gis = group_info_store_clone.clone();
                    let cb = callbacks_clone.clone();
                    let pr = pending_requests_clone.clone();
                    let mls = mls_holder_clone.clone();
                    let b_url = b_url.clone();
                    let tok = tok.clone();

                    wasm_bindgen_futures::spawn_local(async move {
                        if let Ok(server_msg) = deserialize_proto::<firefly::ServerMessage<'_>>(&bytes) {
                            match server_msg.message {
                                firefly::mod_ServerMessage::OneOfmessage::userMessage(um) => {
                                    let mut ks_guard = ks.write().await;
                                    let other_addr = ProtocolAddress::new(
                                        um.fromUsername.to_string(),
                                        DeviceId::new(um.fromDeviceId as u8).unwrap(),
                                    );
                                    if let Ok(decrypted) = ks_guard.decrypt(other_addr, um.text.to_vec(), um.type_pb as u8).await {
                                        let _ = ums.add(um.id, &um.fromUsername, &decrypted, true).await;
                                        cb.on_message(UserMessage {
                                            id: um.id,
                                            other: um.fromUsername.to_string(),
                                            message: decrypted,
                                            sent_by_other: true,
                                        }).await;
                                    }
                                }
                                firefly::mod_ServerMessage::OneOfmessage::groupMessage(gm) => {
                                    if let Some(mls_guard) = mls.read().await.as_ref() {
                                        let grp_ident = gis.get(gm.groupId).await.map(|g| g.identifier).unwrap_or_default();
                                        if let Ok(group) = mls_guard.load_group(gm.groupId, grp_ident).await {
                                            if let Ok(firefly_client::group::FireflyMlsReceivedMessage::Message(decrypted)) = group.process(gm.message.to_vec()).await {
                                                let _ = group.save().await;
                                                let channel_id = deserialize_proto::<firefly::GroupMessageInner>(&decrypted.message)
                                                    .map(|inner| inner.channelId)
                                                    .unwrap_or(0);
                                                let _ = gms.add(gm.id, gm.groupId, channel_id, gm.epoch, &decrypted.sender, &decrypted.message).await;
                                                cb.on_group_message(GroupMessage {
                                                    id: gm.id,
                                                    group_id: gm.groupId,
                                                    by: decrypted.sender,
                                                    message: decrypted.message,
                                                    channel_id,
                                                    epoch: gm.epoch,
                                                }).await;
                                            }
                                        }
                                    }
                                }
                                firefly::mod_ServerMessage::OneOfmessage::groupMessages(msgs) => {
                                    if let Some(mls_guard) = mls.read().await.as_ref() {
                                        for gm in msgs.messages {
                                            let grp_ident = gis.get(gm.groupId).await.map(|g| g.identifier).unwrap_or_default();
                                            if let Ok(group) = mls_guard.load_group(gm.groupId, grp_ident).await {
                                                if let Ok(firefly_client::group::FireflyMlsReceivedMessage::Message(decrypted)) = group.process(gm.message.to_vec()).await {
                                                    let _ = group.save().await;
                                                    let channel_id = deserialize_proto::<firefly::GroupMessageInner>(&decrypted.message)
                                                        .map(|inner| inner.channelId)
                                                        .unwrap_or(0);
                                                    let _ = gms.add(gm.id, gm.groupId, channel_id, gm.epoch, &decrypted.sender, &decrypted.message).await;
                                                    cb.on_group_message(GroupMessage {
                                                        id: gm.id,
                                                        group_id: gm.groupId,
                                                        by: decrypted.sender,
                                                        message: decrypted.message,
                                                        channel_id,
                                                        epoch: gm.epoch,
                                                    }).await;
                                                }
                                            }
                                        }
                                    }
                                }
                                firefly::mod_ServerMessage::OneOfmessage::groupInvite(invite) => {
                                    if let Some(mls_guard) = mls.read().await.as_ref() {
                                        let group_id = invite.groupId;
                                        if let Ok(group) = mls_guard.join_group(group_id, invite.welcomeMessage.to_vec()).await {
                                            let _ = group.save().await;
                                            let cur_token = tok.lock().unwrap().clone().unwrap_or_default();
                                            let grp_url = format!("{}/group?id={}", b_url, group_id);
                                            if let Ok(resp) = HTTP_CLIENT.get(&grp_url).bearer_auth(&cur_token).send().await {
                                                if resp.status().is_success() {
                                                    if let Ok(b) = resp.bytes().await {
                                                        if let Ok(info) = deserialize_proto::<firefly::Group>(&b) {
                                                            let ident = group.group_identifier().await.unwrap_or_default();
                                                            let _ = gis.set(group_id, info.name.to_string(), info.description.to_string(), ident).await;
                                                            let _ = gms.update_cursor(invite.commitId, group_id, group.epoch().await as u32).await;
                                                        }
                                                    }
                                                }
                                            }
                                            let member_url = format!("{}/group/member?groupId={}&address={}", b_url, group_id, addr_id);
                                            let update = firefly::GroupMemberUpdate {
                                                group_id,
                                                last_epoch: group.epoch().await as u32,
                                                last_message_seen: invite.commitId,
                                            };
                                            if let Ok(body) = serialize_proto(&update) {
                                                let _ = HTTP_CLIENT.post(member_url).bearer_auth(&cur_token).body(body.to_vec()).send().await;
                                            }
                                            cb.on_group_joined(group_id).await;
                                        }
                                    }
                                }
                                firefly::mod_ServerMessage::OneOfmessage::groupCommits(commits) => {
                                    if let Some(mls_guard) = mls.read().await.as_ref() {
                                        for commit in commits.commits {
                                            let grp_ident = gis.get(commit.groupId).await.map(|g| g.identifier).unwrap_or_default();
                                            if let Ok(group) = mls_guard.load_group(commit.groupId, grp_ident).await {
                                                let _ = group.process(commit.commit.to_vec()).await;
                                                let _ = group.save().await;
                                            }
                                        }
                                    }
                                }
                                firefly::mod_ServerMessage::OneOfmessage::groupJoinRequests(requests) => {
                                    if let Some(mls_guard) = mls.read().await.as_ref() {
                                        for request in requests.requests {
                                            let grp_ident = gis.get(request.group_id).await.map(|g| g.identifier).unwrap_or_default();
                                            if let Ok(group) = mls_guard.load_group(request.group_id, grp_ident).await {
                                                if let Ok(id) = group.add_member(request.username.to_string(), 0).await {
                                                    let _ = gms.update_cursor(id, request.group_id, group.epoch().await as u32).await;
                                                }
                                            }
                                        }
                                    }
                                }
                                firefly::mod_ServerMessage::OneOfmessage::groupReAddRequests(requests) => {
                                    if let Some(mls_guard) = mls.read().await.as_ref() {
                                        for request in requests.requests {
                                            let grp_ident = gis.get(request.group_id).await.map(|g| g.identifier).unwrap_or_default();
                                            if let Ok(group) = mls_guard.load_group(request.group_id, grp_ident).await {
                                                if let Ok(id) = group.add_member(request.username.to_string(), 0).await {
                                                    let _ = gms.update_cursor(id, request.group_id, group.epoch().await as u32).await;
                                                }
                                            }
                                        }
                                    }
                                }
                                firefly::mod_ServerMessage::OneOfmessage::response(resp) => {
                                    let mut pr_guard = pr.write().await;
                                    if let Some(sender) = pr_guard.remove(&resp.id) {
                                        if let Ok(resp_bytes) = serialize_proto(&resp) {
                                            let _ = sender.send(resp_bytes.to_vec());
                                        }
                                    }
                                }
                                firefly::mod_ServerMessage::OneOfmessage::callSignal(sig) => {
                                    cb.on_call_signal(CallSignal {
                                        call_id: sig.call_id,
                                        sender_username: sig.sender_username.to_string(),
                                        receiver_username: sig.receiver_username.to_string(),
                                        signal_type: sig.type_pb as i32,
                                        sdp: sig.sdp.to_string(),
                                        candidate: sig.candidate.to_string(),
                                        sdp_m_line_index: sig.sdp_m_line_index,
                                        sdp_mid: sig.sdp_mid.to_string(),
                                        sender_device_id: sig.sender_device_id,
                                    }).await;
                                }
                                firefly::mod_ServerMessage::OneOfmessage::groupMeetingSignal(sig) => {
                                    cb.on_group_meeting_signal(GroupMeetingSignal {
                                        group_id: sig.group_id,
                                        channel_id: sig.channel_id,
                                        session_id: sig.session_id,
                                        signal_type: sig.type_pb as i32,
                                        username: sig.username.to_string(),
                                        cf_meeting_id: sig.cf_meeting_id.to_string(),
                                    }).await;
                                }
                                _ => {}
                            }
                        }
                    });
                }
            }) as Box<dyn FnMut(MessageEvent)>);

            ws.set_onmessage(Some(onmessage_callback.as_ref().unchecked_ref()));
            onmessage_callback.forget();

            let state_clone = state_holder.clone();
            let is_init_clone = is_initialized_atomic.clone();
            let onopen_callback = Closure::wrap(Box::new(move || {
                let st = state_clone.clone();
                let init = is_init_clone.clone();
                wasm_bindgen_futures::spawn_local(async move {
                    *st.write().await = "Connected".to_string();
                    init.store(true, Ordering::Relaxed);
                });
            }) as Box<dyn FnMut()>);
            ws.set_onopen(Some(onopen_callback.as_ref().unchecked_ref()));
            onopen_callback.forget();

            let state_clone2 = state_holder.clone();
            let is_init_clone2 = is_initialized_atomic.clone();
            let onclose_callback = Closure::wrap(Box::new(move || {
                let st = state_clone2.clone();
                let init = is_init_clone2.clone();
                wasm_bindgen_futures::spawn_local(async move {
                    *st.write().await = "Disconnected".to_string();
                    init.store(false, Ordering::Relaxed);
                });
            }) as Box<dyn FnMut()>);
            ws.set_onclose(Some(onclose_callback.as_ref().unchecked_ref()));
            onclose_callback.forget();

            *ws_holder.write().await = Some(ws);

            // Sync groups from server
            let groups_url = format!("{}/groups", base_url);
            if let Ok(resp) = HTTP_CLIENT.get(&groups_url).bearer_auth(&token).send().await {
                if resp.status().is_success() {
                    if let Ok(bytes) = resp.bytes().await {
                        if let Ok(groups) = deserialize_proto::<firefly::Groups<'_>>(&bytes) {
                            for g in groups.groups {
                                let _ = group_info_store.set(g.id, g.name.to_string(), g.description.to_string(), Vec::new()).await;
                            }
                        }
                    }
                }
            }

            Ok(JsValue::NULL)
        })
    }

    #[wasm_bindgen]
    pub fn is_initialized(&self) -> bool {
        self.is_initialized.load(Ordering::Relaxed)
    }

    #[wasm_bindgen]
    pub fn get_connection_state(&self) -> String {
        let state = self.connection_state.clone();
        if let Ok(guard) = state.try_read() {
            guard.clone()
        } else {
            "Disconnected".to_string()
        }
    }

    #[wasm_bindgen]
    pub fn dispose(&self) -> js_sys::Promise {
        let ws_holder = self.ws.clone();
        let disposed = self.disposed.clone();
        let is_init = self.is_initialized.clone();

        future_to_promise(async move {
            disposed.store(true, Ordering::Relaxed);
            is_init.store(false, Ordering::Relaxed);
            if let Some(ws) = ws_holder.write().await.take() {
                let _ = ws.close();
            }
            Ok(JsValue::NULL)
        })
    }

    #[wasm_bindgen]
    pub fn encrypt_and_send(&self, to: String, payload: Vec<u8>) -> js_sys::Promise {
        let key_stores = self.key_stores.clone();
        let user_messages_store = self.user_messages_store.clone();
        let ws_holder = self.ws.clone();
        let callbacks = self.callbacks.clone();
        let address_id_atomic = self.address_id.clone();
        let device_id_atomic = self.device_id.clone();
        let base_url = self.firefly_base_url.clone();
        let next_request_id = self.next_request_id.clone();

        future_to_promise(async move {
            let address_id = address_id_atomic.load(Ordering::Relaxed);
            let device_id = device_id_atomic.load(Ordering::Relaxed) as u8;
            let mut ks = key_stores.write().await;

            let mut other_addresses = ks.address_store.get(&to).await.unwrap_or_default();
            if other_addresses.is_empty() {
                let token = callbacks.get_access_token().await.unwrap_or_default();
                let url = format!("{}/user/preKeyBundles?other={}", base_url, to);
                if let Ok(resp) = HTTP_CLIENT.get(&url).bearer_auth(&token).send().await {
                    if resp.status().is_success() {
                        if let Ok(bytes) = resp.bytes().await {
                            if let Ok(entries) = deserialize_proto::<firefly::PreKeyBundleEntries>(&bytes) {
                                for entry in entries.entries {
                                    if let Some(bundle) = entry.bundle {
                                        let _ = ks.process_pre_key_bundle(entry.username.to_string(), bundle.into()).await;
                                        let _ = ks.address_store.add(entry.address, &entry.username, entry.device_id as u8).await;
                                    }
                                }
                            }
                        }
                    }
                }
                other_addresses = ks.address_store.get(&to).await.unwrap_or_default();
            }

            let target_device_id = other_addresses.first().map(|a| a.device_id).unwrap_or(1);
            let target_address_id = other_addresses.first().map(|a| a.address_id).unwrap_or(0);

            let other_addr = ProtocolAddress::new(
                to.clone(),
                DeviceId::new(target_device_id).unwrap_or_else(|_| DeviceId::new(1).unwrap()),
            );
            let encrypted = ks
                .encrypt(other_addr, payload.clone())
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;

            let msg_id = firefly_client::utils::get_current_timestamp_millis_since_epoch();

            let user_msg = firefly::UserMessage {
                id: msg_id,
                toId: target_address_id,
                fromId: address_id,
                text: encrypted.cipher_text.clone().into(),
                type_pb: encrypted.ty as u32,
                settings: 0,
                hashValue: 0,
                fromUsername: callbacks.name().into(),
                fromDeviceId: device_id as u32,
            };

            let req_id = next_request_id.fetch_add(1, Ordering::Relaxed);
            let client_msg = firefly::ClientMessage {
                message: firefly::mod_ClientMessage::OneOfmessage::request(firefly::Request {
                    id: req_id,
                    payload: firefly::mod_Request::OneOfpayload::uploadUserMessage(
                        firefly::UploadUserMessage {
                            messages: vec![user_msg],
                        },
                    ),
                }),
            };

            if let Ok(proto_bytes) = serialize_proto(&client_msg) {
                if let Some(ws) = ws_holder.read().await.as_ref() {
                    let _ = ws.send_with_u8_array(&proto_bytes);
                }
            }

            let _ = user_messages_store.add(msg_id, &to, &payload, false).await;

            let res = JsUserMessage {
                id: msg_id as f64,
                other: to,
                message: payload,
                sent_by_other: false,
            };

            serde_wasm_bindgen::to_value(&res).map_err(|e| JsValue::from_str(&e.to_string()))
        })
    }

    #[wasm_bindgen]
    pub fn encrypt_and_send_group(&self, group_id: f64, payload: Vec<u8>) -> js_sys::Promise {
        let mls_holder = self.mls_client.clone();
        let group_messages_store = self.group_messages_store.clone();
        let group_info_store = self.group_info_store.clone();
        let ws_holder = self.ws.clone();
        let callbacks = self.callbacks.clone();
        let pending_requests = self.pending_requests.clone();
        let next_request_id = self.next_request_id.clone();

        future_to_promise(async move {
            let gid = group_id as u64;
            let mls = mls_holder.read().await;
            let mls_client = mls.as_ref().ok_or_else(|| JsValue::from_str("MLS client uninitialized"))?;

            let grp_ident = group_info_store.get(gid).await.map(|g| g.identifier).unwrap_or_default();
            let group = mls_client
                .load_group(gid, grp_ident)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;

            let encrypted = group
                .encrypt(payload.clone())
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;

            let _ = group.save().await;

            let group_msg = firefly::GroupMessage {
                id: 0,
                groupId: gid,
                message: encrypted.into(),
                epoch: group.epoch().await as u32,
            };

            let req_id = next_request_id.fetch_add(1, Ordering::Relaxed);
            let (tx, rx) = tokio::sync::oneshot::channel();
            pending_requests.write().await.insert(req_id, tx);

            let client_msg = firefly::ClientMessage {
                message: firefly::mod_ClientMessage::OneOfmessage::request(firefly::Request {
                    id: req_id,
                    payload: firefly::mod_Request::OneOfpayload::uploadGroupMessage(group_msg),
                }),
            };

            if let Ok(proto_bytes) = serialize_proto(&client_msg) {
                if let Some(ws) = ws_holder.read().await.as_ref() {
                    let _ = ws.send_with_u8_array(&proto_bytes);
                }
            }

            let msg_id = match rx.await {
                Ok(resp_bytes) => {
                    if let Ok(resp) = deserialize_proto::<firefly::Response<'_>>(&resp_bytes) {
                        match resp.body {
                            firefly::mod_Response::OneOfbody::groupMessageUploaded(up) => up.id,
                            _ => firefly_client::utils::get_current_timestamp_millis_since_epoch(),
                        }
                    } else {
                        firefly_client::utils::get_current_timestamp_millis_since_epoch()
                    }
                }
                Err(_) => firefly_client::utils::get_current_timestamp_millis_since_epoch(),
            };

            let channel_id = deserialize_proto::<firefly::GroupMessageInner>(&payload)
                .map(|inner| inner.channelId)
                .unwrap_or(0);

            let _ = group_messages_store
                .add(msg_id, gid, channel_id, group.epoch().await as u32, callbacks.name(), &payload)
                .await;

            Ok(JsValue::from_f64(msg_id as f64))
        })
    }

    #[wasm_bindgen]
    pub fn create_group(&self, name: String, description: String, _settings: Option<u32>) -> js_sys::Promise {
        let mls_holder = self.mls_client.clone();
        let group_info_store = self.group_info_store.clone();

        future_to_promise(async move {
            let mls = mls_holder.read().await;
            let mls_client = mls.as_ref().ok_or_else(|| JsValue::from_str("MLS client uninitialized"))?;

            let group = mls_client
                .create_group(name.clone())
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;

            let gid = group.group_id();
            let _ = group_info_store.set(gid, name.clone(), description.clone(), group.group_identifier().await.unwrap_or_default()).await;

            let res = JsGroupInfo {
                id: gid as f64,
                name,
                description,
                pending: false,
                owner: mls_client.username().unwrap_or_default(),
                has_local_state: true,
            };

            serde_wasm_bindgen::to_value(&res).map_err(|e| JsValue::from_str(&e.to_string()))
        })
    }

    #[wasm_bindgen]
    pub fn add_group_member(&self, group_id: f64, username: String, role_id: u32) -> js_sys::Promise {
        let mls_holder = self.mls_client.clone();
        let group_info_store = self.group_info_store.clone();

        future_to_promise(async move {
            let gid = group_id as u64;
            let mls = mls_holder.read().await;
            let mls_client = mls.as_ref().ok_or_else(|| JsValue::from_str("MLS client uninitialized"))?;

            let grp_ident = group_info_store.get(gid).await.map(|g| g.identifier).unwrap_or_default();
            let group = mls_client
                .load_group(gid, grp_ident)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;

            group
                .add_member(username, role_id)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;

            Ok(JsValue::NULL)
        })
    }

    #[wasm_bindgen]
    pub fn kick_group_member(&self, group_id: f64, username: String) -> js_sys::Promise {
        let mls_holder = self.mls_client.clone();
        let group_info_store = self.group_info_store.clone();

        future_to_promise(async move {
            let gid = group_id as u64;
            let mls = mls_holder.read().await;
            let mls_client = mls.as_ref().ok_or_else(|| JsValue::from_str("MLS client uninitialized"))?;

            let grp_ident = group_info_store.get(gid).await.map(|g| g.identifier).unwrap_or_default();
            let group = mls_client
                .load_group(gid, grp_ident)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;

            group
                .kick_member(username)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;

            Ok(JsValue::NULL)
        })
    }

    #[wasm_bindgen]
    pub fn delete_group(&self, group_id: f64) -> js_sys::Promise {
        let group_info_store = self.group_info_store.clone();

        future_to_promise(async move {
            let gid = group_id as u64;
            let _ = group_info_store.delete(gid).await;
            Ok(JsValue::NULL)
        })
    }

    #[wasm_bindgen]
    pub fn create_join_link(&self, group_id: f64, expires_in_seconds: f64, max_uses: u32) -> js_sys::Promise {
        let ws_holder = self.ws.clone();
        let pending_requests = self.pending_requests.clone();
        let next_request_id = self.next_request_id.clone();

        future_to_promise(async move {
            let req_id = next_request_id.fetch_add(1, Ordering::Relaxed);
            let (tx, rx) = tokio::sync::oneshot::channel();
            pending_requests.write().await.insert(req_id, tx);

            let client_msg = firefly::ClientMessage {
                message: firefly::mod_ClientMessage::OneOfmessage::request(firefly::Request {
                    id: req_id,
                    payload: firefly::mod_Request::OneOfpayload::createJoinLink(
                        firefly::CreateJoinLinkRequest {
                            group_id: group_id as u64,
                            expires_in_seconds: expires_in_seconds as u64,
                            max_uses,
                        },
                    ),
                }),
            };

            let proto_bytes = serialize_proto(&client_msg)
                .map_err(|e| JsValue::from_str(&e.to_string()))?;

            if let Some(ws) = ws_holder.read().await.as_ref() {
                let _ = ws.send_with_u8_array(&proto_bytes);
            }

            let resp_bytes = rx.await
                .map_err(|_| JsValue::from_str("create_join_link request timed out or cancelled"))?;

            let response = deserialize_proto::<firefly::Response<'_>>(&resp_bytes)
                .map_err(|e| JsValue::from_str(&e.to_string()))?;

            if let Some(error) = response.error {
                return Err(JsValue::from_str(&format!("Server error: {} ({})", error.error, error.errorCode)));
            }

            match response.body {
                firefly::mod_Response::OneOfbody::createJoinLink(res) => Ok(JsValue::from_str(&res.token)),
                _ => Err(JsValue::from_str("Unexpected response from server")),
            }
        })
    }

    #[wasm_bindgen]
    pub fn join_via_link(&self, link_token: String) -> js_sys::Promise {
        let ws_holder = self.ws.clone();
        let pending_requests = self.pending_requests.clone();
        let next_request_id = self.next_request_id.clone();

        future_to_promise(async move {
            let req_id = next_request_id.fetch_add(1, Ordering::Relaxed);
            let (tx, rx) = tokio::sync::oneshot::channel();
            pending_requests.write().await.insert(req_id, tx);

            let client_msg = firefly::ClientMessage {
                message: firefly::mod_ClientMessage::OneOfmessage::request(firefly::Request {
                    id: req_id,
                    payload: firefly::mod_Request::OneOfpayload::joinViaLink(
                        firefly::JoinViaLinkRequest {
                            token: link_token.into(),
                        },
                    ),
                }),
            };

            let proto_bytes = serialize_proto(&client_msg)
                .map_err(|e| JsValue::from_str(&e.to_string()))?;

            if let Some(ws) = ws_holder.read().await.as_ref() {
                let _ = ws.send_with_u8_array(&proto_bytes);
            }

            let resp_bytes = rx.await
                .map_err(|_| JsValue::from_str("join_via_link request timed out or cancelled"))?;

            let response = deserialize_proto::<firefly::Response<'_>>(&resp_bytes)
                .map_err(|e| JsValue::from_str(&e.to_string()))?;

            if let Some(error) = response.error {
                return Err(JsValue::from_str(&format!("Server error: {} ({})", error.error, error.errorCode)));
            }

            match response.body {
                firefly::mod_Response::OneOfbody::joinViaLinkSuccess(_) => Ok(JsValue::NULL),
                _ => Err(JsValue::from_str("Unexpected response from server")),
            }
        })
    }

    #[wasm_bindgen]
    pub fn request_to_join(&self, group_id: f64) -> js_sys::Promise {
        let base_url = self.firefly_base_url.clone();
        let callbacks = self.callbacks.clone();
        let address_id = self.address_id.load(Ordering::Relaxed);
        let device_id = self.device_id.load(Ordering::Relaxed);

        future_to_promise(async move {
            let token = callbacks
                .get_access_token()
                .await
                .unwrap_or_default();
            let url = format!(
                "{}/group/reAdd?address={}&device_id={}&groupIds={}",
                base_url, address_id, device_id, group_id as u64
            );

            let resp = HTTP_CLIENT
                .post(&url)
                .bearer_auth(&token)
                .send()
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;

            if resp.status().is_success() {
                Ok(JsValue::NULL)
            } else {
                Err(JsValue::from_str(&format!("Failed to request to join: {}", resp.status())))
            }
        })
    }

    #[wasm_bindgen]
    pub fn sync_group_joins_and_readds(&self, group_id: f64) -> js_sys::Promise {
        let base_url = self.firefly_base_url.clone();
        let callbacks = self.callbacks.clone();
        let address_id = self.address_id.load(Ordering::Relaxed);
        let device_id = self.device_id.load(Ordering::Relaxed) as u8;
        let mls_holder = self.mls_client.clone();
        let group_info_store = self.group_info_store.clone();
        let group_messages_store = self.group_messages_store.clone();

        future_to_promise(async move {
            let token = callbacks
                .get_access_token()
                .await
                .unwrap_or_default();

            if let Some(mls_guard) = mls_holder.read().await.as_ref() {
                let _ = join_groups_helper(
                    &base_url,
                    &token,
                    address_id,
                    device_id,
                    mls_guard,
                    &group_info_store,
                    &group_messages_store,
                    &callbacks,
                ).await;
            }

            if group_id > 0.0 {
                let url = format!(
                    "{}/group/reAdd?address={}&device_id={}&groupIds={}",
                    base_url, address_id, device_id, group_id as u64
                );
                let _ = HTTP_CLIENT
                    .post(&url)
                    .bearer_auth(&token)
                    .send()
                    .await;
            }

            Ok(JsValue::NULL)
        })
    }

    #[wasm_bindgen]
    pub fn load_all_groups(&self) -> js_sys::Promise {
        let group_info_store = self.group_info_store.clone();
        let mls_holder = self.mls_client.clone();

        future_to_promise(async move {
            let mls = mls_holder.read().await;
            if let Some(mls_client) = mls.as_ref() {
                if let Ok(groups) = group_info_store.get_all().await {
                    for g in groups {
                        let _ = mls_client.load_group(g.id, g.identifier).await;
                    }
                }
            }
            Ok(JsValue::NULL)
        })
    }

    #[wasm_bindgen]
    pub fn get_group_infos(&self) -> js_sys::Promise {
        let group_info_store = self.group_info_store.clone();
        let callbacks = self.callbacks.clone();

        future_to_promise(async move {
            let groups = group_info_store
                .get_all()
                .await
                .unwrap_or_default();
            let js_groups: Vec<JsGroupInfo> = groups
                .into_iter()
                .map(|g: GroupInfo| JsGroupInfo {
                    id: g.id as f64,
                    name: g.name,
                    description: g.description,
                    pending: false,
                    owner: callbacks.name().to_string(),
                    has_local_state: true,
                })
                .collect();

            serde_wasm_bindgen::to_value(&js_groups).map_err(|e| JsValue::from_str(&e.to_string()))
        })
    }

    #[wasm_bindgen]
    pub fn get_group_messages(&self, group_id: f64, start_before: f64, limit: u32) -> js_sys::Promise {
        let group_messages_store = self.group_messages_store.clone();

        future_to_promise(async move {
            let messages = group_messages_store
                .get(group_id as u64, start_before as u64, limit)
                .await
                .unwrap_or_default();

            let js_messages: Vec<JsGroupMessage> = messages
                .into_iter()
                .map(|m: GroupMessage| JsGroupMessage {
                    id: m.id as f64,
                    group_id: m.group_id as f64,
                    by: m.by,
                    message: m.message,
                    channel_id: m.channel_id,
                    epoch: m.epoch,
                })
                .collect();

            serde_wasm_bindgen::to_value(&js_messages).map_err(|e| JsValue::from_str(&e.to_string()))
        })
    }

    #[wasm_bindgen]
    pub fn get_online_status(&self, usernames: Vec<String>) -> js_sys::Promise {
        let ws_holder = self.ws.clone();
        let pending_requests = self.pending_requests.clone();
        let next_request_id = self.next_request_id.clone();

        future_to_promise(async move {
            if usernames.is_empty() {
                return serde_wasm_bindgen::to_value(&Vec::<String>::new()).map_err(|e| JsValue::from_str(&e.to_string()));
            }

            let req_id = next_request_id.fetch_add(1, Ordering::Relaxed);
            let (tx, rx) = tokio::sync::oneshot::channel();
            pending_requests.write().await.insert(req_id, tx);

            let client_msg = firefly::ClientMessage {
                message: firefly::mod_ClientMessage::OneOfmessage::request(firefly::Request {
                    id: req_id,
                    payload: firefly::mod_Request::OneOfpayload::userOnlineStatus(
                        firefly::UserOnlineStatusRequest {
                            usernames: usernames.iter().map(|s| std::borrow::Cow::Borrowed(s.as_str())).collect(),
                        },
                    ),
                }),
            };

            let proto_bytes = serialize_proto(&client_msg)
                .map_err(|e| JsValue::from_str(&e.to_string()))?;

            if let Some(ws) = ws_holder.read().await.as_ref() {
                let _ = ws.send_with_u8_array(&proto_bytes);
            }

            let resp_bytes = rx.await
                .map_err(|_| JsValue::from_str("get_online_status request timed out or cancelled"))?;

            let response = deserialize_proto::<firefly::Response<'_>>(&resp_bytes)
                .map_err(|e| JsValue::from_str(&e.to_string()))?;

            if let Some(error) = response.error {
                return Err(JsValue::from_str(&format!("Server error: {} ({})", error.error, error.errorCode)));
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
                    serde_wasm_bindgen::to_value(&online_users).map_err(|e| JsValue::from_str(&e.to_string()))
                }
                _ => Err(JsValue::from_str("Unexpected response body for userOnlineStatus")),
            }
        })
    }

    #[wasm_bindgen]
    pub fn read_user_messages_upto(&self, _other: String, _upto_message_id: f64) -> js_sys::Promise {
        future_to_promise(async move { Ok(JsValue::NULL) })
    }

    #[wasm_bindgen]
    pub fn upload_fcm_token(&self, _token: Option<String>) -> js_sys::Promise {
        future_to_promise(async move { Ok(JsValue::NULL) })
    }

    #[wasm_bindgen]
    pub fn get_conversations(&self, _token: String) -> js_sys::Promise {
        future_to_promise(async move {
            let list: Vec<JsConversation> = Vec::new();
            serde_wasm_bindgen::to_value(&list).map_err(|e| JsValue::from_str(&e.to_string()))
        })
    }

    #[wasm_bindgen]
    pub fn get_group_extension(&self, _group_id: f64) -> js_sys::Promise {
        future_to_promise(async move {
            let empty: Vec<u8> = Vec::new();
            serde_wasm_bindgen::to_value(&empty).map_err(|e| JsValue::from_str(&e.to_string()))
        })
    }

    #[wasm_bindgen]
    pub fn export_group_meeting_key(&self, _group_id: f64) -> js_sys::Promise {
        future_to_promise(async move {
            let empty: Vec<u8> = Vec::new();
            serde_wasm_bindgen::to_value(&empty).map_err(|e| JsValue::from_str(&e.to_string()))
        })
    }
}
