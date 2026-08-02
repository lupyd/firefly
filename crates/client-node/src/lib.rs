use std::sync::{Arc, Mutex};
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode, ErrorStrategy, ThreadSafeCallContext};
use napi_derive::napi;
use napi::{Env, JsFunction, JsObject, Result, Status};
use firefly_client::callbacks::{FireflyWsClientCallback, CallSignal, GroupMeetingSignal};
use firefly_client::websocket::FfiFireflyWsClient;
use firefly_client::db::messages::UserMessage;
use firefly_client::db::group_messages::GroupMessage;
use firefly_client::websocket::ConnectionState;

#[napi(object)]
#[derive(Clone, serde::Serialize)]
pub struct NapiUserMessage {
    pub id: f64,
    pub other: String,
    pub message: Vec<u8>,
    pub sent_by_other: bool,
}

#[napi(object)]
#[derive(Clone, serde::Serialize)]
pub struct NapiGroupMessage {
    pub id: f64,
    pub group_id: f64,
    pub by: String,
    pub message: Vec<u8>,
    pub channel_id: u32,
    pub epoch: u32,
}

#[napi(object)]
#[derive(Clone)]
pub struct NapiCallSignal {
    pub call_id: f64,
    pub sender_username: String,
    pub receiver_username: String,
    pub signal_type: i32,
    pub sdp: String,
    pub candidate: String,
    pub sdp_m_line_index: i32,
    pub sdp_mid: String,
    pub sender_device_id: u32,
}

#[napi(object)]
#[derive(Clone)]
pub struct NapiGroupMeetingSignal {
    pub group_id: f64,
    pub channel_id: u32,
    pub session_id: f64,
    pub signal_type: i32,
    pub username: String,
    pub cf_meeting_id: String,
}

#[napi(object)]
#[derive(Clone)]
pub struct NapiConversation {
    pub other: String,
    pub settings: f64,
}

#[napi(object)]
#[derive(Clone)]
pub struct NapiGroupInfo {
    pub id: f64,
    pub name: String,
    pub description: String,
    pub pending: bool,
    pub owner: String,
    pub has_local_state: bool,
}

#[napi(object)]
#[derive(Clone)]
pub struct NapiGroupInfoDB {
    pub id: f64,
    pub name: String,
    pub description: String,
}

#[napi(object)]
#[derive(Clone)]
pub struct NapiUpdateUserProposal {
    pub username: String,
    pub role_id: u32,
}

#[napi(object)]
#[derive(Clone)]
pub struct NapiUpdateRoleProposal {
    pub name: String,
    pub role_id: u32,
    pub permissions: u32,
    pub delete: bool,
    pub color: u32,
}

struct NodeClientCallbacks {
    name: String,
    token: Arc<Mutex<Option<String>>>,
    on_message_fn: ThreadsafeFunction<String, ErrorStrategy::CalleeHandled>,
    on_group_message_fn: ThreadsafeFunction<String, ErrorStrategy::CalleeHandled>,
    on_group_joined_fn: ThreadsafeFunction<u64, ErrorStrategy::CalleeHandled>,
    on_call_signal_fn: ThreadsafeFunction<NapiCallSignal, ErrorStrategy::CalleeHandled>,
    on_group_meeting_signal_fn: ThreadsafeFunction<NapiGroupMeetingSignal, ErrorStrategy::CalleeHandled>,
}

#[async_trait::async_trait]
impl FireflyWsClientCallback for NodeClientCallbacks {
    fn name(&self) -> &str {
        &self.name
    }

    async fn get_access_token(&self) -> Option<String> {
        self.token.lock().unwrap().clone()
    }

    async fn on_message(&self, message: UserMessage) {
        let msg = NapiUserMessage {
            id: message.id as f64,
            other: message.other,
            message: message.message,
            sent_by_other: message.sent_by_other,
        };
        let json = serde_json::to_string(&msg).unwrap_or_default();
        let status = self.on_message_fn.call(Ok(json), ThreadsafeFunctionCallMode::NonBlocking);
        if status != napi::Status::Ok {
            eprintln!("Failed to call JS onMessage callback: {:?}", status);
        }
    }

    async fn on_group_message(&self, group_message: GroupMessage) {
        let msg = NapiGroupMessage {
            id: group_message.id as f64,
            group_id: group_message.group_id as f64,
            by: group_message.by,
            message: group_message.message,
            channel_id: group_message.channel_id,
            epoch: group_message.epoch,
        };
        let json = serde_json::to_string(&msg).unwrap_or_default();
        let status = self.on_group_message_fn.call(Ok(json), ThreadsafeFunctionCallMode::NonBlocking);
        if status != napi::Status::Ok {
            eprintln!("Failed to call JS onGroupMessage callback: {:?}", status);
        }
    }

    async fn on_group_joined(&self, group_id: u64) {
        let status = self.on_group_joined_fn.call(Ok(group_id), ThreadsafeFunctionCallMode::NonBlocking);
        if status != napi::Status::Ok {
            eprintln!("Failed to call JS onGroupJoined callback: {:?}", status);
        }
    }

    async fn on_call_signal(&self, signal: CallSignal) {
        let sig = NapiCallSignal {
            call_id: signal.call_id as f64,
            sender_username: signal.sender_username,
            receiver_username: signal.receiver_username,
            signal_type: signal.signal_type,
            sdp: signal.sdp,
            candidate: signal.candidate,
            sdp_m_line_index: signal.sdp_m_line_index,
            sdp_mid: signal.sdp_mid,
            sender_device_id: signal.sender_device_id,
        };
        let _ = self.on_call_signal_fn.call(Ok(sig), ThreadsafeFunctionCallMode::NonBlocking);
    }

    async fn on_group_meeting_signal(&self, signal: GroupMeetingSignal) {
        let sig = NapiGroupMeetingSignal {
            group_id: signal.group_id as f64,
            channel_id: signal.channel_id,
            session_id: signal.session_id as f64,
            signal_type: signal.signal_type,
            username: signal.username,
            cf_meeting_id: signal.cf_meeting_id,
        };
        let _ = self.on_group_meeting_signal_fn.call(Ok(sig), ThreadsafeFunctionCallMode::NonBlocking);
    }
}

fn extract_callbacks(callbacks_obj: JsObject, token: Arc<Mutex<Option<String>>>) -> Result<NodeClientCallbacks> {
    let name_js: String = callbacks_obj.get_named_property("name")?;
    let on_message_js: JsFunction = callbacks_obj.get_named_property("onMessage")?;
    let on_group_message_js: JsFunction = callbacks_obj.get_named_property("onGroupMessage")?;
    let on_group_joined_js: JsFunction = callbacks_obj.get_named_property("onGroupJoined")?;
    let on_call_signal_js: JsFunction = callbacks_obj.get_named_property("onCallSignal")?;
    let on_group_meeting_signal_js: JsFunction = callbacks_obj.get_named_property("onGroupMeetingSignal")?;

    let on_message_fn = on_message_js.create_threadsafe_function(0, |ctx: ThreadSafeCallContext<String>| {
        eprintln!("on_message_fn closure value: {}", ctx.value);
        let js_str = ctx.env.create_string(&ctx.value)?;
        Ok(vec![js_str.into_unknown()])
    })?;

    let on_group_message_fn = on_group_message_js.create_threadsafe_function(0, |ctx: ThreadSafeCallContext<String>| {
        eprintln!("on_group_message_fn closure value: {}", ctx.value);
        let js_str = ctx.env.create_string(&ctx.value)?;
        Ok(vec![js_str.into_unknown()])
    })?;

    let on_group_joined_fn = on_group_joined_js.create_threadsafe_function(0, |ctx: ThreadSafeCallContext<u64>| {
        Ok(vec![ctx.env.create_double(ctx.value as f64)?.into_unknown()])
    })?;

    let on_call_signal_fn = on_call_signal_js.create_threadsafe_function(0, |ctx: ThreadSafeCallContext<NapiCallSignal>| {
        let mut obj = ctx.env.create_object()?;
        obj.set_named_property("callId", ctx.value.call_id)?;
        obj.set_named_property("senderUsername", ctx.env.create_string(&ctx.value.sender_username)?)?;
        obj.set_named_property("receiverUsername", ctx.env.create_string(&ctx.value.receiver_username)?)?;
        obj.set_named_property("signalType", ctx.value.signal_type)?;
        obj.set_named_property("sdp", ctx.env.create_string(&ctx.value.sdp)?)?;
        obj.set_named_property("candidate", ctx.env.create_string(&ctx.value.candidate)?)?;
        obj.set_named_property("sdpMLineIndex", ctx.value.sdp_m_line_index)?;
        obj.set_named_property("sdpMid", ctx.env.create_string(&ctx.value.sdp_mid)?)?;
        obj.set_named_property("senderDeviceId", ctx.value.sender_device_id)?;
        Ok(vec![obj])
    })?;

    let on_group_meeting_signal_fn = on_group_meeting_signal_js.create_threadsafe_function(0, |ctx: ThreadSafeCallContext<NapiGroupMeetingSignal>| {
        let mut obj = ctx.env.create_object()?;
        obj.set_named_property("groupId", ctx.value.group_id)?;
        obj.set_named_property("channelId", ctx.value.channel_id)?;
        obj.set_named_property("sessionId", ctx.value.session_id)?;
        obj.set_named_property("signalType", ctx.value.signal_type)?;
        obj.set_named_property("username", ctx.env.create_string(&ctx.value.username)?)?;
        obj.set_named_property("cfMeetingId", ctx.env.create_string(&ctx.value.cf_meeting_id)?)?;
        Ok(vec![obj])
    })?;

    Ok(NodeClientCallbacks {
        name: name_js,
        token,
        on_message_fn,
        on_group_message_fn,
        on_group_joined_fn,
        on_call_signal_fn,
        on_group_meeting_signal_fn,
    })
}

#[napi]
pub fn init_logger(file_path: String) {
    firefly_client::init_logger(file_path);
}

#[napi]
pub struct FireflyClientNode {
    inner: Arc<FfiFireflyWsClient>,
    token: Arc<Mutex<Option<String>>>,
}

#[napi]
impl FireflyClientNode {
    #[napi]
    pub fn create(
        env: Env,
        firefly_base_url: String,
        firefly_base_ws_url: String,
        retry_interval_in_ms: f64,
        callbacks_obj: JsObject,
        key_stores_pathname: String,
        request_timeout_in_ms: f64,
    ) -> Result<JsObject> {
        let initial_token: Option<String> = callbacks_obj.get_named_property("initialToken").ok();
        let token = Arc::new(Mutex::new(initial_token));
        let callbacks = extract_callbacks(callbacks_obj, token.clone())?;

        let token_clone = token.clone();
        env.execute_tokio_future(
            async move {
                let inner = FfiFireflyWsClient::create(
                    firefly_base_url,
                    firefly_base_ws_url,
                    retry_interval_in_ms as u64,
                    Box::new(callbacks),
                    key_stores_pathname,
                    request_timeout_in_ms as u64,
                )
                .await
                .map_err(|e| napi::Error::new(Status::GenericFailure, format!("Failed to create client: {}", e)))?;

                Ok(FireflyClientNode {
                    inner: Arc::new(inner),
                    token: token_clone,
                })
            },
            |&mut _env, client| Ok(client),
        )
    }

    #[napi]
    pub fn set_access_token(&self, token: String) {
        *self.token.lock().unwrap() = Some(token);
    }

    #[napi]
    pub async fn initialize_with_retrying(&self) -> Result<()> {
        self.inner.initialize_with_retrying().await
            .map_err(|e| napi::Error::new(Status::GenericFailure, format!("{}", e)))
    }

    #[napi]
    pub async fn check_setup(&self) -> Result<()> {
        self.inner.check_setup().await
            .map_err(|e| napi::Error::new(Status::GenericFailure, format!("{}", e)))
    }

    #[napi]
    pub async fn dispose(&self) {
        self.inner.dispose().await;
    }

    #[napi]
    pub async fn encrypt_and_send(&self, to: String, payload: Vec<u8>) -> Result<NapiUserMessage> {
        let msg = self.inner.encrypt_and_send(to, payload).await
            .map_err(|e| napi::Error::new(Status::GenericFailure, format!("{}", e)))?;
        Ok(NapiUserMessage {
            id: msg.id as f64,
            other: msg.other,
            message: msg.message,
            sent_by_other: msg.sent_by_other,
        })
    }

    #[napi]
    pub async fn upload_fcm_token(&self, token: Option<String>) -> Result<()> {
        self.inner.upload_fcm_token(token).await
            .map_err(|e| napi::Error::new(Status::GenericFailure, format!("{}", e)))
    }

    #[napi]
    pub fn get_connection_state(&self) -> String {
        match self.inner.get_connection_state() {
            ConnectionState::Disconnected => "Disconnected".to_string(),
            ConnectionState::Initializing => "Initializing".to_string(),
            ConnectionState::Retrying => "Retrying".to_string(),
            ConnectionState::Connected => "Connected".to_string(),
            ConnectionState::CheckingSetup => "CheckingSetup".to_string(),
        }
    }

    #[napi]
    pub fn is_initialized(&self) -> bool {
        self.inner.is_initialized()
    }

    #[napi]
    pub async fn get_conversations(&self, token: String) -> Result<Vec<NapiConversation>> {
        let conversations = self.inner.get_conversations(token).await
            .map_err(|e| napi::Error::new(Status::GenericFailure, format!("{}", e)))?;
        Ok(conversations.into_iter().map(|c| NapiConversation {
            other: c.other,
            settings: c.settings as f64,
        }).collect())
    }

    #[napi]
    pub async fn create_group(&self, name: String, description: String) -> Result<NapiGroupInfoDB> {
        let group = self.inner.create_group(name, description).await
            .map_err(|e| napi::Error::new(Status::GenericFailure, format!("{}", e)))?;
        Ok(NapiGroupInfoDB {
            id: group.id as f64,
            name: group.name,
            description: group.description,
        })
    }

    #[napi]
    pub async fn encrypt_and_send_group(&self, group_id: f64, payload: Vec<u8>) -> Result<f64> {
        let res = self.inner.encrypt_and_send_group(group_id as u64, payload).await
            .map_err(|e| napi::Error::new(Status::GenericFailure, format!("{}", e)))?;
        Ok(res as f64)
    }

    #[napi]
    pub async fn get_group_extension(&self, group_id: f64) -> Result<Vec<u8>> {
        self.inner.get_group_extension(group_id as u64).await
            .map_err(|e| napi::Error::new(Status::GenericFailure, format!("{}", e)))
    }

    #[napi]
    pub async fn export_group_meeting_key(&self, group_id: f64) -> Result<Vec<u8>> {
        self.inner.export_group_meeting_key(group_id as u64).await
            .map_err(|e| napi::Error::new(Status::GenericFailure, format!("{}", e)))
    }

    #[napi]
    pub async fn load_all_groups(&self) -> Result<()> {
        self.inner.load_all_groups().await
            .map_err(|e| napi::Error::new(Status::GenericFailure, format!("{}", e)))
    }

    #[napi]
    pub async fn update_group_users(&self, group_id: f64, users: Vec<NapiUpdateUserProposal>) -> Result<f64> {
        let mapped = users.into_iter().map(|u| firefly_client::group::UpdateUserProposalFfi {
            username: u.username,
            role_id: u.role_id,
        }).collect();
        let res = self.inner.update_group_users(group_id as u64, mapped).await
            .map_err(|e| napi::Error::new(Status::GenericFailure, format!("{}", e)))?;
        Ok(res as f64)
    }

    #[napi]
    pub async fn update_group_channel(
        &self,
        group_id: f64,
        id: u32,
        is_delete: bool,
        name: String,
        channel_ty: u8,
        default_permissions: u32,
    ) -> Result<f64> {
        let res = self.inner.update_group_channel(group_id as u64, id, is_delete, name, channel_ty, default_permissions).await
            .map_err(|e| napi::Error::new(Status::GenericFailure, format!("{}", e)))?;
        Ok(res as f64)
    }

    #[napi]
    pub async fn update_group_roles(&self, group_id: f64, roles: Vec<NapiUpdateRoleProposal>) -> Result<f64> {
        let mapped = roles.into_iter().map(|r| firefly_client::group::UpdateRoleProposalFfi {
            name: r.name,
            role_id: r.role_id,
            permissions: r.permissions,
            delete: r.delete,
            color: r.color,
        }).collect();
        let res = self.inner.update_group_roles(group_id as u64, mapped).await
            .map_err(|e| napi::Error::new(Status::GenericFailure, format!("{}", e)))?;
        Ok(res as f64)
    }

    #[napi]
    pub async fn update_group_roles_in_channel(&self, group_id: f64, channel_id: u32, roles: Vec<NapiUpdateRoleProposal>) -> Result<f64> {
        let mapped = roles.into_iter().map(|r| firefly_client::group::UpdateRoleProposalFfi {
            name: r.name,
            role_id: r.role_id,
            permissions: r.permissions,
            delete: r.delete,
            color: r.color,
        }).collect();
        let res = self.inner.update_group_roles_in_channel(group_id as u64, channel_id, mapped).await
            .map_err(|e| napi::Error::new(Status::GenericFailure, format!("{}", e)))?;
        Ok(res as f64)
    }

    #[napi]
    pub async fn add_group_member(&self, group_id: f64, username: String, role_id: u32) -> Result<()> {
        self.inner.add_group_member(group_id as u64, username, role_id).await
            .map_err(|e| napi::Error::new(Status::GenericFailure, format!("{}", e)))
    }

    #[napi]
    pub async fn kick_group_member(&self, group_id: f64, username: String) -> Result<()> {
        self.inner.kick_group_member(group_id as u64, username).await
            .map_err(|e| napi::Error::new(Status::GenericFailure, format!("{}", e)))
    }

    #[napi]
    pub async fn delete_group(&self, group_id: f64) -> Result<()> {
        self.inner.delete_group(group_id as u64).await
            .map_err(|e| napi::Error::new(Status::GenericFailure, format!("{}", e)))
    }

    #[napi]
    pub async fn create_join_link(&self, group_id: f64, expires_in_seconds: f64, max_uses: u32) -> Result<String> {
        self.inner.create_join_link(group_id as u64, expires_in_seconds as u64, max_uses).await
            .map_err(|e| napi::Error::new(Status::GenericFailure, format!("{}", e)))
    }

    #[napi]
    pub async fn join_via_link(&self, token: String) -> Result<()> {
        self.inner.join_via_link(&token).await
            .map_err(|e| napi::Error::new(Status::GenericFailure, format!("{}", e)))
    }

    #[napi]
    pub fn generate_call_id(&self) -> f64 {
        self.inner.generate_call_id() as f64
    }

    #[napi]
    pub async fn initiate_call(&self, call_id: f64, receiver_username: String, sdp_offer: String) -> Result<()> {
        self.inner.initiate_call(call_id as u64, receiver_username, sdp_offer).await
            .map_err(|e| napi::Error::new(Status::GenericFailure, format!("{}", e)))
    }

    #[napi]
    pub async fn accept_call(&self, call_id: f64, caller_username: String, sdp_answer: String) -> Result<()> {
        self.inner.accept_call(call_id as u64, caller_username, sdp_answer).await
            .map_err(|e| napi::Error::new(Status::GenericFailure, format!("{}", e)))
    }

    #[napi]
    pub async fn reject_call(&self, call_id: f64, caller_username: String) -> Result<()> {
        self.inner.reject_call(call_id as u64, caller_username).await
            .map_err(|e| napi::Error::new(Status::GenericFailure, format!("{}", e)))
    }

    #[napi]
    pub async fn cancel_call(&self, call_id: f64, receiver_username: String) -> Result<()> {
        self.inner.cancel_call(call_id as u64, receiver_username).await
            .map_err(|e| napi::Error::new(Status::GenericFailure, format!("{}", e)))
    }

    #[napi]
    pub async fn hangup_call(&self, call_id: f64, other_username: String) -> Result<()> {
        self.inner.hangup_call(call_id as u64, other_username).await
            .map_err(|e| napi::Error::new(Status::GenericFailure, format!("{}", e)))
    }

    #[napi]
    pub async fn send_ice_candidate(
        &self,
        call_id: f64,
        other_username: String,
        candidate: String,
        sdp_mid: String,
        sdp_m_line_index: i32,
    ) -> Result<()> {
        self.inner.send_ice_candidate(call_id as u64, other_username, candidate, sdp_mid, sdp_m_line_index).await
            .map_err(|e| napi::Error::new(Status::GenericFailure, format!("{}", e)))
    }

    #[napi]
    pub async fn get_group_infos(&self) -> Result<Vec<NapiGroupInfo>> {
        let infos = self.inner.get_group_infos().await
            .map_err(|e| napi::Error::new(Status::GenericFailure, format!("{}", e)))?;
        Ok(infos.into_iter().map(|g| NapiGroupInfo {
            id: g.id as f64,
            name: g.name,
            description: g.description,
            pending: g.pending,
            owner: g.owner,
            has_local_state: g.has_local_state,
        }).collect())
    }

    #[napi]
    pub async fn get_group_messages(
        &self,
        group_id: f64,
        start_before: f64,
        limit: u32,
    ) -> Result<Vec<NapiGroupMessage>> {
        let store = self.inner.group_message_store();
        let messages = store
            .get(group_id as u64, start_before as u64, limit)
            .await
            .map_err(|e| napi::Error::new(Status::GenericFailure, format!("{}", e)))?;
        Ok(messages
            .into_iter()
            .map(|gm| NapiGroupMessage {
                id: gm.id as f64,
                group_id: gm.group_id as f64,
                by: gm.by,
                message: gm.message,
                channel_id: gm.channel_id,
                epoch: gm.epoch,
            })
            .collect())
    }

    #[napi]
    pub async fn get_online_status(&self, usernames: Vec<String>) -> Result<Vec<String>> {
        self.inner.get_online_status(usernames).await
            .map_err(|e| napi::Error::new(Status::GenericFailure, format!("{}", e)))
    }
}
