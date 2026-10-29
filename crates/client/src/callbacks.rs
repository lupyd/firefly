use crate::storage::{GroupMessage, UserMessage};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CallSignal {
    pub call_id: u64,
    pub sender_username: String,
    pub receiver_username: String,
    pub signal_type: i32, // Maps to CallSignalType enum: 0=REQUEST, 1=ANSWER, 2=REJECT, 3=CANCEL, 4=HANGUP, 5=DISMISS, 6=ICECANDIDATE
    pub sdp: String,
    pub candidate: String,
    pub sdp_m_line_index: i32,
    pub sdp_mid: String,
    pub sender_device_id: u32,
    /// Maps to CallRejectReason: 0=NONE, 1=NOT_ACCEPTED (message request pending), 2=BLOCKED.
    #[serde(default)]
    pub reject_reason: i32,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct GroupMeetingSignal {
    pub group_id: u64,
    pub channel_id: u32,
    pub session_id: u64,
    pub signal_type: i32, // Maps to MeetingSignalType enum: 0=STARTED, 1=JOINED, 2=LEFT, 3=ENDED
    pub username: String,
    pub cf_meeting_id: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ReadUserMessagesUpto {
    pub other: String,
    pub upto_message_id: u64,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct GroupHistorySignal {
    pub group_id: u64,
    pub signal_type: i32, // Maps to HistorySignalType enum: 0=REQUEST_CREATED, 1=CHUNK_PUBLISHED, 2=CHUNK_DISAPPROVED, 3=REQUEST_FULFILLED
    pub request_id: u64,
    pub chunk_id: u64,
    pub username: String,
}

/// Another device of this user asks for older direct messages with `other` (see
/// `FireflyWsClient::send_direct_history_page`).
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct DirectHistoryRequest {
    pub request_id: u64,
    pub other: String,
    pub before_id: u64,
    pub limit: u32,
    pub requester_address: u64,
    pub requester_device_id: u32,
}

/// Older direct messages with `other` sent by another device of this user, answering a request.
#[derive(Clone, Debug)]
pub struct DirectHistoryPage {
    pub request_id: u64,
    pub other: String,
    pub messages: Vec<UserMessage>,
    pub has_more: bool,
}

#[async_trait::async_trait]
pub trait FireflyWsClientCallback: Send + Sync {
    fn name(&self) -> &str;

    async fn get_access_token(&self) -> Option<String>;

    async fn on_message(&self, message: UserMessage);

    async fn on_group_message(&self, group_message: GroupMessage);

    async fn on_group_message_updated(&self, message: GroupMessage) { self.on_group_message(message).await; }

    async fn on_group_joined(&self, _group_id: u64) {}

    async fn on_group_updated(&self, _group_id: u64) {}

    async fn on_unauthenticated(&self) {}

    async fn on_call_signal(&self, _signal: CallSignal) {}

    async fn on_group_meeting_signal(&self, _signal: GroupMeetingSignal) {}

    async fn on_group_history_signal(&self, _signal: GroupHistorySignal) {}

    async fn on_read_user_messages_upto(&self, _read: ReadUserMessagesUpto) {}

    async fn on_direct_backup_key_sync(&self, _sync: firefly_protos::firefly::DirectBackupKeySync<'static>) {}

    async fn on_direct_pin_snapshot(&self, _peer: String, _secret: firefly_protos::firefly::DirectPinSnapshotSecret<'static>) {}

    /// Called whenever a connection is established, with `"webtransport"` or `"websocket"`.
    async fn on_transport_changed(&self, _transport: String) {}

    /// The user removed this device from another one: the client stopped and forgot its
    /// registration; the app should sign this device out (revoking its own session).
    async fn on_device_revoked(&self) {}
    /// Answer with `send_direct_history_page` from the local message store.
    async fn on_direct_history_request(&self, _request: DirectHistoryRequest) {}
    /// Store the messages (they are older than what this device has).
    async fn on_direct_history_page(&self, _page: DirectHistoryPage) {}
}

