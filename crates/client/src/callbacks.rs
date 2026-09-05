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

#[async_trait::async_trait]
pub trait FireflyWsClientCallback: Send + Sync {
    fn name(&self) -> &str;

    async fn get_access_token(&self) -> Option<String>;

    async fn on_message(&self, message: UserMessage);

    async fn on_group_message(&self, group_message: GroupMessage);

    async fn on_group_joined(&self, _group_id: u64) {}

    async fn on_call_signal(&self, _signal: CallSignal) {}

    async fn on_group_meeting_signal(&self, _signal: GroupMeetingSignal) {}

    async fn on_read_user_messages_upto(&self, _read: ReadUserMessagesUpto) {}
}
