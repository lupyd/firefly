//! What happened since the agent last looked: messages from others and group changes, kept
//! in memory with an increasing sequence number so `wait_for_events` can resume from a cursor.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::Duration;

use schemars::JsonSchema;
use serde::Serialize;
use tokio::sync::watch;

use crate::content::{Content, id_time};

/// How many events are kept; a cursor older than that misses the ones dropped.
const CAPACITY: usize = 2000;

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventKind {
    /// A direct message, edit, delete or reaction from someone.
    Direct {
        from: String,
        message_id: u64,
        #[serde(flatten)]
        content: EventContent,
    },
    /// A message, edit, delete or reaction in a group channel.
    Group {
        group_id: u64,
        channel_id: u32,
        from: String,
        message_id: u64,
        #[serde(flatten)]
        content: EventContent,
    },
    /// This account was added to a group.
    AddedToGroup { group_id: u64 },
    /// People joined or left a group.
    MembersChanged { group_id: u64, joined: Vec<String>, left: Vec<String> },
    /// A group's name, channels, roles or settings changed.
    GroupUpdated { group_id: u64 },
    /// The account was signed out or this device was removed; run `login` again.
    SignedOut { reason: String },
    /// The account already has the most devices it may have, so this device isn't added (and
    /// can't send or receive) until its owner removes one of these in the app. It then connects
    /// by itself.
    DeviceLimitReached { devices: Vec<DeviceView> },
}

/// One of the account's devices.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct DeviceView {
    pub device_id: u32,
    /// What it called itself ("Android", "Web browser", ...); empty for older devices.
    pub name: String,
    pub web: bool,
    /// RFC 3339, UTC.
    pub last_connected: String,
}

impl From<&firefly_client::websocket::RegisteredDevice> for DeviceView {
    fn from(device: &firefly_client::websocket::RegisteredDevice) -> Self {
        let last_connected = chrono::DateTime::from_timestamp_millis(device.last_connected_at_ms as i64)
            .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
            .unwrap_or_default();
        Self { device_id: device.device_id, name: device.name.clone(), web: device.web, last_connected }
    }
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EventContent {
    Message {
        text: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        reply_to: Option<u64>,
        #[serde(skip_serializing_if = "is_zero")]
        files: usize,
    },
    Edit { of: u64, text: String },
    Delete { of: u64 },
    /// An empty emoji means the reaction was taken back.
    Reaction { to: u64, emoji: String },
    Call,
}

fn is_zero(n: &usize) -> bool {
    *n == 0
}

impl EventContent {
    /// `None` for messages people never see.
    pub fn from_content(content: Content) -> Option<Self> {
        Some(match content {
            Content::Message { text, reply_to, files } => Self::Message { text, reply_to, files },
            Content::Edit { of, text } => Self::Edit { of, text },
            Content::Delete { of } => Self::Delete { of },
            Content::Reaction { to, emoji } => Self::Reaction { to, emoji },
            Content::Call => Self::Call,
            Content::Hidden => return None,
        })
    }
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct Event {
    pub seq: u64,
    /// When it happened (RFC 3339, UTC): the server's time for messages, local time otherwise.
    pub at: String,
    #[serde(flatten)]
    pub kind: EventKind,
}

pub struct EventLog {
    events: Mutex<VecDeque<Event>>,
    latest: watch::Sender<u64>,
}

impl Default for EventLog {
    fn default() -> Self {
        Self { events: Mutex::new(VecDeque::new()), latest: watch::channel(0).0 }
    }
}

impl EventLog {
    pub fn push(&self, kind: EventKind) {
        let at = match &kind {
            EventKind::Direct { message_id, .. } | EventKind::Group { message_id, .. } => id_time(*message_id),
            _ => chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        };
        let mut events = self.events.lock().unwrap();
        let seq = events.back().map_or(0, |e| e.seq) + 1;
        events.push_back(Event { seq, at, kind });
        while events.len() > CAPACITY {
            events.pop_front();
        }
        drop(events);
        self.latest.send_replace(seq);
    }

    pub fn latest(&self) -> u64 {
        *self.latest.borrow()
    }

    fn after(&self, cursor: u64, limit: usize) -> Vec<Event> {
        self.events.lock().unwrap().iter().filter(|e| e.seq > cursor).take(limit).cloned().collect()
    }

    /// Events after `cursor`, waiting up to `timeout` for the first one.
    pub async fn wait_after(&self, cursor: u64, limit: usize, timeout: Duration) -> Vec<Event> {
        let mut latest = self.latest.subscribe();
        let _ = tokio::time::timeout(timeout, latest.wait_for(|seq| *seq > cursor)).await;
        self.after(cursor, limit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn waits_for_events_after_the_cursor() {
        let log = std::sync::Arc::new(EventLog::default());
        log.push(EventKind::GroupUpdated { group_id: 1 });
        assert_eq!(log.wait_after(0, 10, Duration::from_millis(10)).await.len(), 1);
        assert!(log.wait_after(1, 10, Duration::from_millis(10)).await.is_empty());

        let pusher = log.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            pusher.push(EventKind::AddedToGroup { group_id: 2 });
        });
        let events = log.wait_after(1, 10, Duration::from_secs(5)).await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].seq, 2);
    }
}
