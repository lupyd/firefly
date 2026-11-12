//! Message contents as the Lupyd apps write them, and the readable form tools return.
//!
//! A direct message is a `UserMessageInner` and a group message a `GroupMessageInner`. Edits,
//! deletes and reactions are messages of their own that name an earlier message by id;
//! [`fold`] applies them so a page reads like the chat on screen.

use std::borrow::Cow;
use std::collections::BTreeMap;

use firefly_protos::firefly::{
    GroupMessageInner, MessagePayload, Reaction, UserMessageInner, mod_GroupMessageInner, mod_MessagePayload,
    mod_UserMessageInner,
};
use schemars::JsonSchema;
use serde::Serialize;

/// What one stored message says.
#[derive(Debug, Clone, PartialEq)]
pub enum Content {
    Message { text: String, reply_to: Option<u64>, files: usize },
    Edit { of: u64, text: String },
    Delete { of: u64 },
    /// An empty emoji takes the sender's reaction back.
    Reaction { to: u64, emoji: String },
    Call,
    /// Device-to-device sync, pins and other messages people never see.
    Hidden,
}

/// What a tool sends.
#[derive(Debug, Clone)]
pub enum Outgoing {
    Message { text: String, reply_to: Option<u64> },
    Edit { of: u64, text: String },
    Delete { of: u64 },
    Reaction { to: u64, emoji: String },
}

fn payload_content(p: &MessagePayload<'_>) -> Content {
    let files = p.files.as_ref().map_or(0, |f| f.files.len());
    match p.ext {
        mod_MessagePayload::OneOfext::editedOf(of) if of != 0 => Content::Edit { of, text: p.text.to_string() },
        mod_MessagePayload::OneOfext::deleted(of) if of != 0 => Content::Delete { of },
        mod_MessagePayload::OneOfext::replyingTo(id) if id != 0 => {
            Content::Message { text: p.text.to_string(), reply_to: Some(id), files }
        }
        _ => Content::Message { text: p.text.to_string(), reply_to: None, files },
    }
}

fn reaction_content(r: &Reaction) -> Content {
    if r.reacting_to == 0 {
        return Content::Hidden;
    }
    Content::Reaction { to: r.reacting_to, emoji: u32_to_emoji(r.reaction) }
}

pub fn decode_direct(bytes: &[u8]) -> Content {
    let Ok(inner) = firefly_protos::deserialize_proto::<UserMessageInner>(bytes) else {
        return Content::Hidden;
    };
    match &inner.message {
        mod_UserMessageInner::OneOfmessage::messagePayload(p) => payload_content(p),
        mod_UserMessageInner::OneOfmessage::plainText(b) => {
            Content::Message { text: String::from_utf8_lossy(b).into_owned(), reply_to: None, files: 0 }
        }
        mod_UserMessageInner::OneOfmessage::reaction(r) => reaction_content(r),
        mod_UserMessageInner::OneOfmessage::callMessage(_) => Content::Call,
        _ => Content::Hidden,
    }
}

/// The channel and contents of a group message.
pub fn decode_group(bytes: &[u8]) -> (u32, Content) {
    let Ok(inner) = firefly_protos::deserialize_proto::<GroupMessageInner>(bytes) else {
        return (0, Content::Hidden);
    };
    let content = match &inner.message {
        mod_GroupMessageInner::OneOfmessage::messagePayload(p) => payload_content(p),
        mod_GroupMessageInner::OneOfmessage::reaction(r) => reaction_content(r),
        _ => Content::Hidden,
    };
    (inner.channelId, content)
}

fn payload(text: String, ext: mod_MessagePayload::OneOfext) -> MessagePayload<'static> {
    MessagePayload { text: Cow::Owned(text), ext, ..Default::default() }
}

fn outgoing_parts(message: Outgoing) -> Result<(Option<MessagePayload<'static>>, Option<Reaction>), String> {
    Ok(match message {
        Outgoing::Message { text, reply_to } => {
            let ext = reply_to.map_or(mod_MessagePayload::OneOfext::None, mod_MessagePayload::OneOfext::replyingTo);
            (Some(payload(text, ext)), None)
        }
        Outgoing::Edit { of, text } => (Some(payload(text, mod_MessagePayload::OneOfext::editedOf(of))), None),
        Outgoing::Delete { of } => (Some(payload(String::new(), mod_MessagePayload::OneOfext::deleted(of))), None),
        Outgoing::Reaction { to, emoji } => {
            let reaction = emoji_to_u32(&emoji);
            if reaction == 0 && !emoji.is_empty() {
                return Err(format!("{emoji:?} is not a single emoji"));
            }
            (None, Some(Reaction { reacting_to: to, reaction }))
        }
    })
}

pub fn encode_direct(message: Outgoing) -> Result<Vec<u8>, String> {
    let message = match outgoing_parts(message)? {
        (Some(p), _) => mod_UserMessageInner::OneOfmessage::messagePayload(p),
        (None, Some(r)) => mod_UserMessageInner::OneOfmessage::reaction(r),
        (None, None) => unreachable!(),
    };
    // A random nonce tells a resent copy of a message apart from the same words sent again.
    let inner = UserMessageInner { nonce: rand_nonce(), message_type: 0, message };
    firefly_protos::serialize_proto(&inner).map(|b| b.to_vec()).map_err(|e| e.to_string())
}

pub fn encode_group(channel_id: u32, message: Outgoing) -> Result<Vec<u8>, String> {
    let message = match outgoing_parts(message)? {
        (Some(p), _) => mod_GroupMessageInner::OneOfmessage::messagePayload(p),
        (None, Some(r)) => mod_GroupMessageInner::OneOfmessage::reaction(r),
        (None, None) => unreachable!(),
    };
    let inner = GroupMessageInner { channelId: channel_id, message_type: 0, message };
    firefly_protos::serialize_proto(&inner).map(|b| b.to_vec()).map_err(|e| e.to_string())
}

fn rand_nonce() -> u32 {
    rand::random()
}

/// The apps pack an emoji's UTF-8 bytes (without variation selectors, at most 4) into a u32.
pub fn emoji_to_u32(emoji: &str) -> u32 {
    let normalized: String = emoji.chars().filter(|c| !matches!(c, '\u{FE0E}' | '\u{FE0F}')).collect();
    let text = if normalized.is_empty() { emoji } else { normalized.as_str() };
    if text.len() > 4 {
        return 0;
    }
    text.bytes().fold(0u32, |acc, b| (acc << 8) | b as u32)
}

pub fn u32_to_emoji(value: u32) -> String {
    let bytes: Vec<u8> = value.to_be_bytes().into_iter().skip_while(|b| *b == 0).collect();
    String::from_utf8(bytes).unwrap_or_default()
}

/// Message ids are the server's time of the message in microseconds.
pub fn id_time(id: u64) -> String {
    chrono::DateTime::from_timestamp_micros(id as i64)
        .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_default()
}

/// One stored message: who sent it and what it says.
#[derive(Debug, Clone)]
pub struct Raw {
    pub id: u64,
    pub from: String,
    pub content: Content,
}

#[derive(Debug, Clone, Serialize, JsonSchema, PartialEq)]
pub struct ReactionView {
    pub emoji: String,
    pub by: Vec<String>,
}

/// A message as people see it in the chat.
#[derive(Debug, Clone, Serialize, JsonSchema, PartialEq)]
pub struct MessageView {
    pub id: u64,
    pub from: String,
    /// When the server received it (RFC 3339, UTC).
    pub at: String,
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<u64>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub edited: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub deleted: bool,
    /// Attached files (photos, documents); their contents aren't available here.
    #[serde(skip_serializing_if = "is_zero")]
    pub files: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub reactions: Vec<ReactionView>,
}

fn is_zero(n: &usize) -> bool {
    *n == 0
}

/// Applies edits, deletes and reactions to the messages they name, oldest first. Only the
/// sender of a message can edit or delete it; a later reaction of the same person replaces an
/// earlier one. Changes to messages outside `raws` are left out.
pub fn fold(mut raws: Vec<Raw>) -> Vec<MessageView> {
    raws.sort_by_key(|r| r.id);
    let mut views: BTreeMap<u64, MessageView> = BTreeMap::new();
    let mut reactions: BTreeMap<u64, BTreeMap<String, String>> = BTreeMap::new();
    for raw in raws {
        match raw.content {
            Content::Message { text, reply_to, files } => {
                views.insert(
                    raw.id,
                    MessageView {
                        id: raw.id,
                        at: id_time(raw.id),
                        from: raw.from,
                        text,
                        reply_to,
                        edited: false,
                        deleted: false,
                        files,
                        reactions: Vec::new(),
                    },
                );
            }
            Content::Edit { of, text } => {
                if let Some(view) = views.get_mut(&of).filter(|v| v.from == raw.from && !v.deleted) {
                    view.text = text;
                    view.edited = true;
                }
            }
            Content::Delete { of } => {
                if let Some(view) = views.get_mut(&of).filter(|v| v.from == raw.from) {
                    view.text.clear();
                    view.files = 0;
                    view.deleted = true;
                }
            }
            Content::Reaction { to, emoji } => {
                let by = reactions.entry(to).or_default();
                if emoji.is_empty() {
                    by.remove(&raw.from);
                } else {
                    by.insert(raw.from, emoji);
                }
            }
            Content::Call | Content::Hidden => {}
        }
    }
    for (id, by) in reactions {
        let Some(view) = views.get_mut(&id) else { continue };
        let mut grouped: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (user, emoji) in by {
            grouped.entry(emoji).or_default().push(user);
        }
        view.reactions = grouped.into_iter().map(|(emoji, by)| ReactionView { emoji, by }).collect();
    }
    views.into_values().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(id: u64, from: &str, bytes: Vec<u8>) -> Raw {
        Raw { id, from: from.into(), content: decode_direct(&bytes) }
    }

    #[test]
    fn emoji_round_trip_matches_the_apps() {
        // emojiToU32 in lupyd-desktop/src/lib/emoji-reaction.ts.
        assert_eq!(emoji_to_u32("👍"), 0xF09F_918D);
        assert_eq!(emoji_to_u32("❤️"), 0x00E2_9DA4);
        assert_eq!(u32_to_emoji(0xF09F_918D), "👍");
        assert_eq!(u32_to_emoji(0x00E2_9DA4), "❤");
        assert_eq!(emoji_to_u32(""), 0);
        assert_eq!(emoji_to_u32("👍🏽"), 0, "longer than 4 bytes");
    }

    #[test]
    fn edits_deletes_and_reactions_apply_to_their_message() {
        let send = |m| encode_direct(m).unwrap();
        let folded = fold(vec![
            raw(1, "alice", send(Outgoing::Message { text: "hello".into(), reply_to: None })),
            raw(2, "bob", send(Outgoing::Message { text: "hi".into(), reply_to: Some(1) })),
            raw(3, "alice", send(Outgoing::Edit { of: 1, text: "hello there".into() })),
            raw(4, "bob", send(Outgoing::Edit { of: 1, text: "not bob's".into() })),
            raw(5, "bob", send(Outgoing::Reaction { to: 1, emoji: "👍".into() })),
            raw(6, "carol", send(Outgoing::Reaction { to: 1, emoji: "👍".into() })),
            raw(7, "carol", send(Outgoing::Reaction { to: 1, emoji: String::new() })),
            raw(8, "bob", send(Outgoing::Delete { of: 2 })),
        ]);
        assert_eq!(folded.len(), 2);
        assert_eq!(folded[0].text, "hello there");
        assert!(folded[0].edited);
        assert_eq!(folded[0].reactions, vec![ReactionView { emoji: "👍".into(), by: vec!["bob".into()] }]);
        assert!(folded[1].deleted);
        assert_eq!(folded[1].reply_to, Some(1));
        assert_eq!(folded[1].text, "");
    }

    #[test]
    fn group_messages_carry_their_channel() {
        let bytes = encode_group(7, Outgoing::Message { text: "in #seven".into(), reply_to: None }).unwrap();
        assert_eq!(
            decode_group(&bytes),
            (7, Content::Message { text: "in #seven".into(), reply_to: None, files: 0 })
        );
    }

    #[test]
    fn ids_read_as_server_time() {
        assert_eq!(id_time(1_762_000_000_000_000), "2025-11-01T12:26:40Z");
    }
}
