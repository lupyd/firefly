//! The MCP tools: what an agent signed in as an ordinary user can read and do.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use firefly_client::db::search::SearchScope;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::{Json, Parameters};
use rmcp::model::{Implementation, ServerCapabilities, ServerConfig};
use rmcp::{ServerHandler, tool, tool_handler, tool_router};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::agent::Agent;
use crate::content::{MessageView, Outgoing, Raw, decode_direct, decode_group, fold};
use crate::events::Event;

const INSTRUCTIONS: &str = "You are signed in to a Firefly (Lupyd) chat account as an ordinary user. \
People message you directly or add you to groups; group members decide what you may do there. \
Call wait_for_events in a loop to hear about new messages and group changes, read a conversation \
with read_direct_messages or read_group_messages, and answer with send_direct_message or \
send_group_message. Messages are end-to-end encrypted; this server decrypts them locally on this \
machine. Message ids are the server's time in microseconds; pass them as reply_to, message_id or before_id.";

type ToolResult<T> = Result<Json<T>, String>;

/// `FireflyGroupChannel.type` of a text channel, the apps' default.
const TEXT_CHANNEL: u8 = 1;

fn err(e: impl std::fmt::Display) -> String {
    format!("{e:#}")
}

#[derive(Clone)]
pub struct FireflyMcp {
    agent: Arc<Agent>,
    name: String,
    tool_router: ToolRouter<Self>,
}

impl FireflyMcp {
    /// `name` is how the server introduces itself to the MCP client.
    pub fn new(agent: Arc<Agent>, name: impl Into<String>) -> Self {
        Self { agent, name: name.into(), tool_router: Self::tool_router() }
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for FireflyMcp {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new(self.name.clone(), env!("CARGO_PKG_VERSION")))
            .with_instructions(INSTRUCTIONS)
    }
}

// ---- parameters and results ----

#[derive(Debug, Deserialize, JsonSchema)]
pub struct Empty {}

#[derive(Debug, Serialize, JsonSchema)]
pub struct WhoAmI {
    pub username: String,
    /// Disconnected, Initializing, Retrying, Connected or CheckingSetup.
    pub connection: String,
    pub device_id: u32,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct DirectConversation {
    pub username: String,
    pub unread: u32,
    /// False while it is a message request this account hasn't accepted; replying accepts it.
    pub accepted: bool,
    pub last_message_id: u64,
    pub last_message_from: String,
    pub last_message_preview: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct GroupSummary {
    pub group_id: u64,
    pub name: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub description: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct Conversations {
    pub direct: Vec<DirectConversation>,
    pub groups: Vec<GroupSummary>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ReadDirect {
    pub username: String,
    /// Read messages older than this id (from an earlier page's older_before_id).
    pub before_id: Option<u64>,
    /// At most this many messages (default 30, at most 100).
    pub limit: Option<u32>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct DirectPage {
    /// Oldest first.
    pub messages: Vec<MessageView>,
    /// Pass as before_id to read further back; absent when there is nothing older here.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub older_before_id: Option<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ReadGroup {
    pub group_id: u64,
    /// One channel; all channels the account can see when omitted.
    pub channel_id: Option<u32>,
    pub before_id: Option<u64>,
    /// At most this many messages (default 30, at most 100).
    pub limit: Option<u32>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct GroupMessageView {
    pub channel_id: u32,
    #[serde(flatten)]
    pub message: MessageView,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct GroupPage {
    /// Oldest first.
    pub messages: Vec<GroupMessageView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub older_before_id: Option<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GroupId {
    pub group_id: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ChannelInfo {
    pub id: u32,
    pub name: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct RoleInfo {
    pub id: u32,
    pub name: String,
    /// Firefly permission bits granted to the role across the group.
    pub permissions: u32,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct MemberInfo {
    pub username: String,
    pub role_id: u32,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct GroupDetails {
    pub group_id: u64,
    pub name: String,
    pub description: String,
    pub channels: Vec<ChannelInfo>,
    pub roles: Vec<RoleInfo>,
    pub members: Vec<MemberInfo>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SendDirect {
    pub username: String,
    pub text: String,
    /// The id of the message this answers.
    pub reply_to: Option<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SendGroup {
    pub group_id: u64,
    /// The channel; the group's first channel when omitted.
    pub channel_id: Option<u32>,
    pub text: String,
    pub reply_to: Option<u64>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct Sent {
    pub message_id: u64,
}

/// A message in a direct conversation (`username`) or a group (`group_id`).
#[derive(Debug, Deserialize, JsonSchema)]
pub struct MessageRef {
    pub username: Option<String>,
    pub group_id: Option<u64>,
    pub message_id: u64,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct EditMessage {
    #[serde(flatten)]
    pub target: MessageRef,
    pub text: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct React {
    #[serde(flatten)]
    pub target: MessageRef,
    /// One emoji, e.g. "👍". An empty string takes this account's reaction back.
    pub emoji: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct WaitForEvents {
    /// Return events after this sequence number (the previous call's next_after_seq). 0, or
    /// omitted, returns everything since the server started.
    pub after_seq: Option<u64>,
    /// Wait at most this long for the first event (default 60, at most 600).
    pub timeout_seconds: Option<u64>,
    /// At most this many events (default 50, at most 500).
    pub limit: Option<u32>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct Events {
    pub events: Vec<Event>,
    /// Pass as after_seq next time.
    pub next_after_seq: u64,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct Search {
    pub query: String,
    /// Only the direct conversation with this user.
    pub username: Option<String>,
    /// Only this group.
    pub group_id: Option<u64>,
    /// At most this many results (default 20, at most 100).
    pub limit: Option<u32>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct SearchHit {
    pub message_id: u64,
    pub from: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel_id: Option<u32>,
    pub text: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct SearchResults {
    pub results: Vec<SearchHit>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct MessageRequest {
    pub username: String,
    /// True accepts the request; false declines it.
    pub accept: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct Done {
    pub ok: bool,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CreateGroup {
    pub name: String,
    pub description: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CreateChannel {
    pub group_id: u64,
    pub name: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ChannelCreated {
    pub channel_id: u32,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct AddMember {
    pub group_id: u64,
    pub username: String,
    /// A role id from get_group; the role with the fewest permissions when omitted.
    pub role_id: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct RemoveMember {
    pub group_id: u64,
    pub username: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CreateJoinLink {
    pub group_id: u64,
    /// Default 7 days.
    pub expires_in_seconds: Option<u64>,
    /// 0 or omitted: unlimited.
    pub max_uses: Option<u32>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct JoinLink {
    /// The invite code; the apps open it as /groups/invite/<code>.
    pub code: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct JoinViaLink {
    /// An invite code or a link ending in /groups/invite/<code>.
    pub link: String,
}

// ---- helpers ----

fn page_limit(limit: Option<u32>) -> usize {
    limit.unwrap_or(30).clamp(1, 100) as usize
}

/// Raw rows to fetch for `limit` messages: edits and reactions are rows too.
fn raw_limit(limit: usize) -> u32 {
    (limit * 3).min(100) as u32
}

fn invite_code(link: &str) -> &str {
    let link = link.trim().trim_end_matches('/');
    let path = link.split(['?', '#']).next().unwrap_or(link);
    path.rsplit('/').next().unwrap_or(path)
}

struct Extension {
    name: String,
    default_permissions: u32,
    channels: Vec<ChannelInfo>,
    roles: Vec<RoleInfo>,
    members: Vec<MemberInfo>,
}

impl FireflyMcp {
    async fn extension(&self, group_id: u64) -> Result<Extension, String> {
        let bytes = self.agent.client.get_group_extension(group_id).await.map_err(err)?;
        let ext = firefly_protos::deserialize_proto::<firefly_protos::firefly::FireflyGroupExtension>(&bytes).map_err(err)?;
        Ok(Extension {
            name: ext.name.to_string(),
            default_permissions: ext.default_permissions,
            channels: ext.channels.iter().map(|c| ChannelInfo { id: c.id, name: c.name.to_string() }).collect(),
            roles: ext
                .roles
                .iter()
                .map(|r| RoleInfo { id: r.id, name: r.name.to_string(), permissions: r.permissions })
                .collect(),
            members: ext.members.iter().map(|m| MemberInfo { username: m.username.to_string(), role_id: m.role }).collect(),
        })
    }

    async fn send_direct(&self, username: String, message: Outgoing) -> Result<u64, String> {
        let bytes = crate::content::encode_direct(message)?;
        let sent = self.agent.client.encrypt_and_send_with_type(username, bytes, 0).await.map_err(err)?;
        let id = sent.id;
        if let Err(e) = self.agent.store.insert_user_message(sent).await {
            log::warn!("failed to store a sent message: {e:#}");
        }
        Ok(id)
    }

    async fn send_group(&self, group_id: u64, channel_id: u32, message: Outgoing) -> Result<u64, String> {
        let bytes = crate::content::encode_group(channel_id, message)?;
        self.agent.client.encrypt_and_send_group_with_type(group_id, bytes, 0).await.map_err(err)
    }

    async fn first_channel(&self, group_id: u64) -> Result<u32, String> {
        let ext = self.extension(group_id).await?;
        ext.channels.first().map(|c| c.id).ok_or_else(|| "this group has no channels yet; add one with create_channel".to_string())
    }

    /// Sends an edit, delete or reaction to where the message is.
    async fn send_about(&self, target: MessageRef, message: Outgoing) -> ToolResult<Sent> {
        let message_id = match (target.username, target.group_id) {
            (Some(username), None) => self.send_direct(username, message).await?,
            (None, Some(group_id)) => {
                let original = self
                    .agent
                    .client
                    .group_message_store()
                    .get_message(group_id, target.message_id)
                    .await
                    .map_err(err)?
                    .ok_or_else(|| format!("no message {} in group {group_id} on this device", target.message_id))?;
                let (channel_id, _) = decode_group(&original.message);
                self.send_group(group_id, channel_id, message).await?
            }
            _ => return Err("give either username or group_id".into()),
        };
        Ok(Json(Sent { message_id }))
    }
}

#[tool_router(router = tool_router)]
impl FireflyMcp {
    #[tool(description = "This account's username and connection state.")]
    async fn whoami(&self, _: Parameters<Empty>) -> ToolResult<WhoAmI> {
        Ok(Json(WhoAmI {
            username: self.agent.username.clone(),
            connection: format!("{:?}", self.agent.client.get_connection_state()),
            device_id: self.agent.client.get_device_id().await,
        }))
    }

    #[tool(description = "Direct conversations (latest first, with unread counts and message requests) and the groups this account is in.")]
    async fn list_conversations(&self, _: Parameters<Empty>) -> ToolResult<Conversations> {
        let latest = self.agent.store.get_last_message_from_all_conversations().await.map_err(err)?;
        let accepted: std::collections::HashSet<String> =
            self.agent.client.accepted_users().await.unwrap_or_default().into_iter().collect();
        let mut direct: Vec<DirectConversation> = latest
            .into_iter()
            .map(|c| {
                let m = c.message;
                let preview = match decode_direct(&m.message) {
                    crate::content::Content::Message { text, .. } | crate::content::Content::Edit { text, .. } => text,
                    crate::content::Content::Delete { .. } => "(deleted a message)".into(),
                    crate::content::Content::Reaction { emoji, .. } => format!("(reacted {emoji})"),
                    crate::content::Content::Call => "(call)".into(),
                    crate::content::Content::Hidden => String::new(),
                };
                DirectConversation {
                    accepted: accepted.contains(&m.other),
                    unread: c.count,
                    last_message_from: if m.sent_by_other { m.other.clone() } else { self.agent.username.clone() },
                    username: m.other,
                    last_message_id: m.id,
                    last_message_preview: preview.chars().take(200).collect(),
                }
            })
            .collect();
        direct.sort_by_key(|c| std::cmp::Reverse(c.last_message_id));
        let groups = self
            .agent
            .client
            .get_local_group_infos()
            .await
            .map_err(err)?
            .into_iter()
            .filter(|g| !g.pending)
            .map(|g| GroupSummary { group_id: g.id, name: g.name, description: g.description })
            .collect();
        Ok(Json(Conversations { direct, groups }))
    }

    #[tool(description = "Read a direct conversation, oldest first, with edits, deletes and reactions applied. Reading the latest page marks the conversation read.")]
    async fn read_direct_messages(&self, Parameters(p): Parameters<ReadDirect>) -> ToolResult<DirectPage> {
        let limit = page_limit(p.limit);
        let before = p.before_id.unwrap_or(i64::MAX as u64).min(i64::MAX as u64) as i64;
        let rows = self.agent.store.get_last_messages_of(&p.username, before, raw_limit(limit) as i64).await.map_err(err)?;
        let older = (rows.len() >= raw_limit(limit) as usize).then(|| rows.iter().map(|m| m.id).min()).flatten();
        let newest = rows.iter().map(|m| m.id).max();
        let raws = rows
            .into_iter()
            .filter(|m| m.message_type & firefly_protos::MESSAGE_TYPE_HIDDEN == 0)
            .map(|m| Raw {
                id: m.id,
                from: if m.sent_by_other { m.other.clone() } else { self.agent.username.clone() },
                content: decode_direct(&m.message),
            })
            .collect();
        let mut messages = fold(raws);
        let skip = messages.len().saturating_sub(limit);
        messages.drain(..skip);
        if p.before_id.is_none()
            && let Some(newest) = newest
        {
            let _ = self.agent.store.mark_as_read_until(&p.username, newest as i64).await;
            let _ = self.agent.client.read_user_messages_upto(p.username.clone(), newest).await;
        }
        Ok(Json(DirectPage { messages, older_before_id: older }))
    }

    #[tool(description = "Read a group's messages, oldest first, with edits, deletes and reactions applied: one channel, or every channel this account can see.")]
    async fn read_group_messages(&self, Parameters(p): Parameters<ReadGroup>) -> ToolResult<GroupPage> {
        let limit = page_limit(p.limit);
        let before = p.before_id.unwrap_or(i64::MAX as u64);
        let store = self.agent.client.group_message_store();
        let rows = match p.channel_id {
            Some(channel) => store.get_channel_page(p.group_id, channel, before, raw_limit(limit)).await,
            None => store.get(p.group_id, before, raw_limit(limit)).await,
        }
        .map_err(err)?;
        let older = (rows.len() >= raw_limit(limit) as usize).then(|| rows.iter().map(|m| m.id).min()).flatten();
        let mut channels = HashMap::new();
        let raws = rows
            .into_iter()
            .map(|m| {
                let (channel_id, content) = decode_group(&m.message);
                channels.insert(m.id, channel_id);
                Raw { id: m.id, from: m.by, content }
            })
            .collect();
        let mut messages: Vec<GroupMessageView> = fold(raws)
            .into_iter()
            .map(|message| GroupMessageView { channel_id: channels.get(&message.id).copied().unwrap_or(0), message })
            .collect();
        let skip = messages.len().saturating_sub(limit);
        messages.drain(..skip);
        Ok(Json(GroupPage { messages, older_before_id: older }))
    }

    #[tool(description = "A group's name, channels, roles and members.")]
    async fn get_group(&self, Parameters(p): Parameters<GroupId>) -> ToolResult<GroupDetails> {
        let ext = self.extension(p.group_id).await?;
        let description = self.agent.client.group_info_store().get(p.group_id).await.map(|g| g.description).unwrap_or_default();
        Ok(Json(GroupDetails {
            group_id: p.group_id,
            name: ext.name,
            description,
            channels: ext.channels,
            roles: ext.roles,
            members: ext.members,
        }))
    }

    #[tool(description = "Send a direct message. Messaging someone accepts their message request.")]
    async fn send_direct_message(&self, Parameters(p): Parameters<SendDirect>) -> ToolResult<Sent> {
        let message_id = self.send_direct(p.username, Outgoing::Message { text: p.text, reply_to: p.reply_to }).await?;
        Ok(Json(Sent { message_id }))
    }

    #[tool(description = "Send a message to a group channel.")]
    async fn send_group_message(&self, Parameters(p): Parameters<SendGroup>) -> ToolResult<Sent> {
        let channel_id = match p.channel_id {
            Some(c) => c,
            None => self.first_channel(p.group_id).await?,
        };
        let message_id = self.send_group(p.group_id, channel_id, Outgoing::Message { text: p.text, reply_to: p.reply_to }).await?;
        Ok(Json(Sent { message_id }))
    }

    #[tool(description = "Change the text of a message this account sent (give username for a direct conversation, or group_id).")]
    async fn edit_message(&self, Parameters(p): Parameters<EditMessage>) -> ToolResult<Sent> {
        let of = p.target.message_id;
        self.send_about(p.target, Outgoing::Edit { of, text: p.text }).await
    }

    #[tool(description = "Delete a message this account sent, for everyone (give username for a direct conversation, or group_id).")]
    async fn delete_message(&self, Parameters(p): Parameters<MessageRef>) -> ToolResult<Sent> {
        let of = p.message_id;
        self.send_about(p, Outgoing::Delete { of }).await
    }

    #[tool(description = "React to a message with an emoji, or take this account's reaction back with an empty emoji.")]
    async fn react(&self, Parameters(p): Parameters<React>) -> ToolResult<Sent> {
        let to = p.target.message_id;
        self.send_about(p.target, Outgoing::Reaction { to, emoji: p.emoji }).await
    }

    #[tool(description = "Wait for new direct messages, group messages and group changes. Call it in a loop, passing next_after_seq back as after_seq.")]
    async fn wait_for_events(&self, Parameters(p): Parameters<WaitForEvents>) -> ToolResult<Events> {
        // A cursor from before this server restarted is ahead of the new log: start over.
        let after = p.after_seq.unwrap_or(0);
        let after = if after > self.agent.events.latest() { 0 } else { after };
        let timeout = Duration::from_secs(p.timeout_seconds.unwrap_or(60).min(600));
        let limit = p.limit.unwrap_or(50).clamp(1, 500) as usize;
        let events = self.agent.events.wait_after(after, limit, timeout).await;
        let next_after_seq = events.last().map_or(after, |e| e.seq);
        Ok(Json(Events { events, next_after_seq }))
    }

    #[tool(description = "Full-text search over the messages on this device.")]
    async fn search_messages(&self, Parameters(p): Parameters<Search>) -> ToolResult<SearchResults> {
        let scope = match (p.username, p.group_id) {
            (Some(other), _) => SearchScope::User { other: Some(other) },
            (None, Some(group_id)) => SearchScope::Group { group_id: Some(group_id), channel_id: None },
            (None, None) => SearchScope::All,
        };
        let results = self
            .agent
            .client
            .search_messages(p.query, scope, p.limit.unwrap_or(20).clamp(1, 100), 0)
            .await
            .map_err(err)?
            .into_iter()
            .map(|r| SearchHit {
                message_id: r.message_id,
                from: r.by,
                username: r.other,
                group_id: r.group_id,
                channel_id: r.channel_id,
                text: r.text,
            })
            .collect();
        Ok(Json(SearchResults { results }))
    }

    #[tool(description = "Accept or decline someone's message request.")]
    async fn respond_to_message_request(&self, Parameters(p): Parameters<MessageRequest>) -> ToolResult<Done> {
        self.agent.client.set_user_accepted(p.username, p.accept).await.map_err(err)?;
        Ok(Json(Done { ok: true }))
    }

    #[tool(description = "Create a group owned by this account.")]
    async fn create_group(&self, Parameters(p): Parameters<CreateGroup>) -> ToolResult<GroupSummary> {
        let group = self.agent.client.create_group(p.name, p.description.unwrap_or_default(), 0).await.map_err(err)?;
        Ok(Json(GroupSummary { group_id: group.id, name: group.name, description: group.description }))
    }

    #[tool(description = "Add a text channel to a group (needs the group's permission to manage channels). New groups have none until one is added.")]
    async fn create_channel(&self, Parameters(p): Parameters<CreateChannel>) -> ToolResult<ChannelCreated> {
        // As the apps do: the next free id, a text channel, the group's default permissions.
        let ext = self.extension(p.group_id).await?;
        let channel_id = ext.channels.iter().map(|c| c.id).max().unwrap_or(0) + 1;
        self.agent
            .client
            .update_group_channel(p.group_id, channel_id, false, p.name, TEXT_CHANNEL, ext.default_permissions)
            .await
            .map_err(err)?;
        Ok(Json(ChannelCreated { channel_id }))
    }

    #[tool(description = "Add someone to a group (needs the group's permission to manage members).")]
    async fn add_group_member(&self, Parameters(p): Parameters<AddMember>) -> ToolResult<Done> {
        let role_id = match p.role_id {
            Some(role) => role,
            None => self
                .extension(p.group_id)
                .await?
                .roles
                .iter()
                .min_by_key(|r| (r.permissions.count_ones(), r.id))
                .map(|r| r.id)
                .ok_or_else(|| "this group has no roles".to_string())?,
        };
        self.agent.client.add_group_member(p.group_id, p.username, role_id).await.map_err(err)?;
        Ok(Json(Done { ok: true }))
    }

    #[tool(description = "Remove someone from a group (needs the group's permission to manage members).")]
    async fn remove_group_member(&self, Parameters(p): Parameters<RemoveMember>) -> ToolResult<Done> {
        self.agent.client.kick_group_member(p.group_id, p.username).await.map_err(err)?;
        Ok(Json(Done { ok: true }))
    }

    #[tool(description = "Leave a group.")]
    async fn leave_group(&self, Parameters(p): Parameters<GroupId>) -> ToolResult<Done> {
        self.agent.client.leave_group(p.group_id).await.map_err(err)?;
        Ok(Json(Done { ok: true }))
    }

    #[tool(description = "Create an invite code for a group.")]
    async fn create_join_link(&self, Parameters(p): Parameters<CreateJoinLink>) -> ToolResult<JoinLink> {
        let code = self
            .agent
            .client
            .create_join_link(p.group_id, p.expires_in_seconds.unwrap_or(7 * 86400), p.max_uses.unwrap_or(0))
            .await
            .map_err(err)?;
        Ok(Json(JoinLink { code }))
    }

    #[tool(description = "Join a group with an invite code or link.")]
    async fn join_group_via_link(&self, Parameters(p): Parameters<JoinViaLink>) -> ToolResult<Done> {
        self.agent.client.join_via_link(invite_code(&p.link)).await.map_err(err)?;
        Ok(Json(Done { ok: true }))
    }
}

#[cfg(test)]
mod tests {
    use super::invite_code;

    #[test]
    fn invite_codes_come_from_links_or_bare_codes() {
        assert_eq!(invite_code("abc123"), "abc123");
        assert_eq!(invite_code("https://lupyd.com/groups/invite/abc123/"), "abc123");
        assert_eq!(invite_code("lupyd://m.lupyd.com/groups/invite/abc123?x=1"), "abc123");
    }
}
