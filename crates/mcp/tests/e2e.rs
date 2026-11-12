//! Two firefly-mcp servers, each a device of its own account, talking through a firefly-mls
//! server in emulator mode (the token is the username; see `FIREFLY_BASE_URL` in .env.test).
//! Each is driven by an MCP client over an in-memory pipe, the way an agent harness drives it.

use std::sync::Arc;
use std::time::{Duration, Instant};

use firefly_mcp::agent::{Agent, Endpoints};
use firefly_mcp::auth::{Auth, AuthConfig, Session};
use firefly_mcp::server::FireflyMcp;
use rmcp::model::CallToolRequestParams;
use rmcp::service::RunningService;
use rmcp::{RoleClient, ServiceExt};
use serde_json::{Value, json};

fn endpoints() -> Endpoints {
    dotenv::from_filename(concat!(env!("CARGO_MANIFEST_DIR"), "/../../.env.test")).ok();
    let api_url = std::env::var("FIREFLY_BASE_URL").expect("FIREFLY_BASE_URL (a firefly-mls server in emulator mode)");
    let ws_url = std::env::var("FIREFLY_WS_URL").unwrap_or_else(|_| api_url.replace("http://", "ws://"));
    Endpoints { api_url, ws_url, wt_url: None }
}

struct Peer {
    username: String,
    agent: Arc<Agent>,
    client: RunningService<RoleClient, ()>,
    cursor: u64,
    seen: Vec<Value>,
}

impl Peer {
    async fn start(prefix: &str, dir: &std::path::Path) -> Self {
        let username = format!("{prefix}_{}", rand::random::<u32>() % 1_000_000_000);
        let data_dir = dir.join(&username);
        std::fs::create_dir_all(&data_dir).unwrap();
        // Emulator mode takes the username as the token; it never needs renewing.
        let session = Session {
            email: format!("{username}@example.com"),
            username: username.clone(),
            access_token: username.clone(),
            refresh_token: "unused".into(),
            expires_at: u64::MAX / 2,
        };
        let path = data_dir.join("session.json");
        std::fs::write(&path, serde_json::to_vec(&session).unwrap()).unwrap();
        let config = AuthConfig {
            domain: "dummy.auth0.com".into(),
            client_id: String::new(),
            audience: String::new(),
            realm: String::new(),
        };
        let auth = Arc::new(Auth::load(config, path).unwrap());
        let agent = Agent::start(auth, &endpoints(), data_dir, "firefly-mcp-test").await.unwrap();
        let (server_io, client_io) = tokio::io::duplex(1 << 16);
        let server = FireflyMcp::new(agent.clone(), "firefly-mcp-test");
        tokio::spawn(async move {
            if let Ok(service) = server.serve(server_io).await {
                let _ = service.waiting().await;
            }
        });
        let client = ().serve(client_io).await.unwrap();
        let peer = Self { username, agent, client, cursor: 0, seen: Vec::new() };
        let deadline = Instant::now() + Duration::from_secs(60);
        while peer.call("whoami", json!({})).await["connection"] != "Connected" {
            assert!(Instant::now() < deadline, "{} never connected", peer.username);
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        peer
    }

    async fn try_call(&self, tool: &str, args: Value) -> Result<Value, String> {
        let params = CallToolRequestParams::new(tool.to_string()).with_arguments(args.as_object().unwrap().clone());
        let result = self.client.call_tool(params).await.map_err(|e| e.to_string())?;
        let text = || result.content.iter().filter_map(|c| c.as_text()).map(|t| t.text.clone()).collect::<Vec<_>>().join(" ");
        if result.is_error == Some(true) {
            return Err(text());
        }
        Ok(result.structured_content.clone().unwrap_or(Value::Null))
    }

    async fn call(&self, tool: &str, args: Value) -> Value {
        match self.try_call(tool, args).await {
            Ok(value) => value,
            Err(e) => panic!("{} {tool}: {e}", self.username),
        }
    }

    /// Follows wait_for_events until an event matches.
    async fn event(&mut self, matches: impl Fn(&Value) -> bool) -> Value {
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            if let Some(found) = self.seen.iter().find(|e| matches(e)) {
                return found.clone();
            }
            assert!(Instant::now() < deadline, "{}: no matching event; saw {:?}", self.username, self.seen);
            let page = self.call("wait_for_events", json!({ "after_seq": self.cursor, "timeout_seconds": 10 })).await;
            self.seen.extend(page["events"].as_array().unwrap().iter().cloned());
            self.cursor = page["next_after_seq"].as_u64().unwrap();
        }
    }

    async fn stop(self) {
        let _ = self.client.cancel().await;
        self.agent.dispose().await;
    }
}

fn by_id(messages: &Value, id: &Value) -> Value {
    messages["messages"].as_array().unwrap().iter().find(|m| m["id"] == *id).cloned().unwrap_or(Value::Null)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn agents_chat_directly_and_in_groups() {
    let _ = env_logger::builder().is_test(true).try_init();
    let dir = tempdir();
    let mut alice = Peer::start("mcp_alice", &dir).await;
    let mut bob = Peer::start("mcp_bob", &dir).await;

    // A direct message is a request until answered.
    let hello = alice.call("send_direct_message", json!({ "username": bob.username, "text": "hello bob" })).await;
    let heard = bob.event(|e| e["type"] == "direct" && e["text"] == "hello bob").await;
    assert_eq!(heard["from"], alice.username.as_str());
    assert_eq!(heard["message_id"], hello["message_id"]);
    let conversations = bob.call("list_conversations", json!({})).await;
    let request = conversations["direct"].as_array().unwrap().iter().find(|c| c["username"] == alice.username.as_str()).unwrap();
    assert_eq!(request["accepted"], false);
    assert_eq!(request["unread"], 1);

    // Replying accepts; reactions and edits arrive as events and fold into the conversation.
    let reply = bob
        .call("send_direct_message", json!({ "username": alice.username, "text": "hi alice", "reply_to": hello["message_id"] }))
        .await;
    bob.call("react", json!({ "username": alice.username, "message_id": hello["message_id"], "emoji": "👍" })).await;
    bob.call("edit_message", json!({ "username": alice.username, "message_id": reply["message_id"], "text": "hi alice!" })).await;
    alice.event(|e| e["type"] == "direct" && e["kind"] == "edit" && e["text"] == "hi alice!").await;
    let page = alice.call("read_direct_messages", json!({ "username": bob.username })).await;
    let theirs = by_id(&page, &reply["message_id"]);
    assert_eq!(theirs["text"], "hi alice!");
    assert_eq!(theirs["edited"], true);
    assert_eq!(theirs["reply_to"], hello["message_id"]);
    assert_eq!(by_id(&page, &hello["message_id"])["reactions"], json!([{ "emoji": "👍", "by": [bob.username] }]));
    let conversations = bob.call("list_conversations", json!({})).await;
    assert_eq!(conversations["direct"][0]["accepted"], true);

    // Only the sender can delete; the deletion reaches the other side.
    bob.call("delete_message", json!({ "username": alice.username, "message_id": reply["message_id"] })).await;
    alice.event(|e| e["type"] == "direct" && e["kind"] == "delete").await;
    let page = alice.call("read_direct_messages", json!({ "username": bob.username })).await;
    assert_eq!(by_id(&page, &reply["message_id"])["deleted"], true);

    // Groups: alice adds bob, who hears about it, reads the members and talks in the channel.
    let group = alice.call("create_group", json!({ "name": "mcp test" })).await;
    let group_id = group["group_id"].clone();
    alice.call("add_group_member", json!({ "group_id": group_id, "username": bob.username })).await;
    bob.event(|e| e["type"] == "added_to_group" && e["group_id"] == group_id).await;
    let details = bob.call("get_group", json!({ "group_id": group_id })).await;
    let mut members: Vec<String> =
        details["members"].as_array().unwrap().iter().map(|m| m["username"].as_str().unwrap().to_string()).collect();
    members.sort();
    let mut expected = vec![alice.username.clone(), bob.username.clone()];
    expected.sort();
    assert_eq!(members, expected);

    // New groups have no channels until a member adds one.
    let channel = alice.call("create_channel", json!({ "group_id": group_id, "name": "general" })).await;
    let details = alice.call("get_group", json!({ "group_id": group_id })).await;
    assert_eq!(details["channels"], json!([{ "id": channel["channel_id"], "name": "general" }]));
    alice.call("send_group_message", json!({ "group_id": group_id, "text": "welcome bob" })).await;
    let said = bob.event(|e| e["type"] == "group" && e["text"] == "welcome bob").await;
    bob.call(
        "send_group_message",
        json!({ "group_id": group_id, "channel_id": said["channel_id"], "text": "thanks", "reply_to": said["message_id"] }),
    )
    .await;
    alice.event(|e| e["type"] == "group" && e["text"] == "thanks").await;
    let page = bob.call("read_group_messages", json!({ "group_id": group_id })).await;
    let texts: Vec<&str> = page["messages"].as_array().unwrap().iter().map(|m| m["text"].as_str().unwrap()).collect();
    assert!(texts.contains(&"welcome bob") && texts.contains(&"thanks"), "{page}");

    // Leaving: bob is no longer a member on alice's side, and the group goes from his list.
    // (Alice's device commits his removal itself, so she sees it as a group update.)
    bob.call("leave_group", json!({ "group_id": group_id })).await;
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let details = alice.call("get_group", json!({ "group_id": group_id })).await;
        let members = details["members"].as_array().unwrap();
        if !members.iter().any(|m| m["username"] == bob.username.as_str()) {
            break;
        }
        assert!(Instant::now() < deadline, "bob is still a member: {details}");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let deadline = Instant::now() + Duration::from_secs(90);
    while bob.call("list_conversations", json!({})).await["groups"].as_array().unwrap().iter().any(|g| g["group_id"] == group_id) {
        assert!(Instant::now() < deadline, "the group is still in bob's list");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // A second server on the same data directory refuses to start.
    let second = Agent::start(
        Arc::new(Auth::load(
            AuthConfig { domain: "dummy.auth0.com".into(), client_id: String::new(), audience: String::new(), realm: String::new() },
            dir.join(&bob.username).join("session.json"),
        )
        .unwrap()),
        &endpoints(),
        dir.join(&bob.username),
        "firefly-mcp-test",
    )
    .await;
    assert!(second.err().is_some_and(|e| e.to_string().contains("already running")));

    alice.stop().await;
    bob.stop().await;
}

fn tempdir() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("firefly-mcp-e2e-{}", rand::random::<u64>()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}
