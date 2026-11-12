# firefly-mcp

A local [MCP](https://modelcontextprotocol.io) server that signs in to Firefly as an ordinary user, so an LLM can take part in chats: read and answer direct messages, and talk in the groups people add it to.

It doesn't segregate agents. The agent is an account like anyone's, with its own email and password. People message it or add it to their groups, and the group's members decide its role and permissions. What the agent does with the chats is up to the LLM and the harness running it.

Everything runs on your machine. firefly-mcp is a device of the account, like a phone: it holds the account's encryption keys, decrypts messages locally and keeps them in its data directory. Nothing is hosted, and the Firefly server never sees the messages.

Lupyd ships it preconfigured as `lupyd-mcp` (`lupyd-desktop/crates/mcp`). Use that for Lupyd; `firefly-mcp` needs the endpoints passed in.

## Setup

1. **Create the agent's account** the way anyone signs up (in the app or on the web), with an email and password, and choose its username.
2. **Sign it in once:**
   ```bash
   lupyd-mcp login --email helper@example.com      # asks for the password
   ```
   `FIREFLY_MCP_PASSWORD` supplies the password non-interactively. The sign-in is kept in the data directory (owner-only permissions) and renews itself.
3. **Let your MCP client start it.** For example, in Claude Desktop's `claude_desktop_config.json`, Claude Code's `.mcp.json`, or any harness that launches stdio servers:
   ```json
   {
     "mcpServers": {
       "lupyd": { "command": "lupyd-mcp", "args": ["serve"] }
     }
   }
   ```

Several agents on one machine: give each a profile (`--profile helper`), or a data directory (`--data-dir`), on both `login` and `serve`.

## Tools

| Tool | |
|---|---|
| `whoami` | The account's username and connection state |
| `list_conversations` | Direct conversations (unread counts, message requests) and groups |
| `read_direct_messages` | A direct conversation, oldest first, with edits, deletes and reactions applied; reading the latest page marks it read |
| `read_group_messages` | A group channel (or all channels), the same way |
| `get_group` | A group's channels, roles and members |
| `wait_for_events` | Long-polls for new messages and group changes; loop on it, passing `next_after_seq` back |
| `send_direct_message`, `send_group_message` | Send, optionally as a reply |
| `edit_message`, `delete_message`, `react` | On the account's own messages / any message |
| `search_messages` | Full-text search over the messages on this device |
| `respond_to_message_request` | Accept or decline a message request (replying also accepts) |
| `create_group`, `create_channel`, `add_group_member`, `remove_group_member`, `leave_group`, `create_join_link`, `join_group_via_link` | Groups and membership, as far as the group's permissions allow (a new group has no channels until one is added) |

Message ids are the server's time in microseconds. Attachments show as a count; their contents aren't available.

## Commands and settings

| Command | |
|---|---|
| `login --email E` | Sign in (password asked for, read from stdin, or `FIREFLY_MCP_PASSWORD`) |
| `serve` | The MCP server on stdin/stdout. Logs go to stderr (`RUST_LOG`) |
| `status` | Prints the signed-in username |
| `logout` | Forgets the sign-in; the device keys stay, so logging in again keeps the groups |

| Flag | Environment | |
|---|---|---|
| `--data-dir` | `FIREFLY_MCP_DATA_DIR` | Default `<user data dir>/<lupyd-mcp or firefly-mcp>/<profile>` |
| `--profile` | `FIREFLY_MCP_PROFILE` | Default `default` |
| `--api-url`, `--ws-url`, `--wt-url` | `FIREFLY_API_URL`, `FIREFLY_WS_URL`, `FIREFLY_WT_URL` | Firefly endpoints; `--wt-url none` turns WebTransport off |
| `--auth0-domain`, `--auth0-client-id`, `--auth0-audience`, `--auth0-realm` | `AUTH0_DOMAIN`, … | The tenant the server trusts |

Exit code 78 means the account must sign in again: a wrong password, or a revoked or expired refresh token.

## Tests

`tests/e2e.rs` runs two servers against a firefly-mls in emulator mode (CI's `cargo test` step):

```bash
EMULATOR_MODE=true FIREFLY_BASE_URL=http://127.0.0.1:39209 cargo test -p firefly-mcp --features firefly-client/test-hooks
```

lupyd-testsuite's `37_mcp_agent` drives `lupyd-mcp` against the Lupyd apps.

## Look after the data directory

- It holds the device's keys and the decrypted messages. Keep it private and back it up. If it's lost, the account comes back as a new device and must be added to its groups again.
- Only one `serve` can run per data directory; a second one refuses to start.
- The LLM sees whatever the account can read, and the messages it reads go to whichever model you run. People in your groups can see the agent's account but not where its model runs. Tell them.
