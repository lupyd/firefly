# Favourite Messages Store Implementation Plan

Design and implement a persistent, secure, and performant store for saving and managing favourite (starred/saved) messages across 1:1 user messages and group/channel messages, with full client-side `SeeMessage` permission enforcement, in-memory trait abstraction, and library API exposure.

## Architecture & Design

### 1. Data Model (`FavouriteMessage`)
A unified favourite message representation that supports both 1:1 user messages and group/channel messages:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FavouriteSource {
    User,
    Group,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FavouriteMessage {
    pub id: u64,                  // Unique favourite record ID
    pub source: FavouriteSource,  // User or Group
    pub message_id: u64,          // Original message ID
    pub other: Option<String>,    // Contact username (for 1:1 user messages)
    pub group_id: Option<u64>,    // Group ID (for group messages)
    pub channel_id: Option<u32>,  // Channel ID (for group messages)
    pub by: String,               // Sender of the message
    pub text: String,             // Plaintext preview/content
    pub message: Vec<u8>,         // Original payload bytes
    pub message_type: u32,        // Bitflags / message type
    pub epoch: Option<u32>,       // Group epoch (for group messages)
    pub created_at: u64,          // Timestamp when favourited
}
```

### 2. SQLite Database Schema (`favourite_messages`)
Stored in SQLite via `FavouriteMessagesStore`:
```sql
CREATE TABLE IF NOT EXISTS favourite_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    message_id INTEGER NOT NULL,
    other TEXT,
    group_id INTEGER,
    channel_id INTEGER,
    by TEXT NOT NULL,
    text TEXT NOT NULL DEFAULT '',
    message BLOB NOT NULL,
    message_type INTEGER NOT NULL DEFAULT 0,
    epoch INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS favourite_messages_user_uniq
    ON favourite_messages (source, message_id, other)
    WHERE source = 'user';

CREATE UNIQUE INDEX IF NOT EXISTS favourite_messages_group_uniq
    ON favourite_messages (source, message_id, group_id)
    WHERE source = 'group';

CREATE INDEX IF NOT EXISTS favourite_messages_created_idx
    ON favourite_messages (created_at DESC);
```

### 3. In-Memory Trait Abstraction (`FavouriteMessageStorage`)
In `crates/client/src/storage.rs`:
- Define `pub trait FavouriteMessageStorage: Send + Sync` with core CRUD methods.
- Implement `MemoryFavouriteMessageStore` for WASM, unit testing, and memory storage.
- Implement `FavouriteMessageStorage` on `FavouriteMessagesStore` in `crates/client/src/db/favourites.rs`.

### 4. Client-Side Security & Permission Enforcement (`SeeMessage`)
- Like `GroupMessagesStore`, `FavouriteMessagesStore` supports `with_read_access(...)`.
- When fetching favourite group messages, results pass through `can_see_channel(group_id, channel_id)` with memoized channel permissions to ensure messages in revoked/unauthorized channels are excluded.

### 5. API Exposure via Library
- **`crates/client/src/lib.rs`**: Expose `favourites` module re-exporting `FavouriteMessage`, `FavouriteSource`, `FavouriteMessageStorage`, `MemoryFavouriteMessageStore`, and `FavouriteMessagesStore`.
- **`crates/client/src/websocket.rs`**:
  - Add `favourite_messages_store: FavouriteMessagesStore` to `FireflyWsClient`.
  - Expose `pub fn favourite_messages_store(&self) -> FavouriteMessagesStore` on `FireflyWsClient` and `FfiFireflyWsClient`.
  - Expose convenient helper methods:
    - `add_user_message_favourite`
    - `add_group_message_favourite`
    - `remove_user_favourite`
    - `remove_group_favourite`
    - `is_user_favourite`
    - `is_group_favourite`
    - `get_favourites`
    - `get_user_favourites`
    - `get_group_favourites`

---

## Verification Plan
1. Unit tests for `MemoryFavouriteMessageStore` in `storage.rs`.
2. Unit tests for `FavouriteMessagesStore` in `crates/client/src/db/favourites.rs`.
3. Integration tests in `crates/client/tests/favourites_test.rs` covering:
   - Adding and retrieving 1:1 user favourite messages.
   - Adding and retrieving group favourite messages.
   - Idempotency & uniqueness (favouriting same message multiple times).
   - Removing favourites and verifying existence checks.
   - Filter by user / group / channel / all.
   - Permission enforcement (revoked `SeeMessage` hides favourite group messages).
4. `cargo check` and `cargo check -p firefly-client-node --target wasm32-unknown-unknown`.
