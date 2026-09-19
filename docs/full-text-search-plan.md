# SQLite Full-Text Search (FTS5) Implementation Plan

Implement an efficient, production-ready Full-Text Search (FTS) engine in SQLite using FTS5 for `user_messages` (1:1 conversations) and `group_messages` (groups and channels), supporting scoped search (by user, group, channel) and unified app-wide search, with automated migration, zero data loss, and client-side `SeeMessage` permission enforcement.

## Architecture & Design

### 1. FTS5 External Content Virtual Tables
To ensure maximum storage efficiency without duplicate text copies, SQLite FTS5 external content tables are used:
- `group_messages_fts`: `USING fts5(text, content='group_messages', content_rowid='rowid')`
- `user_messages_fts`: `USING fts5(text, content='user_messages', content_rowid='rowid')`

### 2. Automatic Sync Triggers
SQLite triggers automatically sync inserts, updates, and deletes between the base table and FTS5 virtual tables:
- `AFTER INSERT`: `INSERT INTO table_fts(rowid, text) VALUES (new.rowid, new.text);`
- `AFTER DELETE`: `INSERT INTO table_fts(table_fts, rowid, text) VALUES('delete', old.rowid, old.text);`
- `AFTER UPDATE`: deletes old entry and inserts new entry into FTS5.

### 3. Backwards Compatibility & Automated Migration
Existing database files without `text` columns or FTS tables will be automatically upgraded:
1. `ALTER TABLE group_messages ADD COLUMN text TEXT NOT NULL DEFAULT ''` (and same for `user_messages`).
2. FTS5 virtual tables and triggers are created idempotently (`CREATE ... IF NOT EXISTS`).
3. Backfill migration checks for rows where `text = ''` and `length(message) > 0`. It decodes the message payload (extracting text from `GroupMessageInner` / `UserMessageInner` protobuf or UTF-8 fallback) and updates `text`.
4. Executes `INSERT INTO ..._fts(..._fts) VALUES('rebuild');` to index any backfilled records.
5. All future writes populate `text` directly, keeping FTS in sync via triggers.

### 4. Search Scopes
- **User messages**: Search within 1:1 messages, across all contacts or optionally filtered by `other` (contact).
- **Group messages**: Search across all groups, or scoped to a specific `group_id`.
- **Channel messages**: Search scoped to a specific `channel_id` within a `group_id`.
- **App-wide unified search**: Executes a unified query across both user and group messages, returning ranked results with snippet highlighting (`<b>...</b>`).

### 5. Security & Permission Enforcement
Group and channel search results will pass through `visible_messages` / `can_see_message(channel_id)` when the store is initialized with `read_access`. Messages in channels where `SeeMessage` has been revoked or is not granted are excluded from search results.
Performance optimization: Memoize `(group_id, channel_id) -> bool` checks within a search request to prevent redundant MLS group deserialization.

---

## Proposed Code Changes

1. **`crates/client/src/db/messages.rs`**:
   - Fix schema initialization: replace `sql.split(';')` with `conn.execute(...)` or `pool.execute(...)` to preserve trigger semicolons and prevent syntax errors.
   - Verify `text` column migration and backfill.
   - Add unit tests for `search`.

2. **`crates/client/src/db/group_messages.rs`**:
   - Memoize channel permission checks in `search` for high performance.
   - Add unit tests for `search`.

3. **`crates/client/src/db/search.rs`**:
   - Update `SearchEngine` struct to optionally hold `GroupMessagesStore`.
   - Implement `SearchEngine::with_group_messages_store(pool, group_messages_store)`.
   - Integrate `can_see_message` permission filtering for `SearchScope::All` and `SearchScope::Group` results.
   - Enhance `sanitize_fts5_query` to handle special punctuation and syntax characters.
   - Add convenience methods: `search_all`, `search_user`, `search_group`.

4. **`crates/client/src/websocket.rs`**:
   - Add `messages_store: MessagesStore` field to `FireflyWsClient`.
   - Expose `messages_store(&self) -> MessagesStore` on `FireflyWsClient` and `FfiFireflyWsClient`.
   - Expose `search_messages`, `search_user_messages`, and `search_group_messages` on `FireflyWsClient` and `FfiFireflyWsClient`.

5. **`crates/client/tests/search_test.rs`**:
   - Migration test (unmigrated DB -> backfill -> FTS search).
   - 1:1 user message search (all vs scoped).
   - Group message search (all vs group vs channel).
   - App-wide unified search (`SearchScope::All`).
   - Permission check (`SeeMessage` enforcement).
   - Trigger synchronization test (insert, update, delete).

---

## Verification Plan

1. Unit tests: `cargo test -p firefly-client --lib -- db::`
2. Integration tests: `cargo test -p firefly-client --test search_test`
3. Client compilation: `cargo check` and `cargo check -p firefly-client-node --target wasm32-unknown-unknown`
