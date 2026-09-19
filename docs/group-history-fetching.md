# Group History Fetching with Encrypted Chunks & Single-Disagreement Verification

We have implemented a secure, distributed group history fetching mechanism that allows group members (especially newly joined members) to fetch past group messages packed into compressed, encrypted chunks without re-encrypting past messages individually.

## Architecture & Security Properties

1. **Chunk Boundaries & Single-Worker Locks**:
   - A member requests a message range (`CreateHistoryRequest`).
   - Any online member claims a lease (`ClaimHistoryRequest`) via atomic row locking on `group_history_chunk_locks` (`FOR UPDATE`) with a 60-second timeout, preventing duplicate work.
2. **End-to-End Encryption, CDN Storage & Integrity**:
   - The worker packs messages in chunks of 50 (`DEFAULT_CHUNK_SIZE`), compresses with Gzip/Deflate, generates a random 32-byte AES-256-GCM key and 12-byte nonce, and computes an unencrypted SHA-256 hash.
   - **Direct CDN Upload (Zero DB Blob Storage)**: The worker uploads the encrypted chunk directly via HTTP `PUT` to `CDN_URL/group_chunks/<randomID>` (`../lupyd-cdn`). Chunks are **never** stored in PostgreSQL or relayed as blobs through the Firefly server.
   - Only the metadata — unencrypted SHA-256 hash, direct CDN link, message range, and group ID — is published to PostgreSQL (`group_history_chunks`). **Neither the Firefly server nor the CDN ever learns the encryption keys or plaintext content.**
3. **Key Sharing via MLS Group**:
   - The worker shares the chunk encryption keys directly inside the MLS group chat using a hidden message (`MESSAGE_TYPE_HIDDEN | MESSAGE_TYPE_HISTORY_KEYS`).
   - The server stores the message cursor normally without seeing plaintext.
   - Client SQLite saves the keys into `group_history_chunk_keys` while advancing the cursor and filtering the message from the chat UI.
4. **Optimistic Verification with Single-Disagreement Invalidation**:
   - When fetching, new members download the chunk directly from CDN, verify the unencrypted SHA-256 hash against what the server reported, and decrypt.
   - If decryption or SHA-256 verification fails (or if existing members verify against their local database and find a discrepancy), a single member can disapprove the chunk (`DisapproveHistoryChunkRequest`).
   - Upon disapproval, the server marks the chunk invalid (`status = 2`), removes it from active chunks, and notifies the group via `GroupHistorySignal(CHUNK_DISAPPROVED)` so another worker can re-encrypt and publish valid chunks.

---

## Changes Made

### 1. Database Migrations
- **[`firefly/initdb.sql`](file:///old-arch/home/ash/lupyd-foundation/firefly/initdb.sql)** & **`../firefly-mls/initdb.sql`**:
  - `group_history_requests`: Tracks history requests, range (`[start_msg_id, end_msg_id]`), status, and leases.
  - `group_history_chunks`: Stores chunk boundaries, unencrypted SHA-256 hash, CDN URL, status (active/disapproved), and disapproval reason. Zero blob storage in Postgres.
  - `group_history_chunk_locks`: Enforces single-worker leases to avoid racing workers.
- **[`crates/client/src/db/migrations.rs`](file:///old-arch/home/ash/lupyd-foundation/firefly/crates/client/src/db/migrations.rs)**:
  - Added migration version 2 (`history_chunks_v2`) creating `group_history_chunk_keys` and `group_history_disapprovals` with O(1) schema migration check.

### 2. CDN (`../lupyd-cdn`)
- **`../lupyd-cdn/src/index.ts`**:
  - Added `PUT /group_chunks/<chunkId>` endpoint with authenticated user check and 1-year immutable caching.
  - GET requests automatically fetch chunks via `env.FILES.get(key)`.

### 3. Server (`../firefly-mls`)
- **`../firefly-mls/crates/server/src/group/history.rs`**:
  - Implemented typed queries for lease claiming, chunk metadata publishing, single-disagreement invalidation, and history signaling.
- **`../firefly-mls/crates/server/src/session/dispatcher.rs`**:
  - Handled all history request variants in WebSocket dispatcher.

### 4. Client Engine & Stores (`crates/client`)
- **[`crates/client/src/history.rs`](file:///old-arch/home/ash/lupyd-foundation/firefly/crates/client/src/history.rs)**:
  - `pack_messages_into_chunk`: Gzip compression + AES-256-GCM encryption + SHA-256 hash calculation.
  - `decrypt_and_unpack_chunk`: Nonce parsing + AES-256-GCM decryption + SHA-256 hash validation + Gzip decompression.
  - `compute_unencrypted_hash`: Computes SHA-256 on unencrypted message sequence.
- **[`crates/client/src/db/history_keys.rs`](file:///old-arch/home/ash/lupyd-foundation/firefly/crates/client/src/db/history_keys.rs)**:
  - `HistoryKeysStore` for persisting chunk symmetric keys and tracking disapprovals.
- **[`crates/client/src/websocket.rs`](file:///old-arch/home/ash/lupyd-foundation/firefly/crates/client/src/websocket.rs)**:
  - Added `set_cdn_url` / `get_cdn_url` configuration.
  - `upload_chunk_blob`: Sends HTTP `PUT` directly to `{CDN_URL}/group_chunks/<randomID>` and returns the CDN link.
  - `download_chunk_blob`: Fetches chunk bytes directly from CDN URL.
- **[`crates/client/src/db/group_messages.rs`](file:///old-arch/home/ash/lupyd-foundation/firefly/crates/client/src/db/group_messages.rs)**:
  - `get_range(group_id, start_msg_id, end_msg_id)`: Fetches local messages in range.
  - `visible_messages`: Always filters out `MESSAGE_TYPE_HIDDEN` before returning to callers.
- **[`crates/client/src/callbacks.rs`](file:///old-arch/home/ash/lupyd-foundation/firefly/crates/client/src/callbacks.rs)**:
  - Added `GroupHistorySignal` struct and `on_group_history_signal` callback method.
- **[`crates/client/src/websocket.rs`](file:///old-arch/home/ash/lupyd-foundation/firefly/crates/client/src/websocket.rs)**:
  - Stored and passed `history_keys_store` through `Connection::new`, `sync_all_group_messages`, and `on_group_message`.
  - In `on_group_message`: Decodes hidden `MESSAGE_TYPE_HISTORY_KEYS` payloads and saves chunk keys to SQLite without emitting UI callbacks.
  - In `on_server_message`: Handles incoming `groupHistorySignal` and routes to callbacks.
  - Implemented high-level API methods on `FireflyWsClient`:
    - `request_group_history`
    - `claim_group_history_request`
    - `publish_group_history_chunk`
    - `disapprove_group_history_chunk`
    - `get_group_history_chunks`
    - `get_pending_group_history_requests`
    - `close_group_history_request`
    - `share_group_history_keys`
    - `upload_chunk_blob`
    - `download_chunk_blob`
    - `fulfill_group_history`
    - `fetch_and_verify_group_history`
    - `verify_published_chunks`

---

## Verification Results

1. **`cargo check`**:
   - `cargo check`: Passed with 0 errors across all workspace crates.
   - `cargo check --tests`: Passed with 0 errors across all integration and unit test targets.
   - `cargo check --manifest-path ../firefly-mls/Cargo.toml`: Passed with 0 errors.

2. **Unit Tests (`cargo test -p firefly-client --lib`)**:
   - 48 tests passed (0 failed).
   - Key tests verified:
     - `history::tests::test_pack_and_unpack_chunk ... ok`
     - `history::tests::test_tampered_ciphertext_fails_decryption ... ok`
     - `history::tests::test_tampered_hash_fails_verification ... ok`
     - `db::history_keys::tests::test_history_keys_store_crud ... ok`

3. **Integration Tests (`cargo test --test group_history_tests`)**:
   - `test test_history_keys_proto_hidden_sharing ... ok`
   - `test test_history_chunk_pack_unpack_integrity ... ok`
