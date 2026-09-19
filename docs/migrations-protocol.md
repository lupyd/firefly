# Database Migrations Protocol & Backwards Compatibility Standard

This document defines the formal migrations protocol for the Firefly MLS project across both the **Client (SQLite)** and **Server (PostgreSQL)** database layers.

Every schema change must be backwards-compatible and executed without data loss, downtime, or performance degradation on existing installations.

---

## 1. Client SQLite Migrations

Client-side databases run on embedded SQLite instances (mobile, desktop, Node.js, WASM environments). Unlike centralized servers, clients cannot be updated simultaneously, and databases may be opened after long periods across multiple software versions.

### 1.1 Core Architecture

- **Location**: All migrations live in [`crates/client/src/db/migrations.rs`](file:///old-arch/home/ash/lupyd-foundation/firefly/crates/client/src/db/migrations.rs).
- **Automatic Execution**: Migrations execute automatically whenever a database pool is initialized (`setup_pool`, `setup_pool_from_path`) or a message store is instantiated (`MessagesStore::new`, `GroupMessagesStore::new`, `FavouriteMessagesStore::new`).
- **In-Process Concurrency Guard**: All migration routines MUST acquire `MIGRATION_LOCK` (`tokio::sync::Mutex`) before touching the database to prevent DDL lock conflicts between concurrent store initializations.
- **Migration Tracking**: Applied migrations are recorded in `_schema_migrations (version INTEGER PRIMARY KEY, name TEXT, applied_at DATETIME)`.

### 1.2 Performance & Startup Fast-Path

Store initializations must be microsecond-level operations. Never block startup on database scans.

1. **$O(1)$ Fast-Path Exit**:
   At the beginning of each migration routine, query the tracking table:
   ```rust
   let applied: Option<i64> = sqlx::query_scalar(
       "SELECT 1 FROM _schema_migrations WHERE version = ?"
   )
   .bind(VERSION)
   .fetch_optional(pool)
   .await?;

   if applied.is_some() {
       return Ok(()); // Already applied, exit immediately!
   }
   ```
2. **Never Full-Table Scan on Startup**:
   - ❌ **NEVER** run `SELECT count(*) FROM table` or `SELECT count(*) FROM fts_table` to check if backfill or index rebuild is needed. On databases with 500,000+ messages, `count(*)` causes disk I/O stalls and ANRs (Application Not Responding).
   - ✔️ **ALWAYS** use `SELECT 1 FROM table [WHERE condition] LIMIT 1` to test for presence or absence of records in $O(1)$ time.

### 1.3 Strict DDL Dependency Ordering

SQLite will immediately fail with `no such column: <column_name>` if an `INDEX`, `TRIGGER`, or `VIEW` references a column that has not yet been physically created on the table.

When introducing or modifying tables:
1. **Create Base Tables**:
   ```sql
   CREATE TABLE IF NOT EXISTS my_table (...);
   ```
2. **Ensure Existing Columns (Introspection & Alteration)**:
   For any columns added in newer versions, check if they exist before creating indexes:
   ```rust
   ensure_column(pool, "my_table", "my_column", "TEXT NOT NULL DEFAULT ''").await?;
   ```
   `ensure_column` inspects `PRAGMA table_info(my_table)` and runs `ALTER TABLE my_table ADD COLUMN ...` only if absent.
3. **Create Dependent Indexes & Triggers**:
   Only after step 2 is complete, execute:
   ```sql
   CREATE INDEX IF NOT EXISTS my_table_col_idx ON my_table (my_column);
   ```
4. **Bounded Batch Backfills**:
   If a new column requires backfilling data (e.g. computing extracted search text or plaintext from JSON payloads), process rows in bounded chunks:
   ```sql
   SELECT id, payload FROM my_table WHERE my_column = '' LIMIT 500;
   ```
   Loop until no unpopulated rows remain. This prevents out-of-memory errors on constrained mobile devices and keeps SQLite write transactions short.

### 1.4 How to Add a New Client Migration

1. In [`crates/client/src/db/migrations.rs`](file:///old-arch/home/ash/lupyd-foundation/firefly/crates/client/src/db/migrations.rs):
   - Define a new version number (e.g. `const VERSION_2: i64 = 2;`).
   - Implement `async fn migrate_v2(pool: &SqlitePool) -> Result<(), ClientError>`.
   - Implement the fast-path check for `VERSION_2`.
   - Perform table/column/index additions following the strict DDL ordering.
   - Record the completion in `_schema_migrations`:
     ```sql
     INSERT INTO _schema_migrations (version, name, applied_at)
     VALUES (2, 'migration_name', CURRENT_TIMESTAMP);
     ```
   - Add `migrate_v2(pool).await?;` inside `run_migrations(pool)`.

2. Write a regression test in `crates/client/tests/`:
   - Setup a raw SQLite database simulating the previous schema version.
   - Run `run_migrations` and verify that the database upgrades seamlessly without data loss or crashes.

---

## 2. Server PostgreSQL Migrations

The central MLS server uses PostgreSQL and transactional connection pooling (e.g. PgBouncer).

### 2.1 Append-Only Schema Definitions (`initdb.sql`)

- Any changes to the server database schema must be **appended to the very end** of [`initdb.sql`](file:///old-arch/home/ash/lupyd-foundation/firefly/initdb.sql).
- Schema additions must be marked after a comment explaining the motivation:
  ```sql
  -- # migrations: Add favourite messages storage and index for user query acceleration
  CREATE TABLE IF NOT EXISTS favourite_messages (
      user_id VARCHAR NOT NULL,
      message_id VARCHAR NOT NULL,
      created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (user_id, message_id)
  );
  ```
- **Never rewrite or reorder prior statements** in `initdb.sql` that have already been deployed to production environments.

### 2.2 Database Query Conventions (Typed Queries)

For enhanced type safety and to minimize network round trips across transactional poolers, **NEVER** use standard untyped queries:
- ❌ Do not use: `query`, `execute`, `query_one`, or `query_option`.
- ✔️ Always use:
  - `query_typed`
  - `query_typed_one`
  - `execute_typed`
  - `query_typed_option`

---

## 3. JS & WASM Synchronization

Client stores and database APIs are bound to Node.js and browser runtimes through WebAssembly and TypeScript packages:
- `crates/client-node`
- `crates/firefly-client-js`

Whenever client stores, methods, or database models change:
1. Update WASM bindings in [`crates/client-node/src/lib.rs`](file:///old-arch/home/ash/lupyd-foundation/firefly/crates/client-node/src/lib.rs).
2. Recompile WASM:
   ```bash
   wasm-pack build --target nodejs --out-dir wasm crates/client-node
   ```
3. Update TypeScript definitions and export aliases in:
   - `crates/client-node/src/index.ts`
   - `crates/firefly-client-js/src/index.ts`
4. Rebuild TypeScript artifacts:
   ```bash
   cd crates/client-node && npm run build
   cd crates/firefly-client-js && npm run build
   ```

---

## 4. Verification Checklist

Before opening a pull request or completing database migration tasks:
- [ ] `cargo check` compiles with zero warnings or errors.
- [ ] Automated regression tests pass:
  ```bash
  EMULATOR_MODE=true RUST_LOG=info cargo test
  ```
- [ ] Migration fast-path was verified with a legacy database fixture.
- [ ] No `count(*)` full table scans added to startup code paths.
- [ ] TypeScript and WASM bundles recompiled and git-synced.
