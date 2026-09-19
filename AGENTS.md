# Agent Instructions

This document provides essential guidelines and workflows for agents working on the Firefly MLS project.

## Development Workflow

### 1. Database Setup
Before running any tests, you must ensure the database is up and running. Use Docker Compose to start the PostgreSQL instance:
```bash
docker compose up -d
```
The database will be initialized using `initdb.sql`.

Any changes to the db schema must be added to end of file only of initdb.sql and marked after a comment # migrations explaining the reason

### 2. Post-Change Verification
After making any code changes, always run `cargo check` to verify that the project still compiles and there are no type errors or lint warnings:
```bash
cargo check
```

### 3. Running Tests
To run the project's tests, use the following command (requires the database to be running):
```bash
EMULATOR_MODE=true RUST_LOG=info cargo test
```

## Database Query Conventions

For enhanced type safety and to minimize network round trips (which can break transactional pooling), **NEVER** use the regular `query`, `execute`, `query_one`, or `query_option` methods. Instead, always use their "typed" counterparts:

-   **`query_typed`**: Instead of `query`
-   **`query_typed_one`**: Instead of `query_one`
-   **`execute_typed`**: Instead of `execute`
-   **`query_typed_option`**: Instead of `query_option`

These methods are specifically designed for this project to ensure parameter types are explicitly handled and performance is optimized for transactional pooling.


## JS/wasm packages must be updated and with sync

## Client SQLite Migrations Protocol

All client-side SQLite database changes must adhere to the following backwards compatibility and migration standard:

### 1. Centralized and Automatic Execution
- All database migrations are registered in `crates/client/src/db/migrations.rs` (`run_migrations`).
- Migrations run automatically when setting up pools (`setup_pool`, `setup_pool_from_path`) and when initializing any store (`MessagesStore::new`, `GroupMessagesStore::new`, `FavouriteMessagesStore::new`).
- Always guard migration runs with `MIGRATION_LOCK` to prevent in-process DDL concurrency races.

### 2. Strict DDL Dependency Ordering
- **Never** create indexes, triggers, or FTS virtual tables referencing new columns until those columns are guaranteed to exist on disk.
- Sequence of operations for every migration:
  1. `CREATE TABLE IF NOT EXISTS` for new tables.
  2. For existing tables, check column existence with `PRAGMA table_info(table)` via `ensure_column`. If missing, execute `ALTER TABLE table ADD COLUMN ...` with safe defaults.
  3. Only *after* all columns exist, execute `CREATE INDEX IF NOT EXISTS` and `CREATE TRIGGER IF NOT EXISTS`.
  4. If new columns require data backfills (e.g., extracted plaintext for FTS), process updates in bounded batches (`LIMIT 500`) to keep memory and lock times minimal.

### 3. Fast-Path Exit and No Full Table Scans
- The `_schema_migrations (version, name, applied_at)` table tracks applied migrations.
- Check `SELECT 1 FROM _schema_migrations WHERE version = ?` at the start of migration functions to return immediately (`O(1)`) on routine store initializations.
- **Never** execute `SELECT count(*)` on message or FTS tables to check for data presence during startup. Always use `SELECT 1 FROM table [WHERE ...] LIMIT 1` to ensure `O(1)` microsecond performance regardless of database size.