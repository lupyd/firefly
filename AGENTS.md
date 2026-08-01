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
