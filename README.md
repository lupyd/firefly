# Firefly MLS (Client & Core Libraries)

Firefly MLS is an open-source suite of client-side libraries for end-to-end encrypted messaging, group management, and key state persistence using the Message Layer Security (MLS) protocol ([RFC 9420](https://datatracker.ietf.org/doc/html/rfc9420)).

## Repository Structure

This repository contains the client-side crates and language bindings:

- **`crates/core` (`firefly-core`)**: Core MLS protocol rules, custom proposals, identity provider implementation, and group extension handlers.
- **`crates/protos` (`firefly-protos`)**: Protocol buffer definitions for wire messages, group structures, and user payloads.
- **`crates/client` (`firefly-client`)**: High-level async client engine managing WebSocket connectivity, local SQLite persistence (for messages, group state, and key material), address rotation, and group management.
- **`crates/client-node` (`firefly-client-node`)**: Node.js / TypeScript native N-API bindings for `firefly-client`.
- **`crates/firefly-client-js` (`firefly-client-js`)**: Web / JS bindings for client integration.

## Getting Started

### Prerequisites

- [Rust toolchain](https://rustup.rs/) (edition 2024 / 1.85+)
- Node.js & pnpm (optional, for Node.js client bindings)

### Building

To verify and compile all workspace crates:

```bash
cargo check
```

To build release artifacts:

```bash
cargo build --release
```

## Running Tests

### 1. Client Unit Tests
All client-side unit tests, cryptographic rules, SQLite stores, cursor advances, and key material storage providers run in-memory and require no running external dependencies:

```bash
cargo test --lib
```

### 2. Running All Workspace Tests
```bash
cargo test
```

### 3. End-to-End Server Integration Tests
Client integration tests can be executed against a running Firefly MLS server instance:

1. Start the backend PostgreSQL database and server in the `firefly-mls` repository:
   ```bash
   cd ../firefly-mls
   docker compose up -d
   cargo run -p firefly-server
   ```
2. Run client integration tests pointing to the test server:
   ```bash
   FIREFLY_BASE_URL=http://127.0.0.1:39205 FIREFLY_WS_URL=ws://127.0.0.1:39205/ RUST_LOG=info cargo test
   ```

If `FIREFLY_BASE_URL` is not specified, server integration tests skip gracefully so offline CI/CD pipelines run standard unit tests quickly.

## License

MIT
