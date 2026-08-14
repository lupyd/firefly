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

### 1. Standalone / Unit Tests

All client-side unit tests, cryptographic rules, and local storage provider tests run out-of-the-box without requiring any running server or database:

```bash
cargo test
```

### 2. End-to-End Server Integration Tests

Client integration tests can be executed against any running Firefly MLS server instance.

To run tests against an external or locally hosted Firefly MLS server:

1. Ensure your Firefly MLS server is running (e.g. at `http://127.0.0.1:39205`).
2. Pass the server endpoint via environment variables:

```bash
FIREFLY_BASE_URL=http://127.0.0.1:39205 FIREFLY_WS_URL=ws://127.0.0.1:39205/ RUST_LOG=info cargo test
```

If `FIREFLY_BASE_URL` is not set, server integration tests automatically skip gracefully, allowing offline CI/CD pipelines to run standard unit tests clean and fast.

## License

Apache-2.0 / MIT