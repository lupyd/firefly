---
title: Overview & Architecture
description: Learn the cryptographic principles, protocol layers, and architectural structure of Firefly MLS.
---

# Firefly MLS Architecture & Overview

Firefly MLS is an end-to-end encrypted messaging suite built on the **Message Layer Security (MLS) protocol ([RFC 9420](https://datatracker.ietf.org/doc/html/rfc9420))**. It brings modern, group-scale, forward-secret, and post-compromise secure communication to Node.js, Bun, browsers, and embedded clients.

---

## What is Message Layer Security (MLS)?

Traditional end-to-end encryption protocols (such as Signal's Double Ratchet for pairwise messaging) scale with $O(N)$ computational complexity when used in group settings. Every message sent to an $N$-person group requires $N-1$ individual pairwise encryptions.

**RFC 9420 (MLS)** solves this with a **Ratchet Tree**:
- Members of a group form leaves in a binary tree.
- Updating keys or sending group messages operates with $O(\log N)$ or $O(1)$ efficiency.
- Provides **Forward Secrecy (FS)**: Past messages remain secure even if future keys are compromised.
- Provides **Post-Compromise Security (PCS)**: If a client key is temporarily leaked, regular ratchet updates automatically restore full security.

---

## Repository & Crate Architecture

The Firefly workspace contains modular layers, separating protocol rules, serialization, networking, native bindings, and high-level developer SDKs:

```
                      ┌────────────────────────────────────────┐
                      │          Applications & Bots           │
                      │  (Chatbots, Web Widgets, Services)     │
                      └───────────────────┬────────────────────┘
                                          │
                      ┌───────────────────▼────────────────────┐
                      │    crates/firefly-client-js (TS/JS)    │
                      │   High-level Bot & Client framework    │
                      └───────────────────┬────────────────────┘
                                          │
                      ┌───────────────────▼────────────────────┐
                      │    crates/client-node (N-API FFI)      │
                      │   Node.js / Bun native bindings        │
                      └───────────────────┬────────────────────┘
                                          │
                      ┌───────────────────▼────────────────────┐
                      │         crates/client (Rust)           │
                      │   Async Engine, SQLite Store, WS Sync  │
                      └───────────────────┬────────────────────┘
                                          │
                      ┌───────────────────▼────────────────────┐
                      │          crates/core (Rust)            │
                      │   MLS RFC 9420 Core, Ratchets, Cryptography │
                      └───────────────────┬────────────────────┘
                                          │
                      ┌───────────────────▼────────────────────┐
                      │         crates/protos (Rust/TS)        │
                      │   Protocol Buffer Wire Formats         │
                      └────────────────────────────────────────┘
```

### 1. `crates/core` (`firefly-core`)
Implements the core MLS protocol rules, custom proposals, identity providers, cryptographic ratchet trees, and group extension handlers.

### 2. `crates/protos` (`firefly-protos`)
Protocol buffer definitions specifying wire format serialization for direct messages (`UserMessageInner`), group messages (`GroupMessageInner`), call signals, and member presence.

### 3. `crates/client` (`firefly-client`)
Async Rust client engine that handles:
- WebSocket connection to Firefly MLS servers.
- Local SQLite database persistence for key material, credentials, ratchet tree states, and message histories.
- Automatic address rotation and group epoch synchronization.

### 4. `crates/client-node` (`firefly-client-node`)
High-performance native N-API bindings generated for Node.js and Bun environments.

### 5. `crates/firefly-client-js` (`firefly-client-js`)
Developer-facing TypeScript/JavaScript library providing the `FireflyClient` and `FireflyBot` classes, Auth0 PKCE browser authentication, command dispatching, and reply context management.

---

## Next Steps

- **[Installation & Setup](./installation/)**: Install dependencies and build native bindings.
- **[Quickstart](./quickstart/)**: Send your first encrypted message in 5 minutes.
- **[Client SDK Guide](../client/js-client/)**: Learn how to use the JavaScript/TypeScript client.
- **[Chatbot Framework](../bot/framework/)**: Build command-driven bots and auto-responders.
- **[Web Embedding](../embedding/web-embedding/)**: Embed Firefly into any web application.
