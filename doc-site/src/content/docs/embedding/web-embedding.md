---
title: Embedding Firefly into Web Pages
description: How to embed end-to-end encrypted Firefly MLS chat experiences directly into any web application or website.
---

# Embedding Firefly into Web Pages

Firefly MLS can be embedded into any website, web application, SaaS dashboard, or portal to provide native end-to-end encrypted messaging, customer support chat, team rooms, or bot interfaces.

---

## Architectural Approaches

When embedding Firefly into a web frontend, there are two primary integration patterns:

```
Pattern A: Direct Web / WASM Client
┌────────────────────────────────────────────────────────┐
│ Browser Web Page                                       │
│ ┌────────────────────────────────────────────────────┐ │
│ │  Firefly JS SDK (IndexedDB Keystore + WebSockets)  │ │
│ └──────────────────────────┬─────────────────────────┘ │
└────────────────────────────┼───────────────────────────┘
                             │ Encrypted MLS Traffic
                             ▼
                ┌─────────────────────────┐
                │   Firefly MLS Server    │
                └─────────────────────────┘

Pattern B: Secure Gateway / Embedded Widget Bridge
┌────────────────────────────────────────────────────────┐
│ Browser Web Page (Chat Widget UI)                      │
│ ┌────────────────────────────────────────────────────┐ │
│ │  Embeddable Floating Chat Widget (HTML/React/Vue)  │ │
│ └──────────────────────────┬─────────────────────────┘ │
└────────────────────────────┼───────────────────────────┘
                             │ Secure Session / SSE / WS
                             ▼
┌────────────────────────────────────────────────────────┐
│ Your Application Backend                               │
│ ┌────────────────────────────────────────────────────┐ │
│ │  Firefly Client SDK / Bot Daemon (Node.js/Rust)    │ │
│ └──────────────────────────┬─────────────────────────┘ │
└────────────────────────────┼───────────────────────────┘
                             │ Encrypted MLS Traffic
                             ▼
                ┌─────────────────────────┐
                │   Firefly MLS Server    │
                └─────────────────────────┘
```

---

## 1. Direct Web Client Integration
With direct integration, the browser connects directly to the Firefly WebSocket server using `firefly-client-js`. Cryptographic ratchet states are persisted locally in the user's browser (IndexedDB / LocalStorage / OPFS).

### Advantages:
- True client-to-client end-to-end encryption right inside the user's browser.
- No intermediary server has access to plaintext messages or private key material.

---

## 2. Gateway Bridge / Backend Agent
With the gateway model, your backend runs a dedicated `FireflyClient` or `FireflyBot` instance. Your frontend communicates with your backend over standard WebSockets/REST, and your backend forwards messages across the MLS network.

### Advantages:
- Extremely lightweight on mobile browsers.
- Centralized auth token management integrated with your existing session cookies or JWTs.
- Zero local compilation or native addon requirements in the client bundle.

---

## Next Steps

- **[Building an Embeddable Chat Widget](./chat-widget/)**: Drop-in chat component example.
- **[Security & Token Bridging](./security-and-auth/)**: Session security and token management.
- **[Chatbot Framework](../bot/framework/)**: Pair your embedded widget with an autonomous backend bot.
