---
title: JavaScript / TypeScript Client
description: Complete guide to using FireflyClient in JavaScript and TypeScript environments.
---

# JavaScript & TypeScript Client Guide

The `firefly-client-js` package provides a high-level, idiomatic TypeScript/JavaScript client built on top of native N-API bindings. It automates token renewal, PKCE authentication, SQLite keystores, and incoming message decoding.

---

## Installation

```bash
pnpm add firefly-client-js firefly-client-node
```

---

## Client Configuration Options

When creating an instance of `FireflyClient`, you can customize networking, database paths, and authentication:

```typescript
import { FireflyClient, ClientConfig } from 'firefly-client-js';

const config: ClientConfig = {
  // Username / identity
  username: 'my_user',

  // Network endpoints
  apiBaseUrl: 'https://firefly.lupyd.com',
  wsUrl: 'wss://firefly.lupyd.com/',

  // Persistence paths
  sessionFile: './client-session.json', // Auth0 tokens cache
  dbFile: './client-store.db',          // SQLite MLS keystore & message history

  // Authentication settings
  port: 38295,                          // Local port for OAuth redirect callback
  auth0Domain: 'https://auth.lupyd.com',
  auth0ClientId: 'your_auth0_client_id',
  auth0Audience: 'https://lupyd.com',

  // Testing & Offline Mode
  emulatorMode: false,                  // Set true to skip Auth0 in tests
};

const client = new FireflyClient(config);
```

---

## Lifecycle & Starting the Connection

To start the client and connect to the Firefly MLS network:

```typescript
await client.start();
```

When `start()` is invoked:
1. `_loadSession()` checks if cached access/refresh tokens are valid.
2. If expired or missing, it starts PKCE browser login (or loads direct username if in emulator mode).
3. The underlying Rust client is initialized and runs `checkSetup()`.
4. `initializeWithRetrying()` runs in the background while the client polls `isInitialized()`.
5. Group ratchet states are loaded with `loadAllGroups()`.

---

## Token Renewal

`getOrRenewAccessToken()` automatically refreshes tokens when they are within 2 minutes of expiration using Auth0 refresh token grants.

```typescript
const token = await client.getOrRenewAccessToken();
```

---

## Accessing the Underlying Native Client

If you need low-level control over raw MLS operations or custom queries:

```typescript
const nativeClient = client.client; // Instance of FireflyClientNode
```

---

## Next Steps

- **[Encrypted Messaging](./messaging/)**: Send direct and group messages.
- **[Groups & Ratchet Trees](./groups/)**: Manage group channels and membership.
- **[Native Node Client](./node-client/)**: Use raw N-API bindings.
- **[API Reference](../reference/api-reference/)**: Full method and parameter documentation.
