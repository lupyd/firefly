---
title: FireflyClient & FireflyBot API
description: Full API documentation for FireflyClient, FireflyBot, configuration options, and methods.
---

# `FireflyClient` & `FireflyBot` API Reference

`FireflyClient` (and its alias `FireflyBot`) is the main entry point in `firefly-client-js`.

---

## Constructor

```typescript
new FireflyClient(options?: ClientConfig)
```

### `ClientConfig` Parameters

| Property | Type | Default | Description |
|---|---|---|---|
| `username` | `string` | `'example_client'` | Local username or identity identifier. |
| `apiBaseUrl` | `string` | `'https://firefly.lupyd.com'` | Firefly MLS REST API base URL. |
| `wsUrl` | `string` | `'wss://firefly.lupyd.com/'` | Firefly MLS WebSocket gateway URL. |
| `dbFile` | `string` | `'./client-store.db'` | Path to SQLite database for keys & messages. |
| `sessionFile` | `string` | `'./client-session.json'` | Path to cached JSON session credentials. |
| `emulatorMode` | `boolean` | `false` | When true, skips OAuth and uses direct username. |
| `auth0Domain` | `string` | `'https://auth.lupyd.com'` | Auth0 domain for PKCE login. |
| `auth0ClientId` | `string` | `'GnfEyGY0JdD0...'` | Auth0 application client ID. |
| `auth0Audience` | `string` | `'https://lupyd.com'` | Target API audience identifier. |
| `port` | `number` | `38295` | Local redirect port for PKCE authorization. |

---

## Methods

### `start(): Promise<void>`
Initializes session credentials, connects to the Firefly MLS WebSocket network, executes `checkSetup()`, synchronizes groups, and starts background listening loops.

### `command(name: string, handler: CommandHandler): void`
Registers a command handler triggered when an incoming 1-on-1 or group message starts with `/<name>` or `<name>`.

### `getOrRenewAccessToken(): Promise<string | null>`
Returns the current valid access token, proactively refreshing it via refresh tokens if within 2 minutes of expiration.

### `getGroupMembersOnlineStatus(groupId: number): Promise<GroupMembersOnlineStatus>`
Queries the online presence status for all members of the specified group.

### `readUserMessagesUpto(other: string, uptoMessageId: bigint | number): Promise<void>`
Sends a read receipt acknowledging that all messages up to `uptoMessageId` from `other` have been read.
