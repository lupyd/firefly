# Firefly Client Node FFI bindings

This is an automatically generated Node.js & Bun FFI wrapper around `firefly-client` using `napi-rs`.

## Features
- Fully typed TypeScript definitions out-of-the-box (`index.d.ts`).
- Native performance using pre-compiled `.node` binary.
- Asynchronous APIs mapped directly to JS/TS Promises.
- Supports Node.js and Bun runtime.

## Installation
Build the native addon using pnpm:
```bash
pnpm install
pnpm run build
```

## Usage Example

```typescript
import { FireflyClientNode, NapiUserMessage, NapiGroupMessage } from './index';

async function run() {
  const callbacks = {
    name: "test-client",
    getAccessToken: async () => {
      // Return access token or null
      return "my-access-token";
    },
    onMessage: (message: NapiUserMessage) => {
      console.log("New user message received:", message);
    },
    onGroupMessage: (message: NapiGroupMessage) => {
      console.log("New group message received:", message);
    },
    onGroupJoined: (groupId: number) => {
      console.log("Joined group:", groupId);
    },
    onCallSignal: (signal: any) => {
      console.log("Call signal received:", signal);
    },
    onGroupMeetingSignal: (signal: any) => {
      console.log("Group meeting signal:", signal);
    }
  };

  // Create client instance (returns a Promise resolving to the client class instance)
  const client = await FireflyClientNode.create(
    "http://localhost:8080", // firefly_base_url
    "ws://localhost:8080/ws", // firefly_base_ws_url
    5000, // retry_interval_in_ms
    callbacks,
    "./db.sqlite", // SQLite database and keystore path
    10000 // request_timeout_in_ms
  ) as FireflyClientNode;

  // Initialize client
  await client.initializeWithRetrying();

  // Send a message
  const sentMsg = await client.encryptAndSend("recipient_username", Buffer.from("Hello world"));
  console.log("Sent message details:", sentMsg);
}

run().catch(console.error);
```
