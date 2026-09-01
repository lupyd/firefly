---
title: Node.js FFI Native Client
description: Low-level native N-API client bindings with direct Rust performance.
---

# Node.js FFI Native Client (`firefly-client-node`)

The `firefly-client-node` package is a high-performance native N-API bridge connecting Node.js and Bun directly to the core Rust MLS implementation.

---

## Overview

The native client is ideal when:
- You need maximum cryptographic throughput.
- You are implementing custom callback handlers or protocol extensions.
- You are integrating Firefly into a custom backend or daemon.

---

## Direct Usage Example

```typescript
import { FireflyClientNode, NapiUserMessage, NapiGroupMessage } from 'firefly-client-node';

async function run() {
  const callbacks = {
    name: 'backend-service',
    initialToken: 'bearer-token-here',

    getAccessToken: async () => {
      return 'refreshed-bearer-token';
    },

    onMessage: (err: any, msgJson: string) => {
      if (err) return console.error('Error in onMessage:', err);
      const msg = JSON.parse(msgJson);
      console.log(`Received direct message from ${msg.other}:`, msg);
    },

    onGroupMessage: (err: any, msgJson: string) => {
      if (err) return console.error('Error in onGroupMessage:', err);
      const msg = JSON.parse(msgJson);
      console.log(`Received group message in group ${msg.group_id}:`, msg);
    },

    onGroupJoined: async (groupId: number) => {
      console.log(`Successfully joined group ${groupId}`);
    },

    onCallSignal: (err: any, signalJson: string) => {
      console.log('WebRTC Call Signal:', signalJson);
    },

    onGroupMeetingSignal: (err: any, signalJson: string) => {
      console.log('Group Meeting Signal:', signalJson);
    },

    onReadUserMessagesUpto: (err: any, infoJson: string) => {
      console.log('Read receipt updated:', infoJson);
    },
  };

  // 1. Create native instance
  const client = await FireflyClientNode.create(
    'https://firefly.lupyd.com',  // firefly_base_url
    'wss://firefly.lupyd.com/',   // firefly_base_ws_url
    2000,                         // retry_interval_in_ms
    callbacks,                    // Callbacks interface
    './native-store.db',          // SQLite path
    15000                         // request_timeout_in_ms
  );

  // 2. Setup and initialize
  await client.checkSetup();
  client.initializeWithRetrying().catch(console.error);

  // Wait until initialized
  while (!client.isInitialized()) {
    await new Promise((resolve) => setTimeout(resolve, 300));
  }

  console.log('Native Firefly Client is initialized and running!');
}

run().catch(console.error);
```
