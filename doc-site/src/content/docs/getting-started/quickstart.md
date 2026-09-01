---
title: Quickstart
description: Get up and running with Firefly MLS in under 5 minutes.
---

# Quickstart

In this guide, you will learn how to initialize the Firefly client, connect to the encrypted network, and send your first end-to-end encrypted message.

---

## 1. Quick Example: Using FireflyClient

Create a new file `client.ts`:

```typescript
import { FireflyClient } from 'firefly-client-js';

async function main() {
  // 1. Initialize client
  const client = new FireflyClient({
    username: 'alice',
    apiBaseUrl: 'https://firefly.lupyd.com',
    wsUrl: 'wss://firefly.lupyd.com/',
    dbFile: './alice-store.db',
  });

  // 2. Register custom command or message listener
  client.command('ping', async (ctx) => {
    console.log(`Received ping from @${ctx.sender}`);
    await ctx.reply('pong! 🏓');
  });

  // 3. Connect to network
  await client.start();
  console.log('Alice is now online and listening for messages!');
}

main().catch(console.error);
```

---

## 2. Running the Client

Execute directly with Bun or Node.js:

```bash
# With Bun (supports TypeScript natively)
bun run client.ts

# Or with Node.js (via tsx)
pnpm dlx tsx client.ts
```

### What happens behind the scenes:
1. **Authentication**: If no session is cached in `client-session.json`, an interactive browser window opens for Auth0 PKCE login (or bypasses if `EMULATOR_MODE=true`).
2. **Keystore Initialization**: The client opens/creates `./alice-store.db` to securely persist cryptographic ratchet state and identity keys.
3. **Network Connection**: Connects to the Firefly MLS WebSocket server and runs `checkSetup()` to synchronize group ratchet epochs.
4. **Event Dispatching**: Decodes incoming encrypted protobuf messages and routes commands to your handler functions.
