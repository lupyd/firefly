# Firefly MLS Example Chatbot & Bot Framework (TypeScript + Bun)

This is a reusable framework and example implementation of a client-side chatbot written in TypeScript and executed natively using Bun, showing how to integrate with the `firefly-client-node` FFI bindings and use the Firefly MLS network.

It provides a clean, class-based framework API similar to popular bot frameworks (like Telegraf or Discord.js) so that anyone can easily build and deploy their own custom commands on the Firefly network.

---

## Features

1. **Native TypeScript execution:** Powered by Bun, meaning TypeScript files run directly (`bun run src/index.ts`) with no separate build/compile step.
2. **Clean Framework API:** Define commands using `bot.command(name, handler)` and respond using the context-aware `ctx.reply(text)` method.
3. **Personal (Direct) & Group Chat Support:** Automatically handles command triggers sent via direct messages (personal chats) and replies back in the same personal chat.
4. **Interactive CLI Auth0 PKCE Flow:** Spins up a temporary local redirect server to capture credentials from a web browser login, exchanges the authorization code for tokens, and persists the session.
5. **Emulator Mode (`EMULATOR_MODE=true`):** Bypasses OAuth logins entirely when running tests against the local emulator server.
6. **Dynamic Protobuf Compilation:** Loads [message.proto](file:///home/ash/lupyd-foundation/firefly-mls/examples/chatbot/message.proto) at runtime using `protobufjs` to serialize/deserialize group and direct message payloads.

---

## Reusable API Example

Building a custom bot is simple:

```typescript
import { FireflyBot, BotContext } from './src/framework';

const bot = new FireflyBot({
  username: 'my_custom_bot', // Sets bot username
  dbFile: './bot-store.db',  // Path to local SQLite store
});

// A greeting command
bot.command('hi', async (ctx: BotContext) => {
  // ctx.reply works for both group chats and direct messages (personal chats)
  await ctx.reply(`Hello, @${ctx.sender}!`);
});

// A command with arguments
bot.command('echo', async (ctx: BotContext) => {
  await ctx.reply(`You said: ${ctx.args.join(' ')}`);
});

// Launch the bot
bot.start();
```

---

## Prerequisites

1. **Local Database & Servers:** Ensure you have the PostgreSQL server running using Docker:
   ```bash
   docker compose up -d
   ```
2. **Crate Build:** Make sure you've built the local Node.js FFI bindings. From the root directory:
   ```bash
   pnpm --filter firefly-client-node run build:debug
   ```

---

## Setup & Installation

From this directory (`examples/chatbot`), install dependencies:
```bash
bun install
```

---

## How to Run

### 1. Standard Production Mode (Auth0 Flow)
Run the bot with standard defaults:
```bash
bun start
```
The bot will:
* Detect if there's an existing session in `./bot-session.json`.
* If not, it will spin up a local redirect server and automatically open your default browser to authorize via Auth0.
* On successful authorization, the browser redirects back to the local server, code exchange is done, and the bot registers.
* It will create an SQLite DB locally (`./bot-store.db`) to hold cryptographic identity keys, sessions, and messages.

### 2. Local Emulator Mode (Testing)
To test the bot against a local server environment (like the one used in Rust integration tests):
```bash
export EMULATOR_MODE=true
export BOT_USERNAME=my_local_bot
export FIREFLY_BASE_URL=http://127.0.0.1:30000
export FIREFLY_BASE_WS_URL=ws://127.0.0.1:30000/
bun start
```

### 3. Running Unit Tests
To execute the unit tests for the chatbot and framework:
```bash
bun test
```
This runs the Jest-compatible unit tests natively in Bun.
