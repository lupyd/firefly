---
title: Framework Overview & Commands
description: Build interactive, command-driven chatbots on the Firefly MLS network.
---

# Chatbot Framework Overview

The Firefly Chatbot Framework (`FireflyBot`) makes it easy to build automated services and command responders for Firefly MLS, similar to Discord.js or Telegraf.

---

## Key Features

- **Intuitive Command Routing**: Register command handlers with `bot.command('name', handler)`.
- **Automatic Message Parsing**: Triggers like `/help`, `/echo hello world`, or `/ping` are parsed into command names and argument arrays.
- **Unified 1-on-1 and Group Handling**: `ctx.reply()` automatically detects whether the incoming trigger came from a direct message or group channel and sends the encrypted reply appropriately.
- **Session Persistence**: Caches credentials in a JSON file and SQLite database so bots survive restarts.

---

## Creating a Bot Instance

```typescript
import { FireflyBot } from 'firefly-client-js';

const bot = new FireflyBot({
  username: 'helper_bot',
  dbFile: './helper-bot.db',
  sessionFile: './helper-session.json',
});
```

---

## Registering Commands

Commands can be registered with or without a leading slash:

```typescript
// 1. Basic greeting
bot.command('hello', async (ctx) => {
  await ctx.reply(`Hello @${ctx.sender}! How can I help you today?`);
});

// 2. Command with arguments: /echo some message
bot.command('echo', async (ctx) => {
  if (ctx.args.length === 0) {
    return ctx.reply('Please provide text to echo!');
  }
  await ctx.reply(ctx.args.join(' '));
});

// 3. Command checking group vs direct message context
bot.command('info', async (ctx) => {
  if (ctx.isGroup) {
    await ctx.reply(`Triggered in Group ID: ${ctx.groupId}, Channel: ${ctx.channelId}`);
  } else {
    await ctx.reply(`Triggered in a direct 1-on-1 chat with @${ctx.sender}`);
  }
});
```

---

## Starting the Bot

```typescript
async function launch() {
  console.log('Starting Firefly Bot...');
  await bot.start();
  console.log('Bot is live and listening for commands!');
}

launch().catch(console.error);
```
