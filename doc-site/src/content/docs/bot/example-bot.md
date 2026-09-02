---
title: Complete Chatbot Tutorial
description: Step-by-step walkthrough creating an interactive multipurpose bot with commands, help menus, and uptime tracking.
---

# Complete Chatbot Tutorial

In this tutorial, we will build a full-featured bot that provides:
- `/help`: Displaying available commands
- `/ping`: Responding with latency pong
- `/stats`: Reporting uptime and memory usage
- `/echo`: Echoing arguments

---

## 1. Project Setup

Create a file `bot.ts`:

```typescript
import { FireflyBot, BotContext } from 'firefly-client-js';

const bot = new FireflyBot({
  username: process.env.BOT_USERNAME || 'moderator_bot',
  dbFile: './moderator-bot.db',
  sessionFile: './moderator-session.json',
});

// Help command
bot.command('help', async (ctx: BotContext) => {
  const menu = [
    '🤖 **Firefly Bot Commands**',
    '• `/help` - Show this help menu',
    '• `/ping` - Check responsiveness',
    '• `/stats` - View bot runtime statistics',
    '• `/echo <text>` - Echo back your text',
  ].join('\n');

  await ctx.reply(menu);
});

// Ping command
bot.command('ping', async (ctx: BotContext) => {
  await ctx.reply(`🏓 Pong! Hello @${ctx.sender}`);
});

// Stats command
bot.command('stats', async (ctx: BotContext) => {
  const mem = process.memoryUsage();
  const mb = (bytes: number) => (bytes / 1024 / 1024).toFixed(2);
  const uptime = Math.floor(process.uptime());

  const stats = [
    `📊 **Bot Health & Stats**`,
    `• Uptime: ${uptime}s`,
    `• Heap Used: ${mb(mem.heapUsed)} MB`,
    `• RSS: ${mb(mem.rss)} MB`,
  ].join('\n');

  await ctx.reply(stats);
});

// Echo command
bot.command('echo', async (ctx: BotContext) => {
  if (ctx.args.length === 0) {
    return ctx.reply('⚠️ Please provide text to echo. Example: `/echo hello world`');
  }
  await ctx.reply(ctx.args.join(' '));
});

// Start the bot
async function run() {
  console.log('Starting Firefly Chatbot...');
  await bot.start();
}

run().catch(console.error);
```

---

## 2. Running Your Bot

```bash
# Run with Bun
bun run bot.ts

# Or with Node.js
pnpm dlx tsx bot.ts
```

Once running, send `/help` or `/ping` in any 1-on-1 chat with the bot or mention it in a group chat, and the bot will reply with end-to-end encrypted messages!

---

## Next Steps

- **[Embedding Firefly into Web Pages](../embedding/web-embedding/)**: Connect your bot with frontend chat widgets.
- **[API Reference](../reference/api-reference/)**: Complete method list and configuration options.
