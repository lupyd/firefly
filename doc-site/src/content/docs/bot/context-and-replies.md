---
title: Context & Auto-Replies
description: Understanding BotContext, argument parsing, and intelligent reply routing.
---

# Context & Auto-Replies

Every command handler in `FireflyBot` receives a `BotContext` object containing metadata about the trigger, sender, parameters, and reply methods.

---

## `BotContext` Interface

```typescript
export interface BotContext {
  bot: FireflyClient;              // The bot instance
  client: FireflyClient;           // Alias for client
  sender: string;                  // Sender username (e.g., 'alice')
  text: string;                    // Full raw message text (e.g., '/calc 2 + 2')
  command: string;                 // Extracted command name with slash (e.g., '/calc')
  args: string[];                  // Array of string arguments (e.g., ['2', '+', '2'])
  isGroup: boolean;                // True if message was sent in a group chat
  groupId: number | null;          // Group ID (or null if direct message)
  channelId: number | null;        // Channel ID inside group (or null if direct)
  reply: (text: string) => Promise<void>; // Sends an encrypted reply
}
```

---

## The `ctx.reply()` Method

The `reply(text)` helper handles all the complexity of:
1. Creating the appropriate protobuf message payload (`UserMessageInner` or `GroupMessageInner`).
2. Setting nonces and reply metadata.
3. Serializing to binary format.
4. Calling `encryptAndSend()` for 1-on-1 chats or `encryptAndSendGroup()` for group channels.

```typescript
bot.command('status', async (ctx) => {
  const uptime = process.uptime();
  await ctx.reply(`Bot Uptime: ${Math.floor(uptime)} seconds`);
});
```

---

## Handling Arguments

The `ctx.args` array breaks the message down by whitespace:

```typescript
bot.command('add', async (ctx) => {
  const [num1Str, num2Str] = ctx.args;
  const a = parseFloat(num1Str);
  const b = parseFloat(num2Str);

  if (isNaN(a) || isNaN(b)) {
    return ctx.reply('Usage: /add <number1> <number2>');
  }

  await ctx.reply(`Result: ${a + b}`);
});
```

---

## Next Steps

- **[Authentication & Emulator Mode](./auth-and-emulator/)**: Learn about session persistence and test modes.
- **[Complete Chatbot Tutorial](./example-bot/)**: Put everything together into a full chatbot service.
- **[Protobuf Wire Formats](../reference/protobufs/)**: Explore payload schemas.
