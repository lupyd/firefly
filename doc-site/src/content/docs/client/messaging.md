---
title: Encrypted Messaging
description: Sending direct 1-on-1 messages, group messages, read receipts, and presence status in Firefly MLS.
---

# Encrypted Messaging

Firefly MLS provides high-level APIs for end-to-end encrypted direct messaging and multi-party group communication.

---

## 1. Direct (1-on-1) Messaging

Direct messages are encrypted with the recipient's MLS key material:

```typescript
import { FireflyClient, protos } from 'firefly-client-js';

const UserMessageInner = protos.UserMessageInner;

async function sendDirectMessage(client: FireflyClient, recipient: string, text: string) {
  // Construct message payload
  const payload = {
    messagePayload: {
      text: text,
      files: undefined,
      replyingTo: 0n,
    },
    nonce: Math.floor(Math.random() * 9_999_999),
  };

  // Encode with Protocol Buffers
  const messageBytes = UserMessageInner.encode(payload).finish();

  // Encrypt and send via MLS
  await client.client.encryptAndSend(recipient, Array.from(messageBytes));
  console.log(`Encrypted message sent to ${recipient}!`);
}
```

---

## 2. Group Messaging

Group messages leverage the MLS Ratchet Tree for efficient multi-recipient encryption:

```typescript
import { FireflyClient, protos } from 'firefly-client-js';

const GroupMessageInner = protos.GroupMessageInner;

async function sendGroupMessage(client: FireflyClient, groupId: number, channelId: number, text: string) {
  const payload = {
    messagePayload: {
      text: text,
      files: undefined,
      replyingTo: 0n,
    },
    channelId: channelId,
  };

  const messageBytes = GroupMessageInner.encode(payload).finish();

  // Encrypt and send to group ratchet tree
  await client.client.encryptAndSendGroup(groupId, Array.from(messageBytes));
  console.log(`Encrypted message sent to group ${groupId}, channel ${channelId}!`);
}
```

---

## 3. Read Receipts

To acknowledge messages and update the read cursor for a user conversation:

```typescript
// Mark messages from 'alice' as read up to message ID 42
await client.readUserMessagesUpto('alice', 42);
```

---

## 4. Online Presence & Status

To query whether group members are currently connected:

```typescript
const status = await client.getGroupMembersOnlineStatus(groupId);
console.log('Group members online:', status);
```

---

## Next Steps

- **[Groups & Ratchet Trees](./groups/)**: Dive deeper into group states and multi-channel architecture.
- **[Chatbot Framework](../bot/framework/)**: Build autonomous command bots with auto-reply routing.
- **[Building a Chat Widget](../embedding/chat-widget/)**: Render real-time messages in a web widget.
