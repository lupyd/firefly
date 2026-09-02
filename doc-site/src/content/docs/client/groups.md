---
title: Groups & Ratchet Trees
description: Managing MLS group states, epochs, channels, and membership changes.
---

# Groups & Ratchet Trees

In Message Layer Security (RFC 9420), groups are represented as binary cryptographic trees called **Ratchet Trees**.

---

## Group Lifecycle in Firefly

1. **Group Creation / Welcome**: When a user creates a group or adds a new member, an MLS `Welcome` package is created with the initial epoch state.
2. **Epoch Updates**: Whenever a member joins, leaves, or periodically rotates keys for Post-Compromise Security (PCS), an MLS `Commit` transitions the group to a new epoch.
3. **Local Persistence**: `firefly-client` automatically persists ratchet tree states, parent hashes, and secret ratchet keys in the local SQLite database.

---

## Channels Within Groups

Firefly supports multi-channel group architectures. Each group can have multiple channels sharing the group's underlying MLS ratchet tree:

- `groupId`: Uniquely identifies the MLS group ratchet context.
- `channelId`: Identifies the sub-topic or room within the group.

```typescript
// Sending to Channel #1 (General) in Group #100
await sendGroupMessage(client, 100, 1, "Hello General channel!");

// Sending to Channel #2 (Announcements) in Group #100
await sendGroupMessage(client, 100, 2, "Important announcement!");
```

---

## Handling Group Events

When your client is added to a new group:

```typescript
client.client.onGroupJoined = async (groupId: number) => {
  console.log(`Joined new group: ${groupId}`);
  // Reload group ratchet caches
  await client.client.loadAllGroups();
};
```

---

## Next Steps

- **[Encrypted Messaging](./messaging/)**: Learn about group and 1-on-1 message payloads.
- **[Chatbot Framework](../bot/framework/)**: Handle group commands with contextual auto-replies.
- **[Web Embedding](../embedding/web-embedding/)**: Embed group chat rooms in web pages.
