---
title: Protocol Buffer Wire Format
description: Specification of Protocol Buffer schemas used for Firefly MLS encrypted payloads.
---

# Protocol Buffer Wire Format Reference

Firefly MLS uses Protocol Buffers to serialize and deserialize structured payload data inside encrypted MLS messages.

---

## Direct Message Inner (`UserMessageInner`)

Used for 1-on-1 direct conversations.

```protobuf
syntax = "proto3";
package firefly.protos;

message MessagePayload {
  string text = 1;
  repeated FileAttachment files = 2;
  uint64 replying_to = 3;
}

message UserMessageInner {
  MessagePayload message_payload = 1;
  uint32 nonce = 2;
}

message FileAttachment {
  string file_id = 1;
  string file_name = 2;
  uint64 file_size = 3;
  string mime_type = 4;
}
```

---

## Group Message Inner (`GroupMessageInner`)

Used for multi-party group chats and channels.

```protobuf
syntax = "proto3";
package firefly.protos;

message GroupMessageInner {
  MessagePayload message_payload = 1;
  uint32 channel_id = 2;
}
```

---

## Encoding & Decoding with JavaScript/TypeScript

```typescript
import { protos } from 'firefly-client-js';

// Encoding
const payload = {
  messagePayload: { text: "Hello Firefly!", replyingTo: 0n },
  nonce: 123456,
};
const encodedBytes = protos.UserMessageInner.encode(payload).finish();

// Decoding
const decoded = protos.UserMessageInner.decode(encodedBytes);
console.log(decoded.messagePayload.text); // "Hello Firefly!"
```

---

## Next Steps

- **[Encrypted Messaging](../../client/messaging/)**: See complete message encryption and sending code.
- **[FireflyClient & FireflyBot API](./api-reference/)**: Methods and options reference.
- **[Callbacks & Event Hooks](./callbacks/)**: Event interfaces and callback hooks.
