---
title: Callbacks & Event Hooks
description: Detailed reference of client callback hooks for messages, group events, and signaling.
---

# Callbacks & Event Hooks Reference

The underlying `FireflyClientNode` supports event callbacks passed at initialization.

---

## Callback Table

| Callback Hook | Signature | Description |
|---|---|---|
| `getAccessToken` | `() => Promise<string \| null> \| string \| null` | Invoked when the client needs an access token for requests. |
| `onMessage` | `(err: any, msgJson: string) => void` | Triggered on incoming 1-on-1 direct messages. |
| `onGroupMessage` | `(err: any, msgJson: string) => void` | Triggered on incoming group messages across all joined groups. |
| `onGroupJoined` | `(groupId: number) => void` | Triggered when the client is added to a new group. |
| `onCallSignal` | `(err: any, signalJson: string) => void` | Triggered on incoming WebRTC call signaling payloads. |
| `onGroupMeetingSignal` | `(err: any, signalJson: string) => void` | Triggered on group meeting signaling updates. |
| `onReadUserMessagesUpto` | `(err: any, infoJson: string) => void` | Triggered when another user acknowledges messages up to a given ID. |

---

## Message JSON Payload Structures

### Direct Message JSON (`msgJson` in `onMessage`):
```json
{
  "other": "alice",
  "sent_by_other": true,
  "message": [/* array of encoded protobuf bytes */],
  "id": 105,
  "created_at": 1725200000000
}
```

### Group Message JSON (`msgJson` in `onGroupMessage`):
```json
{
  "by": "bob",
  "group_id": 42,
  "channel_id": 1,
  "message": [/* array of encoded protobuf bytes */],
  "id": 312,
  "created_at": 1725200000000
}
```
