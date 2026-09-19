// Run after `wasm-pack build --dev --target nodejs --out-dir wasm` and `tsc`.
// FIREFLY_BASE_URL=http://127.0.0.1:39209 bun test ./tests/message-permissions.test.cjs
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { FireflyClientNode } = require('../wasm/firefly_client_node.js');
const { protos, UserPermission, DEFAULT_GROUP_PERMISSIONS } = require('../dist/index.js');
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

async function eventually(predicate) {
  for (let i = 0; i < 100; i++) {
    if (await predicate()) return;
    await sleep(50);
  }
  assert.fail('Timed out waiting for client state');
}

test('WASM clients enforce message and pin rules with an opaque server', {
  skip: !process.env.FIREFLY_BASE_URL, timeout: 30000,
}, async () => {
  assert.equal(DEFAULT_GROUP_PERMISSIONS, 5);
  assert.equal(UserPermission.SeeMessage, 1);
  assert.equal(UserPermission.PinMessage, 2);
  const base = process.env.FIREFLY_BASE_URL;
  const suffix = Math.floor(Math.random() * 1e9);
  const clients = [];
  const events = [];
  const create = async name => {
    const client = await FireflyClientNode.create(base, base.replace(/^http/, 'ws') + '/', 1000, {
      name, initialToken: name, getAccessToken: () => name,
      onGroupMessage: (error, json) => { assert.equal(error, null); events.push({ name, message: JSON.parse(json) }); },
    }, '', 5000);
    clients.push(client);
    await client.checkSetup();
    await client.initializeWithRetrying();
    await eventually(() => client.isInitialized());
    return client;
  };
  const message = (channelId, messageType = 0, nestedType = 0) => protos.GroupMessageInner.encode({
    channelId, messageType,
    messagePayload: protos.MessagePayload.fromPartial({ text: 'permission test', messageType: nestedType }),
  }).finish();
  try {
    const alice = await create(`wasm_a_${suffix}`);
    const bobName = `wasm_b_${suffix}`;
    const bob = await create(bobName);
    const group = await alice.createGroup('wasm permissions', '', 0);
    const id = group.id;
    const extension = protos.FireflyGroupExtension.decode(await alice.getGroupExtension(id));
    assert.equal(extension.defaultPermissions, 5);
    await alice.addGroupMember(id, bobName, 0);
    await bob.checkSetup();
    await bob.encryptAndSendGroup(id, message(0));
    for (const [outer, nested] of [[1, 0], [0, 1], [1, 1]]) {
      await assert.rejects(bob.encryptAndSendGroup(id, message(0, outer, nested)), /PinMessage/);
    }
    // Neither a plain nor a pinned message can bypass unknown-channel denial.
    await assert.rejects(bob.encryptAndSendGroup(id, message(999)), /SeeMessage/);
    await assert.rejects(alice.encryptAndSendGroup(id, message(999, 1)), /SeeMessage/);
    const pinnedId = await alice.encryptAndSendGroup(id, message(0, 1));
    await eventually(() => events.some(e => e.name === bobName && e.message.id === pinnedId));
    assert.ok((await bob.getPinnedGroupMessages(id)).some(m => m.id === pinnedId));
    assert.ok((await bob.getGroupMessages(id, Number.MAX_SAFE_INTEGER, 100)).some(m => m.id === pinnedId));
  } finally {
    for (const client of clients) await client.dispose();
  }
});

test('pin codecs preserve old callers and both authenticated pin flags', () => {
  assert.equal(protos.GroupMessageInner.encode({ channelId: 0 }).finish().length, 0);
  assert.equal(protos.UserMessageInner.encode({ nonce: 0 }).finish().length, 0);
  assert.equal(protos.GroupMessageInner.decode(new Uint8Array()).messageType, 0);
  assert.equal(protos.MessagePayload.decode(protos.MessagePayload.encode({ text: 'old caller' }).finish()).messageType, 0);
  for (const [outer, nested] of [[1, 0], [0, 1], [1, 1], [128, 129]]) {
    const encoded = protos.GroupMessageInner.encode({
      channelId: 7, messageType: outer,
      messagePayload: { text: 'pin', messageType: nested },
    }).finish();
    const decoded = protos.GroupMessageInner.decode(encoded);
    assert.equal(decoded.messageType, outer);
    assert.equal(decoded.messagePayload.messageType, nested);
  }
});
