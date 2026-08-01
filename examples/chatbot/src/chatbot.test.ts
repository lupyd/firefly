import test from 'node:test';
import * as assert from 'node:assert';
import { FireflyBot, protos } from 'firefly-client-js';

const GroupMessageInner = protos.GroupMessageInner;
const UserMessageInner = protos.UserMessageInner;

test('FireflyBot - command registration and normalization', () => {
  const bot = new FireflyBot({ username: 'mock_bot', emulatorMode: true });
  
  bot.command('hello', async () => {});
  bot.command('/joke', async () => {});

  assert.ok(bot.commands.has('/hello'));
  assert.ok(bot.commands.has('/joke'));
});

test('FireflyBot - command routing for group message and replying', async () => {
  const bot = new FireflyBot({ username: 'mock_bot', emulatorMode: true });
  
  let groupSentGroupId: number | null = null;
  let groupSentPayload: number[] | null = null;

  // Mock FFI Client
  bot.client = {
    encryptAndSendGroup: async (groupId: number, payload: number[]) => {
      groupSentGroupId = groupId;
      groupSentPayload = payload;
      return 123;
    }
  };

  let handlerExecuted = false;
  bot.command('testgrp', async (ctx) => {
    handlerExecuted = true;
    assert.strictEqual(ctx.sender, 'user1');
    assert.strictEqual(ctx.isGroup, true);
    assert.strictEqual(ctx.groupId, 999);
    assert.strictEqual(ctx.channelId, 1);
    assert.deepStrictEqual(ctx.args, ['arg1', 'arg2']);
    
    await ctx.reply('replying to group');
  });

  // Simulate incoming group message
  await bot._handleMessage({
    text: '/testgrp arg1 arg2',
    sender: 'user1',
    isGroup: true,
    groupId: 999,
    channelId: 1
  });

  assert.strictEqual(handlerExecuted, true);
  assert.strictEqual(groupSentGroupId, 999);
  assert.ok(Array.isArray(groupSentPayload));

  // Decode the sent payload to verify correctness
  const decoded = GroupMessageInner.decode(new Uint8Array(groupSentPayload as any)) as any;
  assert.strictEqual(decoded.channelId, 1);
  assert.strictEqual(decoded.messagePayload?.text, 'replying to group');
});

test('FireflyBot - command routing for direct message (personal chat) and replying', async () => {
  const bot = new FireflyBot({ username: 'mock_bot', emulatorMode: true });
  
  let dmSentTo: string | null = null;
  let dmSentPayload: number[] | null = null;

  // Mock FFI Client
  bot.client = {
    encryptAndSend: async (to: string, payload: number[]) => {
      dmSentTo = to;
      dmSentPayload = payload;
      return {};
    }
  };

  let handlerExecuted = false;
  bot.command('testdm', async (ctx) => {
    handlerExecuted = true;
    assert.strictEqual(ctx.sender, 'user2');
    assert.strictEqual(ctx.isGroup, false);
    assert.strictEqual(ctx.groupId, null);
    
    await ctx.reply('replying to personal chat');
  });

  // Simulate incoming direct message
  await bot._handleMessage({
    text: '/testdm',
    sender: 'user2',
    isGroup: false,
    groupId: null,
    channelId: null
  });

  assert.strictEqual(handlerExecuted, true);
  assert.strictEqual(dmSentTo, 'user2');
  assert.ok(Array.isArray(dmSentPayload));

  // Decode the sent payload to verify correctness
  const decoded = UserMessageInner.decode(new Uint8Array(dmSentPayload as any)) as any;
  assert.strictEqual(decoded.messagePayload?.text, 'replying to personal chat');
});
