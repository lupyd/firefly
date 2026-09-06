import test from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as path from 'path';
import { FireflyBot, FireflyClient } from 'firefly-client-js';

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

test('FireflyBot - command registration and normalization', () => {
  const bot = new FireflyBot({ username: 'mock_bot', emulatorMode: true });

  bot.command('hello', async () => {});
  bot.command('/joke', async () => {});

  assert.ok(bot.commands.has('/hello'));
  assert.ok(bot.commands.has('/joke'));
});

test('FireflyBot - direct message command handling against actual server', { timeout: 60000 }, async () => {
  const baseUrl = process.env.FIREFLY_BASE_URL || 'http://127.0.0.1:39209';
  const wsUrl =
    process.env.FIREFLY_WS_URL ||
    baseUrl.replace(/^http:\/\//, 'ws://').replace(/^https:\/\//, 'wss://') + '/';

  const runId = Math.floor(Math.random() * 1000000);
  const testDir = `/tmp/firefly/bot_dm_test_${runId}`;
  fs.mkdirSync(testDir, { recursive: true });

  const botUsername = `bot_dm_${runId}`;
  const userUsername = `user_dm_${runId}`;

  const bot = new FireflyBot({
    username: botUsername,
    emulatorMode: true,
    apiBaseUrl: baseUrl,
    wsUrl,
    dbFile: path.resolve(testDir, `${botUsername}.db`),
    sessionFile: path.resolve(testDir, `${botUsername}-session.json`),
  });

  const user = new FireflyClient({
    username: userUsername,
    emulatorMode: true,
    apiBaseUrl: baseUrl,
    wsUrl,
    dbFile: path.resolve(testDir, `${userUsername}.db`),
    sessionFile: path.resolve(testDir, `${userUsername}-session.json`),
  });

  let botReceivedSender: string | null = null;
  let botReceivedArgs: string[] = [];
  let botCommandExecuted = false;

  bot.command('testdm', async (ctx) => {
    botCommandExecuted = true;
    botReceivedSender = ctx.sender;
    botReceivedArgs = ctx.args;
    assert.strictEqual(ctx.isGroup, false);
    assert.strictEqual(ctx.groupId, null);
    await ctx.reply(`replying to personal chat: ${ctx.args.join(' ')}`);
  });

  let userReceivedReply: string | null = null;
  user.onMessage(async (ctx) => {
    userReceivedReply = ctx.text;
  });

  try {
    await bot.start();
    await user.start();
    await sleep(2000);

    console.log(`User (${userUsername}) sending /testdm hello world to Bot (${botUsername})...`);
    await user.sendUserMessage(botUsername, '/testdm hello world');

    for (let i = 0; i < 30; i++) {
      if (botCommandExecuted && userReceivedReply) break;
      await sleep(500);
    }

    assert.strictEqual(botCommandExecuted, true, 'Bot command handler should have executed');
    assert.strictEqual(botReceivedSender, userUsername, 'Bot should identify user as sender');
    assert.deepStrictEqual(botReceivedArgs, ['hello', 'world'], 'Bot should parse command arguments');
    assert.strictEqual(
      userReceivedReply,
      'replying to personal chat: hello world',
      'User should receive bot reply message'
    );
    console.log('✓ Bot direct message test passed against actual server!');
  } finally {
    await bot.dispose();
    await user.dispose();
    try {
      if (fs.existsSync(testDir)) {
        fs.rmSync(testDir, { recursive: true, force: true });
      }
    } catch (_) {}
  }
});

test('FireflyBot - group message command handling against actual server', { timeout: 60000 }, async () => {
  const baseUrl = process.env.FIREFLY_BASE_URL || 'http://127.0.0.1:39209';
  const wsUrl =
    process.env.FIREFLY_WS_URL ||
    baseUrl.replace(/^http:\/\//, 'ws://').replace(/^https:\/\//, 'wss://') + '/';

  const runId = Math.floor(Math.random() * 1000000);
  const testDir = `/tmp/firefly/bot_grp_test_${runId}`;
  fs.mkdirSync(testDir, { recursive: true });

  const botUsername = `bot_grp_${runId}`;
  const userUsername = `user_grp_${runId}`;

  const bot = new FireflyBot({
    username: botUsername,
    emulatorMode: true,
    apiBaseUrl: baseUrl,
    wsUrl,
    dbFile: path.resolve(testDir, `${botUsername}.db`),
    sessionFile: path.resolve(testDir, `${botUsername}-session.json`),
  });

  const user = new FireflyClient({
    username: userUsername,
    emulatorMode: true,
    apiBaseUrl: baseUrl,
    wsUrl,
    dbFile: path.resolve(testDir, `${userUsername}.db`),
    sessionFile: path.resolve(testDir, `${userUsername}-session.json`),
  });

  let botReceivedSender: string | null = null;
  let botReceivedGroupId: number | null = null;
  let botReceivedChannelId: number | null = null;
  let botReceivedArgs: string[] = [];
  let botGroupCommandExecuted = false;

  bot.command('testgrp', async (ctx) => {
    botGroupCommandExecuted = true;
    botReceivedSender = ctx.sender;
    botReceivedGroupId = ctx.groupId;
    botReceivedChannelId = ctx.channelId;
    botReceivedArgs = ctx.args;
    assert.strictEqual(ctx.isGroup, true);
    await ctx.reply(`replying to group: ${ctx.args.join(' ')}`);
  });

  let userReceivedGroupReply: string | null = null;
  user.onGroupMessage(async (ctx) => {
    if (ctx.sender === botUsername) {
      userReceivedGroupReply = ctx.text;
    }
  });

  try {
    await bot.start();
    await user.start();
    await sleep(2000);

    console.log(`User (${userUsername}) creating group and adding Bot (${botUsername})...`);
    const group = await user.createGroup(`BotGroup_${runId}`, 'Bot test group');
    const groupId = Number(group.id);

    await user.addGroupMember(groupId, botUsername, 0);
    await sleep(3000);

    console.log(`User (${userUsername}) sending /testgrp arg1 arg2 to Group ${groupId}...`);
    await user.sendGroupMessage(groupId, '/testgrp arg1 arg2', 1);

    for (let i = 0; i < 30; i++) {
      if (botGroupCommandExecuted && userReceivedGroupReply) break;
      await sleep(500);
    }

    assert.strictEqual(botGroupCommandExecuted, true, 'Bot group command handler should have executed');
    assert.strictEqual(botReceivedSender, userUsername, 'Bot should identify user as sender');
    assert.strictEqual(botReceivedGroupId, groupId, 'Bot should identify correct group ID');
    assert.strictEqual(botReceivedChannelId, 1, 'Bot should identify correct channel ID');
    assert.deepStrictEqual(botReceivedArgs, ['arg1', 'arg2'], 'Bot should parse group arguments');
    assert.strictEqual(
      userReceivedGroupReply,
      'replying to group: arg1 arg2',
      'User should receive bot group reply message'
    );
    console.log('✓ Bot group message test passed against actual server!');
  } finally {
    await bot.dispose();
    await user.dispose();
    try {
      if (fs.existsSync(testDir)) {
        fs.rmSync(testDir, { recursive: true, force: true });
      }
    } catch (_) {}
  }
});
