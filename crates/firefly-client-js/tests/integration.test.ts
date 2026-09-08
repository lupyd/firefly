import test from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as path from 'path';
import {
  FireflyClient,
  StorageProviders,
  RawUserMessage,
  RawGroupMessage,
  RawGroupInfo,
} from '../src/index';

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

test('firefly-client-js - End-to-End integration test against actual server', { timeout: 120000 }, async () => {
  const baseUrl = process.env.FIREFLY_BASE_URL || 'http://127.0.0.1:39209';
  const wsUrl =
    process.env.FIREFLY_WS_URL ||
    baseUrl.replace(/^http:\/\//, 'ws://').replace(/^https:\/\//, 'wss://') + '/';

  console.log(`[firefly-client-js test] Connecting to server at: ${baseUrl} (${wsUrl})`);

  // Verify server is reachable
  try {
    const res = await fetch(`${baseUrl}/health`).catch(() => fetch(`${baseUrl}/`));
    if (!res.ok && res.status !== 404) {
      console.warn(`[firefly-client-js test] Server returned status ${res.status}`);
    }
  } catch (err) {
    console.error(`[firefly-client-js test] Cannot reach server at ${baseUrl}. Ensure server is running.`);
    throw err;
  }

  const runId = Math.floor(Math.random() * 1000000);
  const testDir = `/tmp/firefly/js_test_${runId}`;
  fs.mkdirSync(testDir, { recursive: true });

  const createClientHelper = (username: string, storage?: StorageProviders) => {
    return new FireflyClient({
      username,
      emulatorMode: true,
      apiBaseUrl: baseUrl,
      wsUrl,
      dbFile: path.resolve(testDir, `${username}.db`),
      sessionFile: path.resolve(testDir, `${username}-session.json`),
      storageProviders: storage,
    });
  };

  const cleanupFiles = () => {
    try {
      if (fs.existsSync(testDir)) {
        fs.rmSync(testDir, { recursive: true, force: true });
      }
    } catch (_) {}
  };

  const aliceUser = `alice_${runId}`;
  const bobUser = `bob_${runId}`;
  const charlieUser = `charlie_${runId}`;
  const offlineUser = `offline_${runId}`;

  const alice = createClientHelper(aliceUser);
  const bob = createClientHelper(bobUser);

  try {
    // =========================================================================
    // SCENARIO 1 & 2: Direct Messaging (DM) + Group Messaging Flow
    // =========================================================================
    console.log('\n--- Running Scenario 1: Direct Messaging Flow ---');
    let aliceReceivedPing = false;
    let bobReceivedPong = false;

    alice.command('ping', async (ctx) => {
      console.log(`[${aliceUser}] received /ping from ${ctx.sender}`);
      aliceReceivedPing = true;
      await ctx.reply('/pong');
    });

    bob.command('pong', async (ctx) => {
      console.log(`[${bobUser}] received /pong reply from ${ctx.sender}`);
      bobReceivedPong = true;
    });

    let aliceReceivedGroupPing = false;
    let bobReceivedGroupPong = false;

    alice.command('groupping', async (ctx) => {
      console.log(`[${aliceUser}] received /groupping from ${ctx.sender}`);
      aliceReceivedGroupPing = true;
      await ctx.reply('/grouppong');
    });

    bob.command('grouppong', async (ctx) => {
      console.log(`[${bobUser}] received /grouppong reply from ${ctx.sender}`);
      bobReceivedGroupPong = true;
    });

    await alice.start();
    await bob.start();
    await sleep(2000);

    console.log(`Testing Direct Message (${bobUser} -> ${aliceUser})...`);
    await bob.sendUserMessage(aliceUser, '/ping');

    for (let i = 0; i < 30; i++) {
      if (aliceReceivedPing && bobReceivedPong) break;
      await sleep(500);
    }
    assert.ok(aliceReceivedPing, 'Alice should have received Bob DM');
    assert.ok(bobReceivedPong, 'Bob should have received Alice reply DM');
    console.log('✓ Direct messaging flow passed!');

    // 2. Group Creation & Invitation
    console.log('\n--- Running Scenario 2: Group Creation and Messaging Flow ---');
    const group = await alice.createGroup(`Group_${runId}`, 'Integration test group');
    const groupId = Number(group.id);
    console.log('Group created with ID:', groupId);

    await alice.addGroupMember(groupId, bobUser, 0);
    await bob.client.checkSetup();
    await sleep(2000);

    console.log('Testing Group Message exchange...');
    await bob.sendGroupMessage(groupId, '/groupping', 1);

    for (let i = 0; i < 30; i++) {
      if (aliceReceivedGroupPing && bobReceivedGroupPong) break;
      await sleep(500);
    }
    assert.ok(aliceReceivedGroupPing, 'Alice should have received Bob group message');
    assert.ok(bobReceivedGroupPong, 'Bob should have received Alice group reply');
    console.log('✓ Group messaging flow passed!');

    // =========================================================================
    // SCENARIO 3: Online Status Flow
    // =========================================================================
    console.log('\n--- Running Scenario 3: Online Status Flow ---');
    const onlineList = await alice.getOnlineStatus([aliceUser, bobUser, offlineUser]);
    console.log('Online list result:', onlineList);
    assert.ok(onlineList.includes(aliceUser), 'Alice should be online');
    assert.ok(onlineList.includes(bobUser), 'Bob should be online');
    assert.ok(!onlineList.includes(offlineUser), 'Offline user should not be online');

    const memberStatus = await alice.getGroupMembersOnlineStatus(groupId);
    assert.ok(memberStatus.members && memberStatus.members.length >= 2);
    const aliceSt = memberStatus.members.find((m: any) => m.username === aliceUser);
    const bobSt = memberStatus.members.find((m: any) => m.username === bobUser);
    assert.ok(aliceSt && aliceSt.isOnline, 'Alice should show online in group status');
    assert.ok(bobSt && bobSt.isOnline, 'Bob should show online in group status');
    console.log('✓ Online status flow passed!');

    await alice.dispose();
    await bob.dispose();

    // =========================================================================
    // SCENARIO 4: Permissions & Kick Member Flow
    // =========================================================================
    console.log('\n--- Running Scenario 4: Permissions & Kick Member Flow ---');
    const kickOwnerUser = `k_alice_${runId}`;
    const kickBobUser = `k_bob_${runId}`;
    const kickCharlieUser = `k_charlie_${runId}`;

    const kickOwner = createClientHelper(kickOwnerUser);
    const kickBob = createClientHelper(kickBobUser);
    const kickCharlie = createClientHelper(kickCharlieUser);

    let kickAliceReceivedMsg: string | null = null;
    let kickCharlieReceivedMsg: string | null = null;

    kickOwner.onGroupMessage(async (ctx) => {
      kickAliceReceivedMsg = ctx.text;
    });
    kickCharlie.onGroupMessage(async (ctx) => {
      kickCharlieReceivedMsg = ctx.text;
    });

    await kickOwner.start();
    await kickBob.start();
    await kickCharlie.start();
    await sleep(2000);

    const kickGroup = await kickOwner.createGroup(`KickGroup_${runId}`, 'Permissions test');
    const kickGroupId = Number(kickGroup.id);

    await kickOwner.addGroupMember(kickGroupId, kickBobUser, 0);
    await kickOwner.addGroupMember(kickGroupId, kickCharlieUser, 0);

    await kickBob.client.checkSetup();
    await kickCharlie.client.checkSetup();
    await sleep(2000);

    // Non-owner Charlie attempts to kick Owner -> MUST FAIL
    let charlieKickAliceFailed = false;
    try {
      await kickCharlie.kickGroupMember(kickGroupId, kickOwnerUser);
    } catch (_) {
      charlieKickAliceFailed = true;
    }
    assert.ok(charlieKickAliceFailed, 'Non-admin member should not be able to kick owner');

    // Owner kicks Bob
    await kickOwner.kickGroupMember(kickGroupId, kickBobUser);
    await sleep(2000);

    // Charlie syncs kick
    await kickCharlie.client.checkSetup();
    await sleep(1000);

    // Kicked Bob attempts to send message -> rejected
    let bobSendFailed = false;
    try {
      await kickBob.sendGroupMessage(kickGroupId, 'Hello from kicked Bob!', 0);
    } catch (_) {
      bobSendFailed = true;
    }

    await sleep(2000);
    assert.ok(kickAliceReceivedMsg !== 'Hello from kicked Bob!');
    assert.ok(kickCharlieReceivedMsg !== 'Hello from kicked Bob!');

    // Active member Charlie sends message -> Alice receives it
    await kickCharlie.sendGroupMessage(kickGroupId, 'Hello from active Charlie!', 0);
    for (let i = 0; i < 20; i++) {
      if (kickAliceReceivedMsg === 'Hello from active Charlie!') break;
      await sleep(500);
    }
    assert.strictEqual(kickAliceReceivedMsg, 'Hello from active Charlie!');
    console.log('✓ Kick member and permissions flow passed!');

    await kickOwner.dispose();
    await kickBob.dispose();
    await kickCharlie.dispose();

    // =========================================================================
    // SCENARIO 5: Public Join Link Flow
    // =========================================================================
    console.log('\n--- Running Scenario 5: Public Join Link Flow ---');
    const linkOwnerUser = `link_owner_${runId}`;
    const linkJoinerUser = `link_joiner_${runId}`;

    const linkOwner = createClientHelper(linkOwnerUser);
    const linkJoiner = createClientHelper(linkJoinerUser);

    let linkOwnerReceivedJoinerMsg = false;
    linkOwner.onGroupMessage(async (ctx) => {
      if (ctx.text === 'Hello via join link!') {
        linkOwnerReceivedJoinerMsg = true;
      }
    });

    await linkOwner.start();
    await linkJoiner.start();
    await sleep(2000);

    const pubGroup = await linkOwner.createGroup(`LinkGroup_${runId}`, 'Join link test');
    const pubGroupId = Number(pubGroup.id);

    const joinToken = await linkOwner.createJoinLink(pubGroupId, 3600, 10);
    console.log('Created join link token:', joinToken);

    await linkJoiner.joinViaLink(joinToken);
    await sleep(4000);

    await linkJoiner.sendGroupMessage(pubGroupId, 'Hello via join link!', 0);
    for (let i = 0; i < 20; i++) {
      if (linkOwnerReceivedJoinerMsg) break;
      await sleep(500);
    }
    assert.ok(linkOwnerReceivedJoinerMsg, 'Owner should receive message from member who joined via link');
    console.log('✓ Public join link flow passed!');

    await linkOwner.dispose();
    await linkJoiner.dispose();

    // =========================================================================
    // SCENARIO 6: Custom Storage Providers Interception Flow
    // =========================================================================
    console.log('\n--- Running Scenario 6: Custom Storage Providers Flow ---');
    const interceptedUserMessages: RawUserMessage[] = [];
    const interceptedGroupMessages: RawGroupMessage[] = [];
    const interceptedGroupInfo = new Map<number, RawGroupInfo>();
    let keyPackagesInserted = 0;
    const rawKeyPackages = new Map<string, Uint8Array>();

    const customStorage: StorageProviders = {
      mlsKeyPackageStorage: {
        insert: (id: Uint8Array, data: Uint8Array) => {
          keyPackagesInserted++;
          rawKeyPackages.set(Buffer.from(id).toString('hex'), data);
          return true;
        },
        delete: (id: Uint8Array) => {
          rawKeyPackages.delete(Buffer.from(id).toString('hex'));
          return true;
        },
        get: (id: Uint8Array) => {
          return rawKeyPackages.get(Buffer.from(id).toString('hex')) || null;
        },
      },
      userMessageStorage: {
        add: (msg: RawUserMessage) => {
          interceptedUserMessages.push(msg);
        },
        getLastMessagesOf: (other: string) => {
          return Promise.resolve(
            interceptedUserMessages.filter((m) => m.other === other || m.from === other || m.to === other)
          );
        },
      },
      groupMessageStorage: {
        add: (msg: RawGroupMessage) => {
          interceptedGroupMessages.push(msg);
        },
        get: (groupId: number | bigint) => {
          return Promise.resolve(interceptedGroupMessages.filter((m) => m.groupId === Number(groupId)));
        },
        getLastMessageOfGroup: (groupId: number | bigint) => {
          const msgs = interceptedGroupMessages.filter((m) => m.groupId === Number(groupId));
          return Promise.resolve(msgs[msgs.length - 1] || null);
        },
        deleteByGroupId: (groupId: number | bigint) => {
          const idx = interceptedGroupMessages.findIndex((m) => m.groupId === Number(groupId));
          if (idx !== -1) interceptedGroupMessages.splice(idx, 1);
        },
        updateCursor: (_id: number | bigint, _groupId: number | bigint, _epoch: number) => {},
      },
      groupInfoStorage: {
        getAll: () => Promise.resolve(Array.from(interceptedGroupInfo.values())),
        get: (groupId: number | bigint) => Promise.resolve(interceptedGroupInfo.get(Number(groupId)) || null),
        set: (group: RawGroupInfo) => {
          interceptedGroupInfo.set(group.id ?? group.groupId!, group);
        },
        delete: (groupId: number | bigint) => {
          interceptedGroupInfo.delete(Number(groupId));
        },
      },
    };

    const sAliceUser = `s_alice_${runId}`;
    const sBobUser = `s_bob_${runId}`;

    const sAlice = createClientHelper(sAliceUser, customStorage);
    const sBob = createClientHelper(sBobUser);

    let sAliceReceivedPing = false;
    let sBobReceivedPong = false;

    sAlice.command('ping', async (ctx) => {
      sAliceReceivedPing = true;
      await ctx.reply('/pong');
    });
    sBob.command('pong', async (ctx) => {
      sBobReceivedPong = true;
    });

    await sAlice.start();
    await sBob.start();
    await sleep(2000);

    await sBob.sendUserMessage(sAliceUser, '/ping');
    for (let i = 0; i < 20; i++) {
      if (sAliceReceivedPing && sBobReceivedPong) break;
      await sleep(500);
    }
    assert.ok(sAliceReceivedPing);
    assert.ok(sBobReceivedPong);
    assert.ok(interceptedUserMessages.length >= 1, 'Custom user storage intercepted messages');

    const sGroup = await sAlice.createGroup(`CustomStorageGroup_${runId}`, 'Testing raw storage');
    const sGroupId = Number(sGroup.id);

    await sAlice.addGroupMember(sGroupId, sBobUser, 0);
    await sBob.client.checkSetup();
    await sleep(2000);

    assert.ok(interceptedGroupInfo.has(sGroupId), 'Custom group info storage intercepted group');

    let sAliceReceivedGroupMsg = false;
    sAlice.onGroupMessage((_) => {
      sAliceReceivedGroupMsg = true;
    });

    await sBob.sendGroupMessage(sGroupId, 'Hello with custom storage!', 1);
    for (let i = 0; i < 20; i++) {
      if (sAliceReceivedGroupMsg) break;
      await sleep(500);
    }
    assert.ok(sAliceReceivedGroupMsg);
    assert.ok(interceptedGroupMessages.length >= 1, 'Custom group message storage intercepted messages');
    assert.ok(keyPackagesInserted > 0, 'Custom MLS key package storage intercepted insertions');
    console.log('✓ Custom storage providers flow passed!');

    await sAlice.dispose();
    await sBob.dispose();

    console.log('\n=============================================');
    console.log('ALL FIREFLY-CLIENT-JS INTEGRATION TESTS PASSED!');
    console.log('=============================================\n');
  } finally {
    cleanupFiles();
  }
});
