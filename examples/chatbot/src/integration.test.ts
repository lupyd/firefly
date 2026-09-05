import test from 'node:test';
import * as assert from 'node:assert';
import { spawn } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import { FireflyClient, protos, initLogger } from 'firefly-client-js';

// Initialize the native Rust client logger
initLogger('/tmp/firefly/js-test.log');

const GroupMessageInner = protos.GroupMessageInner;
const UserMessageInner = protos.UserMessageInner;

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

test('Firefly JS Client Integration Test Suite - Rust Parity', { timeout: 120000 }, async (t) => {
  process.env.EMULATOR_MODE = 'true';
  process.env.NO_TOKEN_VERIFICATION = 'true';
  const baseUrl = process.env.FIREFLY_BASE_URL || 'http://127.0.0.1:39305';
  const wsUrl = process.env.FIREFLY_WS_URL || baseUrl.replace(/^http:\/\//, 'ws://').replace(/^https:\/\//, 'wss://') + '/';
  const port = process.env.PORT ? parseInt(process.env.PORT) : 39305;
  const dbSuffix = Math.floor(Math.random() * 100000);

  const testDir = `/tmp/firefly/ts_test_${dbSuffix}`;
  if (!fs.existsSync(testDir)) {
    fs.mkdirSync(testDir, { recursive: true });
  }

  const createClientHelper = (username: string) => {
    const dbFile = path.resolve(testDir, `${username}.db`);
    const sessionFile = path.resolve(testDir, `${username}-session.json`);
    return new FireflyClient({
      username,
      emulatorMode: true,
      apiBaseUrl: baseUrl,
      wsUrl: wsUrl,
      dbFile,
      sessionFile,
    });
  };

  const cleanupFiles = () => {
    try {
      if (fs.existsSync(testDir)) {
        fs.rmSync(testDir, { recursive: true, force: true });
      }
    } catch (e) {}
  };

  const shouldSpawnServer =
    process.env.SPAWN_SERVER === 'true' ||
    (!process.env.FIREFLY_BASE_URL &&
      (process.env.FIREFLY_SERVER_PATH ||
        fs.existsSync('/home/ash/lupyd/firefly-mls-server/target/debug/firefly-server') ||
        fs.existsSync('/home/ash/.cargo/target/debug/firefly-server')));

  let serverProcess: any = null;
  if (shouldSpawnServer) {
    console.log('Spawning Firefly MLS Server on port', port);
    const serverBin =
      process.env.FIREFLY_SERVER_PATH ||
      (fs.existsSync('/home/ash/lupyd/firefly-mls-server/target/debug/firefly-server')
        ? '/home/ash/lupyd/firefly-mls-server/target/debug/firefly-server'
        : '/home/ash/.cargo/target/debug/firefly-server');

    serverProcess = spawn(serverBin, [], {
      cwd: path.resolve(__dirname, '..'),
      env: {
        ...process.env,
        EMULATOR_MODE: 'true',
        NO_TOKEN_VERIFICATION: 'true',
        PORT: String(port),
        FIREFLY_BASE_URL: `http://127.0.0.1:${port}`,
        RUST_LOG: 'info',
      },
    });

    serverProcess.stdout.on('data', () => {});
    serverProcess.stderr.on('data', () => {});
  } else {
    console.log('Connecting to existing Firefly MLS Server at', baseUrl);
  }

  const killServer = () => {
    if (serverProcess) {
      console.log('Killing Firefly MLS Server...');
      serverProcess.kill('SIGKILL');
    }
  };

  process.on('exit', killServer);

  try {
    // Wait for server to bind
    await sleep(4000);

    // =========================================================================
    // SCENARIO 1 & 2: Direct Messaging (DM) + Group Messaging Flow
    // =========================================================================
    console.log('\n--- Running Scenario 1 & 2: DM and Group Flow ---');
    const alice = createClientHelper('alice');
    const bob = createClientHelper('bob');

    let aliceReceivedPing = false;
    let bobReceivedPong = false;

    alice.command('ping', async (ctx) => {
      console.log('Alice received /ping command from', ctx.sender);
      aliceReceivedPing = true;
      await ctx.reply('/pong');
    });

    bob.command('pong', async (ctx) => {
      console.log('Bob received /pong reply from', ctx.sender);
      bobReceivedPong = true;
    });

    let aliceReceivedGroupPing = false;
    let bobReceivedGroupPong = false;

    alice.command('groupping', async (ctx) => {
      console.log('Alice received /groupping from', ctx.sender);
      aliceReceivedGroupPing = true;
      await ctx.reply('/grouppong');
    });

    bob.command('grouppong', async (ctx) => {
      console.log('Bob received /grouppong from', ctx.sender);
      bobReceivedGroupPong = true;
    });

    await alice.start();
    await bob.start();
    await sleep(2000);

    // 1. DM Ping-Pong
    console.log('Testing Direct Message (Bob -> Alice)...');
    await bob.sendUserMessage('alice', '/ping');

    for (let i = 0; i < 20; i++) {
      if (aliceReceivedPing && bobReceivedPong) break;
      await sleep(500);
    }
    assert.ok(aliceReceivedPing, 'Alice should have received Bob DM');
    assert.ok(bobReceivedPong, 'Bob should have received Alice reply DM');
    console.log('✓ Direct messaging flow passed!');

    // 2. Group Creation & Invitation
    console.log('Testing Group Creation and Member Invite (Alice -> Bob)...');
    const groupInfo = await alice.createGroup('IntegrationGroup', 'Group for integration testing');
    const groupId = Number(groupInfo.id);
    console.log('Group created with ID:', groupId);

    await alice.addGroupMember(groupId, 'bob', 0);
    await bob.client.checkSetup();
    await sleep(2000);

    // Group Ping-Pong
    console.log('Testing Group Message exchange...');
    await bob.sendGroupMessage(groupId, '/groupping', 1);

    for (let i = 0; i < 20; i++) {
      if (aliceReceivedGroupPing && bobReceivedGroupPong) break;
      await sleep(500);
    }
    assert.ok(aliceReceivedGroupPing, 'Alice should have received Bob group message');
    assert.ok(bobReceivedGroupPong, 'Bob should have received Alice group reply');
    console.log('✓ Group messaging flow passed!');

    // =========================================================================
    // SCENARIO 3: Online Status Flow (Individual + Group Members)
    // =========================================================================
    console.log('\n--- Running Scenario 3: Online Status Flow ---');
    console.log('Querying online status for [alice, bob, charlie_offline]...');
    const onlineList1 = await alice.getOnlineStatus(['alice', 'bob', 'charlie_offline']);
    console.log('Online list:', onlineList1);
    assert.ok(onlineList1.includes('alice'), 'Alice should be online');
    assert.ok(onlineList1.includes('bob'), 'Bob should be online');
    assert.ok(!onlineList1.includes('charlie_offline'), 'charlie_offline should be offline');

    console.log('Fetching group members online status...');
    const memberStatus = await alice.getGroupMembersOnlineStatus(groupId);
    assert.ok(memberStatus.members && memberStatus.members.length >= 2);
    const aliceSt = memberStatus.members.find((m: any) => m.username === 'alice');
    const bobSt = memberStatus.members.find((m: any) => m.username === 'bob');
    assert.ok(aliceSt && aliceSt.isOnline, 'Alice should show online in group status');
    assert.ok(bobSt && bobSt.isOnline, 'Bob should show online in group status');
    assert.ok(aliceSt.lastConnectedAt > 0n);
    assert.ok(bobSt.lastConnectedAt > 0n);

    // Disconnect Bob and re-check
    console.log('Disposing Bob client to test disconnect detection...');
    await bob.dispose();
    await sleep(2000);

    const onlineList2 = await alice.getOnlineStatus(['alice', 'bob', 'charlie_offline']);
    console.log('Online list after Bob disposed:', onlineList2);
    assert.ok(onlineList2.includes('alice'), 'Alice should still be online');
    assert.ok(!onlineList2.includes('bob'), 'Bob should now be offline');
    console.log('✓ Online status flow passed!');

    // Dispose Alice before next scenarios
    await alice.dispose();

    // =========================================================================
    // SCENARIO 4: Kick Member & Permission Authorization Flow
    // =========================================================================
    console.log('\n--- Running Scenario 4: Kick Member & Permission Authorization ---');
    const kickOwner = createClientHelper('kick_alice');
    const kickBob = createClientHelper('kick_bob');
    const kickCharlie = createClientHelper('kick_charlie');

    let kickAliceReceivedMsg: string | null = null;
    let kickBobReceivedMsg: string | null = null;
    let kickCharlieReceivedMsg: string | null = null;

    kickOwner.onGroupMessage(async (ctx) => {
      console.log('[kickOwner received group message]', ctx.text, 'from', ctx.sender);
      kickAliceReceivedMsg = ctx.text;
    });
    kickBob.onGroupMessage(async (ctx) => {
      console.log('[kickBob received group message]', ctx.text, 'from', ctx.sender);
      kickBobReceivedMsg = ctx.text;
    });
    kickCharlie.onGroupMessage(async (ctx) => {
      console.log('[kickCharlie received group message]', ctx.text, 'from', ctx.sender);
      kickCharlieReceivedMsg = ctx.text;
    });

    await kickOwner.start();
    await kickBob.start();
    await kickCharlie.start();
    await sleep(2000);

    const kickGroup = await kickOwner.createGroup('Kick Test Group', 'Testing permissions & kicks');
    const kickGroupId = Number(kickGroup.id);

    console.log('Adding Bob and Charlie to Kick Test Group...');
    await kickOwner.addGroupMember(kickGroupId, 'kick_bob', 0);
    await kickOwner.addGroupMember(kickGroupId, 'kick_charlie', 0);

    await kickBob.client.checkSetup();
    await kickCharlie.client.checkSetup();
    await sleep(2000);

    // Charlie (role 0) attempts to kick Alice (owner) - MUST FAIL!
    console.log('Testing: Charlie attempts to kick Owner Alice (must fail)...');
    let charlieKickAliceFailed = false;
    try {
      await kickCharlie.kickGroupMember(kickGroupId, 'kick_alice');
    } catch (err) {
      charlieKickAliceFailed = true;
      console.log('Charlie kick Alice correctly failed with error:', String(err));
    }
    assert.ok(charlieKickAliceFailed, 'Non-admin member should not be able to kick owner');

    // Charlie (role 0) attempts to kick Bob - MUST FAIL!
    console.log('Testing: Charlie attempts to kick Bob (must fail)...');
    let charlieKickBobFailed = false;
    try {
      await kickCharlie.kickGroupMember(kickGroupId, 'kick_bob');
    } catch (err) {
      charlieKickBobFailed = true;
      console.log('Charlie kick Bob correctly failed with error:', String(err));
    }
    assert.ok(charlieKickBobFailed, 'Non-admin member should not be able to kick peer member');

    // Alice kicks Bob
    console.log('Owner Alice kicking Bob...');
    await kickOwner.kickGroupMember(kickGroupId, 'kick_bob');
    await sleep(2000);

    // Charlie syncs kick
    console.log('Charlie syncing group setup...');
    await kickCharlie.client.checkSetup();
    await sleep(1000);

    // Bob attempts to send message
    console.log('Kicked Bob attempting to send group message...');
    let bobSendFailed = false;
    try {
      await kickBob.sendGroupMessage(kickGroupId, 'Hello from kicked Bob!', 0);
    } catch (err) {
      bobSendFailed = true;
      console.log('Bob send immediately rejected by server:', (err as Error).message);
    }

    // Wait and verify Alice and Charlie do not receive anything from Bob
    await sleep(3000);
    assert.ok(
      kickAliceReceivedMsg !== 'Hello from kicked Bob!',
      'Alice should NOT receive messages from kicked Bob'
    );
    assert.ok(
      kickCharlieReceivedMsg !== 'Hello from kicked Bob!',
      'Charlie should NOT receive messages from kicked Bob'
    );

    // Charlie sends message to group -> Alice receives it
    console.log('Charlie sending group message to verify active group membership...');
    await kickCharlie.sendGroupMessage(kickGroupId, 'Hello from Charlie, still here!', 0);

    for (let i = 0; i < 20; i++) {
      if (kickAliceReceivedMsg === 'Hello from Charlie, still here!') break;
      await sleep(500);
    }
    assert.strictEqual(
      kickAliceReceivedMsg,
      'Hello from Charlie, still here!',
      'Alice should receive message from Charlie'
    );
    console.log('✓ Kick member and permissions flow passed!');

    await kickOwner.dispose();
    await kickBob.dispose();
    await kickCharlie.dispose();

    // =========================================================================
    // SCENARIO 5: Public Join Link Flow
    // =========================================================================
    console.log('\n--- Running Scenario 5: Public Join Link Flow ---');
    const linkOwner = createClientHelper('link_alice');
    const linkBob = createClientHelper('link_bob');
    const linkCharlie = createClientHelper('link_charlie');
    const linkDave = createClientHelper('link_dave');

    let linkBobReceivedAliceMsg = false;
    let linkAliceReceivedBobMsg = false;

    linkOwner.onGroupMessage(async (ctx) => {
      console.log('[linkOwner received group message]', ctx.text, 'from', ctx.sender);
      if (ctx.text === 'Hello from Bob via link!') {
        linkAliceReceivedBobMsg = true;
      }
    });

    linkBob.onGroupMessage(async (ctx) => {
      console.log('[linkBob received group message]', ctx.text, 'from', ctx.sender);
      if (ctx.text === 'Hello to public group from Alice!') {
        linkBobReceivedAliceMsg = true;
      }
    });

    await linkOwner.start();
    await linkBob.start();
    await sleep(2000);

    const pubGroup = await linkOwner.createGroup('Public Link Group', 'Test join links');
    const pubGroupId = Number(pubGroup.id);

    console.log('Owner Alice creating join link (expires: 3600s, max_uses: 10)...');
    const joinToken = await linkOwner.createJoinLink(pubGroupId, 3600, 10);
    console.log('Created join link token:', joinToken);

    console.log('Bob joining via link...');
    await linkBob.joinViaLink(joinToken);

    // Wait for Alice to receive groupJoinRequests and auto-process it
    console.log('Waiting for owner Alice to auto-process join request...');
    await sleep(4000);

    console.log('Owner Alice sending message to group...');
    await linkOwner.sendGroupMessage(pubGroupId, 'Hello to public group from Alice!', 0);

    for (let i = 0; i < 20; i++) {
      if (linkBobReceivedAliceMsg) break;
      await sleep(500);
    }
    assert.ok(linkBobReceivedAliceMsg, 'Bob should receive Alice message in link-joined group');

    console.log('Bob sending message to group...');
    await linkBob.sendGroupMessage(pubGroupId, 'Hello from Bob via link!', 0);

    for (let i = 0; i < 20; i++) {
      if (linkAliceReceivedBobMsg) break;
      await sleep(500);
    }
    assert.ok(linkAliceReceivedBobMsg, 'Alice should receive Bob message');

    // Test max uses limit = 1
    console.log('Testing max_uses limit (max_uses: 1)...');
    const maxUseToken = await linkOwner.createJoinLink(pubGroupId, 3600, 1);

    await linkCharlie.start();
    await linkDave.start();
    await sleep(2000);

    console.log('Charlie joins via 1-use token (should succeed)...');
    await linkCharlie.joinViaLink(maxUseToken);

    console.log('Dave joins via already used 1-use token (must fail)...');
    let daveJoinFailed = false;
    try {
      await linkDave.joinViaLink(maxUseToken);
    } catch (err) {
      daveJoinFailed = true;
      const errStr = String(err);
      console.log('Dave join correctly failed with error:', errStr);
      assert.ok(
        errStr.includes('Invalid link'),
        'Error should indicate invalid link'
      );
    }
    assert.ok(daveJoinFailed, 'Dave should fail to join with exhausted link');

    // Test link expiry
    console.log('Testing link expiry (expires_in_seconds: 1)...');
    const expiryToken = await linkOwner.createJoinLink(pubGroupId, 1, 10);
    await sleep(2500); // wait for link to expire

    console.log('Dave joins via expired token (must fail)...');
    let daveExpiryFailed = false;
    try {
      await linkDave.joinViaLink(expiryToken);
    } catch (err) {
      daveExpiryFailed = true;
      const errStr = String(err);
      console.log('Dave join expired link correctly failed with error:', errStr);
      assert.ok(
        errStr.includes('Invalid link'),
        'Error should indicate invalid link'
      );
    }
    assert.ok(daveExpiryFailed, 'Dave should fail to join expired link');
    console.log('✓ Public join link flow passed!');

    await linkOwner.dispose();
    await linkBob.dispose();
    await linkCharlie.dispose();
    await linkDave.dispose();

    console.log('\n=============================================');
    console.log('ALL INTEGRATION TEST SCENARIOS PASSED!');
    console.log('=============================================\n');
  } finally {
    process.off('exit', killServer);
    killServer();
    cleanupFiles();
  }
});
