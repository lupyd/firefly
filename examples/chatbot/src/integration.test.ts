import test from 'node:test';
import * as assert from 'node:assert';
import { spawn } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import { FireflyClient, protos, initLogger } from 'firefly-client-js';

// Initialize the native Rust client logger to write all internal logs to js-test.log
initLogger('/tmp/firefly/js-test.log');

const GroupMessageInner = protos.GroupMessageInner;
const UserMessageInner = protos.UserMessageInner;

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

test('Firefly JS Client Integration Test - DM, Group Invite, and Group Messaging', { timeout: 60000 }, async (t) => {
  process.env.EMULATOR_MODE = 'true';
  process.env.NO_TOKEN_VERIFICATION = 'true';
  process.env.FIREFLY_BASE_URL = 'http://127.0.0.1:39305';
  const port = 39305;
  const dbSuffix = Math.floor(Math.random() * 100000);
  const aliceDb = path.resolve(process.cwd(), `alice-test-${dbSuffix}.db`);
  const bobDb = path.resolve(process.cwd(), `bob-test-${dbSuffix}.db`);
  const aliceSession = path.resolve(process.cwd(), `alice-session-${dbSuffix}.json`);
  const bobSession = path.resolve(process.cwd(), `bob-session-${dbSuffix}.json`);

  // Cleanup pre-existing files
  const cleanupFiles = () => {
    [aliceDb, bobDb, aliceSession, bobSession].forEach((f) => {
      if (fs.existsSync(f)) {
        try {
          fs.unlinkSync(f);
        } catch (e) {}
      }
    });
  };
  cleanupFiles();

  console.log('Spawning Firefly MLS Server on port', port);
  const serverBin = process.env.FIREFLY_SERVER_PATH || (fs.existsSync('/home/ash/lupyd/firefly-mls-server/target/debug/firefly-server') ? '/home/ash/lupyd/firefly-mls-server/target/debug/firefly-server' : '/home/ash/.cargo/target/debug/firefly-server');
  const serverProcess = spawn(serverBin, [], {
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

  serverProcess.stdout.on('data', (data) => {
    console.log('[SERVER STDOUT]', data.toString().trim());
  });
  serverProcess.stderr.on('data', (data) => {
    console.error('[SERVER STDERR]', data.toString().trim());
  });

  // Make sure we kill the server on any exit
  const killServer = () => {
    console.log('Killing Firefly MLS Server...');
    serverProcess.kill('SIGKILL');
  };

  process.on('exit', killServer);

  try {
    // Wait for the server to start up
    await sleep(4000);

    console.log('Initializing Alice Client...');
    const alice = new FireflyClient({
      username: 'alice',
      emulatorMode: true,
      apiBaseUrl: `http://127.0.0.1:${port}`,
      wsUrl: `ws://127.0.0.1:${port}/`,
      dbFile: aliceDb,
      sessionFile: aliceSession,
    });

    console.log('Initializing Bob Client...');
    const bob = new FireflyClient({
      username: 'bob',
      emulatorMode: true,
      apiBaseUrl: `http://127.0.0.1:${port}`,
      wsUrl: `ws://127.0.0.1:${port}/`,
      dbFile: bobDb,
      sessionFile: bobSession,
    });

    // Registrations for DMs
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

    // Registrations for Group Messages
    let aliceReceivedGroupPing = false;
    let bobReceivedGroupPong = false;

    alice.command('groupping', async (ctx) => {
      console.log('Alice received /groupping group command from', ctx.sender);
      aliceReceivedGroupPing = true;
      await ctx.reply('/grouppong');
    });

    bob.command('grouppong', async (ctx) => {
      console.log('Bob received /grouppong group reply from', ctx.sender);
      bobReceivedGroupPong = true;
    });

    console.log('Starting Alice...');
    await alice.start();
    console.log('Alice started!');

    console.log('Starting Bob...');
    await bob.start();
    console.log('Bob started!');

    // Wait a bit for both to sync keys
    await sleep(2000);

    // ----------------------------------------------------
    // TEST 1: Direct Messaging (DM)
    // ----------------------------------------------------
    console.log('Bob sending DM (/ping) to Alice...');
    const dmPayload = {
      messagePayload: {
        text: '/ping',
        files: undefined,
        replyingTo: 0n,
      },
      nonce: Math.floor(Math.random() * 9_999_999),
    };
    const dmBytes = UserMessageInner.encode(dmPayload).finish();
    await bob.client.encryptAndSend('alice', Array.from(dmBytes));

    // Wait and verify
    for (let i = 0; i < 20; i++) {
      if (aliceReceivedPing && bobReceivedPong) break;
      await sleep(500);
    }

    assert.ok(aliceReceivedPing, 'Alice should have received Bob\'s DM');
    assert.ok(bobReceivedPong, 'Bob should have received Alice\'s reply DM');
    console.log('Direct messaging integration test passed!');

    // ----------------------------------------------------
    // TEST 2: Group Creation and Invitation
    // ----------------------------------------------------
    console.log('Alice creating a group...');
    const groupInfo = await alice.client.createGroup('IntegrationGroup', 'Group for integration testing');
    const groupId = groupInfo.id;
    console.log('Group created with ID:', groupId);

    console.log('Alice inviting Bob to the group...');
    await alice.client.addGroupMember(groupId, 'bob', 0);

    console.log('Bob checking setup to fetch and join the group...');
    await bob.client.checkSetup();

    // Wait a bit for both to update group states and epochs
    await sleep(2000);

    // ----------------------------------------------------
    // TEST 2.5: Group Members Online Status and Last Connected
    // ----------------------------------------------------
    console.log('Alice fetching group members online status...');
    const memberStatus = await alice.getGroupMembersOnlineStatus(Number(groupId));
    console.log('Group members status received:', JSON.stringify(memberStatus, (k, v) => typeof v === 'bigint' ? v.toString() : v));

    assert.ok(memberStatus.members && memberStatus.members.length >= 2, 'Should have at least 2 members in group');
    
    const aliceStatus = memberStatus.members.find((m: any) => m.username === 'alice');
    const bobStatus = memberStatus.members.find((m: any) => m.username === 'bob');

    assert.ok(aliceStatus, 'Alice should be in the member status list');
    assert.ok(bobStatus, 'Bob should be in the member status list');

    assert.ok(aliceStatus.isOnline, 'Alice should be online');
    assert.ok(bobStatus.isOnline, 'Bob should be online');

    assert.ok(aliceStatus.lastConnectedAt > 0n, 'Alice last connected timestamp should be set');
    assert.ok(bobStatus.lastConnectedAt > 0n, 'Bob last connected timestamp should be set');

    console.log('Group members online status test passed!');

    // ----------------------------------------------------
    // TEST 3: Group Messaging
    // ----------------------------------------------------
    console.log('Bob sending group message (/groupping)...');
    const grpPayload = {
      messagePayload: {
        text: '/groupping',
        files: undefined,
        replyingTo: 0n,
      },
      channelId: 1,
    };
    const grpBytes = GroupMessageInner.encode(grpPayload).finish();
    await bob.client.encryptAndSendGroup(groupId, Array.from(grpBytes));

    // Wait and verify
    for (let i = 0; i < 20; i++) {
      if (aliceReceivedGroupPing && bobReceivedGroupPong) break;
      await sleep(500);
    }

    assert.ok(aliceReceivedGroupPing, 'Alice should have received Bob\'s group message');
    assert.ok(bobReceivedGroupPong, 'Bob should have received Alice\'s group reply');
    console.log('Group messaging integration test passed!');

  } finally {
    process.off('exit', killServer);
    killServer();
    cleanupFiles();
  }
});
