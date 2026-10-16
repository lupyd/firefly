// End-to-end test of the greetings and reminder bots against a local Firefly server in
// emulator mode. The bots run as separate processes exactly as users start them, and are
// restarted mid-test to prove they keep their keys, groups and data.
//
//   FIREFLY_BASE_URL=http://127.0.0.1:39209 bun test --timeout 400000
import { afterAll, beforeAll, expect, test } from 'bun:test';
import { mkdtempSync, rmSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import { join, resolve } from 'path';
import { FireflyClient, type BotContext } from 'firefly-client-js';

const BASE_URL = process.env.FIREFLY_BASE_URL || 'http://127.0.0.1:39209';
const WS_URL = process.env.FIREFLY_BASE_WS_URL || BASE_URL.replace(/^http/, 'ws') + '/';
const RUN = Math.floor(Math.random() * 1_000_000);
const workDir = mkdtempSync(join(tmpdir(), 'bots-e2e-'));
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

interface Received {
  sender: string;
  text: string;
}

async function waitFor<T>(what: string, check: () => T | undefined, timeoutMs = 30_000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = check();
    if (value) return value;
    await sleep(250);
  }
  throw new Error(`Timed out waiting for ${what}`);
}

class BotProcess {
  private proc: ReturnType<typeof Bun.spawn> | null = null;
  log = '';

  constructor(
    private name: string,
    private script: string,
    private env: Record<string, string>,
  ) {}

  async start(): Promise<void> {
    const before = this.log.length;
    this.proc = Bun.spawn(['bun', 'run', this.script], {
      cwd: resolve(this.script, '..', '..'),
      env: { ...process.env, EMULATOR_MODE: 'true', FIREFLY_BASE_URL: BASE_URL, FIREFLY_BASE_WS_URL: WS_URL, ...this.env },
      stdout: 'pipe',
      stderr: 'pipe',
    });
    for (const stream of [this.proc.stdout, this.proc.stderr] as ReadableStream<Uint8Array>[]) {
      (async () => {
        const decoder = new TextDecoder();
        for await (const chunk of stream) this.log += decoder.decode(chunk);
      })();
    }
    await waitFor(`${this.name} to connect`, () => this.log.slice(before).includes('Client is fully connected'), 60_000);
  }

  sinceStart(marker: string): boolean {
    return this.log.includes(marker);
  }

  async stop(): Promise<void> {
    this.proc?.kill();
    await this.proc?.exited;
    this.proc = null;
  }

  dumpLog(): void {
    writeFileSync(join(workDir, `${this.name}.log`), this.log);
  }
}

function user(name: string) {
  const username = `${name}_${RUN}`;
  const client = new FireflyClient({
    username,
    emulatorMode: true,
    apiBaseUrl: BASE_URL,
    wsUrl: WS_URL,
    sessionFile: join(workDir, `${username}-session.json`),
  });
  const group: Received[] = [];
  const direct: Received[] = [];
  client.onGroupMessage((ctx: BotContext) => { group.push({ sender: ctx.sender, text: ctx.text }); });
  client.onMessage((ctx: BotContext) => { direct.push({ sender: ctx.sender, text: ctx.text }); });
  return { username, client, group, direct };
}

const greeterName = `welcome_bot_${RUN}`;
const reminderName = `reminder_bot_${RUN}`;
const greeter = new BotProcess('greetings-bot', resolve(import.meta.dir, '../greetings-bot/src/index.ts'), {
  BOT_USERNAME: greeterName,
  BOT_SESSION_FILE: join(workDir, 'greeter-session.json'),
  BOT_DB_FILE: join(workDir, 'greeter-state.db'),
  GREETINGS_DB: join(workDir, 'greeter-settings.db'),
});
const reminder = new BotProcess('reminder-bot', resolve(import.meta.dir, '../reminder-bot/src/index.ts'), {
  BOT_USERNAME: reminderName,
  BOT_SESSION_FILE: join(workDir, 'reminder-session.json'),
  BOT_DB_FILE: join(workDir, 'reminder-state.db'),
  REMINDERS_DB: join(workDir, 'reminders.db'),
});

const alice = user('alice');
const carol = user('carol');
const dave = user('dave');

beforeAll(async () => {
  console.log(`Work dir: ${workDir}`);
  await Promise.all([greeter.start(), reminder.start()]);
  for (const person of [alice, carol, dave]) await person.client.start();
  await sleep(1000);
}, 120_000);

afterAll(async () => {
  greeter.dumpLog();
  reminder.dumpLog();
  await Promise.all([greeter.stop(), reminder.stop()]);
  for (const person of [alice, carol, dave]) await person.client.dispose().catch(() => {});
  if (!process.env.KEEP_E2E_FILES) rmSync(workDir, { recursive: true, force: true });
});

const fromBot = (list: Received[], bot: string, includes: string) =>
  list.find((m) => m.sender === bot && m.text.includes(includes));

test('greetings bot welcomes members, enforces admins and survives a restart', async () => {
  const groupName = `E2E Group ${RUN}`;
  const groupId = Number((await alice.client.createGroup(groupName, 'bots e2e')).id);
  // Same default as the bot: the first channel, or the group-wide stream (0) when there are none
  const channels = await alice.client.getGroupChannels(groupId);
  console.log('Group channels:', JSON.stringify(channels));
  const channelId = channels[0]?.id ?? 0;

  await alice.client.addGroupMember(groupId, greeterName, 0);
  await waitFor('the bot to join the group', () => greeter.sinceStart(`[Group Joined] Client joined group: ${groupId}`));
  await sleep(1000);

  await alice.client.addGroupMember(groupId, carol.username, 0);
  const welcome = await waitFor('welcome for carol', () => fromBot(alice.group, greeterName, `@${carol.username}`));
  expect(welcome.text).toBe(`Welcome @${carol.username} to ${groupName}! 👋`);

  await alice.client.sendGroupMessage(groupId, '/welcome set Hi {user}, glad you are here!', channelId);
  await waitFor('admin change confirmed', () => fromBot(alice.group, greeterName, 'Welcome message updated'));

  await carol.client.sendGroupMessage(groupId, '/welcome off', channelId);
  await waitFor('non-admin rejected', () => fromBot(carol.group, greeterName, 'Only group admins'));

  // Restart on the same state files: the bot must come back as the same member of the group
  await greeter.stop();
  await greeter.start();
  await sleep(1000);

  await alice.client.addGroupMember(groupId, dave.username, 0);
  const second = await waitFor('welcome for dave after restart', () => fromBot(alice.group, greeterName, `@${dave.username}`));
  expect(second.text).toBe(`Hi @${dave.username}, glad you are here!`);
}, 180_000);

test('reminder bot confirms, lists and delivers a DM reminder across a restart', async () => {
  await alice.client.sendUserMessage(reminderName, '/remind me in 1m stretch your legs');
  const confirmation = await waitFor('reminder confirmation', () => fromBot(alice.direct, reminderName, 'Got it (#'));
  expect(confirmation.text).toContain('by direct message');

  await alice.client.sendUserMessage(reminderName, '/reminders');
  await waitFor('reminder list', () => fromBot(alice.direct, reminderName, 'stretch your legs'));

  // Restart before it is due: both the reminder and the DM session must survive
  await reminder.stop();
  await reminder.start();

  const delivered = await waitFor(
    'reminder delivery',
    () => fromBot(alice.direct, reminderName, '⏰ Reminder for'),
    120_000,
  );
  expect(delivered.text).toBe(`⏰ Reminder for @${alice.username}: stretch your legs`);
}, 240_000);
