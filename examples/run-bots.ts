// Starts the example bots and keeps them running:  bun examples/run-bots.ts [greetings] [reminder]
//
// With no arguments both bots start. Each bot runs in its own process from its folder, so it
// picks up its own .env. Output is shown with a prefix and appended to examples/logs/<bot>.log
// (rotated to <bot>.log.1 at 10 MB).
// A bot that exits is restarted after a delay that doubles up to 5 minutes, and resets once it
// has stayed up for 10 minutes. Ctrl+C stops everything.
//
// Bots run unattended (FIREFLY_UNATTENDED=true): they renew their sign-in on their own and never
// open a browser. If a bot's saved sign-in can't be renewed any more, it exits with code 78 and
// is not restarted until someone signs it in again by hand.
import { appendFileSync, existsSync, mkdirSync, renameSync, statSync } from 'fs';
import { join, resolve } from 'path';

const examples = resolve(import.meta.dir);
const logs = join(examples, 'logs');
const ALL = { greetings: 'greetings-bot', reminder: 'reminder-bot' } as const;
type BotName = keyof typeof ALL;

const MIN_DELAY_MS = 5_000;
const MAX_DELAY_MS = 5 * 60_000;
const HEALTHY_AFTER_MS = 10 * 60_000;
// Matches EXIT_SIGN_IN_REQUIRED in firefly-client-js
const EXIT_SIGN_IN_REQUIRED = 78;

const requested = process.argv.slice(2).filter((a) => !a.startsWith('-'));
const unknown = requested.filter((a) => !(a in ALL));
if (unknown.length > 0) {
  console.error(`Unknown bot: ${unknown.join(', ')}. Choose from: ${Object.keys(ALL).join(', ')}`);
  process.exit(1);
}
const selected = (requested.length > 0 ? requested : Object.keys(ALL)) as BotName[];

if (!existsSync(join(examples, 'greetings-bot', 'node_modules', 'firefly-client-js'))) {
  console.error('The bots are not set up yet. Run:  bun examples/bots-setup.ts');
  process.exit(1);
}
// Signing in opens a browser, which the runner can't do for a bot it restarts unattended,
// so each bot must have been signed in once by hand
const SESSION_FILES: Record<BotName, string> = { greetings: 'greetings-session.json', reminder: 'reminder-session.json' };
for (const name of selected) {
  if (!existsSync(join(examples, ALL[name], SESSION_FILES[name]))) {
    console.error(`${ALL[name]} is not signed in yet. Sign it in once (a browser opens; sign in as the bot account):`);
    console.error(`  cd examples/${ALL[name]} && bun start`);
    console.error('When it says "Client is fully connected", press Ctrl+C and run this again.');
    process.exit(1);
  }
}

mkdirSync(logs, { recursive: true });
let stopping = false;
const running = new Map<BotName, ReturnType<typeof Bun.spawn>>();

// Each log is capped: past 10 MB it moves to <bot>.log.1 (replacing the previous one), so a
// bot left running for months uses at most about 20 MB of logs
const MAX_LOG_BYTES = 10 * 1024 * 1024;
const logSizes = new Map<BotName, number>();

function writeLog(name: BotName, text: string): void {
  const file = join(logs, `${ALL[name]}.log`);
  let size = logSizes.get(name) ?? (existsSync(file) ? statSync(file).size : 0);
  if (size >= MAX_LOG_BYTES) {
    renameSync(file, `${file}.1`);
    size = 0;
  }
  appendFileSync(file, text);
  logSizes.set(name, size + Buffer.byteLength(text));
}

function output(name: BotName, text: string): void {
  writeLog(name, text);
  for (const line of text.split(/\r?\n/)) {
    if (line.trim()) console.log(`[${name}] ${line}`);
  }
}

async function pipe(name: BotName, stream: ReadableStream<Uint8Array>): Promise<void> {
  const decoder = new TextDecoder();
  for await (const chunk of stream) output(name, decoder.decode(chunk));
}

async function supervise(name: BotName): Promise<void> {
  let delay = MIN_DELAY_MS;
  while (!stopping) {
    const startedAt = Date.now();
    output(name, `\n--- starting ${ALL[name]} at ${new Date().toISOString()} ---\n`);
    const proc = Bun.spawn(['bun', 'run', 'src/index.ts'], {
      cwd: join(examples, ALL[name]),
      env: { ...process.env, FIREFLY_UNATTENDED: 'true' },
      stdout: 'pipe',
      stderr: 'pipe',
    });
    running.set(name, proc);
    await Promise.all([
      pipe(name, proc.stdout as ReadableStream<Uint8Array>),
      pipe(name, proc.stderr as ReadableStream<Uint8Array>),
      proc.exited,
    ]);
    running.delete(name);
    if (stopping) break;

    if (proc.exitCode === EXIT_SIGN_IN_REQUIRED) {
      output(name, `--- ${ALL[name]} must be signed in again; not restarting it. Run:  cd examples/${ALL[name]} && bun start\n`);
      output(name, `--- (Ctrl+C once it says "Client is fully connected"), then restart run-bots.ts ---\n`);
      break;
    }
    if (Date.now() - startedAt > HEALTHY_AFTER_MS) delay = MIN_DELAY_MS;
    output(name, `--- ${ALL[name]} exited with code ${proc.exitCode}; restarting in ${delay / 1000}s ---\n`);
    await Bun.sleep(delay);
    delay = Math.min(delay * 2, MAX_DELAY_MS);
  }
}

async function shutdown(): Promise<void> {
  if (stopping) return;
  stopping = true;
  console.log('\nStopping bots...');
  for (const proc of running.values()) proc.kill();
  await Promise.all([...running.values()].map((p) => p.exited));
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

console.log(`Starting: ${selected.map((n) => ALL[n]).join(', ')}. Logs in examples/logs/. Press Ctrl+C to stop.`);
await Promise.all(selected.map(supervise));
