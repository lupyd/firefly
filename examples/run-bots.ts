// Starts the example bots and keeps them running:  bun examples/run-bots.ts [greetings] [reminder]
//
// With no arguments both bots start. Each bot runs in its own process from its folder, so it
// picks up its own .env. Output is shown with a prefix and appended to examples/logs/<bot>.log.
// A bot that exits is restarted after a delay that doubles up to 5 minutes, and resets once it
// has stayed up for 10 minutes. Ctrl+C stops everything.
import { appendFileSync, existsSync, mkdirSync } from 'fs';
import { join, resolve } from 'path';

const examples = resolve(import.meta.dir);
const logs = join(examples, 'logs');
const ALL = { greetings: 'greetings-bot', reminder: 'reminder-bot' } as const;
type BotName = keyof typeof ALL;

const MIN_DELAY_MS = 5_000;
const MAX_DELAY_MS = 5 * 60_000;
const HEALTHY_AFTER_MS = 10 * 60_000;

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
for (const name of selected) {
  const env = join(examples, ALL[name], '.env');
  const content = existsSync(env) ? await Bun.file(env).text() : '';
  if (!/^BOT_EMAIL=\S+/m.test(content) || !/^BOT_PASSWORD=\S+/m.test(content)) {
    console.error(`Fill in BOT_EMAIL and BOT_PASSWORD in examples/${ALL[name]}/.env first.`);
    process.exit(1);
  }
}

mkdirSync(logs, { recursive: true });
let stopping = false;
const running = new Map<BotName, ReturnType<typeof Bun.spawn>>();

function output(name: BotName, text: string): void {
  appendFileSync(join(logs, `${ALL[name]}.log`), text);
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
