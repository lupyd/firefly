// Token renewal against a fake Auth0 token endpoint. No Firefly server needed:
//   bun test tests/auth.test.ts
import { afterAll, beforeEach, expect, test } from 'bun:test';
import { mkdtempSync, readFileSync, rmSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { FireflyClient, SignInRequiredError } from '../src/index';

const dir = mkdtempSync(join(tmpdir(), 'firefly-auth-'));
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function jwt(uname: string): string {
  const part = (o: object) => Buffer.from(JSON.stringify(o)).toString('base64url');
  return `${part({ alg: 'none' })}.${part({ uname })}.sig`;
}

// Each refresh request takes the next queued response; with none left it succeeds
let queue: Array<{ status: number; body: any }> = [];
let refreshRequests: URLSearchParams[] = [];
const server = Bun.serve({
  port: 0,
  async fetch(req) {
    const params = new URLSearchParams(await req.text());
    refreshRequests.push(params);
    const next = queue.shift() ?? {
      status: 200,
      body: { access_token: jwt('bot'), refresh_token: `r${refreshRequests.length + 1}`, expires_in: 86400 },
    };
    return Response.json(next.body, { status: next.status });
  },
});

afterAll(() => {
  server.stop(true);
  rmSync(dir, { recursive: true, force: true });
});

beforeEach(() => {
  queue = [];
  refreshRequests = [];
});

let n = 0;
function makeBot(opts: { expiresInMs: number; unattended?: boolean; onSignInRequired?: (e: SignInRequiredError) => void }) {
  const sessionFile = join(dir, `session-${++n}.json`);
  const bot: any = new FireflyClient({
    auth0Domain: `http://127.0.0.1:${server.port}`,
    emulatorMode: false,
    sessionFile,
    unattended: opts.unattended ?? true,
    onSignInRequired: opts.onSignInRequired ?? (() => {}),
  });
  bot.session = { access_token: 'old-token', refresh_token: 'r1', expires_at: Date.now() + opts.expiresInMs, username: 'bot' };
  const pushed: string[] = [];
  bot.client = { setAccessToken: (t: string) => pushed.push(t) };
  bot._startOauthFlow = () => {
    throw new Error('opened a browser');
  };
  return { bot, pushed, sessionFile };
}

test('renews before the token expires and hands the new token to the Rust client', async () => {
  // Expires in 2s, so the renewal is scheduled halfway, at about 1s
  const { bot, pushed, sessionFile } = makeBot({ expiresInMs: 2000 });
  bot._scheduleRenewal();
  expect(bot._accessTokenForClient()).toBe('old-token');
  await sleep(1500);

  expect(refreshRequests).toHaveLength(1);
  expect(refreshRequests[0].get('refresh_token')).toBe('r1');
  const fresh = bot._accessTokenForClient();
  expect(fresh).not.toBe('old-token');
  expect(pushed).toEqual([fresh]);
  // The rotated refresh token is saved for the next start
  const saved = JSON.parse(readFileSync(sessionFile, 'utf8'));
  expect(saved.access_token).toBe(fresh);
  expect(saved.refresh_token).toBe('r2');
  // And the following renewal is scheduled about 5 minutes before the new expiry
  expect(bot.renewDueAt - Date.now()).toBeGreaterThan(86400_000 - 6 * 60_000);
  clearTimeout(bot.renewTimer);
});

test('a temporary failure keeps the current token and retries', async () => {
  const { bot, pushed } = makeBot({ expiresInMs: 60_000 });
  queue = [{ status: 503, body: { error: 'unavailable' } }];
  bot.renewRetryMs = 50;
  await bot._renewInBackground();

  expect(bot._accessTokenForClient()).toBe('old-token');
  expect(pushed).toEqual([]);
  await sleep(300);
  expect(refreshRequests).toHaveLength(2);
  expect(bot._accessTokenForClient()).not.toBe('old-token');
  // A success resets the back-off
  expect(bot.renewRetryMs).toBe(15_000);
  clearTimeout(bot.renewTimer);
});

test('a rejected refresh token in unattended mode asks for a sign-in instead of opening a browser', async () => {
  const errors: SignInRequiredError[] = [];
  const { bot } = makeBot({ expiresInMs: 60_000, onSignInRequired: (e) => errors.push(e) });
  queue = [{ status: 403, body: { error: 'invalid_grant' } }];
  await bot._renewInBackground();

  expect(errors).toHaveLength(1);
  expect(errors[0]).toBeInstanceOf(SignInRequiredError);
  // No retry is scheduled: retrying a revoked token can't succeed
  expect(bot.renewTimer).toBeNull();
});

test('starting with a rejected refresh token fails with SignInRequiredError', async () => {
  const { bot } = makeBot({ expiresInMs: -1000 });
  queue = [{ status: 400, body: { error: 'invalid_grant' } }];
  await expect(bot.getOrRenewAccessToken()).rejects.toBeInstanceOf(SignInRequiredError);
});

test('when attended, a rejected refresh token falls back to the browser sign-in', async () => {
  const { bot } = makeBot({ expiresInMs: -1000, unattended: false });
  queue = [{ status: 403, body: { error: 'invalid_grant' } }];
  await expect(bot.getOrRenewAccessToken()).rejects.toThrow('opened a browser');
});

test('a late timer (machine slept) is caught up by the next request', async () => {
  const { bot } = makeBot({ expiresInMs: 60_000 });
  bot._scheduleRenewal(60_000);
  bot.renewDueAt = Date.now() - 10_000;

  expect(bot._accessTokenForClient()).toBe('old-token');
  await sleep(200);
  expect(refreshRequests).toHaveLength(1);
  expect(bot._accessTokenForClient()).not.toBe('old-token');
  clearTimeout(bot.renewTimer);
});

test('a request while the timer is on schedule does not renew', async () => {
  const { bot } = makeBot({ expiresInMs: 60 * 60_000 });
  bot._scheduleRenewal();
  bot._accessTokenForClient();
  await sleep(100);
  expect(refreshRequests).toHaveLength(0);
  clearTimeout(bot.renewTimer);
});

test('concurrent callers share one refresh', async () => {
  const { bot } = makeBot({ expiresInMs: -1000 });
  const [a, b] = await Promise.all([bot.getOrRenewAccessToken(), bot.getOrRenewAccessToken()]);
  expect(refreshRequests).toHaveLength(1);
  expect(a).toBe(b);
  clearTimeout(bot.renewTimer);
});
