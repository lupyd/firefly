# Setting up and running the bots

This guide takes the [greetings bot](greetings-bot/README.md) and the [reminder bot](reminder-bot/README.md) from a fresh checkout to running unattended against Lupyd. Each bot's README covers its commands and settings; this guide covers everything around them.

Overview:

1. [One-time: allow the sign-in callback URL](#1-one-time-allow-the-sign-in-callback-url) (Lupyd admin)
2. [Create a Lupyd account for each bot](#2-create-a-lupyd-account-for-each-bot)
3. [Install the tools](#3-install-the-tools)
4. [Build and link everything](#4-build-and-link-everything)
5. [Sign each bot in once](#5-sign-each-bot-in-once)
6. [Run the bots](#6-run-the-bots)
7. [Keep them running after a reboot](#7-keep-them-running-after-a-reboot)
8. [Look after the bot's files](#8-look-after-the-bots-files)

Further down: [when a bot needs to sign in again](#when-a-bot-needs-to-sign-in-again), [testing against a local server](#testing-against-a-local-server), and [troubleshooting](#troubleshooting).

## 1. One-time: allow the sign-in callback URL

**Status: not done yet. Until it is, the bots can't sign in to Lupyd.** The sign-in page shows *"Callback URL mismatch: http://localhost:38295/callback is not in the list of allowed callback URLs"*.

Bots sign in the way most command-line tools do. The bot starts a small web server at `http://localhost:38295`, opens the Lupyd sign-in page in a browser, and after you log in the page sends the browser back to `http://localhost:38295/callback` with a one-time code. The bot exchanges that code for its tokens.

Lupyd's sign-in service (Auth0) only redirects to addresses on an allow list, and this address isn't on it yet. Someone with admin access to Lupyd's Auth0 tenant needs to:

1. Open the Auth0 dashboard, go to **Applications**, and open the application with client ID `GnfEyGY0JdD0Oige2HSpeErcaWLrvObm`.
2. Add `http://localhost:38295/callback` to **Allowed Callback URLs** and save.

A `localhost` callback is safe to allow: it only reaches the computer the browser runs on.

To check whether it's been done, run the first-time sign-in from step 5. If the browser shows the Lupyd login form instead of the mismatch error, it has.

## 2. Create a Lupyd account for each bot

Each bot is an ordinary Lupyd user. It joins groups as a normal member and reads only the groups it's added to. Create one account per bot in the Lupyd app, for example:

| Bot | Suggested account | Notes |
|---|---|---|
| Greetings | `lupyd_welcome` | Lupyd runs this as its official welcome bot |
| Reminder | e.g. `reminder_bot` | Meant to be self-hosted, so reminder text stays on your machine |

The bot takes its name from the account you sign in with in step 5. `BOT_USERNAME` only matters when testing against a local emulator server.

## 3. Install the tools

| Tool | Why | Install |
|---|---|---|
| [Bun](https://bun.sh) | Runs the bots | `curl -fsSL https://bun.sh/install \| bash` (Windows: `powershell -c "irm bun.sh/install.ps1 \| iex"`) |
| Node.js 20+ | Compiles the TypeScript packages | [nodejs.org](https://nodejs.org) |
| Rust (`cargo`, `rustup`) | Builds the encryption client | [rustup.rs](https://rustup.rs) |
| `wasm-pack` | Compiles the client to WebAssembly | `cargo install wasm-pack` |
| `clang` | Compiles SQLite and crypto C code for WebAssembly | Linux: `apt install clang`. macOS: `brew install llvm`. Windows: `winget install LLVM.LLVM` |

On Windows, LLVM doesn't need to be on `PATH`. The setup script finds it in `C:\Program Files\LLVM`.

## 4. Build and link everything

From the repository root:

```bash
bun examples/bots-setup.ts
```

This builds the WebAssembly client in `crates/client-node` (a few minutes the first time), installs and compiles the JS packages, and links every bot to the code just built. Re-run it after pulling changes to the client. If only TypeScript changed, `--skip-wasm` reuses the existing WebAssembly build.

Each bot reads optional settings from a `.env` file in its own folder (see the variable tables in each bot's README). Bun loads it automatically, and `run-bots.ts` starts each bot from its folder.

## 5. Sign each bot in once

Each bot has to be signed in by a person once. After that it keeps itself signed in.

### On a computer with a browser

```bash
cd examples/greetings-bot
bun start
```

A browser opens on the Lupyd sign-in page. **Log in as the bot's account, not your own.** When the terminal shows `Client is fully connected and listening.`, press Ctrl+C. Repeat in `examples/reminder-bot`.

This leaves two files in each bot folder:

- `*-session.json`: the sign-in (access and refresh tokens)
- `*-state.db`: the bot's encryption keys and group memberships

### On a server without a browser

The browser has to reach `localhost:38295` on the machine where the bot runs. Forward that port over SSH from your own computer:

```bash
ssh -L 38295:127.0.0.1:38295 you@your-server
# then, on the server:
cd firefly/examples/greetings-bot && bun start
```

The bot always prints the sign-in link (`If it doesn't open, open this link ...`). Open it in the browser on your own computer and log in as the bot. The redirect to `localhost:38295` goes through the tunnel to the bot. Press Ctrl+C once it says `Client is fully connected`.

Prefer signing in on the server like this over signing in on a laptop and copying files across. If you do copy, move the session file **and** the state database together, and don't start the bot on the laptop again (see [step 8](#8-look-after-the-bots-files)).

## 6. Run the bots

```bash
bun examples/run-bots.ts              # both bots
bun examples/run-bots.ts reminder     # just one
```

`run-bots.ts`:

- starts each bot as its own process, prefixing its output with the bot's name and appending it to `examples/logs/<bot>.log`
- restarts a bot that exits, waiting 5 seconds at first and doubling up to 5 minutes; the delay resets once a bot has stayed up for 10 minutes
- runs the bots unattended: they renew their sign-in about 5 minutes before it expires and never try to open a browser
- rotates each log to `<bot>.log.1` once it reaches 10 MB
- stops everything on Ctrl+C or SIGTERM

It refuses to start a bot that hasn't been signed in yet and tells you what to run.

After the bots are running, add the greetings bot to groups (group owners add `@lupyd_welcome` like any member) and add the reminder bot to groups or message it directly.

## 7. Keep them running after a reboot

`run-bots.ts` restarts the bots, but something has to start `run-bots.ts` itself.

**Linux (systemd).** Create `/etc/systemd/system/lupyd-bots.service`, adjusting the user and paths:

```ini
[Unit]
Description=Lupyd bots
After=network-online.target
Wants=network-online.target

[Service]
User=bots
WorkingDirectory=/home/bots/firefly
ExecStart=/home/bots/.bun/bin/bun examples/run-bots.ts
Restart=on-failure
RestartSec=30

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now lupyd-bots
journalctl -u lupyd-bots -f      # live output
```

**Windows.** In Task Scheduler, create a task that runs at startup (or at log-on) with program set to the full path of `bun.exe` (usually `%USERPROFILE%\.bun\bin\bun.exe`), arguments `examples\run-bots.ts`, and *Start in* set to the repository folder.

## 8. Look after the bot's files

Each bot folder holds files that must never be committed. They're already in `.gitignore`.

| File | Contains | If it's lost |
|---|---|---|
| `*-state.db` | Encryption keys, sessions, group memberships | The bot comes back as a new device and must be **re-added to every group** |
| `*-session.json` | Sign-in tokens | Sign in again (step 5) |
| `greetings-settings.db`, `reminders.db` | Welcome settings; reminders and time zones | Settings and reminders are gone |

- **Back up** the folder's `.db` files and session file regularly.
- **Keep them private.** Anyone with the state database can read the bot's messages, and anyone with the session file can act as the bot. Both are created readable by the owner only on Linux and macOS.
- **Run one copy per bot.** Two copies on the same state database would break its encryption sessions, so a second copy refuses to start with *"Another copy of this bot is already running"*. If you restore a backup on another machine, stop the old one for good first.

## When a bot needs to sign in again

A bot keeps itself signed in as long as its refresh token stays valid. If the token is revoked (for example, the account's sessions were signed out) or expires for good, the bot logs:

```
<bot> needs to sign in again (no valid refresh token in .../reminder-session.json).
```

It then exits with code 78, and `run-bots.ts` logs `must be signed in again; not restarting it` and leaves that bot stopped. The other bot keeps running.

To fix it, stop `run-bots.ts`, repeat [step 5](#5-sign-each-bot-in-once) for that bot, then start `run-bots.ts` again. The state database is kept, so the bot stays in its groups.

Short outages of the sign-in server don't trigger this. The bot keeps its current token and retries (15 seconds at first, backing off to 5 minutes) until it gets through.

## Testing against a local server

To run the bots or their end-to-end test without touching Lupyd, run a Firefly server locally in emulator mode, which skips sign-in. You need Docker.

The published server image checks a subscription service before it creates groups, and has no built-in fallback. [`bots-e2e/fake-subscriptions.ts`](bots-e2e/fake-subscriptions.ts) stands in for it.

```bash
# From the repository root
bun examples/bots-e2e/fake-subscriptions.ts &        # listens on 8088

docker network create ff-test
docker run -d --name ff-postgres --network ff-test \
  -e POSTGRES_DB=firefly -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres \
  -v "$PWD/initdb.sql:/docker-entrypoint-initdb.d/init.sql:ro" postgres:16-alpine

docker run -d --name ff-server --network ff-test \
  --add-host=host.docker.internal:host-gateway -p 39209:39209 \
  --env-file .env.test \
  -e DB_CONN_STR='postgresql://postgres:postgres@ff-postgres:5432/firefly?sslmode=disable' \
  -e SUBSCRIPTIONS_URL=http://host.docker.internal:8088 \
  -e SUBSCRIPTIONS_AUTHORIZATION='Service test.fake' \
  -v "$PWD/initdb.sql:/initdb.sql:ro" \
  hashtag438/firefly-mls-server:latest
```

On Windows with Git Bash, prefix the `docker run` commands with `MSYS_NO_PATHCONV=1` and use `$(pwd -W)` in place of `$PWD`. Postgres runs on a private Docker network here because Windows often reserves port 54321 (check with `netsh interface ipv4 show excludedportrange protocol=tcp`).

Then:

```bash
# End-to-end test: starts both bots, restarts them mid-test
cd examples/bots-e2e && FIREFLY_BASE_URL=http://127.0.0.1:39209 bun test --timeout 400000

# Unit tests (no server needed)
cd examples/greetings-bot && bun test
cd examples/reminder-bot && bun test
cd examples/bot-storage && bun test
cd crates/firefly-client-js && bun test tests/auth.test.ts
```

To try the bots by hand against the local server, give each bot folder a `.env` with `EMULATOR_MODE=true` and `FIREFLY_BASE_URL=http://127.0.0.1:39209`, then use `bun start` or `run-bots.ts` as usual. **Delete the emulator's `*-session.json` and `*-state.db` files before signing in for real**, or the bot will try to reuse a test identity.

Clean up with `docker rm -f ff-server ff-postgres && docker network rm ff-test`, and stop the fake subscription service.

CI runs all of the above on every pull request (see [`.github/workflows/ci.yml`](../.github/workflows/ci.yml)).

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| Sign-in page: *Callback URL mismatch* | Step 1 hasn't been done yet |
| `The bots are not set up yet` | Run `bun examples/bots-setup.ts` |
| `<bot> is not signed in yet` | Do step 5 for that bot |
| `clang not found` / `failed to find tool "clang"` during setup | Install LLVM (step 3) |
| `Another copy of this bot is already running` | Another process already uses that bot's state database: an old `run-bots.ts`, a `bun start` in another terminal, or a service. Stop it |
| `Client timeout waiting for initialization` | The bot can't reach the server. Check the network. When using a custom server, set `FIREFLY_BASE_URL`; the websocket URL follows it unless `FIREFLY_BASE_WS_URL` is set |
| Bot stopped with `must be signed in again` | See [When a bot needs to sign in again](#when-a-bot-needs-to-sign-in-again) |
| Local server: `SUBSCRIPTIONS_AUTHORIZATION is not configured` or `/lupyd/plans/.../constraints` 404 | Start `fake-subscriptions.ts` and pass `SUBSCRIPTIONS_URL` / `SUBSCRIPTIONS_AUTHORIZATION` to the server as shown above |
| Bot was re-added to groups after a move | Its state database wasn't carried over; restore it from backup (step 8) |
