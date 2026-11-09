# Greetings bot

Welcomes new members when they join a Lupyd group. This is the bot Lupyd runs as its official `@lupyd_welcome` account, so group owners only need to add it to their group.

## How it works

The bot is an ordinary end-to-end encrypted group member. It reads messages only in groups it has been added to, and only in its own client.

When a commit adds people to a group, the Firefly client compares the member list with the last one it saw. It then fires `onGroupMembersChanged`, which the framework exposes as `bot.onMemberJoin(...)`. The bot posts the welcome in the configured channel and, if turned on, also sends a direct message to the newcomer. Members who were already in the group when the bot joined are never welcomed.

## Commands

Run these inside a group. Only members with the **Manage group** permission can change settings, the same rule the Lupyd apps use for admins.

| Command | What it does |
|---|---|
| `/welcome show` | Show the current settings (anyone) |
| `/welcome help` | List commands (anyone) |
| `/welcome on` / `off` | Turn welcomes on or off (on by default) |
| `/welcome set <message>` | Change the welcome. `{user}` becomes an @mention and `{group}` the group name |
| `/welcome channel` | Post welcomes in the channel where you run this (default: the group's first channel) |
| `/welcome dm on` / `off` | Also send new members a direct message |
| `/welcome dm set <message>` | Change the direct message |
| `/welcome test` | Preview the welcome with yourself as the new member |

Messages can be up to 500 characters.

## Running it

For the full setup (bot accounts, first sign-in, running unattended on a server, backups), see [the bots setup guide](../BOTS-SETUP.md).

```bash
bun install
BOT_USERNAME=lupyd_welcome bun start
```

On first start the bot opens a browser window to sign in as its Lupyd account. Afterwards the session is kept in `greetings-session.json`, so later starts (including `bun ../run-bots.ts`) connect without signing in again.

While running, the bot renews its sign-in a few minutes before it expires. Under `run-bots.ts` (or with `FIREFLY_UNATTENDED=true`) it never opens a browser: if the saved sign-in is revoked or has expired for good, the bot exits with code 78 and `run-bots.ts` stops restarting it until you sign it in again with `bun start`.

Lupyd login must allow `http://localhost:38295/callback` as a callback URL for this to work; until then the sign-in page shows "Callback URL mismatch".

| Variable | Default | Purpose |
|---|---|---|
| `BOT_USERNAME` | `lupyd_welcome` | Account name, also used in emulator mode |
| `BOT_SESSION_FILE` | `./greetings-session.json` | Sign-in session |
| `BOT_DB_FILE` | `./greetings-state.db` | The bot's encryption keys and group state (SQLite, see below) |
| `GREETINGS_DB` | `./greetings-settings.db` | Per-group welcome settings (SQLite) |
| `EMULATOR_MODE` | unset | `true` to skip sign-in against a local emulator server |

Run the tests with `bun test`.

## Keeping the bot's state

`BOT_DB_FILE` holds everything the bot needs to come back as the same device after a restart: its Signal identity and sessions, its MLS identity, key packages, group state, and the list of groups it is in. It is written through [`../bot-storage/sqlite-storage.ts`](../bot-storage/sqlite-storage.ts). Message contents are not stored.

The file contains the bot's private keys:

- **Back it up.** If it is lost, the bot registers as a new device and must be re-added to every group.
- **Keep it private.** Anyone with the file can read the bot's messages. It is created owner-only (mode 600) on Linux and macOS.
- **Only one copy of the bot can use the file at a time.** Two copies would both advance the same encryption sessions and break them, so a second copy refuses to start while the first holds the file.

## Known limitations

- **Bursts of joins are not grouped.** Twenty people joining together get twenty separate welcomes.
- **No custom permission for welcome settings.** The admin check uses the group-level Manage group permission and ignores channel overrides.
