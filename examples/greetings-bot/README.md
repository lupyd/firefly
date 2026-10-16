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

```bash
bun install
BOT_USERNAME=lupyd_welcome bun start
```

The bot signs in with its own Lupyd account (email and password, not a Google login). Set the credentials when starting it; `read -s` keeps the password out of your shell history:

```bash
export BOT_EMAIL=bot-account@example.com
read -s -p "Bot password: " BOT_PASSWORD; export BOT_PASSWORD
bun start
```

The password is only sent to the Lupyd login server and is never saved. The resulting session is kept in `greetings-session.json`, so later starts connect without signing in again. Without `BOT_EMAIL`/`BOT_PASSWORD` the bot falls back to a browser sign-in, which Lupyd login currently rejects with "Callback URL mismatch".

| Variable | Default | Purpose |
|---|---|---|
| `BOT_USERNAME` | `lupyd_welcome` | Account name, also used in emulator mode |
| `BOT_EMAIL` / `BOT_PASSWORD` | unset | The bot account to sign in with |
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
- **Don't run two copies of the bot on the same file.** They would both advance the same encryption sessions and break them.

## Known limitations

- **Bursts of joins are not grouped.** Twenty people joining together get twenty separate welcomes.
- **No custom permission for welcome settings.** The admin check uses the group-level Manage group permission and ignores channel overrides.
