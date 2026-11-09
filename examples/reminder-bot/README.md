# Reminder bot

A reminder bot you host yourself. It runs under your own Lupyd bot account, so reminder text stays on your machine.

## Commands

| Command | Delivered |
|---|---|
| `/remind me in 2h check the deploy` | Direct message to you |
| `/remind me tomorrow at 9am send the report` | Direct message to you |
| `/remind here every monday at 10am weekly sync` | In the channel where you set it |
| `/remind @bob in 30m review my PR` | In the channel where you set it, mentioning @bob (groups only) |
| `/reminders` or `/remind list` | Lists your reminders with their ids |
| `/remind cancel 3` | Cancels reminder #3 (only the person who set it can) |
| `/timezone Asia/Kolkata` | Sets your time zone. Without an argument, shows it |

When you leave out the target, `/remind in 10m stretch` means `me`. A leading "to" is dropped, so `/remind me in 5m to stretch` works too.

**When** formats:

- `in 10m`, `in 1h30m`, `in 2 hours and 15 minutes`, `in 3 days`
- `at 5pm`, `at 17:30`, `at noon`: today, or tomorrow if that time has passed
- `today 6pm`, `tomorrow`, `tomorrow at 9am`
- `on friday`, `monday 10am`, `on 2026-12-24 at 8pm`
- `every day`, `every weekday`, or `every monday` (any day of the week), each with an optional time

When no time is given, 09:00 is used. Clock times follow your `/timezone`, which defaults to UTC.

## Rules

- A reminder must be at least 1 minute and at most 1 year away.
- Each person can have up to 25 active reminders.
- Reminder text is limited to 500 characters.
- Reminders due while the bot was offline are sent when it comes back and marked as late. A repeating reminder sends one catch-up message, not one per missed day.
- If a delivery fails, the bot retries every 5 minutes and drops the reminder after 5 failed attempts.

## Running it

```bash
bun install
BOT_USERNAME=my_reminder_bot bun start
```

On first start the bot opens a browser window to sign in as its Lupyd account. Afterwards the session is kept in `reminder-session.json`, so later starts (including `bun ../run-bots.ts`) connect without signing in again.

While running, the bot renews its sign-in a few minutes before it expires. Under `run-bots.ts` (or with `FIREFLY_UNATTENDED=true`) it never opens a browser: if the saved sign-in is revoked or has expired for good, the bot exits with code 78 and `run-bots.ts` stops restarting it until you sign it in again with `bun start`.

Lupyd login must allow `http://localhost:38295/callback` as a callback URL for this to work; until then the sign-in page shows "Callback URL mismatch".

Then add the bot to your groups, or message it directly.

| Variable | Default | Purpose |
|---|---|---|
| `BOT_USERNAME` | `reminder_bot` | Account name, also used in emulator mode |
| `BOT_SESSION_FILE` | `./reminder-session.json` | Sign-in session |
| `BOT_DB_FILE` | `./reminder-state.db` | The bot's encryption keys and group state (SQLite, see below) |
| `REMINDERS_DB` | `./reminders.db` | Reminders and time zones (SQLite) |
| `EMULATOR_MODE` | unset | `true` to skip sign-in against a local emulator server |

Run the tests with `bun test`.

## Keeping the bot's state

The bot uses two files, and both survive restarts:

- **`reminders.db`** holds the reminders and time zones.
- **`BOT_DB_FILE`** holds the bot's encryption keys, sessions and group state, written through [`../bot-storage/sqlite-storage.ts`](../bot-storage/sqlite-storage.ts). It contains private keys. Back it up and keep it private. Only one copy of the bot can use it at a time: a second copy refuses to start. If it is lost, the bot registers as a new device and has to be re-added to groups.

## Known limitations

- Reminders keep the time zone they were created with. Changing `/timezone` affects only new ones.
