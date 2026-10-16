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

The bot signs in with its own Lupyd account (email and password, not a Google login). Set the credentials when starting it; `read -s` keeps the password out of your shell history:

```bash
export BOT_EMAIL=bot-account@example.com
read -s -p "Bot password: " BOT_PASSWORD; export BOT_PASSWORD
bun start
```

The password is only sent to the Lupyd login server and is never saved. The resulting session is kept in `reminder-session.json`, so later starts connect without signing in again. Without `BOT_EMAIL`/`BOT_PASSWORD` the bot falls back to a browser sign-in, which Lupyd login currently rejects with "Callback URL mismatch".

Then add the bot to your groups, or message it directly.

| Variable | Default | Purpose |
|---|---|---|
| `BOT_USERNAME` | `reminder_bot` | Account name, also used in emulator mode |
| `BOT_EMAIL` / `BOT_PASSWORD` | unset | The bot account to sign in with |
| `BOT_SESSION_FILE` | `./reminder-session.json` | Sign-in session |
| `BOT_DB_FILE` | `./reminder-state.db` | The bot's encryption keys and group state (SQLite, see below) |
| `REMINDERS_DB` | `./reminders.db` | Reminders and time zones (SQLite) |
| `EMULATOR_MODE` | unset | `true` to skip sign-in against a local emulator server |

Run the tests with `bun test`.

## Keeping the bot's state

The bot uses two files, and both survive restarts:

- **`reminders.db`** holds the reminders and time zones.
- **`BOT_DB_FILE`** holds the bot's encryption keys, sessions and group state, written through [`../bot-storage/sqlite-storage.ts`](../bot-storage/sqlite-storage.ts). It contains private keys. Back it up, keep it private, and never run two copies of the bot on it. If it is lost, the bot registers as a new device and has to be re-added to groups.

## Known limitations

- Reminders keep the time zone they were created with. Changing `/timezone` affects only new ones.
