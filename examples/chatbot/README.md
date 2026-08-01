# Firefly MLS Example Chatbot

This is an example implementation of a client-side chatbot written in Node.js, showing how to integrate with the `firefly-client-node` FFI bindings and use the Firefly MLS network.

It demonstrates:
1. **Interactive CLI Auth0 PKCE Flow:** Spins up a temporary local redirect server to capture credentials from a web browser login, exchanges the authorization code for tokens, and persists the session.
2. **Emulator Mode (`EMULATOR_MODE=true`):** Bypasses OAuth logins entirely when running tests against the local emulator server.
3. **Protobuf Encoding/Decoding:** Uses the compiled schema from `firefly-client-js` to deserialize incoming group messages and construct outgoing group message payloads.
4. **Command Routing:** Triggers actions based on chat command prefixes (e.g. `/hi`, `/joke`, `/help`).

---

## Prerequisites

1. **Local Database & Servers:** Ensure you have the PostgreSQL server running using Docker:
   ```bash
   docker compose up -d
   ```
2. **Crate Build:** Make sure you've built the local Node.js FFI bindings. From the root directory:
   ```bash
   pnpm --filter firefly-client-node run build:debug
   ```

---

## Setup & Installation

From this directory (`examples/chatbot`), install dependencies:
```bash
pnpm install
```

---

## How to Run

### 1. Standard Production Mode (Auth0 Flow)
Run the bot with standard defaults:
```bash
pnpm start
```
The bot will:
* Detect if there's an existing session in `./bot-session.json`.
* If not, it will spin up a local redirect server and automatically open your default browser to authorize via Auth0.
* On successful authorization, the browser redirects back to the local server, code exchange is done, and the bot registers.
* It will create an SQLite DB locally (`./bot-store.db`) to hold cryptographic identity keys, sessions, and messages.

### 2. Local Emulator Mode (Testing)
To test the bot against a local server environment (like the one used in Rust integration tests):
```bash
export EMULATOR_MODE=true
export BOT_USERNAME=my_local_bot
export FIREFLY_BASE_URL=http://127.0.0.1:30000
export FIREFLY_BASE_WS_URL=ws://127.0.0.1:30000/
pnpm start
```

---

## Commands Supported

When the bot is added to any group (invite it using its username), you can message the following slash commands in the group chat:

* **`/hi`**: The bot will greet the sender.
* **`/joke`**: The bot will fetch a funny dad joke from `https://icanhazdadjoke.com/` and post it.
* **`/help`**: Shows all available commands.
