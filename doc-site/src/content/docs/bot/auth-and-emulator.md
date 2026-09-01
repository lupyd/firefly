---
title: Authentication & Emulator Mode
description: Understanding Auth0 PKCE browser authentication vs Local Emulator Mode for bots.
---

# Authentication & Emulator Mode

Firefly MLS supports two modes of authentication:
1. **Production Mode**: Interactive Auth0 OAuth 2.0 PKCE flow with browser redirection.
2. **Emulator Mode (`EMULATOR_MODE=true`)**: Direct local username authentication for unit tests, CI/CD, and local backend emulators.

---

## 1. Production Mode: Auth0 PKCE

When running in production:
1. The bot spins up a lightweight local HTTP server (default port `38295`).
2. Generates cryptographic `code_verifier` and `code_challenge` (SHA-256).
3. Opens the system default web browser to the Auth0 login page.
4. After you sign in, Auth0 redirects to `http://localhost:38295/callback?code=...`.
5. The bot intercepts the code, exchanges it for JWT access and refresh tokens, and saves them to `sessionFile` (e.g. `./bot-session.json`).

```typescript
const bot = new FireflyBot({
  auth0Domain: 'https://auth.lupyd.com',
  auth0ClientId: 'your_client_id',
  auth0Audience: 'https://lupyd.com',
  port: 38295,
  sessionFile: './bot-session.json',
});
```

---

## 2. Local Emulator Mode

When running automated integration tests or developing against a local backend:

```bash
export EMULATOR_MODE=true
export BOT_USERNAME=my_test_bot
export FIREFLY_BASE_URL=http://127.0.0.1:30000
export FIREFLY_WS_URL=ws://127.0.0.1:30000/
bun start
```

In Emulator Mode:
- No browser window opens.
- The bot sets the access token directly to `BOT_USERNAME`.
- Perfect for non-interactive server environments, Docker containers, and CI/CD test runners.
