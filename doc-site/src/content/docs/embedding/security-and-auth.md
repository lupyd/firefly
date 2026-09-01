---
title: Security & Token Bridging
description: Best practices for securing tokens, identity keys, and storage when embedding Firefly in web applications.
---

# Security & Token Bridging

Embedding end-to-end encrypted messaging in client-facing applications requires careful consideration of credential management, storage security, and identity key persistence.

---

## 1. Token Exchange in Web Browsers

For embedded web apps, do not expose long-lived client secrets. Use:
- **Auth0 PKCE (Proof Key for Code Exchange)**: Standard authorization flow for Single Page Apps (SPAs).
- **Backend Token Bridging**: If your application already authenticates users via HTTP-only session cookies, create a backend route `/api/firefly/token` that validates the session and returns an ephemeral Firefly JWT.

```typescript
// Token provider callback for embedded clients
const callbacks = {
  name: 'current_user',
  getAccessToken: async () => {
    const res = await fetch('/api/auth/firefly-token');
    const data = await res.json();
    return data.access_token;
  },
  // ...
};
```

---

## 2. Keystore Storage in Web Environments

| Environment | Storage Mechanism | Best For |
|---|---|---|
| **Node.js / Bun / Electron** | SQLite Database (`client-store.db`) | Desktop apps, bot daemons, background services |
| **Browser (Direct WASM/JS)** | IndexedDB / OPFS (Origin Private File System) | SPAs, modern web apps |
| **Web Gateway / Proxy** | Server-side SQLite per user session | Low-footprint web widgets & legacy browsers |

---

## 3. Cryptographic Hygiene Checklist

- [x] **Never expose raw private keys or ratchet seeds in global `window` objects.**
- [x] **Always enable TLS/HTTPS and WSS in production.**
- [x] **Ensure SQLite database files have restrictive filesystem permissions (`0600`).**
- [x] **Rotate user keys periodically to preserve Post-Compromise Security (PCS).**
