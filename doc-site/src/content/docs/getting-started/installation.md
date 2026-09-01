---
title: Installation & Setup
description: Guide to installing dependencies, building native bindings with pnpm, and setting up environments.
---

# Installation & Setup

Firefly MLS can be used in Node.js, Bun, and browser applications. This guide walks you through setting up dependencies, compiling native bindings, and running a local development environment.

---

## Prerequisites

- **Node.js**: Version 18+ or **Bun**: Version 1.0+
- **pnpm**: Fast, disk space-efficient package manager
- **Rust toolchain**: Edition 2024 / Rust 1.85+ (needed if building native N-API crates from source)
- **Docker & Docker Compose**: (Optional, for running a local Firefly MLS server and PostgreSQL)

---

## Workspace Installation with pnpm

Clone the repository and install dependencies using `pnpm`:

```bash
# Clone the repository
git clone https://github.com/lupyd-foundation/firefly.git
cd firefly

# Install workspace dependencies
pnpm install
```

---

## Building Native Addons

If you are using `firefly-client-node` or developing locally against native bindings:

```bash
# Build the native N-API FFI addon in debug mode
pnpm --filter firefly-client-node run build:debug

# Or build in release mode for production performance
pnpm --filter firefly-client-node run build
```

---

## Building the High-Level JS Client

To compile the TypeScript source in `firefly-client-js`:

```bash
pnpm --filter firefly-client-js run build
```

---

## Setting Up a Local Server Environment

For local testing and offline development without connecting to live servers:

1. Launch the local backend server (or run in emulator mode):
   ```bash
   # In the backend directory
   docker compose up -d
   ```
2. Enable Emulator Mode in client applications:
   ```bash
   export EMULATOR_MODE=true
   export FIREFLY_BASE_URL=http://127.0.0.1:30000
   export FIREFLY_WS_URL=ws://127.0.0.1:30000/
   ```
