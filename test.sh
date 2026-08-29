#!/usr/bin/env bash
set -e

# Default test server URL
export FIREFLY_BASE_URL="${FIREFLY_BASE_URL:-http://127.0.0.1:39209}"
export FIREFLY_WS_URL="${FIREFLY_WS_URL:-ws://127.0.0.1:39209}"
export EMULATOR_MODE="true"
export RUST_LOG="${RUST_LOG:-info}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=========================================="
echo " Running Firefly Client Tests"
echo " Server URL: $FIREFLY_BASE_URL"
echo "=========================================="

if [ "$#" -eq 0 ]; then
    cargo test -- --test-threads=1
else
    cargo test "$@" -- --test-threads=1
fi
