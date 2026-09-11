#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# If --docker flag is provided, delegate to test-docker.sh with remaining arguments
if [ "$1" = "--docker" ] || [ "$1" = "-d" ] || [ "${SPAWN_DOCKER:-false}" = "true" ]; then
    if [ "$1" = "--docker" ] || [ "$1" = "-d" ]; then
        shift
    fi
    exec "$SCRIPT_DIR/test-docker.sh" "$@"
fi

# Default test server URL
export FIREFLY_BASE_URL="${FIREFLY_BASE_URL:-http://127.0.0.1:39209}"
export FIREFLY_WS_URL="${FIREFLY_WS_URL:-ws://127.0.0.1:39209}"
export EMULATOR_MODE="true"
export RUST_LOG="${RUST_LOG:-info}"

echo "=========================================="
echo " Running Firefly Client Tests"
echo " Server URL: $FIREFLY_BASE_URL"
echo " Note: Run with --docker to spawn temporary containers"
echo "=========================================="

if [ "$#" -eq 0 ]; then
    cargo test
else
    cargo test "$@"
fi
