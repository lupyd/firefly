#!/usr/bin/env bash
set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

RUN_ID="test_$(date +%s)_$RANDOM"
DB_CONTAINER="firefly-temp-db-${RUN_ID}"
SRV_CONTAINER="firefly-temp-srv-${RUN_ID}"
TMP_ENV="$(mktemp /tmp/firefly_env_XXXXXX)"

cleanup() {
    echo ""
    echo "=========================================="
    echo " Cleaning up temporary Docker containers..."
    echo "=========================================="
    docker rm -f "$SRV_CONTAINER" "$DB_CONTAINER" >/dev/null 2>&1 || true
    rm -f "$TMP_ENV"
    echo "Cleanup complete."
}
trap cleanup EXIT INT TERM

# 1. Allocate available host ports
DB_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("", 0)); print(s.getsockname()[1]); s.close()')
SRV_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("", 0)); print(s.getsockname()[1]); s.close()')

echo "=========================================="
echo " Launching Temporary Firefly Test Stack"
echo " Postgres Port: $DB_PORT"
echo " Server Port:   $SRV_PORT"
echo " Run ID:        $RUN_ID"
echo "=========================================="

# 2. Prepare environment file based on .env.test
if [ ! -f "$SCRIPT_DIR/.env.test" ]; then
    echo "Error: .env.test not found in $SCRIPT_DIR"
    exit 1
fi

sed "s|DB_CONN_STR=.*|DB_CONN_STR=postgresql://postgres:postgres@127.0.0.1:${DB_PORT}/firefly?sslmode=disable|g; \
     s|PORT=.*|PORT=${SRV_PORT}|g; \
     s|FIREFLY_BASE_URL=.*|FIREFLY_BASE_URL=http://127.0.0.1:${SRV_PORT}|g; \
     s|RESET_DB=.*|RESET_DB=false|g" "$SCRIPT_DIR/.env.test" > "$TMP_ENV"

# 3. Start temporary Postgres container
echo "[1/3] Starting temporary Postgres instance..."
docker run -d \
    --name "$DB_CONTAINER" \
    -p "127.0.0.1:${DB_PORT}:5432" \
    -e POSTGRES_PASSWORD=postgres \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_DB=firefly \
    -v "$SCRIPT_DIR/initdb.sql:/docker-entrypoint-initdb.d/init.sql:ro" \
    postgres:16-alpine >/dev/null

echo "Waiting for Postgres to fully initialize on TCP port ${DB_PORT}..."
for i in $(seq 1 40); do
    if PGPASSWORD=postgres psql -h 127.0.0.1 -p "$DB_PORT" -U postgres -d firefly -c "SELECT 1 FROM keys LIMIT 1;" >/dev/null 2>&1; then
        echo "✓ Postgres schema and TCP port are ready!"
        sleep 0.5
        break
    fi
    sleep 0.5
done

# 4. Start temporary Firefly MLS Server container
echo "[2/3] Starting temporary Firefly MLS Server..."
docker run -d \
    --name "$SRV_CONTAINER" \
    --network host \
    --env-file "$TMP_ENV" \
    -v "$SCRIPT_DIR/initdb.sql:/initdb.sql:ro" \
    -v "$SCRIPT_DIR/initdb.sql:/app/initdb.sql:ro" \
    hashtag438/firefly-mls-server:latest >/dev/null

echo "Waiting for Firefly server to become ready on http://127.0.0.1:${SRV_PORT}..."
SERVER_READY=false
for i in $(seq 1 30); do
    STATUS_CODE=$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:${SRV_PORT}/jwks.json" || true)
    if [ "$STATUS_CODE" = "200" ]; then
        SERVER_READY=true
        echo "✓ Firefly MLS Server is ready (HTTP 200)!"
        break
    fi
    sleep 0.5
done

if [ "$SERVER_READY" != "true" ]; then
    echo "Error: Server failed to start. Logs:"
    docker logs "$SRV_CONTAINER"
    exit 1
fi

# 5. Export test environment variables
export FIREFLY_BASE_URL="http://127.0.0.1:${SRV_PORT}"
export FIREFLY_WS_URL="ws://127.0.0.1:${SRV_PORT}"
export EMULATOR_MODE="true"
export RUST_LOG="${RUST_LOG:-info}"

echo "[3/3] Running tests against temporary environment..."
echo "=========================================="
echo " Target URL: $FIREFLY_BASE_URL"
echo "=========================================="

if [ "$#" -eq 0 ]; then
    cargo test
else
    cargo test "$@"
fi
