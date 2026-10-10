#!/usr/bin/env bash
set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

RUN_ID="impaired_bench_$(date +%s)_$RANDOM"
DB_CONTAINER="firefly-db-${RUN_ID}"
SRV_CONTAINER="firefly-srv-${RUN_ID}"
TMP_ENV="/tmp/firefly_impaired_env_${RUN_ID}"
WT_KEY_PATH="/tmp/${RUN_ID}_wt_key.pem"
WT_CERT_PATH="/tmp/${RUN_ID}_wt_cert.pem"
OUTPUT_JSON_PATH="$SCRIPT_DIR/docs/real_world_impaired_benchmark_results.json"

MEMBERS="${MEMBERS:-500}"
DEVICES="${DEVICES:-3}"
DURATION="${DURATION:-300}"
CADENCE_MS="${CADENCE_MS:-10}"
NET_DELAY="${NET_DELAY:-30ms}"
NET_JITTER="${NET_JITTER:-5ms}"
NET_LOSS="${NET_LOSS:-1%}"

cleanup() {
    echo ""
    echo "=========================================="
    echo " Cleaning up benchmark Docker containers..."
    echo "=========================================="
    docker rm -f "$SRV_CONTAINER" "$DB_CONTAINER" >/dev/null 2>&1 || true
    rm -f "$TMP_ENV" "$WT_KEY_PATH" "$WT_CERT_PATH"
    echo "Cleanup complete."
}
trap cleanup EXIT INT TERM

# 1. Allocate available host ports
DB_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("", 0)); print(s.getsockname()[1]); s.close()')
SRV_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("", 0)); print(s.getsockname()[1]); s.close()')

echo "================================================================="
echo " FIREFLY REAL-WORLD IMPAIRED NETWORK BENCHMARK"
echo " Members:             $MEMBERS (x $DEVICES devices = $((MEMBERS * DEVICES)) endpoints)"
echo " Duration per mode:   ${DURATION}s"
echo " Network Impairment:  Latency: ${NET_DELAY} ± ${NET_JITTER}, Packet Loss: ${NET_LOSS}"
echo " Host Ports:          Postgres: $DB_PORT | Server: $SRV_PORT"
echo " Output Path:         $OUTPUT_JSON_PATH"
echo "================================================================="

# 2. Prepare environment file based on .env.test
sed "s|DB_CONN_STR=.*|DB_CONN_STR=postgresql://postgres:postgres@127.0.0.1:${DB_PORT}/firefly?sslmode=disable|g; \
     s|PORT=.*|PORT=${SRV_PORT}|g; \
     s|FIREFLY_BASE_URL=.*|FIREFLY_BASE_URL=http://127.0.0.1:${SRV_PORT}|g; \
     s|RESET_DB=.*|RESET_DB=false|g" "$SCRIPT_DIR/.env.test" > "$TMP_ENV"

# 3. Start Postgres container
echo "[1/4] Starting Postgres container..."
docker run -d \
    --name "$DB_CONTAINER" \
    --ulimit nofile=65536:65536 \
    -p "127.0.0.1:${DB_PORT}:5432" \
    -e POSTGRES_PASSWORD=postgres \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_DB=firefly \
    -v "$SCRIPT_DIR/initdb.sql:/docker-entrypoint-initdb.d/init.sql:ro" \
    postgres:16-alpine >/dev/null

echo "Waiting for Postgres to initialize..."
for i in $(seq 1 40); do
    if PGPASSWORD=postgres psql -h 127.0.0.1 -p "$DB_PORT" -U postgres -d firefly -c "SELECT 1 FROM keys LIMIT 1;" >/dev/null 2>&1; then
        echo "✓ Postgres schema and TCP port ready!"
        break
    fi
    sleep 0.5
done

# 4. Generate TLS Certificate
openssl req -x509 -newkey rsa:2048 -keyout "$WT_KEY_PATH" -out "$WT_CERT_PATH" -days 30 -nodes -subj '/CN=localhost' >/dev/null 2>&1

# 5. Start Firefly MLS Server
echo "[2/4] Starting Firefly MLS Server..."
docker run -d \
    --name "$SRV_CONTAINER" \
    --network host \
    --ulimit nofile=65536:65536 \
    --env-file "$TMP_ENV" \
    -e WT_HOST=localhost \
    -e WT_CERT_PATH="$WT_CERT_PATH" \
    -e WT_KEY_PATH="$WT_KEY_PATH" \
    -v "$WT_CERT_PATH:$WT_CERT_PATH:ro" \
    -v "$WT_KEY_PATH:$WT_KEY_PATH:ro" \
    -v "$SCRIPT_DIR/initdb.sql:/initdb.sql:ro" \
    -v "$SCRIPT_DIR/initdb.sql:/app/initdb.sql:ro" \
    hashtag438/firefly-mls-server:latest >/dev/null

echo "Waiting for Firefly server to be healthy..."
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
    echo "Error: Server failed to start."
    docker logs "$SRV_CONTAINER"
    exit 1
fi

# 6. Ensure binary is present
BENCH_BIN="/home/ash/.cargo/target/release/simulation-benchmark"
if [ ! -f "$BENCH_BIN" ]; then
    echo "Building simulation-benchmark binary..."
    cargo build --release --bin simulation-benchmark
fi

# 7. Run impaired benchmark in network-isolated container
echo "[3/4] Launching benchmark container with Linux tc netem traffic impairment..."
echo "      Applying: tc qdisc add dev eth0 root netem limit 50000 delay ${NET_DELAY} ${NET_JITTER} loss ${NET_LOSS}"

docker run --rm \
    --cap-add=NET_ADMIN \
    --ulimit nofile=65536:65536 \
    --add-host host.docker.internal:host-gateway \
    -v "$BENCH_BIN:/bench:ro" \
    -v "$SCRIPT_DIR/docs:/docs:rw" \
    -e FIREFLY_BASE_URL="http://host.docker.internal:${SRV_PORT}" \
    -e FIREFLY_WS_URL="ws://host.docker.internal:${SRV_PORT}" \
    -e FIREFLY_WT_URL="https://host.docker.internal:${SRV_PORT}" \
    -e WT_HOST="localhost" \
    firefly-bench-runner:latest sh -c "
        tc qdisc add dev eth0 root netem limit 50000 delay ${NET_DELAY} ${NET_JITTER} loss ${NET_LOSS}
        /bench --members ${MEMBERS} --devices ${DEVICES} --duration ${DURATION} --cadence-ms ${CADENCE_MS} --transport both --output-json /docs/real_world_impaired_benchmark_results.json
    "

echo "[4/4] Benchmark completed! Results written to $OUTPUT_JSON_PATH"
