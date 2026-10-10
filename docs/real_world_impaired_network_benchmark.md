# Firefly Real-World Impaired Network Benchmark: WebTransport vs WebSocket
**500 Members × 3 Devices (1,500 Endpoints) | 5-Minute Deterministic Simulation Under Cellular Network Impairment**

---

## 1. Executive Summary

This report documents the performance evaluation of **Firefly MLS** under simulated real-world internet and cellular network conditions:
- **Base Round-Trip / One-Way Latency:** 30 ms
- **Latency Jitter:** ±5 ms (normal distribution)
- **Packet Loss:** 1.0% stochastic packet drop rate
- **Scale:** 500 members × 3 devices each = **1,500 active endpoints**
- **Duration:** 300 seconds (5 minutes) per transport protocol
- **Traffic Pattern:** Deterministic pseudo-random RPC requests and cross-device message deliveries (10ms cadence, seed 42)

### Headline Benchmark Results

| Metric | WebSocket (TCP) | WebTransport (QUIC) | Improvement / Delta | Real-World Impact |
| :--- | :--- | :--- | :--- | :--- |
| **Active Endpoints** | 1,500 | 1,500 | Equal | High concurrency |
| **Simulation Duration** | 300.0s | 300.0s | Equal | 5-minute sustained load |
| **Total Messages Sent** | 27,060 | 27,091 | +0.1% | Identical traffic workload |
| **Throughput** | 90.20 msgs/s | 90.30 msgs/s | Equal | Saturated message generation |
| **RPC Latency p50 (Median)** | 33.24 ms | 33.50 ms | +0.8% | Both hit base network floor |
| **RPC Latency p90** | 37.39 ms | 37.60 ms | +0.6% | Comparable under good frames |
| **RPC Latency p95** | 37.97 ms | 38.13 ms | +0.4% | Comparable under good frames |
| **RPC Latency p99 (Tail)** | **258.74 ms** | **51.23 ms** | **-80.2% (5.05× faster)** | **Eliminates TCP HoL blocking stalls** |
| **RPC Latency Max** | **535.96 ms** | **227.13 ms** | **-57.6% (2.36× lower)** | Avoids catastrophic TCP retransmission delays |
| **Latency Std Dev** | **24.37 ms** | **10.13 ms** | **-58.4% lower jitter** | Consistent, predictable responsiveness |
| **Client CPU (Mean)** | 4.82% | 4.60% | -4.4% | Lower client processing overhead |
| **Client CPU (Peak)** | **12.72%** | **10.82%** | **-15.0% lower** | Smoother resource allocation |
| **Server CPU Load (Mean)** | 6.42% | 6.14% | -4.4% | Lower server load |
| **Server CPU Load (Peak)** | **16.97%** | **14.43%** | **-15.0% lower** | Prevents server spikes |
| **Client Memory (Peak RSS)** | 209.1 MB | 281.9 MB | +34.8% | QUIC connection/stream state |
| **Wire Bandwidth** | 98.6 KB/s | 145.9 KB/s | +48.0% | QUIC datagram framing overhead |

> [!IMPORTANT]
> **Key Finding:** Under 1% packet loss and 30ms latency, WebSocket's p99 tail latency collapses to **258.74 ms** (with spikes up to **535.96 ms**) due to TCP Head-of-Line (HoL) blocking. WebTransport retains a tight p99 latency of **51.23 ms**—an **80.2% reduction in tail latency**.

---

## 2. Test Methodology & Network Architecture

### 2.1 Emulation Architecture

In real-world production deployments, backend infrastructure (Firefly MLS Server and PostgreSQL) resides in a high-speed datacenter virtual private cloud (VPC) with sub-millisecond round trips and 0% loss, while end-user devices (smartphones, desktop clients) communicate over public cellular (4G/LTE/5G) or Wi-Fi networks characterized by packet loss and latency jitter.

To replicate this realistically:
1. **Collocated Backend Stack:**
   - PostgreSQL 16 container (`firefly-db`) and Firefly MLS Server container (`firefly-srv`) communicate over local high-speed interfaces (<0.1ms ping, 0% packet loss).
2. **Impaired Client Network Namespace:**
   - The simulation benchmark binary runs inside a dedicated, isolated Linux network container (`firefly-bench-runner`).
   - The container's network interface (`eth0`) is subjected to kernel-level packet shaping via Linux Traffic Control (`tc netem`).
   - All client traffic (HTTP registration, WebSocket TCP streams, WebTransport UDP datagrams) traverses this impaired network path.

```
+-------------------------------------------------------------------------------+
|  CLIENT NETWORK NAMESPACE (firefly-bench-runner container)                    |
|  1,500 Endpoints (500 Members x 3 Devices)                                    |
|                                                                               |
|  Linux tc netem: delay 30ms ± 5ms (normal distribution), loss 1.0%            |
+-------------------------------------------------------------------------------+
                                      |
                 [Impaired Cellular / WAN Network Path]
                 (30ms delay, ±5ms jitter, 1% packet loss)
                                      v
+-------------------------------------------------------------------------------+
|  BACKEND DATACENTER STACK (Host Network)                                      |
|                                                                               |
|  +-----------------------------+         +----------------------------------+ |
|  |  Firefly MLS Server         | <-----> |  PostgreSQL 16 Database          | |
|  |  (HTTP/WS/WebTransport)     | <0.1ms  |  (1,500 Device Records, Schema)  | |
|  +-----------------------------+  0% loss+----------------------------------+ |
+-------------------------------------------------------------------------------+
```

### 2.2 Kernel Traffic Control Configuration

The kernel queuing discipline was configured using `tc qdisc` inside the client container:

```bash
tc qdisc add dev eth0 root netem limit 50000 delay 30ms 5ms loss 1%
```

- **`delay 30ms 5ms`:** Adds a base delay of 30ms with ±5ms jitter drawn from a normal (Gaussian) distribution, accurately simulating real-world packet travel variation over mobile towers.
- **`loss 1%`:** Drops 1 out of every 100 IP packets uniformly at random.
- **`limit 50000`:** Sets a queue buffer depth of 50,000 packets. This prevents burst packet drops during initial concurrent connection establishment (1,500 simultaneous TLS handshakes), ensuring that measured packet loss reflects stochastic network loss rather than buffer exhaustion.

---

## 3. Deep Dive: Head-of-Line Blocking vs Independent Streams

### 3.1 Why WebSocket Degrades Under Packet Loss

WebSocket operates over a single TCP stream. TCP guarantees strictly ordered, reliable byte delivery:
1. When a single TCP packet is dropped (1% loss rate), the receiving operating system's TCP stack **cannot deliver any subsequent packets to the application**, even if those subsequent packets have already arrived successfully.
2. The receiving TCP stack buffers subsequent packets and waits for the missing packet to be retransmitted via TCP Fast Retransmit or Retransmission Timeout (RTO).
3. In this test, when packet loss occurred, all queued messages waiting behind the dropped packet were delayed by **one full RTT + retransmission processing time (~200ms - 500ms)**.
4. This produces the observed **p99 latency of 258.74 ms** and **max latency spike of 535.96 ms** in WebSocket.

### 3.2 Why WebTransport Maintains Sub-55ms Tail Latency

WebTransport operates over QUIC (RFC 9000), which runs over UDP and natively supports multiplexed bidirectional and unidirectional streams:
1. Firefly's WebTransport client maps each RPC request to a **dedicated bidirectional stream** (`session.open_bi()`).
2. If a UDP packet containing data for Stream A is dropped by `tc netem`, QUIC retransmits that packet for Stream A.
3. Crucially, Streams B, C, D... on the same QUIC connection **are completely unaffected** and continue to be read and dispatched immediately by the application.
4. Even with 1% packet loss, 99% of requests experience zero Head-of-Line delay, maintaining a **p99 latency of 51.23 ms** (only ~13ms above the network jitter floor).

```
WEBSOCKET (Single TCP Stream):
[Packet 1] ---> OK (30ms)
[Packet 2] ---> DROPPED! (1% loss)
[Packet 3] ---> BLOCKED at TCP layer (waiting for Packet 2 retransmit)
[Packet 4] ---> BLOCKED at TCP layer (waiting for Packet 2 retransmit)
... Retransmit Packet 2 arrives after 250ms ...
[Packets 2, 3, 4 delivered all at once] ---> Tail Latency: 258.74ms

WEBTRANSPORT (Independent QUIC Streams):
Stream 1: [Datagram 1] ---> OK (30ms)
Stream 2: [Datagram 2] ---> DROPPED! (Stream 2 retransmits)
Stream 3: [Datagram 3] ---> OK (31ms) Delivered immediately!
Stream 4: [Datagram 4] ---> OK (29ms) Delivered immediately!
Stream 2: Retransmit arrives (80ms) ---> Only Stream 2 delayed
Average Tail Latency (p99): 51.23ms
```

---

## 4. Comprehensive Benchmark Metrics

The raw benchmark results are recorded in [`docs/real_world_impaired_benchmark_results.json`](file:///old-arch/home/ash/lupyd-foundation/firefly/docs/real_world_impaired_benchmark_results.json).

### 4.1 RPC Latency Distribution (Round-Trip)

| Percentile | WebSocket | WebTransport | Delta (ms) | Gain (%) |
| :--- | :--- | :--- | :--- | :--- |
| **Min** | 26.91 ms | 27.51 ms | +0.60 ms | Network floor |
| **p50 (Median)** | 33.24 ms | 33.50 ms | +0.26 ms | +0.8% |
| **p90** | 37.39 ms | 37.60 ms | +0.21 ms | +0.6% |
| **p95** | 37.97 ms | 38.13 ms | +0.16 ms | +0.4% |
| **p99 (Tail)** | **258.74 ms** | **51.23 ms** | **-207.51 ms** | **80.2% faster** |
| **Max** | **535.96 ms** | **227.13 ms** | **-308.83 ms** | **57.6% lower** |
| **Mean** | 35.69 ms | 34.42 ms | -1.27 ms | 3.5% faster |
| **Standard Deviation** | 24.37 ms | 10.13 ms | -14.24 ms | 58.4% less jitter |

### 4.2 Cross-Device Delivery Latency

Delivery latency measures the elapsed time from when Member $A$ sends a message until Member $B$'s devices receive it pushed via the server:

| Percentile | WebSocket | WebTransport | Delta (ms) | Gain (%) |
| :--- | :--- | :--- | :--- | :--- |
| **Min** | 26.89 ms | 27.50 ms | +0.61 ms | Network floor |
| **p50** | 33.25 ms | 33.51 ms | +0.26 ms | +0.8% |
| **p90** | 37.42 ms | 37.60 ms | +0.18 ms | +0.5% |
| **p95** | 38.01 ms | 38.14 ms | +0.13 ms | +0.3% |
| **p99** | **237.58 ms** | **51.23 ms** | **-186.35 ms** | **78.4% faster** |
| **Max** | 535.96 ms | 227.13 ms | -308.83 ms | 57.6% lower |
| **Mean** | 35.76 ms | 34.43 ms | -1.33 ms | 3.7% faster |
| **Standard Deviation** | 24.38 ms | 10.13 ms | -14.25 ms | 58.4% less jitter |

### 4.3 System Resource Utilization

| Metric | WebSocket | WebTransport | Difference | Notes |
| :--- | :--- | :--- | :--- | :--- |
| **Client CPU (Mean)** | 4.82% | 4.60% | -4.4% | Quinn async processing efficiency |
| **Client CPU (Peak)** | **12.72%** | **10.82%** | **-15.0%** | Less TCP retransmission buffering spikes |
| **Server CPU (Mean)** | 6.42% | 6.14% | -4.4% | Collocated server load |
| **Server CPU (Peak)** | **16.97%** | **14.43%** | **-15.0%** | Lower peak queue processing |
| **Client Memory (Peak RSS)** | 209.1 MB | 281.9 MB | +72.8 MB (+34.8%) | Per-stream QUIC packet buffers |
| **Bandwidth Rate** | 98.6 KB/s | 145.9 KB/s | +47.3 KB/s (+48.0%) | QUIC packet headers and framing |
| **Wire Overhead per Msg** | 1,119.1 B | 1,654.2 B | +535.1 B | UDP datagram encapsulation |

---

## 5. Comparison: Baseline Local vs Impaired Network

Comparing the baseline local benchmark ([`docs/webtransport_migration_and_benchmark.md`](file:///old-arch/home/ash/lupyd-foundation/firefly/docs/webtransport_migration_and_benchmark.md)) against the real-world impaired benchmark highlights the resilience of WebTransport:

| Metric | Baseline Local WS (<0.1ms ping, 0% loss) | Impaired WS (30ms ping, 1% loss) | Baseline Local WT (<0.1ms ping, 0% loss) | Impaired WT (30ms ping, 1% loss) |
| :--- | :--- | :--- | :--- | :--- |
| **p50 Latency** | 0.81 ms | 33.24 ms | 0.73 ms | 33.50 ms |
| **p95 Latency** | 2.50 ms | 37.97 ms | 1.83 ms | 38.13 ms |
| **p99 Tail Latency** | 10.95 ms | **258.74 ms (+2,263%)** | 4.52 ms | **51.23 ms (+1,033%)** |
| **Max Latency** | 47.96 ms | **535.96 ms** | 19.57 ms | **227.13 ms** |
| **Latency Std Dev** | 1.34 ms | **24.37 ms** | 0.89 ms | **10.13 ms** |

> [!TIP]
> Under perfect network conditions (local loopback), WebTransport's p99 latency was already 58.7% faster than WebSocket. Under realistic mobile network impairment (30ms latency, 1% packet loss), WebTransport's advantage widens to **80.2% faster** (51.23ms vs 258.74ms).

---

## 6. How to Reproduce the Benchmark

The entire benchmark suite is automated via [`run_real_world_benchmark.sh`](file:///old-arch/home/ash/lupyd-foundation/firefly/run_real_world_benchmark.sh).

### 6.1 Prerequisites
- Docker Engine with standard socket access.
- Pre-compiled release binary:
  ```bash
  cargo build --release --bin simulation-benchmark
  ```
- Runner container image (builds in ~10 seconds):
  ```bash
  docker build -t firefly-bench-runner:latest - <<'EOF'
  FROM debian:bookworm-slim
  RUN apt-get update && apt-get install -y iproute2 procps ca-certificates && rm -rf /var/lib/apt/lists/*
  EOF
  ```

### 6.2 Running the Full 5-Minute Impaired Benchmark
Execute the runner script:
```bash
./run_real_world_benchmark.sh
```

### 6.3 Customizing Network Impairment Parameters
Environment variables can be provided to test different network profiles:

```bash
# Example: High latency satellite connection (150ms delay, 20ms jitter, 2% loss)
MEMBERS=500 DEVICES=3 DURATION=300 \
NET_DELAY="150ms" NET_JITTER="20ms" NET_LOSS="2%" \
./run_real_world_benchmark.sh

# Example: Quick 30-second verification run
MEMBERS=50 DEVICES=2 DURATION=30 \
./run_real_world_benchmark.sh
```

---

## 7. Production Takeaways & Deployment Recommendations

1. **Massive Quality-of-Experience (QoE) Gain on Mobile:**
   Mobile devices on cellular networks experience intermittent packet loss (handover between cell towers, fading signals, congested Wi-Fi). Migrating from WebSocket to WebTransport prevents message delivery freezes and keeps p99 chat responsiveness under 55ms.

2. **Resource Budgeting:**
   WebTransport endpoints require approximately **35% more resident memory** (~280MB vs ~210MB for 1,500 connections) to maintain per-stream QUIC packet buffers. When sizing client edge instances, ensure memory allocation accounts for ~185KB per concurrent QUIC endpoint.

3. **Smooth Server CPU Profile:**
   Because WebTransport avoids sudden bursts of unblocked TCP packets, server CPU utilization is 15% lower during peak load periods, reducing the risk of server throttling and GC pauses.
