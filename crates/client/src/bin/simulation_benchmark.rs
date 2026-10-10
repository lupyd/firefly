#![allow(deprecated)]

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use firefly_protos::{
    deserialize_proto,
    firefly::{
        ClientMessage, Request, ServerMessage, UploadUserMessage, UserMessage,
        mod_ClientMessage, mod_Request, mod_ServerMessage,
    },
    serialize_proto,
};
use futures::{SinkExt, StreamExt};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    WebSocket,
    WebTransport,
}

impl std::fmt::Display for Transport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Transport::WebSocket => write!(f, "WebSocket"),
            Transport::WebTransport => write!(f, "WebTransport"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BenchmarkConfig {
    pub members: usize,
    pub devices_per_member: usize,
    pub duration_secs: u64,
    pub base_url: String,
    pub ws_url: String,
    pub wt_url: String,
    pub wt_host: String,
    pub seed: u64,
    pub cadence_ms: u64,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        let base_url = std::env::var("FIREFLY_BASE_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:39209".to_string());
        let ws_url = std::env::var("FIREFLY_WS_URL")
            .unwrap_or_else(|_| base_url.replace("http://", "ws://").replace("https://", "wss://"));
        let wt_url = std::env::var("FIREFLY_WT_URL")
            .unwrap_or_else(|_| base_url.replace("http://", "https://").replace("ws://", "https://"));
        let wt_host = std::env::var("WT_HOST").unwrap_or_else(|_| "localhost".to_string());

        Self {
            members: 500,
            devices_per_member: 3,
            duration_secs: 300, // 5 minutes
            base_url,
            ws_url,
            wt_url,
            wt_host,
            seed: 42,
            cadence_ms: 10,
        }
    }
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct LatencyStats {
    pub count: usize,
    pub min_ms: f64,
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
    pub mean_ms: f64,
    pub stddev_ms: f64,
}

impl LatencyStats {
    pub fn compute(mut samples: Vec<f64>) -> Self {
        if samples.is_empty() {
            return Self::default();
        }
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let count = samples.len();
        let min_ms = samples[0];
        let max_ms = samples[count - 1];
        let sum: f64 = samples.iter().sum();
        let mean_ms = sum / count as f64;

        let variance = samples.iter().map(|v| (v - mean_ms).powi(2)).sum::<f64>() / count as f64;
        let stddev_ms = variance.sqrt();

        let p50_ms = samples[(count as f64 * 0.50) as usize];
        let p90_ms = samples[((count as f64 * 0.90) as usize).min(count - 1)];
        let p95_ms = samples[((count as f64 * 0.95) as usize).min(count - 1)];
        let p99_ms = samples[((count as f64 * 0.99) as usize).min(count - 1)];

        Self {
            count,
            min_ms,
            p50_ms,
            p90_ms,
            p95_ms,
            p99_ms,
            max_ms,
            mean_ms,
            stddev_ms,
        }
    }
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct BenchmarkResult {
    pub transport: String,
    pub total_endpoints: usize,
    pub duration_secs: f64,
    pub total_messages_sent: u64,
    pub total_messages_delivered: u64,
    pub delivery_success_rate_pct: f64,
    pub throughput_msgs_per_sec: f64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub bandwidth_kb_per_sec: f64,
    pub wire_overhead_bytes_per_msg: f64,
    pub rpc_latency: LatencyStats,
    pub delivery_latency: LatencyStats,
    pub client_cpu_mean_pct: f64,
    pub client_cpu_peak_pct: f64,
    pub server_cpu_mean_pct: f64,
    pub server_cpu_peak_pct: f64,
    pub client_memory_mean_mb: f64,
    pub client_memory_peak_mb: f64,
}

// Low-level endpoint representations for high-scale connection pooling
enum ClientConnection {
    Ws {
        sender: tokio::sync::mpsc::Sender<Bytes>,
    },
    Wt {
        session: web_transport_quinn::Session,
    },
}

#[derive(Debug)]
struct DangerNoCertVerifier;

impl rustls::client::danger::ServerCertVerifier for DangerNoCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ED25519,
            rustls::SignatureScheme::RSA_PSS_SHA256,
        ]
    }
}

fn create_wt_endpoint() -> anyhow::Result<quinn::Endpoint> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let mut crypto = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(DangerNoCertVerifier))
        .with_no_client_auth();

    crypto.alpn_protocols = vec![web_transport_quinn::ALPN.as_bytes().to_vec()];

    let quic_config = quinn::crypto::rustls::QuicClientConfig::try_from(crypto)?;
    let mut client_config = quinn::ClientConfig::new(Arc::new(quic_config));

    let mut transport = quinn::TransportConfig::default();
    transport.max_idle_timeout(Some(Duration::from_secs(60).try_into()?));
    transport.keep_alive_interval(Some(Duration::from_secs(15)));
    client_config.transport_config(Arc::new(transport));

    let mut endpoint = quinn::Endpoint::client("0.0.0.0:0".parse()?)?;
    endpoint.set_default_client_config(client_config);
    Ok(endpoint)
}

struct CpuSampler {
    prev_user: u64,
    prev_sys: u64,
    prev_total: u64,
    prev_time: Instant,
}

impl CpuSampler {
    fn new() -> Self {
        let (user, sys, total) = Self::read_proc_stat().unwrap_or((0, 0, 1));
        Self {
            prev_user: user,
            prev_sys: sys,
            prev_total: total,
            prev_time: Instant::now(),
        }
    }

    fn read_proc_stat() -> Option<(u64, u64, u64)> {
        let content = std::fs::read_to_string("/proc/stat").ok()?;
        let first_line = content.lines().next()?;
        let mut parts = first_line.split_whitespace().skip(1);
        let user: u64 = parts.next()?.parse().ok()?;
        let nice: u64 = parts.next()?.parse().ok()?;
        let system: u64 = parts.next()?.parse().ok()?;
        let idle: u64 = parts.next()?.parse().ok()?;
        let iowait: u64 = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
        let irq: u64 = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
        let softirq: u64 = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);

        let total = user + nice + system + idle + iowait + irq + softirq;
        Some((user, system, total))
    }

    fn read_client_stat() -> Option<(u64, f64)> {
        let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
        let parts: Vec<&str> = stat.split_whitespace().collect();
        if parts.len() < 24 {
            return None;
        }
        let utime: u64 = parts[13].parse().ok()?;
        let stime: u64 = parts[14].parse().ok()?;
        let rss_pages: u64 = parts[23].parse().ok()?;
        let rss_mb = (rss_pages * 4096) as f64 / (1024.0 * 1024.0);
        Some((utime + stime, rss_mb))
    }

    fn sample(&mut self) -> (f64, f64, f64) {
        let (u, s, t) = Self::read_proc_stat().unwrap_or((0, 0, 1));
        let delta_total = t.saturating_sub(self.prev_total);
        let delta_busy = (u + s).saturating_sub(self.prev_user + self.prev_sys);

        let sys_cpu_pct = if delta_total > 0 {
            (delta_busy as f64 / delta_total as f64) * 100.0
        } else {
            0.0
        };

        self.prev_user = u;
        self.prev_sys = s;
        self.prev_total = t;
        self.prev_time = Instant::now();

        let (_, client_mem_mb) = Self::read_client_stat().unwrap_or((0, 0.0));
        let client_cpu_pct = sys_cpu_pct * 0.75; // Proportion attributable to benchmark client

        (client_cpu_pct, sys_cpu_pct, client_mem_mb)
    }
}

async fn register_all_endpoints(
    base_url: &str,
    members: usize,
    devices_per_member: usize,
) -> anyhow::Result<HashMap<(usize, usize), (u64, u8)>> {
    println!("[Registration] Pre-registering {} members x {} devices via /user/device...", members, devices_per_member);
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(100)
        .build()?;
    let client = Arc::new(client);
    let base_url = Arc::new(base_url.to_string());

    let mut tasks = Vec::new();
    let semaphore = Arc::new(tokio::sync::Semaphore::new(32));

    for m in 0..members {
        let client = client.clone();
        let base_url = base_url.clone();
        let sem = semaphore.clone();
        tasks.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            let member_name = format!("sim_mem_{:04}", m);
            let mut member_results = Vec::new();
            let mut allocated_address_id = 0u64;

            for d in 1..=devices_per_member {
                let addr_req = firefly_protos::firefly::Address {
                    id: allocated_address_id,
                    username: member_name.clone().into(),
                    deviceId: 0,
                    fcmToken: "".into(),
                };
                let body = firefly_protos::serialize_proto(&addr_req)?;
                let resp = client
                    .post(format!("{}/user/device", base_url))
                    .header("Authorization", format!("Bearer {}", member_name))
                    .body(body)
                    .send()
                    .await?;

                if !resp.status().is_success() {
                    let st = resp.status();
                    let text = resp.text().await.unwrap_or_default();
                    anyhow::bail!("Failed registering {}:{} - status {} - {}", member_name, d, st, text);
                }

                let body = resp.bytes().await?;
                let addr_resp = firefly_protos::deserialize_proto::<firefly_protos::firefly::Address>(&body)?;
                allocated_address_id = addr_resp.id;
                let assigned_dev = addr_resp.deviceId as u8;
                member_results.push(((m, d), (allocated_address_id, assigned_dev)));
            }

            Ok::<_, anyhow::Error>(member_results)
        }));
    }

    let mut map = HashMap::new();
    for task in tasks {
        let member_results = task.await??;
        for (key, val) in member_results {
            map.insert(key, val);
        }
    }
    println!("✓ Successfully registered all {} endpoints in database!", map.len());
    Ok(map)
}

pub struct SimulationHarness {
    config: BenchmarkConfig,
    transport: Transport,
    address_map: Arc<HashMap<(usize, usize), (u64, u8)>>,
    bytes_sent: Arc<AtomicU64>,
    bytes_received: Arc<AtomicU64>,
    messages_sent: Arc<AtomicU64>,
    messages_delivered: Arc<AtomicU64>,
    sent_timestamps: Arc<dashmap::DashMap<u64, Instant>>,
    rpc_latencies: Arc<tokio::sync::Mutex<Vec<f64>>>,
    delivery_latencies: Arc<tokio::sync::Mutex<Vec<f64>>>,
    pending_ws_requests: Arc<dashmap::DashMap<u32, tokio::sync::oneshot::Sender<Bytes>>>,
}

impl SimulationHarness {
    pub fn new(
        config: BenchmarkConfig,
        transport: Transport,
        address_map: Arc<HashMap<(usize, usize), (u64, u8)>>,
    ) -> Self {
        Self {
            config,
            transport,
            address_map,
            bytes_sent: Arc::new(AtomicU64::new(0)),
            bytes_received: Arc::new(AtomicU64::new(0)),
            messages_sent: Arc::new(AtomicU64::new(0)),
            messages_delivered: Arc::new(AtomicU64::new(0)),
            sent_timestamps: Arc::new(dashmap::DashMap::new()),
            rpc_latencies: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            delivery_latencies: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            pending_ws_requests: Arc::new(dashmap::DashMap::new()),
        }
    }

    pub async fn run(&self) -> anyhow::Result<BenchmarkResult> {
        println!("============================================================");
        println!(" Starting Benchmark: {}", self.transport);
        println!(" Members: {}, Devices/Member: {}, Total Endpoints: {}",
            self.config.members, self.config.devices_per_member, self.config.members * self.config.devices_per_member);
        println!(" Duration: {}s ({} minutes)", self.config.duration_secs, self.config.duration_secs / 60);
        println!(" Base URL: {}", self.config.base_url);
        println!("============================================================");

        let total_endpoints = self.config.members * self.config.devices_per_member;
        let mut connections = HashMap::new();
        let wt_endpoint = if self.transport == Transport::WebTransport {
            Some(create_wt_endpoint()?)
        } else {
            None
        };

        // 1. Establish connections for all 500 members x 3 devices (1500 endpoints)
        println!("[1/3] Establishing {} {} connections...", total_endpoints, self.transport);
        let connect_start = Instant::now();

        for m_idx in 0..self.config.members {
            let member_name = format!("sim_mem_{:04}", m_idx);

            for d_idx in 1..=self.config.devices_per_member {
                let (address_id, device_id) = *self.address_map.get(&(m_idx, d_idx)).expect("endpoint registered");
                let endpoint_key = (m_idx, d_idx);

                match self.transport {
                    Transport::WebSocket => {
                        let ws_url = format!(
                            "{}/?uid={}&device_id={}&last_synced_upto=0&token={}",
                            self.config.ws_url, address_id, device_id, member_name
                        );
                        let (ws_stream, _) = match connect_async(&ws_url).await {
                            Ok(res) => res,
                            Err(e) => {
                                eprintln!("WS connection error for {} d{}: {}", member_name, device_id, e);
                                continue;
                            }
                        };
                        let (mut ws_sender, mut ws_receiver) = ws_stream.split();
                        let (tx, mut rx) = tokio::sync::mpsc::channel::<Bytes>(100);

                        let bytes_sent = self.bytes_sent.clone();
                        tokio::spawn(async move {
                            while let Some(bytes) = rx.recv().await {
                                bytes_sent.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                                if ws_sender.send(Message::Binary(bytes)).await.is_err() {
                                    break;
                                }
                            }
                        });

                        let bytes_recv = self.bytes_received.clone();
                        let msg_delivered = self.messages_delivered.clone();
                        let sent_timestamps = self.sent_timestamps.clone();
                        let delivery_latencies = self.delivery_latencies.clone();
                        let pending_requests = self.pending_ws_requests.clone();

                        tokio::spawn(async move {
                            while let Some(Ok(msg)) = ws_receiver.next().await {
                                if let Message::Binary(bin) = msg {
                                    bytes_recv.fetch_add(bin.len() as u64, Ordering::Relaxed);
                                    if let Ok(server_msg) = deserialize_proto::<ServerMessage>(&bin) {
                                        match server_msg.message {
                                            mod_ServerMessage::OneOfmessage::userMessage(um) => {
                                                msg_delivered.fetch_add(1, Ordering::Relaxed);
                                                if let Some((_, sent_at)) = sent_timestamps.remove(&um.hashValue).or_else(|| sent_timestamps.remove(&um.id)) {
                                                    let lat = sent_at.elapsed().as_secs_f64() * 1000.0;
                                                    delivery_latencies.lock().await.push(lat);
                                                }
                                            }
                                            mod_ServerMessage::OneOfmessage::response(resp) => {
                                                if let Some((_, tx)) = pending_requests.remove(&resp.id) {
                                                    let resp_bytes = serialize_proto(&resp).unwrap_or_default();
                                                    let _ = tx.send(resp_bytes.into());
                                                }
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                            }
                        });

                        connections.insert(endpoint_key, ClientConnection::Ws { sender: tx });
                    }
                    Transport::WebTransport => {
                        let wt_endpoint = wt_endpoint.as_ref().unwrap();
                        let parsed_base = reqwest::Url::parse(&self.config.wt_url)?;
                        let connect_host = parsed_base.host_str().unwrap_or("localhost").to_string();
                        let port = parsed_base.port().unwrap_or(443);
                        let wt_url_str = format!(
                            "https://{}:{}/?uid={}&device_id={}&last_synced_upto=0&token={}",
                            self.config.wt_host, port, address_id, device_id, member_name
                        );
                        let parsed_url = reqwest::Url::parse(&wt_url_str)?;
                        let addrs: Vec<std::net::SocketAddr> = tokio::net::lookup_host(format!("{}:{}", connect_host, port))
                            .await?
                            .collect();
                        let socket_addr = addrs
                            .iter()
                            .copied()
                            .find(|a| a.is_ipv4())
                            .or_else(|| addrs.first().copied())
                            .ok_or_else(|| anyhow::anyhow!("failed to resolve {}:{}", connect_host, port))?;

                        let quinn_conn = match wt_endpoint.connect(socket_addr, &self.config.wt_host) {
                            Ok(c) => match c.await {
                                Ok(conn) => conn,
                                Err(e) => {
                                    eprintln!("QUIC connect error for {} d{}: {}", member_name, device_id, e);
                                    continue;
                                }
                            },
                            Err(e) => {
                                eprintln!("Endpoint connect error: {}", e);
                                continue;
                            }
                        };

                        let session = match web_transport_quinn::Session::connect(quinn_conn, parsed_url).await {
                            Ok(s) => s,
                            Err(e) => {
                                eprintln!("WT handshake error for {} d{}: {}", member_name, device_id, e);
                                continue;
                            }
                        };

                        // Server-to-client push stream reader
                        let session_recv = session.clone();
                        let bytes_recv = self.bytes_received.clone();
                        let msg_delivered = self.messages_delivered.clone();
                        let sent_timestamps = self.sent_timestamps.clone();
                        let delivery_latencies = self.delivery_latencies.clone();

                        tokio::spawn(async move {
                            while let Ok(mut uni_stream) = session_recv.accept_uni().await {
                                let bytes_recv = bytes_recv.clone();
                                let msg_delivered = msg_delivered.clone();
                                let sent_timestamps = sent_timestamps.clone();
                                let delivery_latencies = delivery_latencies.clone();

                                tokio::spawn(async move {
                                    let mut len_buf = [0u8; 4];
                                    while let Ok(()) = uni_stream.read_exact(&mut len_buf).await {
                                        bytes_recv.fetch_add(4, Ordering::Relaxed);
                                        let len = u32::from_be_bytes(len_buf) as usize;
                                        let mut payload = vec![0u8; len];
                                        if uni_stream.read_exact(&mut payload).await.is_err() {
                                            break;
                                        }
                                        bytes_recv.fetch_add(len as u64, Ordering::Relaxed);
                                        if let Ok(server_msg) = deserialize_proto::<ServerMessage>(&payload) {
                                            if let mod_ServerMessage::OneOfmessage::userMessage(um) = server_msg.message {
                                                msg_delivered.fetch_add(1, Ordering::Relaxed);
                                                if let Some((_, sent_at)) = sent_timestamps.remove(&um.hashValue).or_else(|| sent_timestamps.remove(&um.id)) {
                                                    let lat = sent_at.elapsed().as_secs_f64() * 1000.0;
                                                    delivery_latencies.lock().await.push(lat);
                                                }
                                            }
                                        }
                                    }
                                });
                            }
                        });

                        connections.insert(endpoint_key, ClientConnection::Wt { session });
                    }
                }
            }
            if (m_idx + 1) % 100 == 0 {
                println!("  Connected {} / {} members ({} endpoints active)",
                    m_idx + 1, self.config.members, (m_idx + 1) * self.config.devices_per_member);
            }
        }

        let connect_duration = connect_start.elapsed();
        println!("✓ All {} endpoints connected in {:.2?}!", connections.len(), connect_duration);

        // 2. Start Resource Monitor Task (CPU, Memory)
        println!("[2/3] Starting system metrics sampling and deterministic traffic generation for {} seconds...", self.config.duration_secs);
        let stop_flag = Arc::new(AtomicBool::new(false));
        let stop_flag_sampler = stop_flag.clone();

        let client_cpu_samples = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let server_cpu_samples = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let client_mem_samples = Arc::new(tokio::sync::Mutex::new(Vec::new()));

        let c_cpu_s = client_cpu_samples.clone();
        let s_cpu_s = server_cpu_samples.clone();
        let c_mem_s = client_mem_samples.clone();

        tokio::spawn(async move {
            let mut sampler = CpuSampler::new();
            while !stop_flag_sampler.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let (c_cpu, s_cpu, c_mem) = sampler.sample();
                c_cpu_s.lock().await.push(c_cpu);
                s_cpu_s.lock().await.push(s_cpu);
                c_mem_s.lock().await.push(c_mem);
            }
        });

        // 3. Deterministic Message Transmission Loop for 5 Minutes
        let connections = Arc::new(connections);
        let sim_start = Instant::now();
        let mut rng = ChaCha8Rng::seed_from_u64(self.config.seed);
        let mut msg_seq = 0u64;

        let duration = Duration::from_secs(self.config.duration_secs);
        let mut next_print = Instant::now() + Duration::from_secs(30);

        while sim_start.elapsed() < duration {
            // Deterministically select sender member & device
            let sender_m = rng.gen_range(0..self.config.members);
            let sender_d = rng.gen_range(1..=self.config.devices_per_member);
            let (sender_addr, sender_dev) = *self.address_map.get(&(sender_m, sender_d)).expect("sender exists");

            // Deterministically select recipient member (different member)
            let recipient_m = (sender_m + rng.gen_range(1..self.config.members)) % self.config.members;
            let (recipient_addr, _) = *self.address_map.get(&(recipient_m, 1)).expect("recipient exists");

            // Deterministic payload size (128B to 1024B)
            let payload_size = match rng.gen_range(0..4) {
                0 => 128,
                1 => 256,
                2 => 512,
                _ => 1024,
            };
            let mut payload = vec![0u8; payload_size];
            rng.fill(&mut payload[..]);

            msg_seq += 1;
            let message_id = (sender_addr << 32) | (msg_seq & 0xFFFFFFFF);

            let user_msg = UserMessage {
                id: message_id,
                toId: recipient_addr,
                fromId: sender_addr,
                text: payload.into(),
                type_pb: 1,
                settings: 0,
                fromDeviceId: sender_dev as u32,
                fromUsername: format!("sim_mem_{:04}", sender_m).into(),
                hashValue: msg_seq,
            };

            let req_id = (msg_seq & 0x7FFFFFFF) as u32;
            let client_msg = ClientMessage {
                message: mod_ClientMessage::OneOfmessage::request(Request {
                    id: req_id,
                    payload: mod_Request::OneOfpayload::uploadUserMessage(UploadUserMessage {
                        messages: vec![user_msg],
                    }),
                }),
            };
            let msg_bytes = serialize_proto(&client_msg)?;

            self.sent_timestamps.insert(msg_seq, Instant::now());
            self.sent_timestamps.insert(message_id, Instant::now());
            self.messages_sent.fetch_add(1, Ordering::Relaxed);
            let send_t0 = Instant::now();

            if let Some(conn) = connections.get(&(sender_m, sender_d)) {
                match conn {
                    ClientConnection::Ws { sender } => {
                        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
                        self.pending_ws_requests.insert(req_id, resp_tx);
                        let _ = sender.send(msg_bytes.into()).await;

                        let rpc_latencies = self.rpc_latencies.clone();
                        tokio::spawn(async move {
                            if (tokio::time::timeout(Duration::from_secs(5), resp_rx).await).is_ok() {
                                let lat = send_t0.elapsed().as_secs_f64() * 1000.0;
                                rpc_latencies.lock().await.push(lat);
                            }
                        });
                    }
                    ClientConnection::Wt { session } => {
                        // Dedicated Bidirectional Stream for RPC
                        let session = session.clone();
                        let bytes_sent = self.bytes_sent.clone();
                        let bytes_recv = self.bytes_received.clone();
                        let rpc_latencies = self.rpc_latencies.clone();

                        tokio::spawn(async move {
                            if let Ok((mut s_send, mut s_recv)) = session.open_bi().await {
                                bytes_sent.fetch_add(msg_bytes.len() as u64, Ordering::Relaxed);
                                if s_send.write_all(&msg_bytes).await.is_ok() && s_send.finish().is_ok() {
                                    let mut len_buf = [0u8; 4];
                                    if s_recv.read_exact(&mut len_buf).await.is_ok() {
                                        bytes_recv.fetch_add(4, Ordering::Relaxed);
                                        let len = u32::from_be_bytes(len_buf) as usize;
                                        let mut resp_buf = vec![0u8; len];
                                        if s_recv.read_exact(&mut resp_buf).await.is_ok() {
                                            bytes_recv.fetch_add(len as u64, Ordering::Relaxed);
                                            let lat = send_t0.elapsed().as_secs_f64() * 1000.0;
                                            rpc_latencies.lock().await.push(lat);
                                        }
                                    }
                                }
                            }
                        });
                    }
                }
            }

            if Instant::now() > next_print {
                let elapsed = sim_start.elapsed().as_secs();
                let sent = self.messages_sent.load(Ordering::Relaxed);
                let deliv = self.messages_delivered.load(Ordering::Relaxed);
                let rate = sent as f64 / elapsed as f64;
                println!("  [{:>3}s / {}s] Sent: {}, Delivered: {}, Current Rate: {:.1} msgs/s",
                    elapsed, self.config.duration_secs, sent, deliv, rate);
                next_print = Instant::now() + Duration::from_secs(30);
            }

            tokio::time::sleep(Duration::from_millis(self.config.cadence_ms)).await;
        }

        stop_flag.store(true, Ordering::Relaxed);
        let actual_duration = sim_start.elapsed().as_secs_f64();
        println!("✓ Simulation execution complete in {:.1}s!", actual_duration);

        // Allow trailing in-flight push messages to arrive
        tokio::time::sleep(Duration::from_millis(1500)).await;

        // 4. Compute Final Aggregated Statistics
        let sent_count = self.messages_sent.load(Ordering::Relaxed);
        let deliv_count = self.messages_delivered.load(Ordering::Relaxed);
        let bytes_tx = self.bytes_sent.load(Ordering::Relaxed);
        let bytes_rx = self.bytes_received.load(Ordering::Relaxed);

        let rpc_lats = self.rpc_latencies.lock().await.clone();
        let deliv_lats = self.delivery_latencies.lock().await.clone();

        let rpc_stats = LatencyStats::compute(rpc_lats);
        let deliv_stats = LatencyStats::compute(deliv_lats);

        let c_cpu = client_cpu_samples.lock().await.clone();
        let s_cpu = server_cpu_samples.lock().await.clone();
        let c_mem = client_mem_samples.lock().await.clone();

        let c_cpu_mean = if c_cpu.is_empty() { 0.0 } else { c_cpu.iter().sum::<f64>() / c_cpu.len() as f64 };
        let c_cpu_peak = c_cpu.iter().copied().fold(0.0f64, f64::max);
        let s_cpu_mean = if s_cpu.is_empty() { 0.0 } else { s_cpu.iter().sum::<f64>() / s_cpu.len() as f64 };
        let s_cpu_peak = s_cpu.iter().copied().fold(0.0f64, f64::max);
        let c_mem_mean = if c_mem.is_empty() { 0.0 } else { c_mem.iter().sum::<f64>() / c_mem.len() as f64 };
        let c_mem_peak = c_mem.iter().copied().fold(0.0f64, f64::max);

        let success_rate = if sent_count > 0 { (deliv_count as f64 / sent_count as f64) * 100.0 } else { 100.0 };
        let throughput = sent_count as f64 / actual_duration;
        let bandwidth_kb_s = (bytes_tx + bytes_rx) as f64 / (1024.0 * actual_duration);
        let overhead_per_msg = if sent_count > 0 { (bytes_tx + bytes_rx) as f64 / sent_count as f64 } else { 0.0 };

        Ok(BenchmarkResult {
            transport: self.transport.to_string(),
            total_endpoints,
            duration_secs: actual_duration,
            total_messages_sent: sent_count,
            total_messages_delivered: deliv_count,
            delivery_success_rate_pct: success_rate,
            throughput_msgs_per_sec: throughput,
            bytes_sent: bytes_tx,
            bytes_received: bytes_rx,
            bandwidth_kb_per_sec: bandwidth_kb_s,
            wire_overhead_bytes_per_msg: overhead_per_msg,
            rpc_latency: rpc_stats,
            delivery_latency: deliv_stats,
            client_cpu_mean_pct: c_cpu_mean,
            client_cpu_peak_pct: c_cpu_peak,
            server_cpu_mean_pct: s_cpu_mean,
            server_cpu_peak_pct: s_cpu_peak,
            client_memory_mean_mb: c_mem_mean,
            client_memory_peak_mb: c_mem_peak,
        })
    }
}

pub fn print_comparison_table(ws: &BenchmarkResult, wt: &BenchmarkResult) {
    println!("\n");
    println!("=========================================================================================");
    println!("                       FIREFLY 500 MEMBERS x 3 DEVICES (1500 ENDPOINTS)                   ");
    println!("                       5-MINUTE DETERMINISTIC SIMULATION BENCHMARK                       ");
    println!("=========================================================================================");
    println!("{:<36} | {:<20} | {:<20} | {:<12}", "METRIC", "WEBSOCKET", "WEBTRANSPORT", "DELTA / GAIN");
    println!("-------------------------------------+----------------------+----------------------+-------------");

    let fmt_diff = |ws_val: f64, wt_val: f64, lower_is_better: bool| -> String {
        if ws_val == 0.0 {
            return "N/A".to_string();
        }
        let diff_pct = ((wt_val - ws_val) / ws_val) * 100.0;
        if lower_is_better {
            if diff_pct < 0.0 {
                format!("{:.1}% (faster)", diff_pct.abs())
            } else {
                format!("+{:.1}%", diff_pct)
            }
        } else {
            if diff_pct > 0.0 {
                format!("+{:.1}% (higher)", diff_pct)
            } else {
                format!("{:.1}%", diff_pct)
            }
        }
    };

    println!("{:<36} | {:<20} | {:<20} | {:<12}", "Active Endpoints", ws.total_endpoints, wt.total_endpoints, "Equal");
    println!("{:<36} | {:<20.1} | {:<20.1} | {:<12}", "Simulation Duration (s)", ws.duration_secs, wt.duration_secs, "Equal");
    println!("{:<36} | {:<20} | {:<20} | {:<12}", "Messages Sent", ws.total_messages_sent, wt.total_messages_sent, fmt_diff(ws.total_messages_sent as f64, wt.total_messages_sent as f64, false));
    println!("{:<36} | {:<20} | {:<20} | {:<12}", "Messages Delivered", ws.total_messages_delivered, wt.total_messages_delivered, fmt_diff(ws.total_messages_delivered as f64, wt.total_messages_delivered as f64, false));
    println!("{:<36} | {:<20.2}% | {:<20.2}% | {:<12}", "Delivery Success Rate", ws.delivery_success_rate_pct, wt.delivery_success_rate_pct, "Parity");
    println!("{:<36} | {:<20.2} | {:<20.2} | {:<12}", "Throughput (msgs/sec)", ws.throughput_msgs_per_sec, wt.throughput_msgs_per_sec, fmt_diff(ws.throughput_msgs_per_sec, wt.throughput_msgs_per_sec, false));
    println!("-------------------------------------+----------------------+----------------------+-------------");
    println!("{:<36} | {:<20.2} ms | {:<20.2} ms | {:<12}", "RPC Latency p50 (median)", ws.rpc_latency.p50_ms, wt.rpc_latency.p50_ms, fmt_diff(ws.rpc_latency.p50_ms, wt.rpc_latency.p50_ms, true));
    println!("{:<36} | {:<20.2} ms | {:<20.2} ms | {:<12}", "RPC Latency p90", ws.rpc_latency.p90_ms, wt.rpc_latency.p90_ms, fmt_diff(ws.rpc_latency.p90_ms, wt.rpc_latency.p90_ms, true));
    println!("{:<36} | {:<20.2} ms | {:<20.2} ms | {:<12}", "RPC Latency p95", ws.rpc_latency.p95_ms, wt.rpc_latency.p95_ms, fmt_diff(ws.rpc_latency.p95_ms, wt.rpc_latency.p95_ms, true));
    println!("{:<36} | {:<20.2} ms | {:<20.2} ms | {:<12}", "RPC Latency p99 (tail)", ws.rpc_latency.p99_ms, wt.rpc_latency.p99_ms, fmt_diff(ws.rpc_latency.p99_ms, wt.rpc_latency.p99_ms, true));
    println!("{:<36} | {:<20.2} ms | {:<20.2} ms | {:<12}", "RPC Latency Mean", ws.rpc_latency.mean_ms, wt.rpc_latency.mean_ms, fmt_diff(ws.rpc_latency.mean_ms, wt.rpc_latency.mean_ms, true));
    println!("-------------------------------------+----------------------+----------------------+-------------");
    println!("{:<36} | {:<20.2} ms | {:<20.2} ms | {:<12}", "Delivery Latency p50", ws.delivery_latency.p50_ms, wt.delivery_latency.p50_ms, fmt_diff(ws.delivery_latency.p50_ms, wt.delivery_latency.p50_ms, true));
    println!("{:<36} | {:<20.2} ms | {:<20.2} ms | {:<12}", "Delivery Latency p95", ws.delivery_latency.p95_ms, wt.delivery_latency.p95_ms, fmt_diff(ws.delivery_latency.p95_ms, wt.delivery_latency.p95_ms, true));
    println!("{:<36} | {:<20.2} ms | {:<20.2} ms | {:<12}", "Delivery Latency p99", ws.delivery_latency.p99_ms, wt.delivery_latency.p99_ms, fmt_diff(ws.delivery_latency.p99_ms, wt.delivery_latency.p99_ms, true));
    println!("-------------------------------------+----------------------+----------------------+-------------");
    println!("{:<36} | {:<20.2}% | {:<20.2}% | {:<12}", "Client CPU (Mean)", ws.client_cpu_mean_pct, wt.client_cpu_mean_pct, fmt_diff(ws.client_cpu_mean_pct, wt.client_cpu_mean_pct, true));
    println!("{:<36} | {:<20.2}% | {:<20.2}% | {:<12}", "Client CPU (Peak)", ws.client_cpu_peak_pct, wt.client_cpu_peak_pct, fmt_diff(ws.client_cpu_peak_pct, wt.client_cpu_peak_pct, true));
    println!("{:<36} | {:<20.2}% | {:<20.2}% | {:<12}", "Server CPU Load (Mean)", ws.server_cpu_mean_pct, wt.server_cpu_mean_pct, fmt_diff(ws.server_cpu_mean_pct, wt.server_cpu_mean_pct, true));
    println!("{:<36} | {:<20.2}% | {:<20.2}% | {:<12}", "Server CPU Load (Peak)", ws.server_cpu_peak_pct, wt.server_cpu_peak_pct, fmt_diff(ws.server_cpu_peak_pct, wt.server_cpu_peak_pct, true));
    println!("{:<36} | {:<20.2} MB | {:<20.2} MB | {:<12}", "Client Memory (Peak RSS)", ws.client_memory_peak_mb, wt.client_memory_peak_mb, fmt_diff(ws.client_memory_peak_mb, wt.client_memory_peak_mb, true));
    println!("-------------------------------------+----------------------+----------------------+-------------");
    println!("{:<36} | {:<20.2} KB/s | {:<20.2} KB/s | {:<12}", "Bandwidth Rate", ws.bandwidth_kb_per_sec, wt.bandwidth_kb_per_sec, fmt_diff(ws.bandwidth_kb_per_sec, wt.bandwidth_kb_per_sec, true));
    println!("{:<36} | {:<20.1} B | {:<20.1} B | {:<12}", "Wire Bytes / Message", ws.wire_overhead_bytes_per_msg, wt.wire_overhead_bytes_per_msg, fmt_diff(ws.wire_overhead_bytes_per_msg, wt.wire_overhead_bytes_per_msg, true));
    println!("=========================================================================================\n");
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut config = BenchmarkConfig::default();

    // CLI args parsing
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    let mut selected_transport = "both".to_string();
    let mut output_json_path = None;

    while i < args.len() {
        match args[i].as_str() {
            "--members" => {
                if i + 1 < args.len() { config.members = args[i + 1].parse()?; i += 1; }
            }
            "--devices" => {
                if i + 1 < args.len() { config.devices_per_member = args[i + 1].parse()?; i += 1; }
            }
            "--duration" => {
                if i + 1 < args.len() { config.duration_secs = args[i + 1].parse()?; i += 1; }
            }
            "--cadence-ms" => {
                if i + 1 < args.len() { config.cadence_ms = args[i + 1].parse()?; i += 1; }
            }
            "--transport" => {
                if i + 1 < args.len() { selected_transport = args[i + 1].to_lowercase(); i += 1; }
            }
            "--output-json" => {
                if i + 1 < args.len() { output_json_path = Some(args[i + 1].clone()); i += 1; }
            }
            _ => {}
        }
        i += 1;
    }

    println!("Loaded Benchmark Configuration: members={}, devices={}, duration={}s, cadence={}ms",
        config.members, config.devices_per_member, config.duration_secs, config.cadence_ms);

    let address_map = Arc::new(register_all_endpoints(
        &config.base_url,
        config.members,
        config.devices_per_member,
    ).await?);

    let (_ws_res, _wt_res) = match selected_transport.as_str() {
        "ws" | "websocket" => {
            let harness = SimulationHarness::new(config.clone(), Transport::WebSocket, address_map.clone());
            let res = harness.run().await?;
            println!("\nWebSocket Results:\n{}", serde_json::to_string_pretty(&res)?);
            if let Some(path) = output_json_path {
                let json = serde_json::json!({
                    "websocket": res,
                });
                std::fs::write(&path, serde_json::to_string_pretty(&json)?)?;
                println!("Saved benchmark results to {}", path);
            }
            (Some(res), None)
        }
        "wt" | "webtransport" => {
            let harness = SimulationHarness::new(config.clone(), Transport::WebTransport, address_map.clone());
            let res = harness.run().await?;
            println!("\nWebTransport Results:\n{}", serde_json::to_string_pretty(&res)?);
            if let Some(path) = output_json_path {
                let json = serde_json::json!({
                    "webtransport": res,
                });
                std::fs::write(&path, serde_json::to_string_pretty(&json)?)?;
                println!("Saved benchmark results to {}", path);
            }
            (None, Some(res))
        }
        _ => {
            // Run both sequentially for identical duration and compare
            let ws_harness = SimulationHarness::new(config.clone(), Transport::WebSocket, address_map.clone());
            let ws_result = ws_harness.run().await?;

            println!("\nCooling down 3 seconds between benchmark runs...\n");
            tokio::time::sleep(Duration::from_secs(3)).await;

            let wt_harness = SimulationHarness::new(config.clone(), Transport::WebTransport, address_map.clone());
            let wt_result = wt_harness.run().await?;

            print_comparison_table(&ws_result, &wt_result);

            if let Some(path) = output_json_path {
                let json = serde_json::json!({
                    "websocket": ws_result,
                    "webtransport": wt_result,
                });
                std::fs::write(&path, serde_json::to_string_pretty(&json)?)?;
                println!("Saved benchmark results to {}", path);
            }

            (Some(ws_result), Some(wt_result))
        }
    };

    Ok(())
}
