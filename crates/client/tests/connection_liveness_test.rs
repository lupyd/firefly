//! A connection whose network silently dies (packets dropped, TCP never told) must be noticed
//! and reconnected once the network is back. Requires a firefly-mls server (`FIREFLY_BASE_URL`).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use firefly_client::callbacks::FireflyWsClientCallback;
use firefly_client::db::{group_messages::GroupMessage, messages::UserMessage};
use firefly_client::websocket::{ConnectionState, FfiFireflyWsClient};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

struct Callbacks {
    name: String,
}

#[async_trait]
impl FireflyWsClientCallback for Callbacks {
    fn name(&self) -> &str {
        &self.name
    }
    async fn get_access_token(&self) -> Option<String> {
        Some(self.name.clone())
    }
    async fn on_message(&self, _message: UserMessage) {}
    async fn on_group_message(&self, _group_message: GroupMessage) {}
}

/// Copies bytes from `from` to `to`, discarding them while `blackhole` is set.
async fn pump(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    blackhole: Arc<AtomicBool>,
) {
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        match from.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                if !blackhole.load(Ordering::Relaxed) && to.write_all(&buf[..n]).await.is_err() {
                    break;
                }
            }
        }
    }
}

/// A TCP proxy to `upstream` that can start dropping all traffic without closing anything.
async fn blackhole_proxy(upstream: String, blackhole: Arc<AtomicBool>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((client, _)) = listener.accept().await {
            if blackhole.load(Ordering::Relaxed) {
                // A dead network never completes new connections either: hold them open silently.
                tokio::spawn(async move {
                    let _held = client;
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                });
                continue;
            }
            let Ok(server) = TcpStream::connect(&upstream).await else { continue };
            let (cr, cw) = client.into_split();
            let (sr, sw) = server.into_split();
            tokio::spawn(pump(cr, sw, blackhole.clone()));
            tokio::spawn(pump(sr, cw, blackhole.clone()));
        }
    });
    port
}

async fn wait_for(client: &FfiFireflyWsClient, timeout: Duration, want: fn(&ConnectionState) -> bool) -> Option<Duration> {
    let started = Instant::now();
    while started.elapsed() < timeout {
        if want(&client.get_connection_state()) {
            return Some(started.elapsed());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    None
}

#[tokio::test]
async fn dead_websocket_is_detected_and_recovered() {
    let _ = std::fs::create_dir_all("/tmp/firefly");
    dotenv::from_filename(".env.test").ok();
    let Ok(base_url) = std::env::var("FIREFLY_BASE_URL") else {
        println!("Skipping integration test: FIREFLY_BASE_URL is not set.");
        return;
    };
    firefly_client::init_logger("/tmp/firefly/test_liveness.log".to_string());
    let upstream = base_url.trim_start_matches("http://").trim_end_matches('/').to_string();

    let blackhole = Arc::new(AtomicBool::new(false));
    let proxy_port = blackhole_proxy(upstream, blackhole.clone()).await;

    let run = rand::random::<u32>();
    let name = format!("liveness_{run}");
    let dir = format!("/tmp/firefly/liveness_{run}");
    std::fs::create_dir_all(&dir).unwrap();
    let client = FfiFireflyWsClient::create(
        base_url.clone(),
        format!("ws://127.0.0.1:{proxy_port}"),
        1000,
        Box::new(Callbacks { name: name.clone() }),
        format!("{dir}/client.db"),
        5000,
    )
    .await
    .expect("create client");
    let init = client.clone();
    tokio::spawn(async move {
        let _ = init.initialize_with_retrying().await;
    });

    assert!(
        wait_for(&client, Duration::from_secs(60), |s| matches!(s, ConnectionState::Connected)).await.is_some(),
        "never connected"
    );

    blackhole.store(true, Ordering::Relaxed);
    let detected = wait_for(&client, Duration::from_secs(90), |s| !matches!(s, ConnectionState::Connected))
        .await
        .expect("a silently dead connection was never noticed");
    println!("dead connection noticed after {detected:?}");

    blackhole.store(false, Ordering::Relaxed);
    let recovered = wait_for(&client, Duration::from_secs(90), |s| matches!(s, ConnectionState::Connected))
        .await
        .expect("did not reconnect after the network came back");
    println!("reconnected {recovered:?} after the network came back");

    client.dispose().await;
    let _ = std::fs::remove_dir_all(&dir);
}
