//! Phase 4 gate, live side: the dashboard's websocket streams snapshots, controls sent over it
//! take effect, and the run's log, with those live changes in it, replays exactly.

use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn next_json(ws: &mut Ws) -> Value {
    loop {
        let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("a message within 5 s")
            .expect("stream open")
            .expect("valid frame");
        if let Message::Text(t) = msg {
            return serde_json::from_str(t.as_str()).expect("json");
        }
    }
}

/// Read snapshots until `pred` holds (or fail after `secs`).
async fn until(ws: &mut Ws, secs: u64, what: &str, pred: impl Fn(&Value) -> bool) -> Value {
    let start = Instant::now();
    loop {
        let v = next_json(ws).await;
        if v["type"] == "snapshot" && pred(&v) {
            return v;
        }
        assert!(
            start.elapsed() < Duration::from_secs(secs),
            "timed out waiting for {what}"
        );
    }
}

async fn send(ws: &mut Ws, v: Value) {
    ws.send(Message::Text(v.to_string().into())).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshots_stream_controls_take_effect_and_the_log_replays() {
    let out = std::env::temp_dir().join(format!("stale-quotes-ws-test-{}", std::process::id()));
    let srv = server::start(server::Options {
        addr: "127.0.0.1:0".parse().unwrap(),
        scenario: "news_jump".into(),
        seed: 3,
        speed_milli: 4_000,
        rule: None,
        block_ms: None,
        out_dir: out.clone(),
    })
    .await
    .unwrap();

    // The page and its assets are served.
    let page = reqwest_like_get(srv.addr, "/").await;
    assert!(page.contains("<title>Stale Quotes</title>"));
    assert!(reqwest_like_get(srv.addr, "/app.js")
        .await
        .contains("new WebSocket"));

    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{}/ws", srv.addr))
        .await
        .unwrap();

    // Snapshots stream at about 30 Hz, with the clock moving.
    let first = until(&mut ws, 5, "a first snapshot", |v| {
        v["run"]["clock_s"].as_f64() > Some(0.0)
    })
    .await;
    assert_eq!(first["run"]["scenario"], "news_jump");
    assert_eq!(first["run"]["rule"], "fcfs");
    let t0 = Instant::now();
    let mut n = 0;
    let mut last_clock = first["run"]["clock_s"].as_f64().unwrap();
    while t0.elapsed() < Duration::from_secs(1) {
        let v = next_json(&mut ws).await;
        let c = v["run"]["clock_s"].as_f64().unwrap();
        assert!(c >= last_clock, "clock went back");
        last_clock = c;
        n += 1;
    }
    assert!((15..=45).contains(&n), "{n} snapshots in 1 s");

    // Controls over the websocket take effect, each one logged as a Config record.
    send(&mut ws, json!({"cmd": "rule", "value": "pga"})).await;
    until(&mut ws, 5, "rule = pga", |v| v["run"]["rule"] == "pga").await;
    send(&mut ws, json!({"cmd": "block_ms", "value": 250})).await;
    send(&mut ws, json!({"cmd": "load", "value": 10})).await;
    let v = until(&mut ws, 5, "B and load", |v| {
        v["run"]["block_ms"] == 250 && v["run"]["load"] == 10
    })
    .await;
    assert_eq!(v["engine"]["configs"], 3);
    // PGA is live: snipers now bid on stale quotes, so the chain earns fees.
    send(&mut ws, json!({"cmd": "speed", "value": 20})).await;
    until(&mut ws, 20, "chain fees", |v| {
        v["fees"]["chain"].as_f64() > Some(0.0)
    })
    .await;
    // The standby replays the same records and agrees with the primary.
    let v = until(&mut ws, 5, "standby in sync", |v| {
        v["engine"]["standby"]["in_sync"] == true
    })
    .await;
    assert_eq!(v["pnl_sum"].as_f64(), Some(0.0));

    // A bad command gets an error back, and changes nothing.
    send(&mut ws, json!({"cmd": "rule", "value": "fifo"})).await;
    let err = loop {
        let v = next_json(&mut ws).await;
        if v["type"] == "error" {
            break v;
        }
    };
    assert!(err["error"].as_str().unwrap().contains("fifo"));

    // Stop: the log is written, re-simulated from the log alone, and replays exactly.
    send(&mut ws, json!({"cmd": "stop"})).await;
    let v = until(&mut ws, 20, "the log's replay check", |v| {
        v["run"]["stopped"] == true && v["log"]["last_run"] == 1 && !v["log"]["replay"].is_null()
    })
    .await;
    assert_eq!(v["log"]["replay"]["ok"], true, "{}", v["log"]);
    let path = v["log"]["path"].as_str().unwrap().to_string();
    let bytes = std::fs::read(&path).unwrap();
    let hash = sim::replay_check(&bytes).unwrap();
    let file = sim::log::decode(&bytes).unwrap();
    let records: Vec<engine::LogRecord> = file.records.iter().map(Into::into).collect();
    assert_eq!(sim::log::replay_fills_hash(&records), hash);
    // rule, B, load, speed and stop.
    assert_eq!(sim::config_records(&records), 5);
    assert!(hash.starts_with(v["log"]["replay"]["fills_hash"].as_str().unwrap()));

    // Switching scenario starts a fresh run with its own log.
    send(&mut ws, json!({"cmd": "scenario", "value": "monday_open"})).await;
    let v = until(&mut ws, 5, "monday open", |v| {
        v["run"]["scenario"] == "monday_open"
    })
    .await;
    assert_eq!(v["run"]["id"], 2);
    assert!(v["run"]["weekend"].is_array());

    // Skip: a minute before the close, then over the hidden weekend to 30 s before the reopen.
    send(&mut ws, json!({"cmd": "skip"})).await;
    let v = until(&mut ws, 10, "the close", |v| {
        v["run"]["seeking"].is_null() && v["run"]["clock_s"].as_f64() >= Some(3_540.0)
    })
    .await;
    assert!(!v["run"]["hidden"].as_bool().unwrap());
    send(&mut ws, json!({"cmd": "skip"})).await;
    let v = until(&mut ws, 30, "the reopen", |v| {
        v["run"]["seeking"].is_null() && v["run"]["clock_s"].as_f64() >= Some(239_370.0)
    })
    .await;
    assert!(v["run"]["clock_s"].as_f64() < Some(239_400.0));
    assert!(v["run"]["hidden"].as_bool().unwrap());
    let v = until(&mut ws, 10, "the reopen tick", |v| {
        v["run"]["hidden"] == false
    })
    .await;
    assert_eq!(v["engine"]["configs"], 0);

    drop(ws);
    srv.shutdown().await;
    let _ = std::fs::remove_dir_all(&out);
}

/// A tiny HTTP GET, so the test needs no HTTP client crate.
async fn reqwest_like_get(addr: std::net::SocketAddr, path: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(format!("GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf).to_string();
    assert!(
        text.starts_with("HTTP/1.1 200"),
        "{path}: {}",
        &text[..text.len().min(80)]
    );
    text
}
