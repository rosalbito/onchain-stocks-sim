//! Phase 4 gate, live side, through a real websocket client:
//! - the server opens on the setup screen; a start command starts a market run;
//! - snapshots stream at about 30 Hz and system history twice a second;
//! - live changes take effect and are logged; the stopped run's log re-simulates exactly;
//! - skip lands before the close and the reopen;
//! - a stress run streams to disk, takes a live rate change, and its log replays and
//!   regenerates to the primary's fills hash.

use std::path::PathBuf;
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

/// Read messages until a snapshot satisfies `pred` (or fail after `secs`).
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

async fn server(name: &str) -> (server::Server, Ws, PathBuf) {
    let out = std::env::temp_dir().join(format!("stale-quotes-{name}-{}", std::process::id()));
    let srv = server::start(server::Options {
        addr: "127.0.0.1:0".parse().unwrap(),
        autostart: None,
        out_dir: out.clone(),
        log_cap_bytes: 200_000_000,
    })
    .await
    .unwrap();
    let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{}/ws", srv.addr))
        .await
        .unwrap();
    (srv, ws, out)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn market_run_streams_takes_live_changes_and_its_log_replays() {
    let (srv, mut ws, out) = server("market").await;

    // The page and its assets are served.
    let page = http_get(srv.addr, "/").await;
    assert!(page.contains("<title>Stale Quotes</title>"));
    assert!(http_get(srv.addr, "/app.js")
        .await
        .contains("new WebSocket"));

    // Setup first: no run until Start.
    let v = until(&mut ws, 5, "setup", |v| v["state"] == "setup").await;
    assert_eq!(v["run"]["id"], 0);
    send(
        &mut ws,
        json!({"cmd":"start","mode":"market","scenario":"news_jump","seed":3,"speed":4,"crowd":false}),
    )
    .await;
    let first = until(&mut ws, 5, "a market run", |v| {
        v["state"] == "market" && v["run"]["clock_s"].as_f64() > Some(0.0)
    })
    .await;
    assert_eq!(first["spec"]["rule"], "fcfs");
    assert!(!first["market"]["chart"]["points"]
        .as_array()
        .unwrap()
        .is_empty());

    // Snapshots at about 30 Hz, the clock moving forward; history twice a second.
    let t0 = Instant::now();
    let (mut snaps, mut hists) = (0, 0);
    let mut last_clock = first["run"]["clock_s"].as_f64().unwrap();
    while t0.elapsed() < Duration::from_secs(2) {
        let v = next_json(&mut ws).await;
        match v["type"].as_str() {
            Some("snapshot") => {
                let c = v["run"]["clock_s"].as_f64().unwrap();
                assert!(c >= last_clock, "clock went back");
                last_clock = c;
                snaps += 1;
            }
            Some("history") => {
                assert!(!v["s"]["msgs"].as_array().unwrap().is_empty());
                hists += 1;
            }
            _ => {}
        }
    }
    assert!((30..=90).contains(&snaps), "{snaps} snapshots in 2 s");
    assert!((2..=6).contains(&hists), "{hists} history messages in 2 s");

    // Live changes take effect, each logged as a Config record.
    send(&mut ws, json!({"cmd": "rule", "value": "pga"})).await;
    send(&mut ws, json!({"cmd": "block_ms", "value": 250})).await;
    send(&mut ws, json!({"cmd": "load", "value": 10})).await;
    let v = until(&mut ws, 5, "rule, B and load", |v| {
        v["market"]["rule"] == "pga" && v["spec"]["block_ms"] == 250 && v["market"]["load"] == 10
    })
    .await;
    assert_eq!(v["market"]["counts"]["configs"], 3);
    // PGA is live: snipers bid on stale quotes, so the chain earns priority fees.
    send(&mut ws, json!({"cmd": "speed", "value": 20})).await;
    until(&mut ws, 20, "priority fees", |v| {
        v["profits"]["priority_fees"].as_f64() > Some(0.0)
    })
    .await;
    let v = until(&mut ws, 5, "standby in sync", |v| {
        v["sys"]["standby"]["in_sync"] == true
    })
    .await;
    assert_eq!(v["profits"]["sum"].as_f64(), Some(0.0));
    assert!(v["sys"]["log"]["bytes"].as_u64() > Some(1_000));

    // A bad command gets an error back.
    send(&mut ws, json!({"cmd": "rule", "value": "fifo"})).await;
    let err = loop {
        let v = next_json(&mut ws).await;
        if v["type"] == "error" {
            break v;
        }
    };
    assert!(err["error"].as_str().unwrap().contains("fifo"));

    // Stop: back to setup; the log is read back from disk and re-simulated exactly.
    send(&mut ws, json!({"cmd": "stop"})).await;
    let v = until(&mut ws, 20, "the log check", |v| {
        v["state"] == "setup" && v["sys"]["log"]["last"]["run"] == 1
    })
    .await;
    assert_eq!(v["sys"]["log"]["last"]["ok"], true, "{}", v["sys"]["log"]);
    let journal = PathBuf::from(v["sys"]["log"]["path"].as_str().unwrap());
    let bytes = std::fs::read(journal.with_extension("log")).unwrap();
    let hash = sim::replay_check(&bytes).unwrap();
    let file = sim::log::decode(&bytes).unwrap();
    let records: Vec<engine::LogRecord> = file.records.iter().map(Into::into).collect();
    assert_eq!(sim::log::replay_fills_hash(&records), hash);
    // rule, B, load, speed and stop.
    assert_eq!(sim::config_records(&records), 5);

    // The talk model (no crowd), whose close is at 1 h. Skip: a minute before the close, then over the hidden weekend to 30 s before the reopen.
    send(
        &mut ws,
        json!({"cmd":"start","mode":"market","scenario":"monday_open","speed":10,"crowd":false}),
    )
    .await;
    until(&mut ws, 5, "monday open", |v| {
        v["state"] == "market" && v["run"]["id"] == 2
    })
    .await;
    send(&mut ws, json!({"cmd": "skip"})).await;
    let v = until(&mut ws, 10, "the close", |v| {
        v["market"]["seeking"].is_null() && v["run"]["clock_s"].as_f64() >= Some(3_540.0)
    })
    .await;
    assert_eq!(v["market"]["hidden"], false);
    send(&mut ws, json!({"cmd": "skip"})).await;
    let v = until(&mut ws, 30, "the reopen", |v| {
        v["market"]["seeking"].is_null() && v["run"]["clock_s"].as_f64() >= Some(239_370.0)
    })
    .await;
    assert!(v["run"]["clock_s"].as_f64() < Some(239_400.0));
    assert_eq!(v["market"]["hidden"], true);
    // The chart window follows the speed (10x: 600 s), not the 67 h run.
    let c = &v["market"]["chart"];
    let span = c["t1"].as_f64().unwrap() - c["t0"].as_f64().unwrap();
    assert!((599.0..=601.0).contains(&span), "{span}");
    until(&mut ws, 10, "the reopen tick", |v| {
        v["market"]["hidden"] == false
    })
    .await;

    drop(ws);
    srv.shutdown().await;
    let _ = std::fs::remove_dir_all(&out);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stress_run_goes_to_disk_takes_a_rate_change_and_replays() {
    let (srv, mut ws, out) = server("stress").await;
    until(&mut ws, 5, "setup", |v| v["state"] == "setup").await;
    send(
        &mut ws,
        json!({"cmd":"start","mode":"stress","rate":20000,"seed":5}),
    )
    .await;
    let v = until(&mut ws, 10, "20k msgs/s", |v| {
        v["state"] == "stress" && v["sys"]["msgs_s"].as_f64() > Some(15_000.0)
    })
    .await;
    assert!(v["sys"]["msgs_s"].as_f64() < Some(25_000.0));
    assert_eq!(v["sys"]["stress"]["target"], 20_000);
    send(&mut ws, json!({"cmd": "rate", "value": 200000})).await;
    until(&mut ws, 10, "200k msgs/s", |v| {
        v["sys"]["stress"]["target"] == 200_000 && v["sys"]["msgs_s"].as_f64() > Some(150_000.0)
    })
    .await;
    let v = until(&mut ws, 10, "the log on disk", |v| {
        v["sys"]["log"]["bytes"].as_u64() > Some(1_000_000)
    })
    .await;
    assert!(v["sys"]["lat"]["p99"].as_u64() > Some(0));

    send(&mut ws, json!({"cmd": "stop"})).await;
    let v = until(&mut ws, 60, "the stress log check", |v| {
        v["state"] == "setup" && v["sys"]["log"]["last"]["run"] == 1
    })
    .await;
    let last = &v["sys"]["log"]["last"];
    assert_eq!(last["ok"], true, "{}", v["sys"]["log"]);
    let journal = PathBuf::from(v["sys"]["log"]["path"].as_str().unwrap());
    let (n, hash) = server::verify_stress_journal(&journal).unwrap();
    assert_eq!(n, last["records"].as_u64().unwrap());
    assert!(n > 100_000, "{n}");
    assert!(last["detail"].as_str().unwrap().contains(&hash[..12]));

    // A second stress run replaces the first one's log.
    send(
        &mut ws,
        json!({"cmd":"start","mode":"stress","rate":10000,"seed":6}),
    )
    .await;
    until(&mut ws, 10, "a second stress run", |v| {
        v["state"] == "stress"
            && v["run"]["id"] == 2
            && v["sys"]["log"]["records"].as_u64() > Some(0)
    })
    .await;
    // The old log goes (on a background thread); the new run's log stays.
    let t = Instant::now();
    while journal.exists() && t.elapsed() < Duration::from_secs(5) {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(!journal.exists());
    let v = until(&mut ws, 5, "the second log", |v| v["run"]["id"] == 2).await;
    let second = PathBuf::from(v["sys"]["log"]["path"].as_str().unwrap());
    assert_ne!(second, journal);
    assert!(second.exists(), "{}", second.display());

    drop(ws);
    srv.shutdown().await;
    let _ = std::fs::remove_dir_all(&out);
}

/// A tiny HTTP GET, so the test needs no HTTP client crate.
async fn http_get(addr: std::net::SocketAddr, path: &str) -> String {
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
