//! The network edge: tokio + axum. Serves the dashboard (embedded static files), pushes the
//! latest snapshot (about 30 a second) and system history (twice a second) over a websocket, and
//! forwards commands into the engine thread's control ring.
//!
//! Commands are JSON, over the websocket or `POST /api/control`:
//! `{"cmd":"start","mode":"market","scenario":"news_jump","rule":"fcfs","block_ms":0,"seed":1,"speed":1}`,
//! `{"cmd":"start","mode":"stress","rate":100000,"seed":1}` (rate 0 = as fast as possible),
//! `{"cmd":"stop"}` (back to setup), `{"cmd":"restart"}`, and live changes:
//! `{"cmd":"rule","value":"pga"}`, `{"cmd":"block_ms","value":250}`, `{"cmd":"load","value":10}`,
//! `{"cmd":"speed","value":0.01}`, `{"cmd":"pause"}`, `{"cmd":"skip"}`, `{"cmd":"rate","value":1000000}`.

use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::Value;
use sim::{Control, OrderingRule};
use tokio::sync::watch;

use crate::engine_thread::{scenario_index, Command, StartSpec};
use crate::sysinfo::Telemetry;

#[derive(Clone)]
pub struct AppState {
    pub snapshot: watch::Receiver<Arc<str>>,
    pub history: watch::Receiver<Arc<str>>,
    /// The single producer of the control ring, shared by every connection. This is the edge,
    /// not the hot path, so a mutex is fine here.
    pub control: Arc<Mutex<rtrb::Producer<Command>>>,
    pub tel: Arc<Telemetry>,
}

fn rule(name: &str) -> Result<OrderingRule, String> {
    OrderingRule::ALL
        .into_iter()
        .find(|r| r.name() == name)
        .ok_or(format!("unknown rule {name}"))
}

/// Parse one command.
pub fn parse_command(v: &Value) -> Result<Command, String> {
    let cmd = v
        .get("cmd")
        .and_then(Value::as_str)
        .ok_or("missing \"cmd\"")?;
    let field = |k: &str| v.get(k);
    let num_of = |k: &str| field(k).and_then(Value::as_f64);
    let int_of = |k: &str, lo: u64, hi: u64| -> Result<Option<u64>, String> {
        let Some(x) = num_of(k) else { return Ok(None) };
        if x.fract() != 0.0 || x < lo as f64 || x > hi as f64 {
            return Err(format!("{cmd}: {k} must be a whole number in [{lo}, {hi}]"));
        }
        Ok(Some(x as u64))
    };
    let need = |x: Option<u64>| x.ok_or(format!("{cmd}: missing value"));
    let speed = |x: f64| -> Result<u64, String> {
        if !(0.0..=100_000.0).contains(&x) {
            return Err("speed: want 0 to 100000".into());
        }
        Ok((x * 1000.0).round() as u64)
    };
    Ok(match cmd {
        "start" => {
            let mut spec = StartSpec {
                stress: match field("mode").and_then(Value::as_str).unwrap_or("market") {
                    "market" => false,
                    "stress" => true,
                    m => return Err(format!("unknown mode {m}")),
                },
                ..StartSpec::default()
            };
            if let Some(name) = field("scenario").and_then(Value::as_str) {
                spec.scenario = scenario_index(name).ok_or(format!("unknown scenario {name}"))?;
            }
            if let Some(name) = field("rule").and_then(Value::as_str) {
                spec.rule = rule(name)?;
            }
            if let Some(b) = int_of("block_ms", 0, 10_000)? {
                spec.block_ms = b;
            }
            if let Some(s) = int_of("seed", 0, u64::MAX >> 11)? {
                spec.seed = s;
            }
            if let Some(x) = num_of("speed") {
                spec.speed_milli = speed(x)?;
            }
            if let Some(r) = int_of("rate", 0, 100_000_000)? {
                spec.rate = r;
            }
            if let Some(c) = field("crowd") {
                spec.crowd = c.as_bool().ok_or("start: crowd must be true or false")?;
            }
            Command::Start(spec)
        }
        "stop" => Command::Stop,
        "restart" => Command::Restart,
        "skip" => Command::Skip,
        "rule" => {
            let name = field("value")
                .and_then(Value::as_str)
                .ok_or("rule: missing name")?;
            Command::Control(Control::Rule(rule(name)?))
        }
        "block_ms" => Command::Control(Control::BlockMs(need(int_of("value", 0, 10_000)?)?)),
        "load" => Command::Control(Control::Load(need(int_of("value", 1, 1_000)?)? as u32)),
        "speed" => Command::Control(Control::Speed(speed(
            num_of("value").ok_or("speed: missing number")?,
        )?)),
        "pause" => Command::Control(Control::Speed(0)),
        "rate" => Command::StressRate(need(int_of("value", 0, 100_000_000)?)?),
        other => return Err(format!("unknown command {other}")),
    })
}

fn send(state: &AppState, c: Command) -> Result<(), String> {
    state
        .control
        .lock()
        .unwrap()
        .push(c)
        .map_err(|_| "control ring full".to_string())
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(|| async { asset("index.html") }))
        .route("/app.js", get(|| async { asset("app.js") }))
        .route("/style.css", get(|| async { asset("style.css") }))
        .route("/fonts/geist.woff2", get(|| async { asset("geist.woff2") }))
        .route(
            "/fonts/geist-mono.woff2",
            get(|| async { asset("geist-mono.woff2") }),
        )
        .route("/ws", get(ws))
        .route("/api/snapshot", get(snapshot))
        .route("/api/history", get(history))
        .route("/api/control", post(control))
        .with_state(state)
}

fn asset(name: &str) -> Response {
    let (body, ty): (&'static [u8], &str) = match name {
        "index.html" => (
            include_bytes!("../../../dashboard/index.html"),
            "text/html; charset=utf-8",
        ),
        "app.js" => (
            include_bytes!("../../../dashboard/app.js"),
            "text/javascript; charset=utf-8",
        ),
        "style.css" => (
            include_bytes!("../../../dashboard/style.css"),
            "text/css; charset=utf-8",
        ),
        "geist.woff2" => (
            include_bytes!("../../../dashboard/fonts/geist-latin.woff2"),
            "font/woff2",
        ),
        "geist-mono.woff2" => (
            include_bytes!("../../../dashboard/fonts/geist-mono-latin.woff2"),
            "font/woff2",
        ),
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    (
        [
            (header::CONTENT_TYPE, ty),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        body,
    )
        .into_response()
}

async fn snapshot(State(state): State<AppState>) -> Response {
    let s = state.snapshot.borrow().clone();
    ([(header::CONTENT_TYPE, "application/json")], s.to_string()).into_response()
}

async fn history(State(state): State<AppState>) -> Response {
    let s = state.history.borrow().clone();
    ([(header::CONTENT_TYPE, "application/json")], s.to_string()).into_response()
}

async fn control(State(state): State<AppState>, Json(v): Json<Value>) -> Response {
    match parse_command(&v).and_then(|c| send(&state, c)) {
        Ok(()) => (StatusCode::OK, "ok\n").into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e}\n")).into_response(),
    }
}

async fn ws(upgrade: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    upgrade.on_upgrade(move |socket| session(socket, state))
}

/// One dashboard connection: push every new snapshot and history, read commands.
async fn session(mut socket: WebSocket, state: AppState) {
    let mut snap = state.snapshot.clone();
    let mut hist = state.history.clone();
    let firsts = [
        hist.borrow_and_update().clone(),
        snap.borrow_and_update().clone(),
    ];
    for first in firsts {
        if socket
            .send(Message::Text(first.to_string().into()))
            .await
            .is_err()
        {
            return;
        }
    }
    loop {
        let out = tokio::select! {
            changed = snap.changed() => {
                if changed.is_err() {
                    return;
                }
                snap.borrow_and_update().clone()
            }
            changed = hist.changed() => {
                if changed.is_err() {
                    return;
                }
                hist.borrow_and_update().clone()
            }
            msg = socket.recv() => {
                let Some(Ok(msg)) = msg else { return };
                let Message::Text(t) = msg else { continue };
                let reply = serde_json::from_str::<Value>(&t)
                    .map_err(|e| e.to_string())
                    .and_then(|v| parse_command(&v))
                    .and_then(|c| send(&state, c));
                match reply {
                    Ok(()) => continue,
                    Err(e) => Arc::from(serde_json::json!({ "type": "error", "error": e }).to_string()),
                }
            }
        };
        if socket
            .send(Message::Text(out.to_string().into()))
            .await
            .is_err()
        {
            return;
        }
        state.tel.ws_frames.fetch_add(1, Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_every_command() {
        let market = StartSpec {
            scenario: 2,
            rule: OrderingRule::Pga,
            block_ms: 250,
            seed: 3,
            speed_milli: 10,
            ..StartSpec::default()
        };
        let stress = StartSpec {
            stress: true,
            rate: 0,
            ..StartSpec::default()
        };
        let ok = [
            (
                json!({"cmd":"start","mode":"market","scenario":"monday_open","rule":"pga","block_ms":250,"seed":3,"speed":0.01}),
                Command::Start(market),
            ),
            (
                json!({"cmd":"start","mode":"stress","rate":0}),
                Command::Start(stress),
            ),
            (
                json!({"cmd":"rule","value":"pga"}),
                Command::Control(Control::Rule(OrderingRule::Pga)),
            ),
            (
                json!({"cmd":"block_ms","value":250}),
                Command::Control(Control::BlockMs(250)),
            ),
            (
                json!({"cmd":"load","value":100}),
                Command::Control(Control::Load(100)),
            ),
            (
                json!({"cmd":"speed","value":0.01}),
                Command::Control(Control::Speed(10)),
            ),
            (json!({"cmd":"pause"}), Command::Control(Control::Speed(0))),
            (
                json!({"cmd":"rate","value":1000000}),
                Command::StressRate(1_000_000),
            ),
            (json!({"cmd":"stop"}), Command::Stop),
            (json!({"cmd":"restart"}), Command::Restart),
            (json!({"cmd":"skip"}), Command::Skip),
        ];
        for (v, want) in ok {
            assert_eq!(parse_command(&v), Ok(want), "{v}");
        }
        for bad in [
            json!({"cmd":"rule","value":"fifo"}),
            json!({"cmd":"block_ms","value":2.5}),
            json!({"cmd":"load","value":0}),
            json!({"cmd":"speed","value":-1}),
            json!({"cmd":"start","mode":"chaos"}),
            json!({"cmd":"start","scenario":"nope"}),
            json!({"value":1}),
        ] {
            assert!(parse_command(&bad).is_err(), "{bad}");
        }
    }
}
