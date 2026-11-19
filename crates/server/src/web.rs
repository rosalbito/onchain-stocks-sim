//! The network edge: tokio + axum. Serves the dashboard (embedded static files), pushes the
//! latest snapshot over a websocket, and forwards controls into the engine thread's control ring.
//!
//! Controls are JSON, over the websocket or `POST /api/control`:
//! `{"cmd":"rule","value":"pga"}`, `{"cmd":"block_ms","value":250}`, `{"cmd":"load","value":10}`,
//! `{"cmd":"speed","value":0.01}`, `{"cmd":"pause"}`, `{"cmd":"stop"}`, `{"cmd":"restart"}`,
//! `{"cmd":"skip"}` (fast-forward to just before the next news jump, close or reopen),
//! `{"cmd":"scenario","value":"monday_open"}`, `{"cmd":"seed","value":2}`.

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

use crate::engine_thread::{scenario_index, Command};

#[derive(Clone)]
pub struct AppState {
    pub snapshot: watch::Receiver<Arc<str>>,
    /// The single producer of the control ring, shared by every connection. This is the edge,
    /// not the hot path, so a mutex is fine here.
    pub control: Arc<Mutex<rtrb::Producer<Command>>>,
}

/// Parse one control message.
pub fn parse_command(v: &Value) -> Result<Command, String> {
    let cmd = v
        .get("cmd")
        .and_then(Value::as_str)
        .ok_or("missing \"cmd\"")?;
    let val = v.get("value");
    let num = || {
        val.and_then(Value::as_f64)
            .ok_or(format!("{cmd}: missing number"))
    };
    let int = |lo: u64, hi: u64| -> Result<u64, String> {
        let x = num()?;
        if x.fract() != 0.0 || x < lo as f64 || x > hi as f64 {
            return Err(format!("{cmd}: want a whole number in [{lo}, {hi}]"));
        }
        Ok(x as u64)
    };
    Ok(match cmd {
        "rule" => {
            let name = val.and_then(Value::as_str).ok_or("rule: missing name")?;
            let rule = OrderingRule::ALL
                .into_iter()
                .find(|r| r.name() == name)
                .ok_or(format!("unknown rule {name}"))?;
            Command::Control(Control::Rule(rule))
        }
        "block_ms" => Command::Control(Control::BlockMs(int(0, 10_000)?)),
        "load" => Command::Control(Control::Load(int(1, 1_000)? as u32)),
        "speed" => {
            let x = num()?;
            if !(0.0..=100_000.0).contains(&x) {
                return Err("speed: want 0 to 100000".into());
            }
            Command::Control(Control::Speed((x * 1000.0).round() as u64))
        }
        "pause" => Command::Control(Control::Speed(0)),
        "stop" => Command::Control(Control::Stop),
        "restart" => Command::Restart,
        "skip" => Command::Skip,
        "scenario" => {
            let name = val
                .and_then(Value::as_str)
                .ok_or("scenario: missing name")?;
            Command::Scenario(scenario_index(name).ok_or(format!("unknown scenario {name}"))?)
        }
        "seed" => Command::Seed(int(0, u64::MAX >> 11)?),
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

async fn control(State(state): State<AppState>, Json(v): Json<Value>) -> Response {
    match parse_command(&v).and_then(|c| send(&state, c)) {
        Ok(()) => (StatusCode::OK, "ok\n").into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e}\n")).into_response(),
    }
}

async fn ws(upgrade: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    upgrade.on_upgrade(move |socket| session(socket, state))
}

/// One dashboard connection: push every new snapshot, read controls.
async fn session(mut socket: WebSocket, state: AppState) {
    let mut rx = state.snapshot.clone();
    let first = rx.borrow_and_update().clone();
    if socket
        .send(Message::Text(first.to_string().into()))
        .await
        .is_err()
    {
        return;
    }
    loop {
        tokio::select! {
            changed = rx.changed() => {
                if changed.is_err() {
                    return;
                }
                let s = rx.borrow_and_update().clone();
                if socket.send(Message::Text(s.to_string().into())).await.is_err() {
                    return;
                }
            }
            msg = socket.recv() => {
                let Some(Ok(msg)) = msg else { return };
                if let Message::Text(t) = msg {
                    let reply = serde_json::from_str::<Value>(&t)
                        .map_err(|e| e.to_string())
                        .and_then(|v| parse_command(&v))
                        .and_then(|c| send(&state, c));
                    if let Err(e) = reply {
                        let err = serde_json::json!({ "type": "error", "error": e }).to_string();
                        if socket.send(Message::Text(err.into())).await.is_err() {
                            return;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_every_command() {
        let ok = [
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
            (json!({"cmd":"stop"}), Command::Control(Control::Stop)),
            (json!({"cmd":"restart"}), Command::Restart),
            (json!({"cmd":"skip"}), Command::Skip),
            (
                json!({"cmd":"scenario","value":"monday_open"}),
                Command::Scenario(2),
            ),
            (json!({"cmd":"seed","value":7}), Command::Seed(7)),
        ];
        for (v, want) in ok {
            assert_eq!(parse_command(&v), Ok(want), "{v}");
        }
        for bad in [
            json!({"cmd":"rule","value":"fifo"}),
            json!({"cmd":"block_ms","value":2.5}),
            json!({"cmd":"load","value":0}),
            json!({"cmd":"speed","value":-1}),
            json!({"cmd":"scenario","value":"nope"}),
            json!({"value":1}),
        ] {
            assert!(parse_command(&bad).is_err(), "{bad}");
        }
    }
}
