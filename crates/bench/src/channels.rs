//! The layer comparison (architecture sections 6 and 8): the same engine and the same message
//! stream behind each way of handing messages to the engine thread.
//!
//! - throughput: the producer sends as fast as the channel takes them; messages/s at the consumer
//! - latency: the producer paces sends (one every `GAP_NS`), stamping each with the send time;
//!   latency = (events out at the consumer) - (send time), into an HdrHistogram

use std::collections::VecDeque;
use std::hint::black_box;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use engine::{Engine, LogRecord};
use hdrhistogram::Histogram;

use crate::profiles;

#[derive(Clone, Copy)]
struct Msg {
    sent: Instant,
    rec: LogRecord,
}

/// Paced rate for the latency runs: 100k messages/s, far below every layer's throughput.
const GAP_NS: u64 = 10_000;
const CAP: usize = 4_096;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Layer {
    Direct,
    Rtrb,
    Crossbeam,
    Tokio,
    StdMpsc,
    MutexDeque,
}

impl Layer {
    pub const ALL: [Layer; 6] = [
        Layer::Direct,
        Layer::Rtrb,
        Layer::Crossbeam,
        Layer::Tokio,
        Layer::StdMpsc,
        Layer::MutexDeque,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Layer::Direct => "engine alone (direct call)",
            Layer::Rtrb => "rtrb SPSC ring, busy-spin",
            Layer::Crossbeam => "crossbeam bounded channel",
            Layer::Tokio => "tokio mpsc (multi-thread runtime)",
            Layer::StdMpsc => "std mpsc sync_channel",
            Layer::MutexDeque => "Mutex<VecDeque>, spin",
        }
    }
}

/// The consumer side: match the message, record its latency if paced.
struct Sink {
    engine: Engine,
    hist: Histogram<u64>,
    paced: bool,
}

impl Sink {
    fn new(paced: bool) -> Self {
        Self {
            engine: Engine::new(),
            hist: Histogram::new_with_bounds(1, 1_000_000_000, 3).unwrap(),
            paced,
        }
    }

    #[inline]
    fn take(&mut self, m: Msg) {
        black_box(self.engine.on_message(&m.rec));
        if self.paced {
            self.hist
                .saturating_record(m.sent.elapsed().as_nanos() as u64);
        }
    }
}

/// The producer side: send every record, paced or flat out.
fn produce(records: &[LogRecord], paced: bool, mut send: impl FnMut(Msg)) {
    let start = Instant::now();
    for (i, r) in records.iter().enumerate() {
        if paced {
            let due = start + Duration::from_nanos(i as u64 * GAP_NS);
            while Instant::now() < due {
                std::hint::spin_loop();
            }
        }
        send(Msg {
            sent: Instant::now(),
            rec: *r,
        });
    }
}

/// Run one layer. Returns (elapsed, the consumer's histogram).
fn run(layer: Layer, records: &[LogRecord], paced: bool) -> (Duration, Histogram<u64>) {
    let n = records.len();
    let t0 = Instant::now();
    let sink = match layer {
        Layer::Direct => {
            let mut sink = Sink::new(paced);
            produce(records, paced, |m| sink.take(m));
            sink
        }
        Layer::Rtrb => {
            let (mut tx, mut rx) = rtrb::RingBuffer::<Msg>::new(CAP);
            let consumer = thread::spawn(move || {
                let mut sink = Sink::new(paced);
                let mut got = 0;
                while got < n {
                    // Drain everything available, then spin.
                    match rx.pop() {
                        Ok(m) => {
                            sink.take(m);
                            got += 1;
                        }
                        Err(_) => std::hint::spin_loop(),
                    }
                }
                sink
            });
            produce(records, paced, |mut m| loop {
                match tx.push(m) {
                    Ok(()) => break,
                    Err(rtrb::PushError::Full(back)) => {
                        m = back;
                        std::hint::spin_loop();
                    }
                }
            });
            consumer.join().unwrap()
        }
        Layer::Crossbeam => {
            let (tx, rx) = crossbeam_channel::bounded::<Msg>(CAP);
            let consumer = thread::spawn(move || {
                let mut sink = Sink::new(paced);
                for _ in 0..n {
                    sink.take(rx.recv().unwrap());
                }
                sink
            });
            produce(records, paced, |m| tx.send(m).unwrap());
            consumer.join().unwrap()
        }
        Layer::Tokio => {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .build()
                .unwrap();
            let (tx, mut rx) = tokio::sync::mpsc::channel::<Msg>(CAP);
            let consumer = rt.spawn(async move {
                let mut sink = Sink::new(paced);
                for _ in 0..n {
                    sink.take(rx.recv().await.unwrap());
                }
                sink
            });
            produce(records, paced, |m| tx.blocking_send(m).unwrap());
            rt.block_on(consumer).unwrap()
        }
        Layer::StdMpsc => {
            let (tx, rx) = std::sync::mpsc::sync_channel::<Msg>(CAP);
            let consumer = thread::spawn(move || {
                let mut sink = Sink::new(paced);
                for _ in 0..n {
                    sink.take(rx.recv().unwrap());
                }
                sink
            });
            produce(records, paced, |m| tx.send(m).unwrap());
            consumer.join().unwrap()
        }
        Layer::MutexDeque => {
            let q = Arc::new(Mutex::new(VecDeque::<Msg>::with_capacity(CAP)));
            let qc = q.clone();
            let consumer = thread::spawn(move || {
                let mut sink = Sink::new(paced);
                let mut got = 0;
                while got < n {
                    let m = qc.lock().unwrap().pop_front();
                    match m {
                        Some(m) => {
                            sink.take(m);
                            got += 1;
                        }
                        None => std::hint::spin_loop(),
                    }
                }
                sink
            });
            produce(records, paced, |m| loop {
                let mut g = q.lock().unwrap();
                if g.len() < CAP {
                    g.push_back(m);
                    break;
                }
                drop(g);
                std::hint::spin_loop();
            });
            consumer.join().unwrap()
        }
    };
    (t0.elapsed(), sink.hist)
}

pub struct Row {
    pub layer: Layer,
    pub msgs_per_s: f64,
    pub p50: u64,
    pub p99: u64,
    pub p999: u64,
}

pub fn measure(layer: Layer, stream: &[LogRecord], latency_msgs: usize, budget: Duration) -> Row {
    // Throughput: repeat full passes until the budget is spent; keep the median pass.
    run(layer, &stream[..stream.len().min(20_000)], false); // warm-up
    let mut rates = Vec::new();
    let start = Instant::now();
    while start.elapsed() < budget || rates.len() < 3 {
        let (d, _) = run(layer, stream, false);
        rates.push(stream.len() as f64 / d.as_secs_f64());
    }
    let msgs_per_s = crate::engine_cost::median(&mut rates);
    let (_, h) = run(layer, &stream[..latency_msgs.min(stream.len())], true);
    Row {
        layer,
        msgs_per_s,
        p50: h.value_at_quantile(0.5),
        p99: h.value_at_quantile(0.99),
        p999: h.value_at_quantile(0.999),
    }
}

pub fn table(budget: Duration) {
    let stream = profiles::long_stream();
    let latency_msgs = 200_000;
    println!(
        "### Layer comparison: {} messages of recorded flow (calm weekday x10, 24 h); latency at {} msgs/s paced\n",
        stream.len(),
        1_000_000_000 / GAP_NS
    );
    println!("| layer | msgs/s (flat out) | p50 ns | p99 ns | p99.9 ns |");
    println!("|---|---:|---:|---:|---:|");
    for layer in Layer::ALL {
        let r = measure(layer, &stream, latency_msgs, budget);
        println!(
            "| {} | {:.2}M | {} | {} | {} |",
            r.layer.name(),
            r.msgs_per_s / 1e6,
            r.p50,
            r.p99,
            r.p999
        );
    }
    println!();
}
