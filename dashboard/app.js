// Stale Quotes dashboard. One websocket: snapshots (~30 Hz) and system history (2 Hz).
// Setup screen first; then three pages (system, market, profits). Live changes are keys only
// (press ? for the list); the server logs each one, so a run still replays exactly.
"use strict";

const $ = (id) => document.getElementById(id);
const CSS = getComputedStyle(document.documentElement);
const tok = (n) => CSS.getPropertyValue(n).trim();
const C = { ink: tok("--ink"), ink2: tok("--ink-2"), ink3: tok("--ink-3"), line: tok("--line"), lineStrong: tok("--line-strong"), accent: tok("--accent"), accentInk: tok("--accent-ink") };
const MONO = '"Geist Mono", ui-monospace, monospace';

const OPTIONS = {
  mode: [["market"], ["stress"]],
  scenario: [["news_jump", "news jump"], ["calm_weekday", "calm weekday"], ["monday_open", "monday open"]],
  rule: [["fcfs", "first come first served"], ["pga", "priority gas auction"], ["cancel_priority", "cancel priority"]],
  block_ms: [[0, "0 · continuous"], [50, "50 ms"], [250, "250 ms"], [1000, "1 s"]],
  speed: [[0.01, "1/100×"], [0.1, "1/10×"], [1, "1×"], [10, "10×"], [100, "100×"], [1000, "1k×"], [10000, "10k×"]],
  rate: [[10000, "10k / s"], [100000, "100k / s"], [1000000, "1M / s"], [0, "max"]],
};
const RULE_NAMES = Object.fromEntries(OPTIONS.rule);
const SPEEDS = OPTIONS.speed.map((o) => o[0]);
const RATES = [10000, 100000, 1000000, 0];
const LOADS = [1, 10, 100];
const BLOCKS = OPTIONS.block_ms.map((o) => o[0]);

let ws = null;
let snap = null;
let hist = null;
let dirty = false;
let page = ["system", "market", "profits"].includes(location.hash.slice(1)) ? location.hash.slice(1) : "system";
let sel = null; // the setup selection
let lastSpeed = 1;
let lastRunId = -1;
const seenSnipes = new Set();

// ---------- connection ----------

function connect() {
  ws = new WebSocket(`${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/ws`);
  ws.onmessage = (ev) => {
    const m = JSON.parse(ev.data);
    if (m.type === "snapshot") snap = m;
    else if (m.type === "history") hist = m;
    else if (m.type === "error") toast(`server: ${m.error}`);
    dirty = true;
  };
  ws.onclose = () => { toast("connection lost · reconnecting"); setTimeout(connect, 800); };
}

function send(cmd, extra = {}) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ cmd, ...extra }));
}

let toastTimer = null;
function toast(text) {
  const el = $("toast");
  el.textContent = text;
  el.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { el.hidden = true; }, 1600);
}

// ---------- formatting ----------

const pad = (n) => String(Math.floor(n)).padStart(2, "0");
function vclock(t, weekend) {
  if (weekend) {
    // t = 0 is Friday 15:00 ET.
    const x = 15 * 3600 + t, days = ["FRI", "SAT", "SUN", "MON", "TUE"];
    const s = x % 86400;
    return `${days[Math.floor(x / 86400)] || ""} ${pad(s / 3600)}:${pad((s % 3600) / 60)}:${pad(s % 60)}`;
  }
  const h = Math.floor(t / 3600), m = Math.floor((t % 3600) / 60), s = t % 60;
  return `${h ? pad(h) + ":" : ""}${pad(m)}:${s.toFixed(1).padStart(4, "0")}`;
}
function rate(v) {
  if (v >= 999500) return `${(v / 1e6).toFixed(2)}M`;
  if (v >= 1e4) return `${(v / 1e3).toFixed(0)}k`;
  if (v >= 1e3) return `${(v / 1e3).toFixed(1)}k`;
  return v.toFixed(0);
}
function ns(v) {
  if (!v) return "–";
  if (v >= 1e9) return `${(v / 1e9).toFixed(1)} s`;
  if (v >= 1e6) return `${(v / 1e6).toFixed(1)} ms`;
  if (v >= 1e4) return `${(v / 1e3).toFixed(0)} µs`;
  if (v >= 1e3) return `${(v / 1e3).toFixed(1)} µs`;
  return `${Math.round(v)} ns`;
}
function bytes(v) {
  if (v >= 1e9) return `${(v / 1e9).toFixed(2)} GB`;
  if (v >= 1e6) return `${(v / 1e6).toFixed(v >= 1e8 ? 0 : 1)} MB`;
  if (v >= 1e3) return `${(v / 1e3).toFixed(0)} kB`;
  return `${v} B`;
}
function usd(v, signed = true) {
  const a = Math.abs(v);
  const sign = v < -0.005 ? "−" : v > 0.005 && signed ? "+" : "";
  if (a >= 1e6) return `${sign}$${(a / 1e6).toFixed(2)}M`;
  if (a >= 1e4) return `${sign}$${(a / 1e3).toFixed(1)}k`;
  return `${sign}$${a.toFixed(2)}`;
}
const num = (v) => Math.round(v).toLocaleString("en-US");
const speedName = (s) => (s === 0 ? "paused" : s < 1 ? `1/${Math.round(1 / s)}×` : `${s}×`);
const rateName = (r) => (r === 0 ? "max" : `${rate(r)} / s`);

// ---------- setup ----------

function buildSetup() {
  for (const row of document.querySelectorAll(".setup .row[data-field]")) {
    const f = row.dataset.field;
    if (f === "mode") continue;
    const box = row.querySelector(".choices");
    for (const [v, label] of OPTIONS[f]) {
      const b = document.createElement("button");
      b.textContent = label;
      b.dataset.value = String(v);
      box.appendChild(b);
    }
  }
  for (const b of document.querySelectorAll(".setup .row[data-field] button")) {
    b.addEventListener("click", () => {
      const f = b.closest(".row").dataset.field;
      const raw = b.dataset.value;
      sel[f] = f === "mode" || f === "scenario" || f === "rule" ? raw : Number(raw);
      dirty = true;
    });
  }
  $("start").addEventListener("click", start);
}

function start() {
  if (!sel) return;
  sel.seed = Math.max(0, Math.floor(Number($("seed").value) || 0));
  send("start", { ...sel });
  page = "system";
}

function renderSetup(s) {
  if (!sel) {
    sel = { ...s.spec };
    $("seed").value = sel.seed;
  }
  for (const row of document.querySelectorAll(".setup .row[data-field]")) {
    const v = sel[row.dataset.field];
    for (const b of row.querySelectorAll("button")) {
      const on = typeof v === "number" ? Math.abs(Number(b.dataset.value) - v) < 1e-9 : b.dataset.value === v;
      b.classList.toggle("on", on);
    }
  }
  const stress = sel.mode === "stress";
  for (const el of document.querySelectorAll(".market-only")) el.hidden = stress;
  for (const el of document.querySelectorAll(".stress-only")) el.hidden = !stress;
  const last = s.sys.log.last;
  $("last-check").textContent = s.sys.log.checking != null
    ? `checking run ${s.sys.log.checking}'s log…`
    : last ? `last run's log: ${last.ok ? "replays exactly" : "REPLAY FAILED"} · ${num(last.records)} records · ${bytes(last.bytes)}` : "";
}

// ---------- keys ----------

function keys(e) {
  if (e.metaKey || e.ctrlKey || e.altKey) return;
  const k = e.key;
  const help = $("help");
  if (k === "?") { help.hidden = !help.hidden; return; }
  if (k === "Escape") {
    if (!help.hidden) { help.hidden = true; return; }
    if (snap && snap.state !== "setup") { send("stop"); toast("stopped · back to setup"); }
    return;
  }
  if (!snap) return;
  if (snap.state === "setup") {
    if (k === "Enter" && document.activeElement !== $("start")) start();
    return;
  }
  const stress = snap.state === "stress";
  const spec = snap.spec;
  if (k === "1" || k === "2" || k === "3") { page = ["system", "market", "profits"][Number(k) - 1]; dirty = true; return; }
  if (k === "r") { send("restart"); toast("restart"); return; }
  if (stress) {
    if (k === "[" || k === "]") {
      const i = Math.max(0, RATES.indexOf(spec.rate));
      const r = RATES[Math.max(0, Math.min(RATES.length - 1, i + (k === "]" ? 1 : -1)))];
      send("rate", { value: r });
      toast(`target rate → ${rateName(r)}`);
    }
    return;
  }
  const rules = { f: "fcfs", p: "pga", c: "cancel_priority" };
  if (rules[k]) { send("rule", { value: rules[k] }); toast(`rule → ${RULE_NAMES[rules[k]]}`); }
  else if (k === "b") {
    const b = BLOCKS[(BLOCKS.indexOf(spec.block_ms) + 1) % BLOCKS.length];
    send("block_ms", { value: b }); toast(`block B → ${b} ms`);
  } else if (k === "l") {
    const l = LOADS[(LOADS.indexOf(snap.market ? snap.market.load : 1) + 1) % LOADS.length];
    send("load", { value: l }); toast(`load → ×${l}`);
  } else if (k === " ") {
    e.preventDefault();
    if (spec.speed > 0) { lastSpeed = spec.speed; send("speed", { value: 0 }); toast("paused"); }
    else { send("speed", { value: lastSpeed }); toast(`speed → ${speedName(lastSpeed)}`); }
  } else if (k === "[" || k === "]") {
    const cur = spec.speed > 0 ? spec.speed : lastSpeed;
    const i = SPEEDS.findIndex((x) => Math.abs(x - cur) < 1e-9);
    const sp = SPEEDS[Math.max(0, Math.min(SPEEDS.length - 1, (i < 0 ? 2 : i) + (k === "]" ? 1 : -1)))];
    send("speed", { value: sp }); toast(`speed → ${speedName(sp)}`);
  } else if (k === "s") { send("skip"); toast("skipping ahead"); }
}

// ---------- render ----------

function render() {
  requestAnimationFrame(render);
  if (!dirty || !snap) return;
  dirty = false;
  const s = snap;
  const setup = s.state === "setup";
  $("setup").hidden = !setup;
  $("run").hidden = setup;
  if (setup) { renderSetup(s); return; }
  if (s.run.id !== lastRunId) { lastRunId = s.run.id; seenSnipes.clear(); }
  for (const b of document.querySelectorAll(".tabs button")) b.classList.toggle("on", b.dataset.page === page);
  for (const p of ["system", "market", "profits"]) $(`page-${p}`).hidden = p !== page;
  renderBar(s);
  if (page === "system") renderSystem(s);
  if (page === "market") renderMarket(s);
  if (page === "profits") renderProfits(s);
}

function renderBar(s) {
  const sp = s.spec;
  if (s.state === "stress") {
    $("summary").innerHTML = `stress · target ${rateName(s.sys.stress.target)} · synthetic flow · seed ${sp.seed}`;
    const t = s.run.clock_s;
    $("clock").textContent = s.run.done ? `stopped · ${pad(t / 60)}:${pad(t % 60)}` : `${pad(t / 60)}:${pad(t % 60)}`;
  } else {
    const m = s.market;
    $("summary").innerHTML = `market · ${m.scenario.replace("_", " ")} · <span class="rule">${RULE_NAMES[m.rule]}</span> · B ${m.block_ms} ms · ${speedName(m.speed)} · load ×${m.load} · seed ${sp.seed}`;
    const state = s.run.done ? (s.run.stopped ? "stopped" : "complete") : m.seeking != null ? "skipping" : m.hidden ? "nasdaq closed" : "";
    $("clock").textContent = `${state ? state + " · " : ""}${vclock(s.run.clock_s, m.weekend)}`;
  }
}

// ---------- system ----------

function series(name) {
  return hist && hist.s && hist.s[name] ? hist.s[name] : [];
}

function spark(canvas, lines, fmt) {
  const ctx = canvas.getContext("2d");
  const dpr = window.devicePixelRatio || 1;
  const W = canvas.clientWidth, H = canvas.clientHeight;
  if (canvas.width !== Math.round(W * dpr) || canvas.height !== Math.round(H * dpr)) {
    canvas.width = Math.round(W * dpr);
    canvas.height = Math.round(H * dpr);
  }
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, W, H);
  const N = 120;
  let max = 0;
  for (const l of lines) for (const v of l.data) max = Math.max(max, v);
  max = max > 0 ? max * 1.15 : 1;
  const fs = parseFloat(CSS.fontSize) || 16;
  const top = fs * 0.9;
  const x = (i, n) => W - ((n - 1 - i) / (N - 1)) * W;
  const y = (v) => top + (1 - v / max) * (H - top - 2);
  ctx.strokeStyle = C.line;
  ctx.lineWidth = 1;
  ctx.beginPath(); ctx.moveTo(0, H - 1); ctx.lineTo(W, H - 1); ctx.stroke();
  ctx.fillStyle = C.ink3;
  ctx.font = `400 ${fs * 0.58}px ${MONO}`;
  ctx.fillText(fmt(max / 1.15), 0, fs * 0.6);
  ctx.textAlign = "right";
  ctx.fillText("last 60 s", W, fs * 0.6);
  ctx.textAlign = "left";
  for (const l of lines) {
    const d = l.data, n = d.length;
    if (n < 2) continue;
    ctx.beginPath();
    d.forEach((v, i) => (i ? ctx.lineTo(x(i, n), y(v)) : ctx.moveTo(x(i, n), y(v))));
    if (l.fill) {
      ctx.save();
      ctx.lineTo(x(n - 1, n), H - 1); ctx.lineTo(x(0, n), H - 1); ctx.closePath();
      ctx.fillStyle = "rgba(123, 97, 255, 0.10)";
      ctx.fill();
      ctx.restore();
      ctx.beginPath();
      d.forEach((v, i) => (i ? ctx.lineTo(x(i, n), y(v)) : ctx.moveTo(x(i, n), y(v))));
    }
    ctx.setLineDash(l.dash ? [fs * 0.3, fs * 0.25] : []);
    ctx.strokeStyle = l.color;
    ctx.lineWidth = l.width || 1.5;
    ctx.stroke();
    ctx.setLineDash([]);
  }
}

const sparkOf = (id) => document.querySelector(`#${id} .spark`);

function renderSystem(s) {
  const y = s.sys, now = y.now || {};
  const stress = s.state === "stress";
  $("s-rate").textContent = rate(y.msgs_s);
  $("s-peak").textContent = `peak ${rate(y.peak_msgs_s)}`;
  $("s-rate-note").textContent = stress
    ? `${num(y.records)} records · ${num(y.fills)} fills · producer sent ${num(y.stress.produced)}`
    : `market flow: what the agents send at ${speedName(s.spec.speed)} · stress mode loads the engine`;
  spark(sparkOf("c-throughput"), [
    { data: series("msgs"), color: C.ink, fill: true },
    ...(stress ? [{ data: series("target"), color: C.ink3, dash: true, width: 1 }] : []),
  ], rate);

  $("s-lat-kind").textContent = stress ? "latency · producer → matched" : "latency · on_message";
  $("s-lat50").textContent = ns(y.lat.p50);
  $("s-lat99").textContent = ns(y.lat.p99);
  $("s-lat-note").textContent = stress
    ? `engine alone (standby): p50 ${ns(y.lat.on_message_p50)} · p99 ${ns(y.lat.on_message_p99)}`
    : `measured on the standby replay · p99.9 ${ns(y.lat.max)}`;
  spark(sparkOf("c-latency"), [{ data: series("lat99"), color: C.accent }, { data: series("lat50"), color: C.ink }], ns);

  $("s-blk-kind").textContent = stress ? "batch · one drain of the input ring" : "block · match one sealed block";
  $("s-blk50").textContent = ns(y.blk.p50);
  $("s-blk99").textContent = ns(y.blk.p99);
  $("s-blk-note").textContent = y.blk.size ? `${y.blk.size.toFixed(y.blk.size < 10 ? 1 : 0)} messages per ${stress ? "batch" : "block"} on average` : "";
  spark(sparkOf("c-block"), [{ data: series("blk99"), color: C.accent }, { data: series("blk50"), color: C.ink }], ns);

  $("s-cpu").textContent = now.cpu != null ? `${now.cpu.toFixed(0)}%` : "–";
  spark(sparkOf("c-cpu"), [{ data: series("cpu"), color: C.ink, fill: true }, { data: series("cpu_engine"), color: C.accent }], (v) => `${v.toFixed(0)}%`);
  $("s-threads").innerHTML = (now.threads || []).map((t) =>
    `<li><span>${t.name}</span><span class="meter"><i class="${t.pct > 90 ? "hot" : ""}" style="width:${Math.min(100, t.pct)}%"></i></span><span class="v">${t.pct.toFixed(0)}%</span></li>`).join("");

  $("s-rss").textContent = now.rss_mb != null ? `${now.rss_mb.toFixed(0)} MB` : "–";
  $("s-mem-note").textContent = `book: ${num(s.book.levels[0] + s.book.levels[1])} price levels${stress ? ` · ${num(y.stress.resting)} orders resting` : ""}`;
  spark(sparkOf("c-mem"), [{ data: series("rss"), color: C.ink, fill: true }], (v) => `${v.toFixed(0)} MB`);

  const lg = y.log;
  $("s-log").textContent = bytes(lg.bytes);
  const left = now.log_mbps > 0.5 && !s.run.done ? (lg.cap - lg.bytes) / 1e6 / now.log_mbps : null;
  $("s-log-cap").textContent = `${num(lg.records)} records · the run stops at ${bytes(lg.cap)}${left != null ? ` · about ${left >= 120 ? `${(left / 60).toFixed(0)} min` : `${left.toFixed(0)} s`} left at this rate` : ""}`;
  $("s-logw").textContent = now.log_mbps != null ? `${now.log_mbps.toFixed(now.log_mbps < 10 ? 1 : 0)} MB/s` : "–";
  spark(sparkOf("c-log"), [{ data: series("log_mbps"), color: C.ink, fill: true }], (v) => `${v.toFixed(0)} MB/s`);
  const chk = $("s-check");
  const free = now.disk_free ? ` · disk free ${bytes(now.disk_free)}` : "";
  if (lg.capped && !s.run.done) { chk.textContent = `reached the cap: stopping${free}`; chk.className = "note"; }
  else if (lg.checking != null) { chk.textContent = `run ${lg.checking}: reading the log back from disk to replay it…${free}`; chk.className = "note"; }
  else if (lg.last) {
    const l = lg.last;
    chk.textContent = `run ${l.run}: ${l.ok ? "" : "FAILED · "}${num(l.records)} records read back in ${l.secs.toFixed(1)} s, ${l.detail}${free}`;
    chk.className = l.ok ? "note ok" : "note bad";
  } else { chk.textContent = `checked against the primary when the run stops${free}`; chk.className = "note"; }

  const waits = now.waits_s || [0, 0, 0];
  const per = { input: waits[0], log: waits[1], standby: waits[2], ui: now.ui_drops_s || 0 };
  const what = { input: "producer waits", log: "engine waits", standby: "engine waits", ui: "frames dropped" };
  $("s-rings").innerHTML = y.rings.filter((r) => r.name !== "control").map((r) => {
    const fill = (100 * r.peak) / r.cap;
    const w = per[r.name];
    return `<li><span>${r.name}</span><span class="meter"><i class="${fill > 90 ? "hot" : ""}" style="width:${Math.max(0.6, fill)}%"></i></span><span class="v">${w >= 1 ? `${rate(w)} ${what[r.name]}/s` : `${fill.toFixed(0)}%`}</span></li>`;
  }).join("");
  const full = y.rings.filter((r) => r.name !== "control" && r.peak >= r.cap * 0.9).map((r) => r.name);
  $("s-ring-note").textContent = full.length
    ? `${full.join(", ")} full: the thread reading ${full.length > 1 ? "them" : "it"} is the bottleneck`
    : "no ring near full: every reader keeps up";

  $("s-ws").textContent = now.ws_fps != null ? now.ws_fps.toFixed(0) : "–";
  spark(sparkOf("c-ws"), [{ data: series("ws_fps"), color: C.ink, fill: true }], (v) => v.toFixed(0));
  const sb = y.standby;
  const sy = $("s-sync");
  sy.textContent = sb.in_sync == null ? "standby catching up" : sb.in_sync
    ? `standby replica in sync at seq ${num(sb.checked_at)} · fills ${y.fills_hash}`
    : "standby DIVERGED from the primary";
  sy.className = sb.in_sync === false ? "note bad" : sb.in_sync ? "note ok" : "note";
}

// ---------- market ----------

function renderMarket(s) {
  const m = s.market;
  const chartCard = document.querySelector(".chart-card"), racesCard = document.querySelector(".races-card");
  chartCard.hidden = !m;
  racesCard.hidden = !m;
  document.querySelector(".stress-note").hidden = !!m;
  renderBook(s);
  if (!m) return;
  $("m-nasdaq").textContent = m.nasdaq.toFixed(2);
  $("m-mid").textContent = m.mid == null ? "–" : m.mid.toFixed(3);
  const gap = m.mid == null ? null : m.nasdaq - m.mid;
  $("m-gap").textContent = gap == null ? "–" : Math.abs(gap) >= 1 ? usd(gap) : `${gap >= 0 ? "+" : "−"}${Math.abs(gap * 100).toFixed(1)}¢`;
  $("m-gap").classList.toggle("wide", gap != null && Math.abs(gap) >= 0.015);
  $("eclipse").classList.toggle("hidden-px", m.hidden);
  drawChart(m);

  const r = m.races;
  const resolved = r.sniper + r.lp + r.other;
  $("m-races").textContent = `sniper ${r.sniper} · LP ${r.lp}${resolved ? ` · LP wins ${Math.round((100 * r.lp) / resolved)}%` : ""}`;
  $("feed").innerHTML = r.feed.slice(0, 5).map((x) => {
    const key = `${x.t}-${x.stake}`;
    const flash = x.winner === "sniper" && !seenSnipes.has(key);
    if (x.winner === "sniper") seenSnipes.add(key);
    return `<tr class="${flash ? "flash" : ""}"><td>${vclock(x.t, m.weekend)}</td><td class="w-${x.winner}">${x.winner} won</td><td>${usd(x.stake, false)} at stake</td><td>${x.ms.toFixed(1)} ms</td></tr>`;
  }).join("") || `<tr><td>no race has ended yet</td></tr>`;
}

function renderBook(s) {
  const asks = s.book.asks.slice(0, 8), bids = s.book.bids.slice(0, 8);
  const max = Math.max(1, ...asks.map((l) => l[1]), ...bids.map((l) => l[1]));
  const v = s.market ? s.market.nasdaq : null;
  const row = (l, stale) => `<li class="${stale ? "stale" : ""}"><span class="bar" style="width:${(100 * l[1]) / max}%"></span><span class="px">${l[0].toFixed(2)}</span><span class="qty">${l[1]}</span></li>`;
  $("asks").innerHTML = asks.slice().reverse().map((l) => row(l, v != null && l[0] < v - 1e-9)).join("");
  $("bids").innerHTML = bids.map((l) => row(l, v != null && l[0] > v + 1e-9)).join("");
  const bb = bids[0], ba = asks[0];
  $("spread").textContent = bb && ba ? `spread ${Math.round((ba[0] - bb[0]) * 100)}¢` : "one side empty";
  const lv = s.book.levels[0] + s.book.levels[1];
  $("m-levels").textContent = lv > 16 ? `${num(lv)} levels` : "";
}

const chart = $("chart");
const cctx = chart.getContext("2d");

function niceStep(range, n) {
  const raw = range / n, mag = Math.pow(10, Math.floor(Math.log10(raw))), f = raw / mag;
  return (f < 1.5 ? 1 : f < 3 ? 2 : f < 7 ? 5 : 10) * mag;
}

function drawChart(m) {
  const dpr = window.devicePixelRatio || 1;
  const W = chart.clientWidth, H = chart.clientHeight;
  if (chart.width !== Math.round(W * dpr) || chart.height !== Math.round(H * dpr)) { chart.width = Math.round(W * dpr); chart.height = Math.round(H * dpr); }
  const ctx = cctx;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, W, H);
  const c = m.chart, pts = c.points;
  if (!pts.length) return;
  const fs = parseFloat(CSS.fontSize) || 16;
  const padL = 4, padR = fs * 3.6, padT = fs * 0.5, padB = fs * 1.4;
  const tOf = (k) => c.t0 + ((k + 1) * (c.t1 - c.t0)) / c.n;
  const x = (t) => padL + ((t - c.t0) / Math.max(1e-9, c.t1 - c.t0)) * (W - padL - padR);
  let lo = Infinity, hi = -Infinity;
  for (const p of pts) { lo = Math.min(lo, p[0], p[1] ?? p[0]); hi = Math.max(hi, p[0], p[1] ?? p[0]); }
  if (hi - lo < 0.1) { const mid = (hi + lo) / 2; lo = mid - 0.05; hi = mid + 0.05; }
  const py = (hi - lo) * 0.08; lo -= py; hi += py;
  const y = (v) => padT + (1 - (v - lo) / (hi - lo)) * (H - padT - padB);
  const wk = m.weekend;
  const hidden = (t) => !!wk && t >= wk[0] && t < wk[1];

  if (wk) {
    const xa = Math.max(padL, x(wk[0])), xb = Math.min(W - padR, x(wk[1]));
    if (xb > xa) {
      ctx.fillStyle = "rgba(123, 97, 255, 0.07)";
      ctx.fillRect(xa, padT, xb - xa, H - padT - padB);
      ctx.fillStyle = C.ink3;
      ctx.font = `400 ${fs * 0.55}px ${MONO}`;
      ctx.fillText("NASDAQ CLOSED · PRICE HIDDEN", xa + fs * 0.4, padT + fs * 0.9);
    }
  }
  const step = niceStep(hi - lo, 4);
  ctx.font = `400 ${fs * 0.6}px ${MONO}`;
  ctx.textBaseline = "middle";
  for (let v = Math.ceil(lo / step) * step; v <= hi; v += step) {
    ctx.strokeStyle = C.line; ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(padL, y(v)); ctx.lineTo(W - padR, y(v)); ctx.stroke();
    ctx.fillStyle = C.ink3; ctx.fillText(v.toFixed(2), W - padR + fs * 0.4, y(v));
  }
  ctx.textBaseline = "alphabetic";
  ctx.fillStyle = C.ink3;
  const span = c.t1 - c.t0;
  const tStep = niceStep(span, 5);
  for (let t = Math.ceil(c.t0 / tStep) * tStep; t <= c.t1; t += tStep) {
    const label = wk && span > 3600 ? vclock(t, wk).slice(0, 9) : vclock(t, wk).replace(/\.\d$/, "");
    const w = ctx.measureText(label).width;
    ctx.fillText(label, Math.max(padL, Math.min(x(t) - w / 2, W - padR - w)), H - fs * 0.3);
  }

  // Steps when the price changes rarely across the window; a line when it changes often, so a
  // busy window reads as a price chart instead of a comb.
  let changes = 0;
  for (let k = 1; k < pts.length; k++) if (pts[k][0] !== pts[k - 1][0]) changes++;
  const stepped = changes < pts.length / 4;
  const path = (vals, keep) => {
    ctx.beginPath();
    let on = false, prev = null;
    for (let k = 0; k < vals.length; k++) {
      const v = vals[k], t = tOf(k);
      if (v == null || !keep(t)) { on = false; prev = null; continue; }
      if (!on) { ctx.moveTo(x(t), y(v)); on = true; }
      else {
        if (stepped) ctx.lineTo(x(t), y(prev));
        ctx.lineTo(x(t), y(v));
      }
      prev = v;
    }
  };
  // Book mid: dashed.
  ctx.setLineDash([fs * 0.35, fs * 0.28]);
  ctx.strokeStyle = C.ink3; ctx.lineWidth = 1.5;
  path(pts.map((p) => p[1]), () => true);
  ctx.stroke();
  ctx.setLineDash([]);
  // Nasdaq: bright while visible; dark with a violet rim while hidden.
  const nas = pts.map((p) => p[0]);
  ctx.lineJoin = "round";
  ctx.save();
  ctx.shadowColor = C.accent; ctx.shadowBlur = fs * 0.5;
  ctx.strokeStyle = "rgba(214, 205, 255, 0.55)"; ctx.lineWidth = 4;
  path(nas, hidden); ctx.stroke();
  ctx.restore();
  ctx.strokeStyle = "#0c0c10"; ctx.lineWidth = 2;
  path(nas, hidden); ctx.stroke();
  ctx.strokeStyle = C.ink; ctx.lineWidth = 2;
  path(nas, (t) => !hidden(t)); ctx.stroke();

  if (wk && wk[1] > c.t0 && wk[1] < c.t1 && tOf(pts.length - 1) >= wk[1]) {
    const xr = x(wk[1]);
    ctx.strokeStyle = C.accent; ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(xr, padT); ctx.lineTo(xr, H - padB); ctx.stroke();
    const kb = pts.findIndex((_, k) => tOf(k) >= wk[1]);
    // The gap that matters is against the book, which still quotes around Friday's price.
    let kmid = kb - 1;
    while (kmid >= 0 && pts[kmid][1] == null) kmid--;
    if (kb > 0 && kmid >= 0) {
      const gap = pts[kb][0] - pts[kmid][1];
      const text = `REOPEN ${gap >= 0 ? "+" : "−"}$${Math.abs(gap).toFixed(2)} vs the book`;
      ctx.font = `400 ${fs * 0.62}px ${MONO}`;
      const tw = ctx.measureText(text).width;
      const tx = Math.min(xr + fs * 0.4, W - padR - tw - fs * 0.3), ty = padT + fs * 1.9;
      ctx.fillStyle = "rgba(5, 5, 7, 0.85)"; ctx.fillRect(tx - fs * 0.25, ty - fs * 0.7, tw + fs * 0.5, fs * 0.95);
      ctx.fillStyle = C.accentInk; ctx.fillText(text, tx, ty);
    }
  }
  const lk = pts.length - 1, lt = tOf(lk);
  ctx.fillStyle = hidden(lt) ? "#0c0c10" : C.ink;
  ctx.strokeStyle = hidden(lt) ? C.accent : C.ink;
  ctx.lineWidth = 2;
  ctx.beginPath(); ctx.arc(x(lt), y(pts[lk][0]), fs * 0.22, 0, Math.PI * 2); ctx.fill(); ctx.stroke();
}

// ---------- profits ----------

const CLASSES = {
  regular: ["traders", "people trading for their own reasons at random times; they don't watch Nasdaq"],
  lp: ["LP", "the liquidity provider: quotes both sides around the last Nasdaq price it saw"],
  sniper: ["snipers", "fast traders who take LP quotes a price move has left stale"],
  protocol: ["protocol", "the exchange: a 1 bp taker fee on every fill"],
  chain: ["chain", "the sequencer: priority fees, bid only under a priority gas auction"],
};

function renderProfits(s) {
  const p = s.profits;
  if (!p) {
    $("p-story").innerHTML = `<li>Stress mode has no accounts: its flow is synthetic. Start a market run to see who gains and who loses.</li>`;
    $("p-classes").innerHTML = "";
    $("p-sum").textContent = ""; $("p-sum-fill").textContent = "";
    return;
  }
  const lines = [];
  lines.push(p.lp_to_sniper > 0.005
    ? `The LP lost <b class="hl">${usd(p.lp_to_sniper, false)}</b> to snipers, who won <b>${p.sniper_races}</b> races; the LP got out first in <b>${p.lp_races}</b>.`
    : `Snipers have taken nothing from the LP yet (${p.sniper_races} sniper wins, ${p.lp_races} LP wins).`);
  lines.push(`Regular traders lost <b>${usd(p.regular_to_lp, false)}</b> to the LP on their fills (the spread they cross, and any stale price they trade at)${Math.abs(p.regular_to_sniper) > 0.005 ? `, and ${p.regular_to_sniper > 0 ? "lost" : "gained"} <b>${usd(Math.abs(p.regular_to_sniper), false)}</b> trading with snipers` : ""}.`);
  lines.push(p.priority_fees > 0.005
    ? `The chain earned <b>${usd(p.priority_fees, false)}</b> in priority fees: snipers bid ${usd(p.sniper_priority_fees, false)}, the LP ${usd(p.lp_priority_fees, false)}.`
    : `The chain earned nothing: priority fees are only bid under a priority gas auction (press <b>p</b>).`);
  lines.push(`The protocol earned <b>${usd(p.taker_fees, false)}</b> in taker fees.`);
  $("p-story").innerHTML = lines.map((l) => `<li>${l}</li>`).join("");
  const cls = (v) => (v < -0.005 ? "neg" : v > 0.005 ? "pos" : "");
  $("p-classes").innerHTML = p.classes.map((c) => {
    const [name, who] = CLASSES[c.key];
    const fees = c.taker_fees + c.priority_fees;
    return `<tr><td>${name}</td><td>${who}</td><td class="${cls(c.take_at_fill)}">${usd(c.take_at_fill)}</td><td>${fees > 0.005 ? usd(-fees) : "–"}</td><td class="${cls(c.pnl)}">${usd(c.pnl)}</td></tr>`;
  }).join("");
  const fill = p.classes.reduce((a, c) => a + c.take_at_fill, 0);
  $("p-sum-fill").textContent = `${usd(fill)} · zero-sum`;
  $("p-sum").textContent = `${usd(p.sum)} · zero-sum, marked at ${p.mark.toFixed(2)}`;
}

// ---------- start ----------

for (const b of document.querySelectorAll(".tabs button")) b.addEventListener("click", () => { page = b.dataset.page; dirty = true; });
$("help-btn").addEventListener("click", () => { $("help").hidden = !$("help").hidden; });
$("help").addEventListener("click", (e) => { if (e.target === $("help")) $("help").hidden = true; });
document.addEventListener("keydown", keys);
window.addEventListener("resize", () => { dirty = true; });
buildSetup();
connect();
requestAnimationFrame(render);
