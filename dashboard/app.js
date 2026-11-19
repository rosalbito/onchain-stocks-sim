// Stale Quotes dashboard: one websocket, the latest snapshot rendered on each animation frame.
// Controls go back over the same socket; the server logs each one as a Config record.
"use strict";

const $ = (id) => document.getElementById(id);
const CSS = getComputedStyle(document.documentElement);
const C = {
  ink: CSS.getPropertyValue("--ink").trim(),
  ink2: CSS.getPropertyValue("--ink-2").trim(),
  ink3: CSS.getPropertyValue("--ink-3").trim(),
  line: CSS.getPropertyValue("--line").trim(),
  lineStrong: CSS.getPropertyValue("--line-strong").trim(),
  accent: CSS.getPropertyValue("--accent").trim(),
};
const MONO = '"Geist Mono", ui-monospace, monospace';

const CONTROLS = {
  scenario: [["news_jump", "news jump", "n"], ["calm_weekday", "calm weekday", "w"], ["monday_open", "monday open", "m"]],
  rule: [["fcfs", "fcfs", "f"], ["pga", "pga", "p"], ["cancel_priority", "cancel first", "c"]],
  block_ms: [[0, "0"], [50, "50 ms"], [250, "250 ms"], [1000, "1 s"]],
  speed: [[0, "pause", " "], [0.01, "1/100"], [0.1, "1/10"], [1, "1×"], [10, "10×"], [100, "100×"], [1000, "1k×"], [10000, "10k×"]],
  load: [[1, "×1"], [10, "×10"], [100, "×100"]],
};
const SPEEDS = CONTROLS.speed.map((s) => s[0]).filter((s) => s > 0);

let ws = null;
let latest = null;
let dirty = false;
let lastSpeed = 1;
const seenSniperWins = new Set();
let feedRun = null;
let pnlScale = { run: null, max: 1 };

// ---------- connection ----------

function connect() {
  const url = `${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/ws`;
  ws = new WebSocket(url);
  ws.onopen = () => setConn("live", false);
  ws.onmessage = (ev) => {
    const msg = JSON.parse(ev.data);
    if (msg.type === "snapshot") {
      latest = msg;
      dirty = true;
    } else if (msg.type === "error") {
      console.warn("server:", msg.error);
    }
  };
  ws.onclose = () => {
    setConn("reconnecting", true);
    setTimeout(connect, 800);
  };
}

function setConn(text, down) {
  const el = $("conn");
  el.textContent = text;
  el.classList.toggle("down", down);
}

function send(cmd, value) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(value === undefined ? { cmd } : { cmd, value }));
  }
}

// ---------- controls ----------

function buildControls() {
  for (const group of document.querySelectorAll(".group[data-cmd]")) {
    const cmd = group.dataset.cmd;
    for (const [value, text, key] of CONTROLS[cmd]) {
      const b = document.createElement("button");
      b.textContent = text;
      b.dataset.value = String(value);
      if (key) b.title = key === " " ? "space" : key;
      b.addEventListener("click", () => send(cmd, value));
      group.appendChild(b);
    }
  }
  $("restart").addEventListener("click", () => send("restart"));
  $("skip").addEventListener("click", () => send("skip"));
  document.addEventListener("keydown", (e) => {
    if (e.metaKey || e.ctrlKey || e.altKey) return;
    const k = e.key.toLowerCase();
    for (const cmd of ["scenario", "rule"]) {
      const hit = CONTROLS[cmd].find((c) => c[2] === k);
      if (hit) return send(cmd, hit[0]);
    }
    const speed = latest ? latest.run.speed : 1;
    if (k === " ") {
      e.preventDefault();
      if (speed > 0) { lastSpeed = speed; send("speed", 0); } else send("speed", lastSpeed);
    } else if (k === "[" || k === "]") {
      const i = SPEEDS.findIndex((s) => Math.abs(s - speed) < 1e-9);
      const j = Math.max(0, Math.min(SPEEDS.length - 1, (i < 0 ? 2 : i) + (k === "]" ? 1 : -1)));
      send("speed", SPEEDS[j]);
    } else if (k === "r") {
      send("restart");
    } else if (k === "s") {
      send("skip");
    } else if (k === "l") {
      const loads = CONTROLS.load.map((l) => l[0]);
      const cur = latest ? latest.run.load : 1;
      send("load", loads[(loads.indexOf(cur) + 1) % loads.length]);
    } else if (k === "b") {
      const bs = CONTROLS.block_ms.map((l) => l[0]);
      const cur = latest ? latest.run.block_ms : 0;
      send("block_ms", bs[(bs.indexOf(cur) + 1) % bs.length]);
    }
  });
}

function markControls(run) {
  const cur = { scenario: run.scenario, rule: run.rule, block_ms: run.block_ms, speed: run.speed, load: run.load };
  for (const group of document.querySelectorAll(".group[data-cmd]")) {
    const v = cur[group.dataset.cmd];
    for (const b of group.querySelectorAll("button")) {
      const bv = b.dataset.value;
      const on = typeof v === "number" ? Math.abs(Number(bv) - v) < 1e-9 : bv === String(v);
      b.classList.toggle("on", on);
    }
  }
}

// ---------- formatting ----------

const RULE_NAMES = { fcfs: "first come, first served", pga: "priority gas auction", cancel_priority: "cancel priority" };
const DAYS = ["FRI", "SAT", "SUN", "MON", "TUE"];

function clock(t, run) {
  if (run.weekend) {
    // t = 0 is Friday 15:00 ET.
    const x = 15 * 3600 + t;
    const day = DAYS[Math.floor(x / 86400)] || "";
    const s = x % 86400;
    return `${day} ${pad(Math.floor(s / 3600))}:${pad(Math.floor((s % 3600) / 60))}:${pad(Math.floor(s % 60))}`;
  }
  const h = Math.floor(t / 3600), m = Math.floor((t % 3600) / 60), s = t % 60;
  return `${h > 0 ? pad(h) + ":" : ""}${pad(m)}:${s.toFixed(3).padStart(6, "0")}`;
}
const pad = (n) => String(n).padStart(2, "0");

function usd(v, digits = 2, signed = true) {
  const a = Math.abs(v);
  const sign = v < -0.0005 ? "−" : v > 0.0005 && signed ? "+" : "";
  if (a >= 1e6) return `${sign}$${(a / 1e6).toFixed(2)}M`;
  if (a >= 1e4) return `${sign}$${(a / 1e3).toFixed(1)}k`;
  return `${sign}$${a.toFixed(digits)}`;
}

function rate(v) {
  if (v >= 1e6) return `${(v / 1e6).toFixed(2)}M`;
  if (v >= 1e4) return `${(v / 1e3).toFixed(1)}k`;
  return v.toFixed(0);
}

function ns(v) {
  if (!v) return "–";
  if (v >= 1e6) return `${(v / 1e6).toFixed(1)} ms`;
  if (v >= 1e3) return `${(v / 1e3).toFixed(1)} µs`;
  return `${v} ns`;
}

// ---------- render ----------

function render() {
  requestAnimationFrame(render);
  if (!dirty || !latest) return;
  dirty = false;
  const s = latest;
  const run = s.run;

  $("st-scenario").textContent = run.scenario.replace("_", " ");
  $("st-rule").textContent = RULE_NAMES[run.rule] || run.rule;
  $("st-block").textContent = run.block_ms === 0 ? "0 (continuous)" : run.block_ms >= 1000 ? `${run.block_ms / 1000} s` : `${run.block_ms} ms`;
  $("st-speed").textContent = run.speed === 0 ? "paused" : run.speed < 1 ? `1/${Math.round(1 / run.speed)}×` : `${run.speed}×`;
  $("st-load").textContent = `×${run.load}`;
  $("st-clock").textContent = clock(run.clock_s, run);
  $("st-clock-label").textContent = run.done ? (run.stopped ? "stopped" : "run complete") : run.seeking != null ? "skipping ahead" : run.hidden ? "nasdaq closed" : "virtual time";
  markControls(run);

  renderBook(s);
  renderPrice(s);
  renderRaces(s);
  renderPnl(s);
  renderEngine(s);
}

function renderBook(s) {
  const asks = s.book.asks.slice(0, 10);
  const bids = s.book.bids.slice(0, 10);
  const max = Math.max(1, ...asks.map((l) => l[1]), ...bids.map((l) => l[1]));
  const nasdaq = s.price.nasdaq;
  const row = (l, stale) =>
    `<li class="${stale ? "stale" : ""}"><span class="bar" style="width:${(100 * l[1]) / max}%"></span>` +
    `<span class="px">${l[0].toFixed(2)}</span><span class="qty">${l[1]}</span></li>`;
  // A quote is stale when Nasdaq has moved through it: an ask below the price, a bid above.
  $("asks").innerHTML = `<li class="side">asks</li>` + asks.slice().reverse().map((l) => row(l, l[0] < nasdaq - 1e-9)).join("");
  $("bids").innerHTML = bids.map((l) => row(l, l[0] > nasdaq + 1e-9)).join("") + `<li class="side">bids</li>`;
  const bb = bids[0], ba = asks[0];
  const spread = bb && ba ? `spread ${Math.round((ba[0] - bb[0]) * 100)}¢` : "one-sided";
  const more = s.book.more_bids + s.book.more_asks;
  $("spread").textContent = `${spread}${more ? ` · +${more} levels` : ""}`;
  const stale = asks.some((l) => l[0] < nasdaq - 1e-9) || bids.some((l) => l[0] > nasdaq + 1e-9);
  $("book-note").textContent = stale ? "stale quotes resting" : "";
}

function renderPrice(s) {
  const p = s.price;
  $("px-nasdaq").textContent = p.nasdaq.toFixed(2);
  $("px-mid").textContent = p.mid == null ? "–" : p.mid.toFixed(3);
  const gap = p.mid == null ? null : p.nasdaq - p.mid;
  $("px-gap").textContent = gap == null ? "–" : Math.abs(gap) >= 1 ? usd(gap) : `${gap >= 0 ? "+" : "−"}${Math.abs(gap * 100).toFixed(1)}¢`;
  $("px-gap").classList.toggle("wide", gap != null && Math.abs(gap) >= 0.015);
  $("eclipse").classList.toggle("hidden-px", s.run.hidden);
  drawChart(s);
}

function renderRaces(s) {
  const r = s.races;
  if (feedRun !== s.run.id) {
    feedRun = s.run.id;
    seenSniperWins.clear();
  }
  $("r-sniper").textContent = r.sniper;
  $("r-lp").textContent = r.lp;
  $("r-rate").textContent = r.lp_win_rate == null ? "–" : `${(r.lp_win_rate * 100).toFixed(0)}%`;
  $("race-open").textContent = r.open ? `${r.open} open` : "";
  const rows = r.feed.slice(0, 4).map((x) => {
    const key = `${x.id}`;
    const flash = x.winner === "sniper" && !seenSniperWins.has(key);
    if (x.winner === "sniper") seenSniperWins.add(key);
    return `<tr class="${flash ? "flash" : ""}"><td>${clock(x.t, s.run)}</td><td class="w-${x.winner}">${x.winner}</td>` +
      `<td>${usd(x.stake_usd, 2, false)}</td><td>${x.ms.toFixed(1)} ms</td><td>${x.rule.replace("_priority", " first")}</td></tr>`;
  });
  $("feed").innerHTML = rows.join("") || `<tr><td colspan="5">no race has ended yet</td></tr>`;
}

const CLASSES = [["regular", "traders"], ["lp", "LP"], ["sniper", "snipers"], ["protocol", "protocol"], ["chain", "chain"]];

function renderPnl(s) {
  const vals = CLASSES.map(([k]) => s.pnl[k]);
  if (pnlScale.run !== s.run.id) pnlScale = { run: s.run.id, max: 1 };
  // The scale only grows within a run, so bars don't jump around.
  pnlScale.max = Math.max(pnlScale.max, ...vals.map(Math.abs));
  $("pnl").innerHTML = CLASSES.map(([k, label], i) => {
    const v = vals[i];
    const w = (50 * Math.abs(v)) / pnlScale.max;
    const style = v >= 0 ? `left:50%;width:${w}%` : `left:${50 - w}%;width:${w}%`;
    return `<li class="${k}"><span class="who">${label}</span><span class="track"><span class="fill ${v < 0 ? "neg" : ""}" style="${style}"></span></span>` +
      `<span class="val">${usd(v)}</span></li>`;
  }).join("");
  $("pnl-sum").textContent = `${usd(s.pnl_sum)} · zero-sum`;
}

function renderEngine(s) {
  const e = s.engine;
  $("e-rate").textContent = rate(e.msgs_per_s);
  $("e-block").textContent = e.block.toLocaleString("en-US");
  $("e-seq").textContent = e.seq.toLocaleString("en-US");
  $("e-p50").textContent = ns(e.latency_ns.p50);
  $("e-p99").textContent = ns(e.latency_ns.p99);
  $("e-p999").textContent = ns(e.latency_ns.p999);
  $("e-lat-n").textContent = e.latency_ns.window ? `standby · n ${e.latency_ns.window.toLocaleString("en-US")}` : "";
  $("rings").innerHTML = e.rings.map((r) => {
    const used = (100 * r.used) / r.cap, peak = (100 * r.peak) / r.cap;
    return `<li><span class="label">${r.name} <b>${r.peak}</b></span>` +
      `<span class="meter"><i style="width:${Math.max(used, 0.5)}%"></i><u style="left:${Math.min(peak, 99.5)}%"></u></span></li>`;
  }).join("");
  const sb = e.standby;
  const sbEl = $("c-standby");
  sbEl.textContent = sb.in_sync == null ? "standby syncing" : sb.in_sync ? `standby in sync at seq ${sb.checked_at.toLocaleString("en-US")}` : "standby DIVERGED";
  sbEl.className = sb.in_sync === false ? "bad" : sb.in_sync ? "ok" : "";
  const lg = s.log;
  const rp = $("c-replay");
  if (!lg.replay) {
    rp.textContent = `logging run ${lg.run}: ${lg.records.toLocaleString("en-US")} records`;
    rp.className = "";
  } else if (lg.replay.ok) {
    rp.textContent = `run ${lg.last_run} log replays exactly · ${lg.replay.fills_hash.slice(0, 8)}`;
    rp.className = "ok";
  } else {
    rp.textContent = `run ${lg.last_run} REPLAY FAILED`;
    rp.className = "bad";
  }
}

// ---------- chart ----------

const chart = $("chart");
const ctx = chart.getContext("2d");

function niceStep(range, n) {
  const raw = range / n;
  const mag = Math.pow(10, Math.floor(Math.log10(raw)));
  const f = raw / mag;
  return (f < 1.5 ? 1 : f < 3 ? 2 : f < 7 ? 5 : 10) * mag;
}

function drawChart(s) {
  const dpr = window.devicePixelRatio || 1;
  const W = chart.clientWidth, H = chart.clientHeight;
  if (chart.width !== Math.round(W * dpr) || chart.height !== Math.round(H * dpr)) {
    chart.width = Math.round(W * dpr);
    chart.height = Math.round(H * dpr);
  }
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, W, H);
  const fs = parseFloat(getComputedStyle(document.documentElement).fontSize);
  const padL = 8, padR = fs * 4.2, padT = fs * 0.6, padB = fs * 1.6;
  const pts = s.price.series;
  if (pts.length < 1) return;
  const [v0, v1] = s.price.view;
  const run = s.run;

  let lo = Infinity, hi = -Infinity;
  for (const p of pts) {
    lo = Math.min(lo, p[1], p[3] ?? p[1]);
    hi = Math.max(hi, p[1], p[3] ?? p[1]);
  }
  const minRange = 0.12;
  if (hi - lo < minRange) { const c = (hi + lo) / 2; lo = c - minRange / 2; hi = c + minRange / 2; }
  const padY = (hi - lo) * 0.08; lo -= padY; hi += padY;
  const x = (t) => padL + ((t - v0) / Math.max(1e-9, v1 - v0)) * (W - padL - padR);
  const y = (v) => padT + (1 - (v - lo) / (hi - lo)) * (H - padT - padB);

  // The hidden weekend.
  if (run.weekend) {
    const [c, r] = run.weekend;
    const xa = Math.max(padL, x(c)), xb = Math.min(W - padR, x(Math.min(r, run.clock_s)));
    if (xb > xa) {
      ctx.fillStyle = "rgba(123, 97, 255, 0.07)";
      ctx.fillRect(xa, padT, xb - xa, H - padT - padB);
      ctx.fillStyle = C.ink3;
      ctx.font = `400 ${fs * 0.58}px ${MONO}`;
      ctx.fillText("NASDAQ CLOSED · PRICE HIDDEN", xa + fs * 0.5, padT + fs * 1.0);
    }
  }

  // Grid and price labels.
  const step = niceStep(hi - lo, 5);
  ctx.font = `400 ${fs * 0.62}px ${MONO}`;
  ctx.textBaseline = "middle";
  for (let v = Math.ceil(lo / step) * step; v <= hi; v += step) {
    ctx.strokeStyle = C.line;
    ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(padL, y(v)); ctx.lineTo(W - padR, y(v)); ctx.stroke();
    ctx.fillStyle = C.ink3;
    ctx.fillText(v.toFixed(2), W - padR + fs * 0.4, y(v));
  }
  // Time labels.
  ctx.textBaseline = "alphabetic";
  const tStep = niceStep(v1 - v0, 5);
  ctx.fillStyle = C.ink3;
  for (let t = Math.ceil(v0 / tStep) * tStep; t <= v1; t += tStep) {
    const label = run.weekend ? clock(t, run).slice(0, 9) : clock(t, run).replace(/\.\d+$/, "");
    const w = ctx.measureText(label).width;
    const lx = Math.max(padL, Math.min(x(t) - w / 2, W - padR - w));
    ctx.fillText(label, lx, H - fs * 0.35);
  }

  const hidden = (t) => !!run.weekend && t >= run.weekend[0] && t < run.weekend[1];

  // Book mid: dashed.
  ctx.setLineDash([fs * 0.4, fs * 0.3]);
  ctx.strokeStyle = C.ink3;
  ctx.lineWidth = 2;
  ctx.beginPath();
  let on = false;
  for (const p of pts) {
    if (p.length < 4) continue; // a path sample: no book mid, bridge over it
    if (p[3] == null) { on = false; continue; }
    if (!on) { ctx.moveTo(x(p[0]), y(p[3])); on = true; } else ctx.lineTo(x(p[0]), y(p[3]));
  }
  ctx.stroke();
  ctx.setLineDash([]);

  // Nasdaq: bright while visible; dark with a violet rim while hidden (the eclipse).
  const segs = [];
  for (let i = 1; i < pts.length; i++) segs.push([pts[i - 1], pts[i], hidden(pts[i - 1][0])]);
  const stroke = (h) => {
    ctx.beginPath();
    for (const [a, b, sh] of segs) {
      if (sh !== h) continue;
      ctx.moveTo(x(a[0]), y(a[1]));
      ctx.lineTo(x(b[0]), y(a[1])); // a step: the price holds until it moves
      ctx.lineTo(x(b[0]), y(b[1]));
    }
    ctx.stroke();
  };
  ctx.lineJoin = "round";
  ctx.save();
  ctx.shadowColor = C.accent;
  ctx.shadowBlur = fs * 0.6;
  ctx.strokeStyle = "rgba(214, 205, 255, 0.55)";
  ctx.lineWidth = 5;
  stroke(true);
  ctx.restore();
  ctx.strokeStyle = "#0c0c10";
  ctx.lineWidth = 3;
  stroke(true);
  ctx.strokeStyle = C.ink;
  ctx.lineWidth = 3;
  stroke(false);

  // The reopen jump.
  if (run.weekend && run.clock_s >= run.weekend[1]) {
    const r = run.weekend[1];
    const before = pts.filter((p) => p[0] < run.weekend[0]).pop();
    const after = pts.find((p) => p[0] >= r);
    const xr = x(r);
    ctx.strokeStyle = C.accent;
    ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(xr, padT); ctx.lineTo(xr, H - padB); ctx.stroke();
    if (before && after) {
      const gap = after[1] - before[1];
      ctx.fillStyle = "#d6cdff";
      ctx.font = `400 ${fs * 0.7}px ${MONO}`;
      const text = `REOPEN ${gap >= 0 ? "+" : "−"}$${Math.abs(gap).toFixed(2)}`;
      const tw = ctx.measureText(text).width;
      const tx = Math.min(xr + fs * 0.4, W - padR - tw - fs * 0.3), ty = padT + fs * 2.2;
      ctx.fillStyle = "rgba(5, 5, 7, 0.85)";
      ctx.fillRect(tx - fs * 0.25, ty - fs * 0.75, tw + fs * 0.5, fs * 1.05);
      ctx.fillStyle = "#d6cdff";
      ctx.fillText(text, tx, ty);
    }
  }

  // Now: the latest Nasdaq price.
  const last = pts[pts.length - 1];
  ctx.fillStyle = s.run.hidden ? "#0c0c10" : C.ink;
  ctx.strokeStyle = s.run.hidden ? C.accent : C.ink;
  ctx.lineWidth = 2;
  ctx.beginPath(); ctx.arc(x(last[0]), y(last[1]), fs * 0.28, 0, Math.PI * 2); ctx.fill(); ctx.stroke();
}

window.addEventListener("resize", () => { dirty = true; });
buildControls();
connect();
requestAnimationFrame(render);
