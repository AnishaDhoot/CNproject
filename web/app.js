"use strict";
const $ = (s) => document.querySelector(s);
const $$ = (s) => [...document.querySelectorAll(s)];

async function post(path, body) {
  try {
    const r = await fetch(path, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body || {}) });
    return { ok: r.ok, data: await r.json() };
  } catch (e) {
    return { ok: false, data: { error: "could not reach the demo server" } };
  }
}
const esc = (s) => String(s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

/* ---------- 01 topic filters ---------- */
const hist = [];
$$("[data-topic]").forEach((b) => b.addEventListener("click", (e) => { e.preventDefault(); $("#topic").value = b.dataset.topic; }));
$$("[data-filter]").forEach((b) => b.addEventListener("click", (e) => { e.preventDefault(); $("#filter").value = b.dataset.filter; }));

$("#f-topic").addEventListener("submit", async (e) => {
  e.preventDefault();
  const filter = $("#filter").value.trim(), topic = $("#topic").value.trim();
  const btn = e.submitter; btn.disabled = true;
  const { ok, data } = await post("/api/topics", { filter, topic });
  btn.disabled = false;
  if (!ok) { $("#r-match").textContent = "error"; $("#r-match-n").textContent = data.error || ""; return; }
  const m = $("#r-match"), d = $("#r-deliv");
  m.textContent = data.matcher ? "match" : "no match"; m.className = "big " + (data.matcher ? "yes" : "no");
  d.textContent = data.delivered ? "delivered" : "nothing"; d.className = "big " + (data.delivered ? "yes" : "no");
  $("#r-match-n").textContent = "topic_match() in protocol.py";
  $("#r-deliv-n").textContent = data.delivered ? `received after ${data.ms} ms` : "no message reached the subscriber";
  hist.unshift({ filter, topic, matcher: data.matcher, delivered: data.delivered, ms: data.ms });
  hist.length = Math.min(hist.length, 6);
  $("#t-log").innerHTML = hist.map((h) =>
    `<li><b>${esc(h.filter)}</b><span>${esc(h.topic)}</span><span>${h.matcher === h.delivered ? "agree" : "DISAGREE"}, ${h.delivered ? "delivered" : "not delivered"}</span></li>`).join("");
});

/* ---------- 02 retained ---------- */
$("#r-store").addEventListener("click", async () => {
  const { ok, data } = await post("/api/retained/publish", { topic: $("#rt").value, payload: $("#rp").value });
  $("#r-store-n").textContent = ok ? (data.cleared ? "retained message cleared" : "stored on the broker") : data.error;
});
$("#r-clear").addEventListener("click", async (e) => {
  e.preventDefault();
  const { ok, data } = await post("/api/retained/publish", { topic: $("#rt").value, payload: "" });
  $("#r-store-n").textContent = ok ? "retained message cleared" : data.error;
});
$("#r-join").addEventListener("click", async () => {
  const { ok, data } = await post("/api/retained/join", { filter: $("#rf").value });
  const ul = $("#r-msgs");
  if (!ok) { ul.innerHTML = `<li class="empty">${esc(data.error)}</li>`; return; }
  ul.innerHTML = data.messages.length
    ? data.messages.map((m) => `<li><span>${esc(m.topic)} = ${esc(m.payload)}</span>${m.retain ? '<span class="tag">retained</span>' : ""}</li>`).join("")
    : '<li class="empty">nothing arrived. no retained message matches this filter.</li>';
});

/* ---------- 03 loss ---------- */
let N = 100, running = false, cells = [[], []], arrived = [0, 0], sums = [0, 0], dropped = 0, retries = 0;

$$("#count button").forEach((b) => b.addEventListener("click", () => {
  if (running) return;
  N = +b.dataset.n;
  $$("#count button").forEach((x) => x.classList.toggle("on", x === b));
}));
$("#loss").addEventListener("input", (e) => { $("#loss-n").textContent = e.target.value; $("#w-loss").textContent = e.target.value; });
$("#loss").addEventListener("change", (e) => post("/api/loss", { loss: +e.target.value / 100 }));
$("#run").addEventListener("click", async () => {
  const { ok, data } = await post("/api/run", { n: N });
  if (!ok) addLog("", "error", data.error || "could not start");
});
$("#reset").addEventListener("click", async (e) => {
  e.preventDefault();
  const { ok, data } = await post("/api/reset");
  addLog("", ok ? "broker" : "error", ok ? "restarted, counters back to zero" : data.error);
});

function build(n) {
  [0, 1].forEach((q) => {
    const box = $("#tape" + q); box.innerHTML = ""; cells[q] = [];
    for (let i = 0; i < n; i++) { const c = document.createElement("div"); c.className = "c"; c.title = "message " + i; box.appendChild(c); cells[q].push(c); }
  });
  arrived = [0, 0]; sums = [0, 0]; dropped = 0; retries = 0;
  ["a0", "a1", "d1", "p1"].forEach((id) => ($("#" + id).textContent = "0"));
  ["l0", "l1"].forEach((id) => ($("#" + id).textContent = "-"));
  $("#w-drop").textContent = "0";
  $("#drops").innerHTML = ""; $("#result").hidden = true;
}
function addLog(time, kind, text) {
  const ul = $("#drops");
  const li = document.createElement("li");
  li.className = kind === "drop" ? "" : "sys";
  li.innerHTML = `<span>${time}</span><span>${kind === "drop" ? "<em>dropped</em>" : esc(kind)}</span><span>${text}</span>`;
  ul.appendChild(li); ul.scrollTop = ul.scrollHeight;
}
let t0 = 0;
const since = () => ((performance.now() - t0) / 1000).toFixed(2) + "s";

function onEvent(ev) {
  switch (ev.t) {
    case "state":
      $("#status").innerHTML = `broker <b>:${ev.broker}</b> &nbsp; proxy <b>:${ev.proxy}</b> &nbsp; ${ev.running ? "run in progress" : "idle"}`;
      $("#w-bp").textContent = ev.broker;
      running = ev.running; $("#run").disabled = running; $("#reset").disabled = running;
      if (!running || document.activeElement !== $("#loss")) {
        $("#loss").value = Math.round(ev.loss * 100); $("#loss-n").textContent = Math.round(ev.loss * 100); $("#w-loss").textContent = Math.round(ev.loss * 100);
      }
      break;
    case "run_start":
      t0 = performance.now(); build(ev.n);
      addLog("0.00s", "run", `${ev.n} messages, proxy dropping ${Math.round(ev.loss * 100)}% of publishes and acks`);
      break;
    case "tx":
      [0, 1].forEach((q) => cells[q][ev.seq] && cells[q][ev.seq].classList.add("sent"));
      if (ev.retries) { retries += ev.retries; $("#p1").textContent = retries; cells[1][ev.seq] && cells[1][ev.seq].classList.add("retry"); }
      break;
    case "rx": {
      const c = cells[ev.q][ev.seq];
      if (c && !c.classList.contains("ok") && !c.classList.contains("late")) {
        c.classList.remove("lost"); c.classList.add(ev.ms >= 150 ? "late" : "ok");
        arrived[ev.q]++; sums[ev.q] += ev.ms;
        $("#a" + ev.q).textContent = arrived[ev.q];
        $("#l" + ev.q).textContent = (sums[ev.q] / arrived[ev.q]).toFixed(1);
      }
      break;
    }
    case "prog": $("#d1").textContent = ev.dups; break;
    case "drop": {
      dropped++; $("#w-drop").textContent = dropped;
      const p = $("#proxy"); p.classList.add("hit"); setTimeout(() => p.classList.remove("hit"), 160);
      const seq = ev.seq != null ? ` #${ev.seq}` : "";
      addLog(since(), "drop", `${esc(ev.type)}${esc(seq)}${ev.topic ? " " + esc(ev.topic) : ev.id != null ? " ack for id " + ev.id : ""}  <span style="opacity:.7">${esc(ev.who)}</span>`);
      break;
    }
    case "done": finish(ev); break;
    case "reset": $("#w-drop").textContent = "0"; break;
    case "error": addLog(since(), "error", esc(ev.message)); break;
  }
}

function finish(ev) {
  [[0, ev.q0], [1, ev.q1]].forEach(([q, r]) => r.lost.forEach((s) => cells[q][s] && cells[q][s].classList.add("lost")));
  $("#d1").textContent = ev.q1.dups;
  addLog(since(), "done", `QoS 0 arrived ${ev.q0.got}/${ev.n}, QoS 1 arrived ${ev.q1.got}/${ev.n}`);
  const pct = (g) => `${g} of ${ev.n} <span style="color:var(--mute)">(${(100 * g / ev.n).toFixed(1)}%)</span>`;
  const ms = (v) => (v == null ? "-" : v.toFixed(1) + " ms");
  const rows = [
    ["Arrived", pct(ev.q0.got), pct(ev.q1.got)],
    ["Never arrived", ev.q0.lost.length, ev.q1.lost.length],
    ["Mean latency", ms(ev.q0.mean), ms(ev.q1.mean)],
    ["Arrived after a retry", "n/a", ev.q1.late],
    ["Publisher retries", "n/a", ev.q1.pub_retries],
    ["Broker retransmissions", "n/a", ev.q1.broker_retx],
    ["Duplicates filtered by the subscriber", "n/a", ev.q1.dups],
    ["Repeated publishes ignored by the broker", "n/a", ev.q1.dup_pub],
  ];
  $("#res-body").innerHTML = rows.map((r) => `<tr><td>${r[0]}</td><td>${r[1]}</td><td>${r[2]}</td></tr>`).join("");
  const p = ev.loss, exp = (100 * (1 - p) * (1 - p)).toFixed(1);
  $("#res-foot").textContent = `The proxy dropped ${ev.dropped} frames in this run. A QoS 0 message has to survive two hops, so with ${Math.round(p * 100)}% loss about ${exp}% should arrive. This run gave ${(100 * ev.q0.got / ev.n).toFixed(1)}%.`;
  $("#result").hidden = false;
}

/* live connection */
build(100);
function connect() {
  const es = new EventSource("/events");
  es.onmessage = (e) => onEvent(JSON.parse(e.data));
  es.onerror = () => { $("#status").textContent = "lost the connection to the demo server, retrying..."; };
}
connect();
