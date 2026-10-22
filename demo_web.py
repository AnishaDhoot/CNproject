"""Browser demo for the pub-sub project.

    python demo_web.py            then open http://127.0.0.1:8765

It starts its own broker (port 4883) and a lossy proxy (port 4884), and the page
talks to them through the real client library. Nothing here is simulated.
"""
import argparse
import asyncio
import itertools
import json
import mimetypes
import os
import subprocess
import sys
import time
import urllib.parse

from client import Client
from protocol import topic_match
from proxy import start_proxy

HERE = os.path.dirname(os.path.abspath(__file__))
WEB = os.path.join(HERE, "web")
HOST = "127.0.0.1"
uid = itertools.count(1)


class Bench:
    def __init__(self, broker_port, proxy_port):
        self.bp, self.pp = broker_port, proxy_port
        self.loss = 0.2
        self.proc = None
        self.proxy = None
        self.counters = {"dropped": 0}
        self.queues = set()
        self.running = False

    # ---- events to every open page ----
    def emit(self, **ev):
        data = json.dumps(ev)
        for q in list(self.queues):
            q.put_nowait(data)

    # ---- broker process ----
    async def start_broker(self):
        if self.proc:
            self.proc.kill()
            self.proc.wait()
        self.proc = subprocess.Popen([sys.executable, os.path.join(HERE, "broker.py"), "--port", str(self.bp)],
                                     stdout=subprocess.DEVNULL)
        for _ in range(100):
            try:
                _, w = await asyncio.open_connection(HOST, self.bp)
                w.close()
                return
            except OSError:
                await asyncio.sleep(0.1)
        raise RuntimeError("broker did not start")

    def on_drop(self, direction, msg):
        who = {("c2b", "PUBLISH"): "publisher to broker", ("c2b", "PUBACK"): "subscriber to broker",
               ("b2c", "PUBLISH"): "broker to subscriber", ("b2c", "PUBACK"): "broker to publisher"}.get((direction, msg["type"]), direction)
        topic = msg.get("topic", "")
        self.emit(t="drop", type=msg["type"], who=who, id=msg.get("id"), topic=topic,
                  seq=int(msg["payload"]) if topic.startswith("loss/") and str(msg.get("payload", "")).isdigit() else None)

    async def start(self):
        await self.start_broker()
        self.proxy, self.counters = await start_proxy(lambda: self.loss, port=self.pp, broker_port=self.bp, on_drop=self.on_drop)

    def state(self):
        return {"loss": self.loss, "broker": self.bp, "proxy": self.pp, "running": self.running}

    async def stats(self):
        m = Client()
        await m.connect(HOST, self.bp, f"monitor-{next(uid)}")
        try:
            return await m.stats()
        finally:
            await m.disconnect()


# ============ API ============
async def api_topics(b, d):
    flt, topic = str(d.get("filter", "")).strip(), str(d.get("topic", "")).strip()
    if not flt or not topic:
        return 400, {"error": "filter and topic are required"}
    got = []
    sub, pub = Client(lambda m, l: got.append(l * 1000)), Client()
    await sub.connect(HOST, b.bp, f"probe-sub-{next(uid)}")
    await pub.connect(HOST, b.bp, f"probe-pub-{next(uid)}")
    try:
        await sub.subscribe(flt, 1)
        await pub.publish(topic, "probe", qos=1)
        await asyncio.sleep(0.35)
    finally:
        await sub.disconnect()
        await pub.disconnect()
    return 200, {"matcher": topic_match(flt, topic), "delivered": bool(got), "ms": round(got[0], 1) if got else None}


async def api_retain_publish(b, d):
    topic, payload = str(d.get("topic", "")).strip(), str(d.get("payload", ""))
    if not topic:
        return 400, {"error": "topic is required"}
    pub = Client()
    await pub.connect(HOST, b.bp, f"ret-pub-{next(uid)}")
    try:
        await pub.publish(topic, payload, qos=1, retain=True)
    finally:
        await pub.disconnect()
    return 200, {"ok": True, "cleared": payload == ""}


async def api_retain_join(b, d):
    flt = str(d.get("filter", "")).strip()
    if not flt:
        return 400, {"error": "filter is required"}
    got = []
    sub = Client(lambda m, l: got.append({"topic": m["topic"], "payload": m["payload"], "retain": m["retain"]}))
    await sub.connect(HOST, b.bp, f"late-sub-{next(uid)}")
    try:
        await sub.subscribe(flt, 1)
        await asyncio.sleep(0.5)
    finally:
        await sub.disconnect()
    return 200, {"messages": got}


async def api_loss(b, d):
    try:
        p = float(d.get("loss"))
    except (TypeError, ValueError):
        return 400, {"error": "loss must be a number"}
    b.loss = min(max(p, 0.0), 0.6)
    b.emit(t="state", **b.state())
    return 200, b.state()


async def api_reset(b, d):
    if b.running:
        return 409, {"error": "a run is in progress"}
    await b.start_broker()
    b.counters["dropped"] = 0
    b.emit(t="reset")
    return 200, {"ok": True}


async def api_run(b, d):
    if b.running:
        return 409, {"error": "a run is already in progress"}
    n = int(min(max(int(d.get("n", 100)), 10), 300))
    asyncio.create_task(run_stream(b, n))
    return 200, {"started": True, "n": n}


async def run_stream(b, n):
    b.running = True
    b.emit(t="state", **b.state())
    run = next(uid)
    got = {0: {}, 1: {}}                       # seq -> latency in ms
    def on_rx(q):
        def cb(m, l):
            seq = int(m["payload"])
            got[q][seq] = l * 1000
            b.emit(t="rx", q=q, seq=seq, ms=round(l * 1000, 1))
        return cb

    sub0, sub1 = Client(on_rx(0)), Client(on_rx(1))
    pub = Client()
    clients = [sub0, sub1, pub]
    try:
        before = await b.stats()
        dropped0 = b.counters["dropped"]
        b.emit(t="run_start", n=n, loss=b.loss)
        await sub0.connect(HOST, b.pp, f"s0-{run}")
        await sub1.connect(HOST, b.pp, f"s1-{run}")
        await pub.connect(HOST, b.pp, f"p-{run}")
        await sub0.subscribe("loss/q0", 0)
        await sub1.subscribe("loss/q1", 1)
        retries = 0
        step = 10
        for i in range(0, n, step):
            seqs = range(i, min(i + step, n))
            res = await asyncio.gather(*[pub.publish("loss/q1", str(s), qos=1) for s in seqs],
                                       *[pub.publish("loss/q0", str(s), qos=0) for s in seqs])
            r1 = res[:len(seqs)]
            retries += sum(r1)
            for s, r in zip(seqs, r1):
                b.emit(t="tx", seq=s, retries=r)
            b.emit(t="prog", dups=sub1.duplicates)
            await asyncio.sleep(0.03)
        last = time.time()
        deadline = last + 2.5
        while time.time() < deadline:
            if len(got[1]) == n and (len(got[0]) == n or time.time() > last + 0.7):
                break
            await asyncio.sleep(0.05)
        after = await b.stats()
        late = lambda q: sum(1 for v in got[q].values() if v >= 150)
        mean = lambda q: round(sum(got[q].values()) / len(got[q]), 1) if got[q] else None
        b.emit(t="done", n=n, loss=b.loss,
               q0={"got": len(got[0]), "mean": mean(0), "late": late(0), "lost": [s for s in range(n) if s not in got[0]]},
               q1={"got": len(got[1]), "mean": mean(1), "late": late(1), "lost": [s for s in range(n) if s not in got[1]],
                   "pub_retries": retries, "broker_retx": after["retransmits"] - before["retransmits"],
                   "dups": sub1.duplicates, "dup_pub": after["dup_pub"] - before["dup_pub"]},
               dropped=b.counters["dropped"] - dropped0)
    except Exception as e:                      # keep the page usable if a run breaks
        b.emit(t="error", message=str(e))
    finally:
        for c in clients:
            await c.disconnect()
        b.running = False
        b.emit(t="state", **b.state())


ROUTES = {("POST", "/api/topics"): api_topics, ("POST", "/api/retained/publish"): api_retain_publish,
          ("POST", "/api/retained/join"): api_retain_join, ("POST", "/api/loss"): api_loss,
          ("POST", "/api/reset"): api_reset, ("POST", "/api/run"): api_run}
STATIC = {"/": "index.html", "/style.css": "style.css", "/app.js": "app.js"}


# ============ tiny HTTP server ============
async def respond(writer, status, body, ctype="application/json"):
    if not isinstance(body, bytes):
        body = json.dumps(body).encode()
    reason = {200: "OK", 400: "Bad Request", 404: "Not Found", 409: "Conflict", 500: "Server Error"}.get(status, "OK")
    writer.write((f"HTTP/1.1 {status} {reason}\r\nContent-Type: {ctype}\r\nContent-Length: {len(body)}\r\n"
                  "Cache-Control: no-store\r\nConnection: close\r\n\r\n").encode() + body)
    await writer.drain()


async def sse(b, writer):
    q = asyncio.Queue()
    b.queues.add(q)
    try:
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-store\r\nConnection: keep-alive\r\n\r\n")
        writer.write(f"data: {json.dumps(dict(t='state', **b.state()))}\n\n".encode())
        await writer.drain()
        while True:
            try:
                data = await asyncio.wait_for(q.get(), 15)
                writer.write(f"data: {data}\n\n".encode())
            except asyncio.TimeoutError:
                writer.write(b": keep-alive\n\n")
            await writer.drain()
    except (ConnectionError, OSError):
        pass
    finally:
        b.queues.discard(q)


def make_handler(b):
    async def handle(reader, writer):
        try:
            head = await reader.readuntil(b"\r\n\r\n")
            lines = head.decode("latin1").split("\r\n")
            method, target, _ = lines[0].split(" ", 2)
            headers = {k.lower(): v.strip() for k, v in (l.split(":", 1) for l in lines[1:] if ":" in l)}
            body = await reader.readexactly(int(headers.get("content-length", 0)))
            path = urllib.parse.urlparse(target).path
            if method == "GET" and path == "/events":
                await sse(b, writer)
            elif method == "GET" and path == "/api/stats":
                await respond(writer, 200, {**await b.stats(), **b.state()})
            elif method == "GET" and path in STATIC:
                with open(os.path.join(WEB, STATIC[path]), "rb") as f:
                    await respond(writer, 200, f.read(), mimetypes.guess_type(STATIC[path])[0] or "text/plain")
            elif (method, path) in ROUTES:
                try:
                    data = json.loads(body or b"{}")
                    status, out = await ROUTES[(method, path)](b, data)
                except (ValueError, KeyError) as e:
                    status, out = 400, {"error": str(e)}
                await respond(writer, status, out)
            else:
                await respond(writer, 404, {"error": "not found"})
        except (asyncio.IncompleteReadError, ConnectionError, OSError):
            pass
        finally:
            writer.close()
    return handle


async def main(a):
    b = Bench(a.broker_port, a.proxy_port)
    await b.start()
    server = await asyncio.start_server(make_handler(b), HOST, a.port)
    print(f"demo page:  http://{HOST}:{a.port}\nbroker {a.broker_port}, proxy {a.proxy_port}   (Ctrl+C to stop)", flush=True)
    try:
        async with server:
            await server.serve_forever()
    finally:
        if b.proc:
            b.proc.kill()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--broker-port", type=int, default=4883)
    ap.add_argument("--proxy-port", type=int, default=4884)
    try:
        asyncio.run(main(ap.parse_args()))
    except KeyboardInterrupt:
        pass
