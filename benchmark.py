"""Experiments. Each run starts a fresh broker (and proxy if loss > 0) as subprocesses.

    python benchmark.py            # full run, 3 repetitions  -> results.json
    python benchmark.py --quick    # 1 repetition, 100 msgs   -> results.json
Then:  python plots.py
"""
import argparse
import asyncio
import json
import os
import statistics
import subprocess
import sys
import time

from client import Client

HERE = os.path.dirname(os.path.abspath(__file__))
BROKER_PORT, PROXY_PORT = 1883, 1884


async def wait_port(port):
    for _ in range(100):
        try:
            _, w = await asyncio.open_connection("127.0.0.1", port)
            w.close()
            return
        except OSError:
            await asyncio.sleep(0.1)
    raise RuntimeError(f"port {port} never came up")


def spawn(script, *args):
    return subprocess.Popen([sys.executable, os.path.join(HERE, script), *map(str, args)],
                            stdout=subprocess.DEVNULL)


async def run(qos, loss, n_subs, payload_size, n_msgs=300, batch=20):
    procs = [spawn("broker.py", "--port", BROKER_PORT)]
    await wait_port(BROKER_PORT)
    port = BROKER_PORT
    if loss > 0:
        procs.append(spawn("proxy.py", "--loss", loss, "--port", PROXY_PORT,
                           "--broker-port", BROKER_PORT))
        await wait_port(PROXY_PORT)
        port = PROXY_PORT
    clients = []
    try:
        latencies, got = [], [set() for _ in range(n_subs)]
        last_rx = [0.0]

        def make_cb(i):
            def cb(msg, lat):
                got[i].add(msg["payload"][:8])
                latencies.append(lat)
                last_rx[0] = time.time()
            return cb

        subs = [Client(make_cb(i)) for i in range(n_subs)]
        clients += subs
        for i, s in enumerate(subs):
            await s.connect("127.0.0.1", port, f"sub{i}")
            await s.subscribe("bench/#", qos)
        pub = Client()
        clients.append(pub)
        await pub.connect("127.0.0.1", port, "pub")
        mon = Client()                       # talks to the broker directly, never via the proxy
        clients.append(mon)
        await mon.connect("127.0.0.1", BROKER_PORT, "monitor")

        pad = "x" * max(0, payload_size - 8)
        t0 = time.time()
        retries = 0
        for start in range(0, n_msgs, batch):
            r = await asyncio.gather(*[
                pub.publish("bench/data", f"{seq:08d}" + pad, qos=qos)
                for seq in range(start, min(start + batch, n_msgs))])
            retries += sum(r)
        # wait for stragglers: up to 2 s, stop early once everything has arrived
        deadline = time.time() + 2
        while time.time() < deadline and sum(map(len, got)) < n_msgs * n_subs:
            await asyncio.sleep(0.05)
        st = await mon.stats()
        unique = sum(map(len, got))
        elapsed = max(last_rx[0] - t0, 1e-6)
        return {
            "qos": qos, "loss": loss, "subs": n_subs, "payload": payload_size, "msgs": n_msgs,
            "delivery_ratio": unique / (n_msgs * n_subs),
            "latency_ms": 1000 * statistics.mean(latencies) if latencies else float("nan"),
            "throughput": unique / elapsed,
            "retransmits": retries + st["retransmits"],
            "pub_retries": retries,
            "broker_retransmits": st["retransmits"],
            "duplicates": sum(s.duplicates for s in subs),
            "dup_pub": st["dup_pub"],
        }
    finally:
        for c in clients:
            await c.disconnect()
        for p in procs:
            p.kill()
            p.wait()
        await asyncio.sleep(0.3)


def average(runs):
    out = dict(runs[0])
    for k in runs[0]:
        if isinstance(runs[0][k], float) or k in ("retransmits", "duplicates", "pub_retries",
                                                  "broker_retransmits", "dup_pub"):
            out[k] = statistics.mean(r[k] for r in runs)
    out["reps"] = len(runs)
    return out


async def main(reps, n_msgs, quick):
    configs = []
    # Exp 1 + 2: QoS 0 vs 1 baseline (loss 0) and effect of loss, 1 subscriber
    for loss in (0, 0.05, 0.10, 0.20):
        for qos in (0, 1):
            configs.append(("loss", dict(qos=qos, loss=loss, n_subs=1, payload_size=64)))
    # Exp 3: scalability
    for n in ((10, 50) if quick else (10, 50, 100)):
        configs.append(("subs", dict(qos=1, loss=0, n_subs=n, payload_size=64)))
    # Exp 4: payload size
    for size in (64, 1024, 16384):
        configs.append(("payload", dict(qos=1, loss=0, n_subs=1, payload_size=size)))

    results = []
    for i, (exp, cfg) in enumerate(configs, 1):
        runs = []
        for _ in range(reps):
            runs.append(await run(n_msgs=n_msgs, **cfg))
        avg = average(runs)
        avg["experiment"] = exp
        results.append(avg)
        print(f"[{i}/{len(configs)}] {exp:8s} {cfg}  ratio={avg['delivery_ratio']:.3f} "
              f"lat={avg['latency_ms']:.1f}ms retx={avg['retransmits']:.1f} "
              f"dups={avg['duplicates']:.1f}", flush=True)
        with open(os.path.join(HERE, "results.json"), "w") as f:
            json.dump(results, f, indent=2)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--msgs", type=int, default=300)
    a = ap.parse_args()
    if a.quick:
        a.reps, a.msgs = 1, 100
    asyncio.run(main(a.reps, a.msgs, a.quick))
