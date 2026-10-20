"""Turn results.json into PNG graphs in ./graphs"""
import json
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "graphs")
os.makedirs(OUT, exist_ok=True)
R = json.load(open(os.path.join(HERE, "results.json")))


def sel(exp, **kw):
    rows = [r for r in R if r["experiment"] == exp and all(r[k] == v for k, v in kw.items())]
    return rows


def save(fig_name):
    plt.tight_layout()
    plt.savefig(os.path.join(OUT, fig_name), dpi=150)
    plt.close()


def vs_loss(metric, ylabel, title, fname, pct=False):
    plt.figure(figsize=(6, 4))
    for qos in (0, 1):
        rows = sorted(sel("loss", qos=qos), key=lambda r: r["loss"])
        xs = [r["loss"] * 100 for r in rows]
        ys = [r[metric] * (100 if pct else 1) for r in rows]
        plt.plot(xs, ys, marker="o", label=f"QoS {qos}")
    plt.xlabel("Emulated message loss (%)")
    plt.ylabel(ylabel)
    plt.title(title)
    plt.grid(alpha=.3)
    plt.legend()
    save(fname)


vs_loss("delivery_ratio", "Delivery ratio (%)", "Delivery ratio vs loss", "delivery_vs_loss.png", pct=True)
vs_loss("latency_ms", "Mean latency (ms)", "Latency vs loss", "latency_vs_loss.png")
vs_loss("retransmits", "Retransmissions (publisher + broker)", "Retransmissions vs loss",
        "retransmits_vs_loss.png")
vs_loss("duplicates", "Duplicates seen by subscriber", "Duplicate deliveries vs loss",
        "duplicates_vs_loss.png")

rows = sorted(sel("subs"), key=lambda r: r["subs"])
if rows:
    plt.figure(figsize=(6, 4))
    plt.plot([r["subs"] for r in rows], [r["latency_ms"] for r in rows], marker="o")
    plt.xlabel("Number of subscribers")
    plt.ylabel("Mean latency (ms)")
    plt.title("Latency vs subscribers (QoS 1, no loss)")
    plt.grid(alpha=.3)
    save("latency_vs_subscribers.png")

rows = sorted(sel("payload"), key=lambda r: r["payload"])
if rows:
    plt.figure(figsize=(6, 4))
    plt.plot([r["payload"] for r in rows], [r["throughput"] for r in rows], marker="o")
    plt.xscale("log")
    plt.xlabel("Payload size (bytes)")
    plt.ylabel("Throughput (messages/s)")
    plt.title("Throughput vs payload size (QoS 1, no loss)")
    plt.grid(alpha=.3)
    save("throughput_vs_payload.png")

print("graphs written to", OUT)
