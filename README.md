# Lightweight Pub-Sub Messaging with QoS 0/1 over TCP

Python 3.10+, asyncio, plain TCP. Only extra dependency: `matplotlib` (graphs).

| File | Purpose |
|---|---|
| `protocol.py` | Framing (4-byte big-endian length + JSON) and wildcard `topic_match` |
| `broker.py` | Broker: CONNECT, SUBSCRIBE (`+`, `#`), retained messages, QoS 0/1, STATS |
| `client.py` | Client library + CLI (`sub` / `pub`) |
| `proxy.py` | Lossy proxy: drops whole PUBLISH/PUBACK frames, optional delay |
| `benchmark.py` | Runs all experiments, writes `results.json` |
| `plots.py` | Makes PNG graphs in `graphs/` |
| `demo.py` | Scripted demo for the presentation |
| `demo_web.py`, `web/` | Browser demo ("Loss bench"): runs the real broker and proxy and shows delivery under loss live |
| `tests/` | Unit tests (wildcards) and end-to-end tests (QoS 1 under loss) |

## Run

```bash
python broker.py                          # port 1883
python proxy.py --loss 0.2                # port 1884 -> broker, 20% loss
python client.py sub "sensor/+/temp" --qos 1 --port 1884
python client.py pub sensor/room1/temp 23.5 --qos 1 --port 1884
python demo.py                            # self-contained demo
python demo_web.py                        # browser demo at http://127.0.0.1:8765
python benchmark.py                       # ~10 min; use --quick for a short run
python plots.py
python -m unittest discover -s tests
```

## Message types

```
CONNECT   {"type":"CONNECT","client_id":"c1"}            -> CONNACK
SUBSCRIBE {"type":"SUBSCRIBE","topic":"a/+/b","qos":1}   -> SUBACK
PUBLISH   {"type":"PUBLISH","topic":..,"payload":..,"qos":0|1,"id":7,"retain":false,"ts":..}
PUBACK    {"type":"PUBACK","id":7}
STATS     {"type":"STATS"}  -> {"type":"STATS","stats":{pub_in,delivered,retransmits,dup_pub,delivery_failed}}
```

CONNACK and SUBACK are small additions to the original spec. They let a client know the
subscription is active before it publishes, which the benchmark depends on. The proxy never
drops them.

## How QoS 1 works

* **Publisher -> broker:** client sends PUBLISH with an `id` and retransmits every 0.2 s (up to 5
  retries) until PUBACK. The broker always re-ACKs, but only routes a `(client_id, id)` once;
  repeats count as `dup_pub`.
* **Broker -> subscriber:** delivery QoS = `min(publish_qos, sub_qos)`. Each delivery gets its
  own `delivery_id` (never the publisher's id), is retried up to 5 times, and runs in its own
  task so a slow subscriber does not block others. The subscriber ACKs every copy but passes
  each delivery id to the application only once (at-least-once on the wire, duplicates
  suppressed at the client; the count is reported as `duplicates`).

## Notes for the report

* The proxy drops whole frames, never raw bytes, so the length-prefixed framing stays intact.
  TCP is reliable underneath, so this emulates loss at the application-message level.
* Not implemented (list under future work): QoS 2, keep-alive / last-will, TLS, live dashboard,
  session persistence across reconnects.
* Loss applies to PUBLISH and PUBACK frames in both directions; CONNECT, SUBSCRIBE, STATS and
  their replies are never dropped.
* Latency is measured as `receive_time - ts` on one machine (one clock). For retransmitted
  messages `ts` is the original send time, so latency includes the retry delay.
* The loss-experiment baseline (Experiment 1) is the 0% points of Experiment 2.
