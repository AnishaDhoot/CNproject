"""Pub-sub broker: wildcard topics, retained messages, QoS 0/1.

Run:  python broker.py [--port 1883]
"""
import argparse
import asyncio
import itertools

from protocol import send_msg, recv_msg, topic_match

RETX_TIMEOUT = 0.2   # seconds to wait for a PUBACK before retransmitting
MAX_RETRIES = 5      # retransmissions after the first send

clients = {}         # client_id -> Conn
subs = []            # [(client_id, filter, qos)]
retained = {}        # topic -> PUBLISH msg
seen_pubs = {}       # client_id -> set of publisher packet ids already routed
pending = {}         # (client_id, delivery_id) -> asyncio.Event
delivery_ids = itertools.count(1)
stats = {"pub_in": 0, "delivered": 0, "retransmits": 0, "dup_pub": 0, "delivery_failed": 0}


class Conn:
    """Writer plus a lock, so concurrent delivery tasks never interleave frames."""

    def __init__(self, writer):
        self.writer = writer
        self.lock = asyncio.Lock()

    async def send(self, msg):
        async with self.lock:
            await send_msg(self.writer, msg)


async def deliver(cid, msg, qos):
    """Send one message to one subscriber; QoS 1 retransmits until PUBACKed."""
    conn = clients.get(cid)
    if conn is None:
        return
    try:
        if qos == 0:
            await conn.send(dict(msg, qos=0))
            stats["delivered"] += 1
            return
        did = next(delivery_ids)               # own id, never the publisher's id
        out = dict(msg, qos=1, id=did)
        ev = asyncio.Event()
        pending[(cid, did)] = ev
        try:
            for attempt in range(MAX_RETRIES + 1):
                if attempt:
                    stats["retransmits"] += 1
                await conn.send(out)
                try:
                    await asyncio.wait_for(ev.wait(), RETX_TIMEOUT)
                    stats["delivered"] += 1
                    return
                except asyncio.TimeoutError:
                    pass
            stats["delivery_failed"] += 1
        finally:
            pending.pop((cid, did), None)
    except (ConnectionError, OSError):
        pass


def route(msg):
    """Fan a PUBLISH out to every matching subscription (one task each)."""
    topic = msg["topic"]
    msg = dict(msg, retain=False)
    for cid, flt, sqos in list(subs):
        if topic_match(flt, topic):
            asyncio.create_task(deliver(cid, msg, min(msg["qos"], sqos)))


async def handle(reader, writer):
    conn, cid = Conn(writer), None
    try:
        while True:
            msg = await recv_msg(reader)
            t = msg.get("type")
            if t == "CONNECT":
                cid = msg["client_id"]
                clients[cid] = conn
                seen_pubs[cid] = set()
                await conn.send({"type": "CONNACK"})
            elif t == "SUBSCRIBE":
                subs.append((cid, msg["topic"], msg.get("qos", 0)))
                await conn.send({"type": "SUBACK", "topic": msg["topic"]})
                for topic, r in list(retained.items()):
                    if topic_match(msg["topic"], topic):
                        asyncio.create_task(
                            deliver(cid, dict(r, retain=True), min(r["qos"], msg.get("qos", 0))))
            elif t == "PUBLISH":
                if msg.get("qos", 0) == 1:
                    await conn.send({"type": "PUBACK", "id": msg["id"]})  # always ACK
                    seen = seen_pubs.setdefault(cid, set())
                    if msg["id"] in seen:
                        stats["dup_pub"] += 1
                        continue
                    seen.add(msg["id"])
                stats["pub_in"] += 1
                if msg.get("retain"):
                    if msg.get("payload") == "":
                        retained.pop(msg["topic"], None)   # empty retained payload clears it
                    else:
                        retained[msg["topic"]] = msg
                route(msg)
            elif t == "PUBACK":
                ev = pending.get((cid, msg["id"]))
                if ev:
                    ev.set()
            elif t == "STATS":
                await conn.send({"type": "STATS", "stats": dict(stats)})
    except (asyncio.IncompleteReadError, ConnectionError, OSError):
        pass
    finally:
        if cid is not None and clients.get(cid) is conn:
            del clients[cid]
            subs[:] = [s for s in subs if s[0] != cid]
        writer.close()


async def main(port):
    server = await asyncio.start_server(handle, "0.0.0.0", port)
    print(f"broker listening on {port}", flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=1883)
    asyncio.run(main(ap.parse_args().port))
