"""Pub-sub client library (QoS 0/1) with a small CLI.

    python client.py sub  "sensor/+/temp" --qos 1 [--port 1884]
    python client.py pub  sensor/room1/temp 23.5 --qos 1 [--retain]
"""
import argparse
import asyncio
import inspect
import time

from protocol import send_msg, recv_msg

RETX_TIMEOUT = 0.2
MAX_RETRIES = 5


class Client:
    def __init__(self, on_message=None):
        self.on_message = on_message           # callback(msg, latency); may be async
        self._lock = asyncio.Lock()
        self._next_id = 1
        self._acks = {}                        # pub id -> Event
        self._waiters = {}                     # "CONNACK"/"SUBACK"/"STATS" -> Future
        self._seen = set()                     # delivery ids already handed to the app
        self.duplicates = 0
        self.received = 0
        self.failed_publishes = 0              # QoS 1 publishes never ACKed
        self._reader_task = None

    async def _send(self, msg):
        async with self._lock:
            await send_msg(self.writer, msg)

    async def _expect(self, kind, msg, timeout=5.0):
        fut = asyncio.get_running_loop().create_future()
        self._waiters[kind] = fut
        await self._send(msg)
        return await asyncio.wait_for(fut, timeout)

    async def connect(self, host, port, client_id):
        self.client_id = client_id
        self.reader, self.writer = await asyncio.open_connection(host, port)
        self._reader_task = asyncio.create_task(self._read_loop())
        await self._expect("CONNACK", {"type": "CONNECT", "client_id": client_id})

    async def subscribe(self, topic, qos=0):
        await self._expect("SUBACK", {"type": "SUBSCRIBE", "topic": topic, "qos": qos})

    async def publish(self, topic, payload, qos=0, retain=False):
        """Returns the number of retransmissions used (0 for QoS 0)."""
        msg = {"type": "PUBLISH", "topic": topic, "payload": payload, "qos": qos,
               "retain": retain, "ts": time.time()}
        if qos == 0:
            await self._send(msg)
            return 0
        msg["id"] = pid = self._next_id
        self._next_id += 1
        ev = self._acks[pid] = asyncio.Event()
        try:
            for attempt in range(MAX_RETRIES + 1):
                await self._send(msg)
                try:
                    await asyncio.wait_for(ev.wait(), RETX_TIMEOUT)
                    return attempt
                except asyncio.TimeoutError:
                    pass
            self.failed_publishes += 1
            return MAX_RETRIES
        finally:
            self._acks.pop(pid, None)

    async def stats(self):
        reply = await self._expect("STATS", {"type": "STATS"})
        return reply["stats"]

    async def disconnect(self):
        if self._reader_task:
            self._reader_task.cancel()
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except (ConnectionError, OSError):
            pass

    async def _read_loop(self):
        try:
            while True:
                msg = await recv_msg(self.reader)
                t = msg["type"]
                if t in ("CONNACK", "SUBACK", "STATS"):
                    fut = self._waiters.pop(t, None)
                    if fut and not fut.done():
                        fut.set_result(msg)
                elif t == "PUBACK":
                    ev = self._acks.get(msg["id"])
                    if ev:
                        ev.set()
                elif t == "PUBLISH":
                    await self._on_publish(msg)
        except (asyncio.IncompleteReadError, ConnectionError, OSError, asyncio.CancelledError):
            pass

    async def _on_publish(self, msg):
        if msg.get("qos") == 1:
            await self._send({"type": "PUBACK", "id": msg["id"]})   # ACK every copy
            if msg["id"] in self._seen:
                self.duplicates += 1
                return
            self._seen.add(msg["id"])
        self.received += 1
        if self.on_message:
            r = self.on_message(msg, time.time() - msg["ts"])
            if inspect.isawaitable(r):
                await r


async def _cli():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["sub", "pub"])
    ap.add_argument("topic")
    ap.add_argument("payload", nargs="?", default="")
    ap.add_argument("--qos", type=int, default=0)
    ap.add_argument("--retain", action="store_true")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=1883)
    a = ap.parse_args()

    if a.mode == "sub":
        def show(msg, lat):
            print(f"[{msg['topic']}] {msg['payload']}  (qos={msg['qos']}, {lat * 1000:.1f} ms)")
        c = Client(show)
        await c.connect(a.host, a.port, f"cli-sub-{int(time.time())}")
        await c.subscribe(a.topic, a.qos)
        print(f"subscribed to {a.topic} at QoS {a.qos}; Ctrl+C to quit")
        await asyncio.Event().wait()
    else:
        c = Client()
        await c.connect(a.host, a.port, f"cli-pub-{int(time.time())}")
        retries = await c.publish(a.topic, a.payload, a.qos, a.retain)
        print(f"published (retries={retries})")
        await c.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(_cli())
    except KeyboardInterrupt:
        pass
