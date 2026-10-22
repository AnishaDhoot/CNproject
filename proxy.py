"""Lossy proxy: sits between clients (port 1884) and the broker (port 1883).

Drops whole PUBLISH / PUBACK frames with probability --loss (in both directions)
and optionally delays every frame. Whole frames are dropped, never raw bytes,
so the length-prefix framing stays intact. Because TCP is reliable underneath,
this emulates loss at the *application-message* level.

Run:  python proxy.py --loss 0.1 [--delay 0.0] [--port 1884] [--broker-port 1883]
"""
import argparse
import asyncio
import json
import random
import struct

DROPPABLE = {"PUBLISH", "PUBACK"}   # never drop CONNECT/SUBSCRIBE/STATS/acks used in setup


async def pipe(reader, writer, loss, delay, counters, direction="", on_drop=None):
    """Copy frames one way. `loss` is a probability or a function returning one,
    so a caller can change the rate while the proxy runs."""
    try:
        while True:
            hdr = await reader.readexactly(4)
            (n,) = struct.unpack(">I", hdr)
            body = await reader.readexactly(n)
            msg = json.loads(body)
            p = loss() if callable(loss) else loss
            if msg["type"] in DROPPABLE and random.random() < p:
                counters["dropped"] += 1
                if on_drop:
                    on_drop(direction, msg)
                continue
            if delay:
                await asyncio.sleep(delay)
            writer.write(hdr + body)         # forward the original raw bytes
            await writer.drain()
    except (asyncio.IncompleteReadError, ConnectionError, OSError):
        pass
    finally:
        writer.close()


async def start_proxy(loss, delay=0.0, port=1884, broker_host="127.0.0.1", broker_port=1883, on_drop=None):
    counters = {"dropped": 0}

    async def handle(creader, cwriter):
        try:
            breader, bwriter = await asyncio.open_connection(broker_host, broker_port)
        except OSError:
            cwriter.close()
            return
        await asyncio.gather(pipe(creader, bwriter, loss, delay, counters, "c2b", on_drop),
                             pipe(breader, cwriter, loss, delay, counters, "b2c", on_drop))

    server = await asyncio.start_server(handle, "0.0.0.0", port)
    return server, counters


async def main(a):
    server, counters = await start_proxy(a.loss, a.delay, a.port, "127.0.0.1", a.broker_port)
    print(f"proxy on {a.port} -> broker {a.broker_port}, loss={a.loss}, delay={a.delay}s", flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--loss", type=float, default=0.0)
    ap.add_argument("--delay", type=float, default=0.0)
    ap.add_argument("--port", type=int, default=1884)
    ap.add_argument("--broker-port", type=int, default=1883)
    asyncio.run(main(ap.parse_args()))
