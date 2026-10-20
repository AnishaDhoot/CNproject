"""Scripted demo: python demo.py   (starts its own broker and proxy)"""
import asyncio
import os
import subprocess
import sys

from client import Client

HERE = os.path.dirname(os.path.abspath(__file__))
B, P = 1883, 1884


def spawn(script, *args):
    return subprocess.Popen([sys.executable, os.path.join(HERE, script), *map(str, args)],
                            stdout=subprocess.DEVNULL)


async def wait_port(port):
    for _ in range(100):
        try:
            _, w = await asyncio.open_connection("127.0.0.1", port)
            w.close()
            return
        except OSError:
            await asyncio.sleep(0.1)


def step(text):
    print(f"\n=== {text} ===")


async def main():
    broker = spawn("broker.py", "--port", B)
    proxy = None
    try:
        await wait_port(B)

        step("1. Wildcard subscriber on sensor/+/temp")
        sub = Client(lambda m, l: print(f"   subscriber got {m['topic']} = {m['payload']} "
                                        f"(qos {m['qos']}, retain={m['retain']})"))
        await sub.connect("127.0.0.1", B, "demo-sub")
        await sub.subscribe("sensor/+/temp", 1)
        pub = Client()
        await pub.connect("127.0.0.1", B, "demo-pub")

        step("2. Publish sensor/room1/temp -> should MATCH")
        await pub.publish("sensor/room1/temp", "23.5", qos=1)
        await asyncio.sleep(0.3)

        step("3. Publish sensor/room1/humidity -> should NOT match (nothing printed)")
        await pub.publish("sensor/room1/humidity", "61", qos=1)
        await asyncio.sleep(0.3)

        step("4. Retained message, then a brand-new subscriber")
        await pub.publish("sensor/room2/temp", "19.0", qos=1, retain=True)
        await asyncio.sleep(0.2)
        print("   new subscriber connects now:")
        late = Client(lambda m, l: print(f"   late subscriber got {m['topic']} = {m['payload']} "
                                         f"(retain={m['retain']})"))
        await late.connect("127.0.0.1", B, "demo-late")
        await late.subscribe("sensor/#", 1)
        await asyncio.sleep(0.3)

        step("5. Proxy with 20% loss: same publisher at QoS 0 and QoS 1 (200 msgs each)")
        proxy = spawn("proxy.py", "--loss", 0.2, "--port", P, "--broker-port", B)
        await wait_port(P)
        rx = {0: set(), 1: set()}
        s = Client(lambda m, l: rx[m["qos"]].add(m["payload"]))
        await s.connect("127.0.0.1", P, "lossy-sub")
        await s.subscribe("loss/q0", 0)
        await s.subscribe("loss/q1", 1)
        p = Client()
        await p.connect("127.0.0.1", P, "lossy-pub")
        n, retries = 200, 0
        for i in range(0, n, 20):
            r = await asyncio.gather(*[p.publish("loss/q0", str(j), qos=0) for j in range(i, i + 20)],
                                     *[p.publish("loss/q1", str(j), qos=1) for j in range(i, i + 20)])
            retries += sum(r)
        await asyncio.sleep(2)
        print(f"   QoS 0: received {len(rx[0])}/{n}  -> {n - len(rx[0])} messages LOST")
        print(f"   QoS 1: received {len(rx[1])}/{n}  -> recovered via retransmission "
              f"(duplicates suppressed: {s.duplicates})")

        step("6. STATS from the broker")
        mon = Client()
        await mon.connect("127.0.0.1", B, "monitor")
        st = await mon.stats()
        for k, v in st.items():
            print(f"   {k:16s} {v}")
        print(f"   (publisher-side retries: {retries})")
    finally:
        for pr in (proxy, broker):
            if pr:
                pr.kill()
                pr.wait()


if __name__ == "__main__":
    asyncio.run(main())
