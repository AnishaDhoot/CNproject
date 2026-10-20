import asyncio
import os
import subprocess
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from client import Client
from proxy import start_proxy

BROKER, PROXY = 18830, 18840


async def wait_port(port):
    for _ in range(50):
        try:
            _, w = await asyncio.open_connection("127.0.0.1", port)
            w.close()
            return
        except OSError:
            await asyncio.sleep(0.1)
    raise RuntimeError("broker did not start")


class E2E(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.proc = subprocess.Popen([sys.executable, os.path.join(ROOT, "broker.py"),
                                      "--port", str(BROKER)], stdout=subprocess.DEVNULL)
        await wait_port(BROKER)

    async def asyncTearDown(self):
        self.proc.kill()
        self.proc.wait()

    async def test_wildcards_and_retained(self):
        got = []
        sub = Client(lambda m, l: got.append(m["topic"]))
        await sub.connect("127.0.0.1", BROKER, "s1")
        await sub.subscribe("sensor/+/temp", 1)
        pub = Client()
        await pub.connect("127.0.0.1", BROKER, "p1")
        await pub.publish("sensor/r1/temp", "20", qos=1)
        await pub.publish("sensor/r1/humidity", "50", qos=1)
        await pub.publish("sensor/r2/temp", "21", qos=0, retain=True)
        await asyncio.sleep(0.3)
        self.assertEqual(sorted(got), ["sensor/r1/temp", "sensor/r2/temp"])
        late = []
        sub2 = Client(lambda m, l: late.append((m["topic"], m["retain"])))
        await sub2.connect("127.0.0.1", BROKER, "s2")
        await sub2.subscribe("sensor/#", 0)
        await asyncio.sleep(0.3)
        self.assertEqual(late, [("sensor/r2/temp", True)])

    async def test_qos1_survives_loss_qos0_does_not(self):
        server, _ = await start_proxy(0.3, port=PROXY, broker_port=BROKER)
        try:
            s0, s1 = Client(), Client()
            await s0.connect("127.0.0.1", PROXY, "s0")
            await s0.subscribe("q0/#", 0)
            await s1.connect("127.0.0.1", PROXY, "s1")
            await s1.subscribe("q1/#", 1)
            pub = Client()
            await pub.connect("127.0.0.1", PROXY, "pub")
            n = 100
            for i in range(n):
                await asyncio.gather(pub.publish("q0/x", str(i), qos=0),
                                     pub.publish("q1/x", str(i), qos=1))
            await asyncio.sleep(2)
            self.assertEqual(s1.received, n)       # QoS 1: everything arrives, exactly once
            self.assertLess(s0.received, n)        # QoS 0: some lost at 30% loss
        finally:
            server.close()


if __name__ == "__main__":
    unittest.main()
