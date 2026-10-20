import json, struct


async def send_msg(writer, msg: dict):
    data = json.dumps(msg).encode()
    writer.write(struct.pack('>I', len(data)) + data)
    await writer.drain()


async def recv_msg(reader):
    hdr = await reader.readexactly(4)
    (n,) = struct.unpack('>I', hdr)
    return json.loads(await reader.readexactly(n))


def topic_match(flt, topic):
    f, t = flt.split('/'), topic.split('/')
    for i, part in enumerate(f):
        if part == '#':
            return True
        if i >= len(t):
            return False
        if part != '+' and part != t[i]:
            return False
    return len(f) == len(t)
