"""Demo for the Task 4 gateway on :8200.

Shows a timeout failover to the secondary, the token budget running out
under 5 parallel requests, and a bad API key.
"""

import asyncio
import socket
import time

import httpx

URL = "http://127.0.0.1:8200/v1/chat/completions"
KEY = {"authorization": "Bearer sk-a"}
BODY = {"max_tokens": 600, "messages": [{"role": "user", "content": "hi"}]}

for _ in range(100):
    try:
        socket.create_connection(("127.0.0.1", 8200), 0.3).close()
        break
    except OSError:
        time.sleep(0.2)
else:
    raise SystemExit("Gateway not reachable on :8200 - is it running?")


def describe(r: httpx.Response) -> str:
    if r.status_code == 200:
        return f"200 via {r.headers['x-gateway-provider']}: {r.json()['choices'][0]['message']['content']}"
    extra = f"  Retry-After={r.headers['retry-after']}" if "retry-after" in r.headers else ""
    return f"{r.status_code} {r.json()}{extra}"


async def main():
    async with httpx.AsyncClient(timeout=30) as c:
        print("=== 1. Single request: primary is slow -> failover after 3000 ms ===")
        t0 = time.perf_counter()
        r = await c.post(URL, json=BODY, headers=KEY)
        print(f"  {describe(r)}   ({time.perf_counter() - t0:.2f}s)")

        print("\n=== 2. Five concurrent requests against a 2000 tokens/min budget ===")
        print("  (actual usage from request 1 is only ~10 tokens, so ~1990 tokens are free)")
        rs = await asyncio.gather(*[c.post(URL, json=BODY, headers=KEY) for _ in range(5)])
        for i, r in enumerate(rs, 1):
            print(f"  #{i}: {describe(r)}")

        print("\n=== 3. Invalid API key ===")
        r = await c.post(URL, json=BODY, headers={"authorization": "Bearer wrong"})
        print(f"  {describe(r)}")


asyncio.run(main())
