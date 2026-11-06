"""Streams a reply through the redacting gateway on :8100 and prints it as it arrives.
Start the mock LLM (:9100) and the gateway first.
"""

import json
import socket
import sys
import time

import httpx

for _ in range(100):
    try:
        socket.create_connection(("127.0.0.1", 8100), 0.3).close()
        break
    except OSError:
        time.sleep(0.2)
else:
    raise SystemExit("Gateway not reachable on :8100 - is it running?")

from mock_llm import DEFAULT_REPLY  # noqa: E402

print("Original model output (what the LLM really said):\n ", DEFAULT_REPLY)
print("\nWhat the client receives through the gateway, streamed live:\n  ", end="")

t0 = time.perf_counter()
first = None
with httpx.stream("POST", "http://127.0.0.1:8100/v1/chat/completions", timeout=60,
                  headers={"x-mock-delay-ms": "60"},
                  json={"stream": True, "messages": [{"role": "user", "content": "hi"}]}) as r:
    for line in r.iter_lines():
        if not line.startswith("data: ") or line == "data: [DONE]":
            continue
        text = json.loads(line[6:])["choices"][0]["delta"].get("content")
        if text:
            first = first or time.perf_counter() - t0
            sys.stdout.write(text)
            sys.stdout.flush()

print(f"\n\nTime to first text: {first * 1000:.0f} ms   total: {time.perf_counter() - t0:.1f} s")
