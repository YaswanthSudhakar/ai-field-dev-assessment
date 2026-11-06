from __future__ import annotations

import json
import os
import random
import socket
import subprocess
import sys
import time
import tracemalloc
from pathlib import Path

import httpx
import pytest

from redactor import REDACTED, StreamingRedactor, luhn_ok, redact

HERE = Path(__file__).parent


def stream_through(text: str, chunk_sizes) -> str:
    r = StreamingRedactor()
    out, i = [], 0
    for n in chunk_sizes:
        if i >= len(text):
            break
        out.append(r.feed(text[i:i + n]))
        i += n
    if i < len(text):
        out.append(r.feed(text[i:]))
    out.append(r.flush())
    return "".join(out)


@pytest.mark.parametrize("pii", [
    "jane.doe@example.com", "a+b_c%d-e@sub.mail.example.co.uk", "X@Y.IO",
    "123-45-6789", "123 45 6789", "123456789", "078-05-1120",
    "4111111111111111", "4111 1111 1111 1111", "4111-1111-1111-1111",
    "5500 0000 0000 0004", "3782 822463 10005", "6011000990139424",
])
def test_detects(pii):
    assert redact(f"value: {pii}.") == f"value: {REDACTED}."


@pytest.mark.parametrize("benign", [
    "2024-01-15", "call 555-1234", "order 1234567890123",
    "4111 1111 1111 1112",
    "000-12-3456", "666-12-3456", "900-12-3456", "123-00-4567", "123-45-0000",
    "123-456789",
    "user@localhost", "@handle", "ref abc4111111111111111", "email me at name at example dot com",
    "version 1.2.3", "price $1,299.99",
])
def test_ignores_benign(benign):
    assert redact(benign) == benign


def test_luhn():
    assert luhn_ok("4111111111111111") and not luhn_ok("4111111111111112")


CORPUS = (
    "Hi! Reach me at john.doe+billing@mail.example.co.uk, or jane@x.io. "
    "SSN 123-45-6789 / 987 65 4321; card 4111 1111 1111 1111 and 5500-0000-0000-0004. "
    "Ticket 1234567890123 and date 2024-01-15 are fine. Unicode café ☕ naïve — ok. "
    "Trailing email at end: last@example.org"
)


def test_every_single_split_point_matches_batch():
    ref = redact(CORPUS)
    for i in range(1, len(CORPUS)):
        assert stream_through(CORPUS, [i, len(CORPUS)]) == ref, f"split at {i}"


def test_char_by_char():
    assert stream_through(CORPUS, [1] * len(CORPUS)) == redact(CORPUS)


@pytest.mark.parametrize("seed", range(200))
def test_random_chunkings(seed):
    rnd = random.Random(seed)
    sizes = [rnd.randint(1, 12) for _ in range(len(CORPUS))]
    assert stream_through(CORPUS, sizes) == redact(CORPUS)


def test_nothing_leaks_before_redaction():
    r = StreamingRedactor()
    emitted = []
    for piece in ["Email: jo", "hn.d", "oe@ex", "ample.c", "om", " thanks"]:
        emitted.append(r.feed(piece))
    emitted.append(r.flush())
    assert "".join(emitted) == f"Email: {REDACTED} thanks"
    assert not any("john" in e or "example" in e for e in emitted)


def test_low_holdback_text_flows_immediately():
    r = StreamingRedactor()
    assert r.feed("Hello there, ") == "Hello there, "
    assert r.feed("wor") == ""
    assert r.feed("ld! ") == "world! "


def test_pending_buffer_stays_small_on_long_prose():
    r = StreamingRedactor()
    text = ("The quick brown fox emails fox@den.com about card 4111 1111 1111 1111. " * 20_000)
    tracemalloc.start()
    total = 0
    for i in range(0, len(text), 7):
        total += len(r.feed(text[i:i + 7]))
    total += len(r.flush())
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert r.peak_pending < 40
    assert peak < 200_000
    assert r.redactions == 40_000


def test_pathological_no_boundary_stream_is_capped():
    r = StreamingRedactor(max_hold=2048)
    blob = "A" * 200_000
    out = "".join(r.feed(blob[i:i + 50]) for i in range(0, len(blob), 50)) + r.flush()
    assert out == blob
    assert r.peak_pending <= 2048 + 50


def test_forced_cut_does_not_leak_pii():
    r = StreamingRedactor(max_hold=800)
    text = "B" * 1500 + "-" + "x" * 10 + "secret.person@example.com" + "-" + "C" * 3000
    out = "".join(r.feed(text[i:i + 9]) for i in range(0, len(text), 9)) + r.flush()
    assert "person@example.com" not in out and REDACTED in out
    assert r.peak_pending <= 800 + 9


def test_numbered_words_do_not_stall_the_stream():
    r = StreamingRedactor()
    assert r.feed("word0 word1 ") == "word0 word1 "
    assert r.feed("item 4111 ") == "item "
    assert r.feed("1111 1111 1111 ok ") == f"{REDACTED} ok "


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture(scope="module")
def servers():
    llm_port, gw_port = _free_port(), _free_port()
    env = {**os.environ, "UPSTREAM_BASE_URL": f"http://127.0.0.1:{llm_port}"}  # Windows needs SYSTEMROOT etc.
    procs = [
        subprocess.Popen([sys.executable, "-m", "uvicorn", "mock_llm:app", "--port", str(llm_port)],
                         cwd=HERE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL),
        subprocess.Popen([sys.executable, "-m", "uvicorn", "gateway:app", "--port", str(gw_port)],
                         cwd=HERE, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL),
    ]
    for port in (llm_port, gw_port):
        for _ in range(100):
            try:
                socket.create_connection(("127.0.0.1", port), timeout=0.1).close()
                break
            except OSError:
                time.sleep(0.05)
    yield f"http://127.0.0.1:{gw_port}"
    for p in procs:
        p.terminate()
        p.wait(5)


def _collect(url: str, reply: str | None = None, delay_ms: int = 20):
    headers = {"x-mock-delay-ms": str(delay_ms)}
    if reply is not None:
        headers["x-mock-reply"] = reply
    t0 = time.perf_counter()
    first_text_at = None
    texts, events = [], []
    with httpx.stream("POST", f"{url}/v1/chat/completions", headers=headers, timeout=30,
                      json={"model": "mock-1", "stream": True,
                            "messages": [{"role": "user", "content": "hi"}]}) as r:
        assert r.status_code == 200
        for line in r.iter_lines():
            if not line.startswith("data: "):
                continue
            data = line[6:]
            events.append(data)
            if data == "[DONE]":
                break
            delta = json.loads(data)["choices"][0]["delta"].get("content")
            if delta:
                if first_text_at is None:
                    first_text_at = time.perf_counter() - t0
                texts.append(delta)
    return "".join(texts), first_text_at, time.perf_counter() - t0, events


def test_gateway_redacts_default_reply(servers):
    from mock_llm import DEFAULT_REPLY
    text, *_ , events = _collect(servers)
    assert text == redact(DEFAULT_REPLY)
    assert "jane.doe" not in text and "123-45-6789" not in text and "4111 1111" not in text
    assert text.count(REDACTED) == 3
    assert events[-1] == "[DONE]"


def test_gateway_streams_incrementally_low_ttft(servers):
    reply = " ".join(f"word{i}" for i in range(100))
    text, ttft, total, _ = _collect(servers, reply=reply, delay_ms=20)
    assert text == reply
    assert ttft < 0.25, f"TTFT {ttft:.3f}s - gateway is buffering"
    assert total > 2.0


def test_pii_at_very_end_is_flushed_and_redacted(servers):
    text, *_ = _collect(servers, reply="contact: someone@example.com", delay_ms=1)
    assert text == f"contact: {REDACTED}"


def test_non_streaming_request_is_redacted(servers):
    r = httpx.post(f"{servers}/v1/chat/completions", headers={"x-mock-reply": "ssn 123-45-6789"},
                   json={"stream": False, "messages": [{"role": "user", "content": "x"}]})
    assert r.json()["choices"][0]["message"]["content"] == f"ssn {REDACTED}"


def test_bad_request(servers):
    r = httpx.post(f"{servers}/v1/chat/completions", content=b"nope")
    assert r.status_code == 400 and r.json()["error"]["type"] == "gateway_error"
