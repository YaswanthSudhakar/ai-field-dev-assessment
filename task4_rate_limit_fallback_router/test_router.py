from __future__ import annotations

import asyncio
import json
import os
import multiprocessing as mp
import socket
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import httpx
import pytest

from app import create_app, hash_key
from rate_limiter import Denied, Reservation, SlidingWindowTokenLimiter
from router import GatewayError, HttpProvider, ModelRouter, ProviderResult

HERE = Path(__file__).parent


class FakeClock:
    def __init__(self, t: float = 1_000_000.0):
        self.t = t

    def __call__(self) -> float:
        return self.t


@pytest.fixture
def db(tmp_path):
    return tmp_path / "rl.sqlite3"


def test_allows_until_limit_then_denies(db):
    rl = SlidingWindowTokenLimiter(db, limit_tokens=50_000, clock=FakeClock())
    assert isinstance(rl.reserve_sync("acme", 30_000), Reservation)
    assert isinstance(rl.reserve_sync("acme", 20_000), Reservation)
    d = rl.reserve_sync("acme", 1)
    assert isinstance(d, Denied) and d.used == 50_000


def test_sliding_window_eviction_and_retry_after(db):
    clk = FakeClock()
    rl = SlidingWindowTokenLimiter(db, limit_tokens=50_000, window_s=60, clock=clk)
    rl.reserve_sync("acme", 40_000)
    clk.t += 30
    rl.reserve_sync("acme", 10_000)
    clk.t += 29
    d = rl.reserve_sync("acme", 5_000)
    assert isinstance(d, Denied)
    assert d.retry_after_s == pytest.approx(1.0)
    clk.t += 1.001
    assert isinstance(rl.reserve_sync("acme", 5_000), Reservation)
    assert rl.usage_sync("acme") == 15_000
    import sqlite3
    rows = sqlite3.connect(db).execute("SELECT COUNT(*) FROM token_usage WHERE tenant='acme'").fetchone()[0]
    assert rows == 2


def test_retry_after_needs_multiple_rows_to_expire(db):
    clk = FakeClock()
    rl = SlidingWindowTokenLimiter(db, limit_tokens=100, window_s=60, clock=clk)
    for _ in range(4):
        rl.reserve_sync("t", 25)
        clk.t += 10
    d = rl.reserve_sync("t", 60)
    assert d.retry_after_s == pytest.approx(20 + 60 - 40)


def test_tenants_are_isolated(db):
    rl = SlidingWindowTokenLimiter(db, limit_tokens=1000, clock=FakeClock())
    assert isinstance(rl.reserve_sync("a", 1000), Reservation)
    assert isinstance(rl.reserve_sync("b", 1000), Reservation)
    assert isinstance(rl.reserve_sync("a", 1), Denied)


def test_request_larger_than_budget_never_fits(db):
    rl = SlidingWindowTokenLimiter(db, limit_tokens=1000, clock=FakeClock())
    d = rl.reserve_sync("a", 1001)
    assert isinstance(d, Denied) and d.retry_after_s == float("inf")


def test_commit_reconciles_and_release_refunds(db):
    rl = SlidingWindowTokenLimiter(db, limit_tokens=10_000, clock=FakeClock())
    r1 = rl.reserve_sync("a", 4000)
    rl.commit_sync(r1, 1200)
    r2 = rl.reserve_sync("a", 3000)
    rl.release_sync(r2)
    assert rl.usage_sync("a") == 1200


def test_state_persists_on_disk_across_instances(db):
    clk = FakeClock()
    SlidingWindowTokenLimiter(db, limit_tokens=1000, clock=clk).reserve_sync("a", 900)
    again = SlidingWindowTokenLimiter(db, limit_tokens=1000, clock=clk)
    assert again.usage_sync("a") == 900
    assert isinstance(again.reserve_sync("a", 200), Denied)


def test_purge_expired(db):
    clk = FakeClock()
    rl = SlidingWindowTokenLimiter(db, limit_tokens=1000, clock=clk)
    rl.reserve_sync("a", 1)
    rl.reserve_sync("b", 1)
    clk.t += 61
    assert rl.purge_expired_sync() == 2


async def test_concurrent_async_reservations_never_overshoot(db):
    rl = SlidingWindowTokenLimiter(db, limit_tokens=50_000)
    results = await asyncio.gather(*[rl.reserve("acme", 2_000) for _ in range(60)])
    granted = [r for r in results if isinstance(r, Reservation)]
    assert len(granted) == 25
    assert await rl.usage("acme") == 50_000


def _worker(db_path: str, n: int, q) -> None:
    rl = SlidingWindowTokenLimiter(db_path, limit_tokens=50_000)
    q.put(sum(isinstance(rl.reserve_sync("acme", 1_000), Reservation) for _ in range(n)))


def test_multi_process_workers_share_one_budget(db):
    SlidingWindowTokenLimiter(db)
    ctx = mp.get_context("spawn")
    q = ctx.Queue()
    procs = [ctx.Process(target=_worker, args=(str(db), 30, q)) for _ in range(4)]
    for p in procs:
        p.start()
    for p in procs:
        p.join(30)
    assert sum(q.get() for _ in procs) == 50


@dataclass
class FakeProvider:
    name: str
    behaviour: str = "ok"
    timeout_s: float = 0.2
    delay_s: float = 0.0
    calls: int = 0
    cancelled: int = 0
    completed: int = 0

    async def complete(self, request):
        self.calls += 1
        try:
            if self.behaviour == "slow":
                await asyncio.sleep(10)
            if self.delay_s:
                await asyncio.sleep(self.delay_s)
        except asyncio.CancelledError:
            self.cancelled += 1
            raise
        if self.behaviour == "conn":
            raise httpx.ConnectError("refused")
        self.completed += 1
        if self.behaviour == "429":
            return ProviderResult(429, {"error": "slow down"}, {"retry-after": "30"})
        if self.behaviour == "500":
            return ProviderResult(500, {"trace": "Traceback ... password=hunter2"})
        if self.behaviour == "400":
            return ProviderResult(400, {"error": "bad messages field; internal ref 0xdead"})
        if self.behaviour == "garbage":
            return ProviderResult(200, None)
        return ProviderResult(200, {"choices": [{"message": {"content": f"hi from {self.name}"}}],
                                    "usage": {"total_tokens": 42}})


async def test_primary_ok():
    p, s = FakeProvider("primary"), FakeProvider("secondary")
    r = await ModelRouter([p, s]).complete({"messages": []})
    assert r.provider == "primary" and s.calls == 0


@pytest.mark.parametrize("failure", ["429", "slow", "500", "conn", "garbage"])
async def test_failover_to_secondary(failure):
    p, s = FakeProvider("primary", failure), FakeProvider("secondary")
    r = await ModelRouter([p, s]).complete({"messages": []})
    assert r.provider == "secondary"
    assert r.attempts[0]["provider"] == "primary" and r.attempts[0]["outcome"] != "ok"


async def test_timeout_cancels_primary_and_respects_deadline():
    p, s = FakeProvider("primary", "slow", timeout_s=0.3), FakeProvider("secondary")
    t0 = time.perf_counter()
    r = await ModelRouter([p, s]).complete({})
    elapsed = time.perf_counter() - t0
    assert r.provider == "secondary"
    assert 0.3 <= elapsed < 0.45
    assert p.cancelled == 1 and p.completed == 0


async def test_answer_just_before_deadline_wins_once():
    p = FakeProvider("primary", delay_s=0.15, timeout_s=0.2)
    s = FakeProvider("secondary")
    r = await ModelRouter([p, s]).complete({})
    assert r.provider == "primary" and s.calls == 0 and p.cancelled == 0


async def test_concurrent_requests_each_get_independent_deadlines():
    p = FakeProvider("primary", delay_s=0.1, timeout_s=0.2)
    s = FakeProvider("secondary")
    router = ModelRouter([p, s])
    results = await asyncio.gather(*[router.complete({}) for _ in range(50)])
    assert all(r.provider == "primary" for r in results)
    assert p.cancelled == 0 and s.calls == 0


async def test_400_does_not_fail_over_and_is_sanitised():
    p, s = FakeProvider("primary", "400"), FakeProvider("secondary")
    with pytest.raises(GatewayError) as ei:
        await ModelRouter([p, s]).complete({})
    assert ei.value.status == 400 and s.calls == 0
    assert "0xdead" not in json.dumps(ei.value.payload("r"))


async def test_all_fail_gives_standard_503_without_leaks():
    p, s = FakeProvider("primary", "500"), FakeProvider("secondary", "slow")
    with pytest.raises(GatewayError) as ei:
        await ModelRouter([p, s]).complete({})
    payload = json.dumps(ei.value.payload("req_1"))
    assert ei.value.status == 503 and ei.value.code == "all_providers_unavailable"
    for leak in ("Traceback", "hunter2", "primary", "secondary", "timeout"):
        assert leak not in payload


async def test_429_retry_after_puts_primary_in_cooldown():
    clk = FakeClock()
    p, s = FakeProvider("primary", "429"), FakeProvider("secondary")
    router = ModelRouter([p, s], clock=clk)
    await router.complete({})
    assert p.calls == 1
    p.behaviour = "ok"
    clk.t += 10
    r = await router.complete({})
    assert r.provider == "secondary" and p.calls == 1
    clk.t += 21
    r = await router.complete({})
    assert r.provider == "primary"


KEY_A, KEY_B = "sk-tenant-a", "sk-tenant-b"


def make_gateway(db, primary="ok", secondary="ok", limit=50_000, clock=None):
    p, s = FakeProvider("primary", primary), FakeProvider("secondary", secondary)
    limiter = SlidingWindowTokenLimiter(db, limit_tokens=limit, clock=clock or time.time)
    app = create_app(limiter, ModelRouter([p, s]), {hash_key(KEY_A): "acme", hash_key(KEY_B): "globex"})
    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw")
    return client, limiter, p, s


def body(max_tokens=100, text="hello"):
    return {"model": "any", "max_tokens": max_tokens, "messages": [{"role": "user", "content": text}]}


async def post(c, payload, key=KEY_A):
    return await c.post("/v1/chat/completions", json=payload, headers={"authorization": f"Bearer {key}"})


async def test_app_success_commits_actual_usage(db):
    c, rl, p, s = make_gateway(db)
    async with c:
        r = await post(c, body(max_tokens=1000))
    assert r.status_code == 200 and r.headers["x-gateway-provider"] == "primary"
    assert rl.usage_sync("acme") == 42


async def test_app_failover_header(db):
    c, *_ = make_gateway(db, primary="429")
    async with c:
        r = await post(c, body())
    assert r.headers["x-gateway-provider"] == "secondary"
    assert r.json()["choices"][0]["message"]["content"] == "hi from secondary"


async def test_app_rate_limit_429_with_retry_after(db):
    c, rl, *_ = make_gateway(db, limit=1000)
    async with c:
        ok = await post(c, body(max_tokens=600))
        rl.commit_sync(Reservation(1, "acme", 0), 600)
        denied = await post(c, body(max_tokens=600))
        other_tenant = await post(c, body(max_tokens=600), key=KEY_B)
    assert ok.status_code == 200
    assert denied.status_code == 429
    err = denied.json()["error"]
    assert err["code"] == "rate_limit_exceeded" and err["type"] == "gateway_error"
    assert int(denied.headers["retry-after"]) >= 1 and err["request_id"].startswith("req_")
    assert other_tenant.status_code == 200


async def test_app_refunds_tokens_when_all_providers_fail(db):
    c, rl, *_ = make_gateway(db, primary="500", secondary="conn")
    async with c:
        r = await post(c, body(max_tokens=5000))
    assert r.status_code == 503
    assert rl.usage_sync("acme") == 0
    assert set(r.json()["error"]) == {"type", "code", "message", "request_id", "retry_after_seconds"}
    assert "hunter2" not in r.text and "Traceback" not in r.text


async def test_app_internal_exception_is_sanitised(db, monkeypatch):
    c, rl, p, s = make_gateway(db)

    async def explode(_):
        raise RuntimeError("KeyError at /srv/secret/path.py line 12, db=10.0.0.5")
    monkeypatch.setattr(p, "complete", explode)
    monkeypatch.setattr(s, "complete", explode)
    async with c:
        r = await post(c, body())
    assert r.status_code == 500
    assert r.json()["error"]["code"] == "internal_error"
    assert "secret" not in r.text and "10.0.0.5" not in r.text
    assert rl.usage_sync("acme") == 0


@pytest.mark.parametrize("payload, code", [
    ({"messages": []}, "invalid_request"),
    ({"messages": "hi"}, "invalid_request"),
    ({"messages": [{"role": "user", "content": "x"}], "max_tokens": -1}, "invalid_request"),
    ({"messages": [{"role": "user", "content": "x"}], "max_tokens": 60_000}, "request_too_large"),
])
async def test_app_validation(db, payload, code):
    c, *_ = make_gateway(db)
    async with c:
        r = await post(c, payload)
    assert r.status_code == 400 and r.json()["error"]["code"] == code


async def test_app_auth(db):
    c, *_ = make_gateway(db)
    async with c:
        r = await post(c, body(), key="sk-wrong")
        r2 = await c.post("/v1/chat/completions", json=body())
    assert r.status_code == 401 == r2.status_code
    assert r.json()["error"]["code"] == "invalid_api_key"


def _free_port():
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p


@pytest.fixture(scope="module")
def real_providers():
    ports = {"primary": _free_port(), "secondary": _free_port()}
    procs = []
    for name, port in ports.items():
        env = {**os.environ, "PROVIDER_NAME": name, "SLOW_S": "6"}  # Windows needs SYSTEMROOT etc.
        procs.append(subprocess.Popen([sys.executable, "-m", "uvicorn", "mock_provider:app", "--port", str(port)],
                                      cwd=HERE, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
    for port in ports.values():
        for _ in range(100):
            try:
                socket.create_connection(("127.0.0.1", port), 0.1).close(); break
            except OSError:
                time.sleep(0.05)
    yield ports
    for p in procs:
        p.terminate(); p.wait(5)


@dataclass
class ModeProvider(HttpProvider):
    mode: str = "ok"

    async def complete(self, request):
        async with httpx.AsyncClient(headers={"x-mock-mode": self.mode}) as c:
            self.client = c
            try:
                return await super().complete(request)
            finally:
                self.client = None


async def test_e2e_real_3000ms_timeout_failover(real_providers):
    primary = ModeProvider("primary", f"http://127.0.0.1:{real_providers['primary']}", "big", mode="slow")
    secondary = ModeProvider("secondary", f"http://127.0.0.1:{real_providers['secondary']}", "small")
    assert primary.timeout_s == 3.0
    t0 = time.perf_counter()
    r = await ModelRouter([primary, secondary]).complete(body())
    elapsed = time.perf_counter() - t0
    assert r.provider == "secondary"
    assert 3.0 <= elapsed < 3.6, elapsed


async def test_e2e_real_429_and_leaky_500(real_providers):
    url = lambda n: f"http://127.0.0.1:{real_providers[n]}"
    r = await ModelRouter([ModeProvider("primary", url("primary"), "m", mode="429"),
                           ModeProvider("secondary", url("secondary"), "m")]).complete(body())
    assert r.provider == "secondary" and r.body["choices"][0]["message"]["content"] == "Hello from secondary"

    with pytest.raises(GatewayError) as ei:
        await ModelRouter([ModeProvider("primary", url("primary"), "m", mode="500"),
                           ModeProvider("secondary", url("secondary"), "m", mode="500")]).complete(body())
    assert "hunter2" not in json.dumps(ei.value.payload("x"))
