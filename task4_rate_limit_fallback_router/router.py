"""Sends requests to the primary provider and fails over to the secondary on
429, a 3 s timeout, 5xx or a connection error.

Other 4xx errors don't fail over, since the backup would reject the same
request. asyncio.timeout cancels the slow call, so a late reply can't be
used or billed twice.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Protocol

import httpx

log = logging.getLogger("llm-router")


class GatewayError(Exception):
    """The only error type that reaches the client."""

    def __init__(self, status: int, code: str, message: str, retry_after_s: float | None = None):
        super().__init__(message)
        self.status, self.code, self.message, self.retry_after_s = status, code, message, retry_after_s

    def payload(self, request_id: str) -> dict[str, Any]:
        err: dict[str, Any] = {"type": "gateway_error", "code": self.code, "message": self.message,
                               "request_id": request_id}
        if self.retry_after_s is not None:
            err["retry_after_seconds"] = round(self.retry_after_s, 3)
        return {"error": err}


@dataclass
class ProviderResult:
    status: int
    body: dict[str, Any] | None
    headers: dict[str, str] = field(default_factory=dict)


class Provider(Protocol):
    name: str
    timeout_s: float

    async def complete(self, request: dict[str, Any]) -> ProviderResult: ...


class AttemptFailed(Exception):
    def __init__(self, reason: str, retry_after_s: float | None = None):
        super().__init__(reason)
        self.reason, self.retry_after_s = reason, retry_after_s


@dataclass
class HttpProvider:
    name: str
    base_url: str
    model: str
    api_key: str = ""
    timeout_s: float = 3.0
    client: httpx.AsyncClient | None = None

    async def complete(self, request: dict[str, Any]) -> ProviderResult:
        body = {**request, "model": self.model, "stream": False}
        headers = {"authorization": f"Bearer {self.api_key}"} if self.api_key else {}
        client = self.client or httpx.AsyncClient()
        try:
            r = await client.post(f"{self.base_url}/v1/chat/completions", json=body, headers=headers,
                                  timeout=None)  # the router's asyncio.timeout owns the deadline
            try:
                data = r.json()
            except ValueError:
                data = None
            return ProviderResult(status=r.status_code, body=data, headers=dict(r.headers))
        finally:
            if self.client is None:
                await client.aclose()


def _parse_retry_after(headers: dict[str, str]) -> float | None:
    v = {k.lower(): val for k, val in headers.items()}.get("retry-after")
    try:
        return max(0.0, float(v)) if v is not None else None
    except ValueError:
        return None


@dataclass
class RoutedResponse:
    provider: str
    body: dict[str, Any]
    attempts: list[dict[str, Any]]


class ModelRouter:
    FAILOVER_STATUSES = {429, 500, 502, 503, 504}

    def __init__(self, providers: list[Provider], clock=time.monotonic) -> None:
        if not providers:
            raise ValueError("at least one provider required")
        self.providers = providers
        self.clock = clock
        self._cooldown_until: dict[str, float] = {}

    async def _attempt(self, p: Provider, request: dict[str, Any]) -> dict[str, Any]:
        try:
            async with asyncio.timeout(p.timeout_s):
                res = await p.complete(request)
        except TimeoutError:
            raise AttemptFailed(f"timeout after {p.timeout_s * 1000:.0f}ms")
        except httpx.HTTPError as exc:
            raise AttemptFailed(f"transport error: {type(exc).__name__}")

        if res.status == 429:
            raise AttemptFailed("429 rate limited", _parse_retry_after(res.headers))
        if res.status in self.FAILOVER_STATUSES:
            raise AttemptFailed(f"upstream {res.status}")
        if res.status >= 400:
            log.warning("provider %s rejected request with %s: %s", p.name, res.status, res.body)
            code = {400: "invalid_request", 401: "upstream_auth_failed", 403: "upstream_auth_failed",
                    404: "model_not_found", 413: "request_too_large", 422: "invalid_request"}.get(res.status,
                                                                                         "upstream_rejected")
            status = 400 if res.status in (400, 404, 413, 422) else 502
            raise GatewayError(status, code, "The model provider rejected the request")
        if not isinstance(res.body, dict) or not res.body.get("choices"):
            raise AttemptFailed("malformed success response")
        return res.body

    async def complete(self, request: dict[str, Any]) -> RoutedResponse:
        attempts: list[dict[str, Any]] = []
        now = self.clock()
        candidates = [p for p in self.providers if self._cooldown_until.get(p.name, 0) <= now]
        if not candidates:
            candidates = list(self.providers)

        for p in candidates:
            t0 = self.clock()
            try:
                body = await self._attempt(p, request)
            except AttemptFailed as f:
                elapsed = (self.clock() - t0) * 1000
                attempts.append({"provider": p.name, "outcome": f.reason, "ms": round(elapsed)})
                log.warning("provider %s failed (%s) after %.0fms; failing over", p.name, f.reason, elapsed)
                if f.retry_after_s:
                    self._cooldown_until[p.name] = self.clock() + f.retry_after_s
                continue
            attempts.append({"provider": p.name, "outcome": "ok", "ms": round((self.clock() - t0) * 1000)})
            return RoutedResponse(provider=p.name, body=body, attempts=attempts)

        log.error("all providers failed: %s", attempts)
        raise GatewayError(503, "all_providers_unavailable",
                           "All model providers are currently unavailable. Please retry shortly.",
                           retry_after_s=1.0)
