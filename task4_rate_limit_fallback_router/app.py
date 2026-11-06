"""LLM gateway for Task 4: per-tenant token limits plus primary/secondary failover.

Tokens are reserved before the call, corrected to the real usage afterwards,
and released if the call fails. Every error goes out in the same
gateway_error format with a request_id, never the upstream details.

Run with: uvicorn app:create_app_from_env --factory --port 8200
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import sys
import uuid
from contextlib import asynccontextmanager
from typing import Any

import httpx
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from rate_limiter import Denied, SlidingWindowTokenLimiter
from router import GatewayError, HttpProvider, ModelRouter, Provider

logging.basicConfig(stream=sys.stderr, level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("llm-gateway")

DEFAULT_MAX_TOKENS = 512
MAX_REQUEST_TOKENS = 50_000


def hash_key(api_key: str) -> str:
    return hashlib.sha256(api_key.encode()).hexdigest()


def estimate_tokens(body: dict[str, Any]) -> int:
    """Rough estimate (about 4 chars per token). It gets corrected to the real usage after the call."""
    chars = 0
    for m in body["messages"]:
        content = m.get("content")
        if isinstance(content, str):
            chars += len(content)
        elif isinstance(content, list):
            chars += sum(len(p.get("text", "")) for p in content if isinstance(p, dict))
    prompt = math.ceil(chars / 4) + 4 * len(body["messages"]) + 3
    completion = body.get("max_tokens") or body.get("max_completion_tokens") or DEFAULT_MAX_TOKENS
    return prompt + int(completion)


def error_response(err: GatewayError, request_id: str) -> JSONResponse:
    headers = {"x-request-id": request_id}
    if err.retry_after_s is not None and math.isfinite(err.retry_after_s):
        headers["retry-after"] = str(max(1, math.ceil(err.retry_after_s)))
    return JSONResponse(err.payload(request_id), status_code=err.status, headers=headers)


def validate(body: Any) -> dict[str, Any]:
    if not isinstance(body, dict):
        raise GatewayError(400, "invalid_request", "Body must be a JSON object")
    msgs = body.get("messages")
    if not isinstance(msgs, list) or not msgs or not all(isinstance(m, dict) for m in msgs):
        raise GatewayError(400, "invalid_request", "'messages' must be a non-empty list of objects")
    mt = body.get("max_tokens")
    if mt is not None and (not isinstance(mt, int) or isinstance(mt, bool) or mt <= 0):
        raise GatewayError(400, "invalid_request", "'max_tokens' must be a positive integer")
    if body.get("stream"):
        raise GatewayError(400, "invalid_request", "Streaming is not supported on this endpoint")
    return body


def create_app(limiter: SlidingWindowTokenLimiter, router: ModelRouter, tenant_keys: dict[str, str]) -> Starlette:
    """tenant_keys maps sha256(api_key) to a tenant id."""

    async def chat(request: Request) -> Response:
        request_id = f"req_{uuid.uuid4().hex[:16]}"
        reservation = None
        try:
            auth = request.headers.get("authorization", "")
            scheme, _, key = auth.partition(" ")
            tenant = tenant_keys.get(hash_key(key.strip())) if scheme.lower() == "bearer" and key else None
            if tenant is None:
                raise GatewayError(401, "invalid_api_key", "Missing or invalid API key")

            try:
                body = validate(json.loads(await request.body()))
            except (json.JSONDecodeError, UnicodeDecodeError):
                raise GatewayError(400, "invalid_json", "Request body must be valid JSON")

            est = estimate_tokens(body)
            if est > limiter.limit:
                raise GatewayError(400, "request_too_large",
                                   f"Request needs ~{est} tokens; the per-minute budget is {limiter.limit}")
            decision = await limiter.reserve(tenant, est)
            if isinstance(decision, Denied):
                log.info("rate limited tenant=%s used=%d req=%d", tenant, decision.used, est)
                raise GatewayError(429, "rate_limit_exceeded",
                                   f"Token rate limit exceeded ({decision.limit} tokens/min)",
                                   retry_after_s=decision.retry_after_s)
            reservation = decision

            routed = await router.complete(body)

            actual = (routed.body.get("usage") or {}).get("total_tokens")
            await limiter.commit(reservation, int(actual) if isinstance(actual, int) else est)
            reservation = None
            return JSONResponse(routed.body, headers={"x-request-id": request_id,
                                                      "x-gateway-provider": routed.provider,
                                                      "x-gateway-attempts": str(len(routed.attempts))})
        except GatewayError as e:
            return error_response(e, request_id)
        except Exception:
            log.exception("unhandled error request_id=%s", request_id)
            return error_response(GatewayError(500, "internal_error", "Internal gateway error"), request_id)
        finally:
            if reservation is not None:  # the call failed, so give the tokens back
                try:
                    await limiter.release(reservation)
                except Exception:
                    log.exception("failed to release reservation %s", reservation.id)

    async def health(_: Request) -> Response:
        return JSONResponse({"status": "ok"})

    return Starlette(routes=[Route("/v1/chat/completions", chat, methods=["POST"]),
                             Route("/healthz", health, methods=["GET"])])


def create_app_from_env() -> Starlette:
    shared = httpx.AsyncClient(limits=httpx.Limits(max_connections=200))
    providers: list[Provider] = [
        HttpProvider("primary", os.environ.get("PRIMARY_URL", "http://127.0.0.1:9201"),
                     os.environ.get("PRIMARY_MODEL", "big-model"), os.environ.get("PRIMARY_KEY", ""),
                     timeout_s=float(os.environ.get("PRIMARY_TIMEOUT_MS", "3000")) / 1000, client=shared),
        HttpProvider("secondary", os.environ.get("SECONDARY_URL", "http://127.0.0.1:9202"),
                     os.environ.get("SECONDARY_MODEL", "backup-model"), os.environ.get("SECONDARY_KEY", ""),
                     timeout_s=float(os.environ.get("SECONDARY_TIMEOUT_MS", "3000")) / 1000, client=shared),
    ]
    keys = {}
    for pair in os.environ.get("TENANT_KEYS", "sk-demo-key:demo").split(","):
        k, _, t = pair.partition(":")
        keys[hash_key(k.strip())] = t.strip()
    limiter = SlidingWindowTokenLimiter(os.environ.get("RATE_DB", "ratelimit.sqlite3"),
                                        limit_tokens=int(os.environ.get("TOKENS_PER_MINUTE", "50000")))
    app = create_app(limiter, ModelRouter(providers), keys)

    @asynccontextmanager
    async def lifespan(_):
        yield
        await shared.aclose()

    app.router.lifespan_context = lifespan
    return app

