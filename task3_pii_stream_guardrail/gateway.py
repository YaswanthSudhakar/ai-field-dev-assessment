"""LLM gateway for Task 3: proxies chat completions and redacts PII in the stream.

Each choice gets its own StreamingRedactor. Text is passed on as soon as it's
safe, and the redactor is flushed when a choice finishes or the stream ends,
so nothing is lost and the full response is never buffered.

Run with: uvicorn gateway:app --port 8100
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

import httpx
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route

from redactor import StreamingRedactor, redact

logging.basicConfig(stream=sys.stderr, level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("llm-gateway")

UPSTREAM_BASE_URL = os.environ.get("UPSTREAM_BASE_URL", "http://127.0.0.1:9100")
UPSTREAM_API_KEY = os.environ.get("UPSTREAM_API_KEY", "")
PASS_HEADERS = ("x-mock-reply", "x-mock-delay-ms")


def _sse(data: str) -> bytes:
    return f"data: {data}\n\n".encode()


def _error(status: int, code: str, message: str) -> JSONResponse:
    return JSONResponse({"error": {"type": "gateway_error", "code": code, "message": message}},
                        status_code=status)


class StreamGuard:
    def __init__(self) -> None:
        self.redactors: dict[int, StreamingRedactor] = defaultdict(StreamingRedactor)
        self.finished: set[int] = set()
        self.last_chunk_meta: dict[str, Any] = {}

    def process(self, chunk: dict[str, Any]) -> dict[str, Any] | None:
        self.last_chunk_meta = {k: chunk[k] for k in ("id", "object", "created", "model") if k in chunk}
        choices = chunk.get("choices")
        if not isinstance(choices, list) or not choices:
            return chunk

        keep = False
        for choice in choices:
            idx = choice.get("index", 0)
            delta = choice.get("delta") or {}
            text = delta.get("content")
            if isinstance(text, str) and text:
                delta["content"] = self.redactors[idx].feed(text)
            if choice.get("finish_reason") is not None and idx not in self.finished:
                tail = self.redactors[idx].flush()
                if tail:
                    delta["content"] = (delta.get("content") or "") + tail
                self.finished.add(idx)
            choice["delta"] = delta
            if delta.get("content") or set(delta) - {"content"} or choice.get("finish_reason") is not None:
                keep = True
        return chunk if keep or "usage" in chunk else None

    def flush_unfinished(self) -> list[dict[str, Any]]:
        """Used when the stream ends before a choice sent finish_reason."""
        out = []
        for idx, r in self.redactors.items():
            if idx in self.finished:
                continue
            tail = r.flush()
            self.finished.add(idx)
            if tail:
                out.append({**self.last_chunk_meta, "object": "chat.completion.chunk",
                            "choices": [{"index": idx, "delta": {"content": tail}, "finish_reason": None}]})
        return out

    @property
    def redaction_count(self) -> int:
        return sum(r.redactions for r in self.redactors.values())


async def guarded_stream(upstream: httpx.Response) -> AsyncIterator[bytes]:
    guard, t0, first = StreamGuard(), time.perf_counter(), None
    try:
        async for line in upstream.aiter_lines():
            if not line.startswith("data:"):
                continue
            data = line[5:].strip()
            if data == "[DONE]":
                for c in guard.flush_unfinished():
                    yield _sse(json.dumps(c))
                yield _sse("[DONE]")
                return
            try:
                chunk = json.loads(data)
            except json.JSONDecodeError:
                log.warning("dropping malformed upstream SSE line")
                continue
            out = guard.process(chunk)
            if out is not None:
                if first is None:
                    first = time.perf_counter() - t0
                yield _sse(json.dumps(out))
        for c in guard.flush_unfinished():
            yield _sse(json.dumps(c))
    except httpx.HTTPError as exc:
        log.error("upstream stream broke: %s", type(exc).__name__)
        for c in guard.flush_unfinished():
            yield _sse(json.dumps(c))
        yield _sse(json.dumps({"error": {"type": "gateway_error", "code": "upstream_stream_interrupted",
                                         "message": "The model stream was interrupted"}}))
    finally:
        await upstream.aclose()
        log.info("stream done: redactions=%d ttft_gateway=%.1fms",
                 guard.redaction_count, (first or 0) * 1000)


def create_app(client: httpx.AsyncClient | None = None, base_url: str = UPSTREAM_BASE_URL,
               api_key: str = UPSTREAM_API_KEY) -> Starlette:
    state: dict[str, httpx.AsyncClient] = {"client": client} if client else {}

    @asynccontextmanager
    async def lifespan(_):
        if client is None:
            state["client"] = httpx.AsyncClient(timeout=httpx.Timeout(10.0, read=60.0))
        try:
            yield
        finally:
            if client is None:
                await state["client"].aclose()

    async def chat(request: Request) -> Response:
        try:
            body = await request.json()
        except (json.JSONDecodeError, UnicodeDecodeError):
            return _error(400, "invalid_json", "Request body must be JSON")
        if not isinstance(body, dict) or not isinstance(body.get("messages"), list):
            return _error(400, "invalid_request", "'messages' must be a list")

        headers = {"content-type": "application/json", "accept": "text/event-stream"}
        if api_key:
            headers["authorization"] = f"Bearer {api_key}"
        headers.update({h: request.headers[h] for h in PASS_HEADERS if h in request.headers})

        c = state["client"]
        req = c.build_request("POST", f"{base_url}/v1/chat/completions", json=body, headers=headers)
        try:
            upstream = await c.send(req, stream=True)
        except httpx.TimeoutException:
            return _error(504, "upstream_timeout", "The model provider timed out")
        except httpx.HTTPError:
            return _error(502, "upstream_unavailable", "The model provider is unavailable")

        if upstream.status_code >= 400:
            await upstream.aread()
            await upstream.aclose()
            log.warning("upstream returned %s", upstream.status_code)
            status = upstream.status_code if upstream.status_code in (400, 401, 403, 404, 429) else 502
            return _error(status, "upstream_error", f"Model provider returned HTTP {upstream.status_code}")

        if body.get("stream"):
            return StreamingResponse(guarded_stream(upstream), media_type="text/event-stream",
                                     headers={"cache-control": "no-cache", "x-accel-buffering": "no"})

        payload = json.loads(await upstream.aread())
        await upstream.aclose()
        for ch in payload.get("choices", []):
            msg = ch.get("message") or {}
            if isinstance(msg.get("content"), str):
                msg["content"] = redact(msg["content"])
        return JSONResponse(payload)

    return Starlette(routes=[Route("/v1/chat/completions", chat, methods=["POST"])], lifespan=lifespan)


app = create_app()
