"""Fake provider with switchable behaviour: ok, 429, slow, 500 or 400.

Set it with the MODE env var or the x-mock-mode header.
"""

from __future__ import annotations

import asyncio
import os

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse
from starlette.routing import Route

NAME = os.environ.get("PROVIDER_NAME", "mock")
calls = {"n": 0}

LEAKY_TRACE = ('Traceback (most recent call last):\n  File "/srv/internal/inference/worker.py", line 88\n'
               "psycopg2.OperationalError: could not connect to 10.0.3.17:5432 user=svc_infer password=hunter2")


async def completions(request: Request):
    calls["n"] += 1
    body = await request.json()
    mode = request.headers.get("x-mock-mode", os.environ.get("MODE", "ok"))
    if mode == "429":
        return JSONResponse({"error": {"message": "Rate limit reached for org-internal-123"}},
                            status_code=429, headers={"retry-after": "5"})
    if mode == "500":
        return PlainTextResponse(LEAKY_TRACE, status_code=500)
    if mode == "400":
        return JSONResponse({"error": {"message": "invalid 'messages' (internal ref ab12)"}}, status_code=400)
    if mode == "slow":
        await asyncio.sleep(float(os.environ.get("SLOW_S", "5")))
    prompt_tokens = sum(len(str(m.get("content", ""))) for m in body.get("messages", [])) // 4 + 5
    return JSONResponse({
        "id": f"cmpl-{NAME}-{calls['n']}", "object": "chat.completion", "model": body.get("model"),
        "choices": [{"index": 0, "finish_reason": "stop",
                     "message": {"role": "assistant", "content": f"Hello from {NAME}"}}],
        "usage": {"prompt_tokens": prompt_tokens, "completion_tokens": 5, "total_tokens": prompt_tokens + 5},
    })


app = Starlette(routes=[Route("/v1/chat/completions", completions, methods=["POST"])])
