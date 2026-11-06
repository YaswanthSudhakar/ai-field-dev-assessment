"""Fake OpenAI-style provider that streams its reply in small pieces.

The x-mock-reply and x-mock-delay-ms headers are only there for the tests.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
import uuid

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, StreamingResponse
from starlette.routing import Route

DEFAULT_REPLY = (
    "Sure! I found the customer record. Their email is jane.doe@example.com and the SSN on "
    "file is 123-45-6789. The card ending in 1111 is 4111 1111 1111 1111, so please verify "
    "with them directly before issuing any refund. Order #20240115-77 is still pending."
)


def tokenize(text: str) -> list[str]:
    """Splits text roughly like a tokenizer, so PII ends up cut across chunks."""
    pieces = re.findall(r"\s*\S+", text)
    out: list[str] = []
    for p in pieces:
        out.extend(p[i:i + 3] for i in range(0, len(p), 3))
    return out


async def chat_completions(request: Request):
    body = await request.json()
    reply = request.headers.get("x-mock-reply", DEFAULT_REPLY)
    delay = float(request.headers.get("x-mock-delay-ms", "20")) / 1000
    cid, created, model = f"chatcmpl-{uuid.uuid4().hex[:12]}", int(time.time()), body.get("model", "mock-1")

    if not body.get("stream"):
        return JSONResponse({"id": cid, "object": "chat.completion", "created": created, "model": model,
                             "choices": [{"index": 0, "finish_reason": "stop",
                                          "message": {"role": "assistant", "content": reply}}]})

    def chunk(delta: dict, finish=None) -> str:
        return "data: " + json.dumps({"id": cid, "object": "chat.completion.chunk", "created": created,
                                      "model": model, "choices": [{"index": 0, "delta": delta,
                                                                   "finish_reason": finish}]}) + "\n\n"

    async def gen():
        yield chunk({"role": "assistant", "content": ""})
        for tok in tokenize(reply):
            await asyncio.sleep(delay)
            yield chunk({"content": tok})
        yield chunk({}, finish="stop")
        yield "data: [DONE]\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream")


app = Starlette(routes=[Route("/v1/chat/completions", chat_completions, methods=["POST"])])
