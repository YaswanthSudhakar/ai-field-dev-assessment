"""Small mock MCP server used behind the gateway.

It keeps a list of every request it gets, so the tests can check that blocked
calls never reach it.
"""

from __future__ import annotations

import json
import logging
import sys
import uuid

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

logging.basicConfig(stream=sys.stderr, level=logging.INFO)
log = logging.getLogger("mock-mcp")

TOOLS = [
    {
        "name": "get_weather",
        "description": "Current weather for a city",
        "inputSchema": {"type": "object", "properties": {"city": {"type": "string"}}, "required": ["city"]},
    },
    {
        "name": "admin_reset_key",
        "description": "Rotate a tenant's API key (admin only)",
        "inputSchema": {"type": "object", "properties": {"tenant": {"type": "string"}}, "required": ["tenant"]},
    },
    {
        "name": "admin_delete_user",
        "description": "Delete a user (admin only)",
        "inputSchema": {"type": "object", "properties": {"user_id": {"type": "string"}}, "required": ["user_id"]},
    },
]

received: list[dict] = []
received_headers: list[dict] = []


def _result(msg_id, result):
    return {"jsonrpc": "2.0", "id": msg_id, "result": result}


def _error(msg_id, code, message):
    return {"jsonrpc": "2.0", "id": msg_id, "error": {"code": code, "message": message}}


async def mcp_endpoint(request: Request) -> Response:
    msg = json.loads(await request.body())
    received.append(msg)
    received_headers.append(dict(request.headers))
    method, msg_id = msg.get("method"), msg.get("id")

    if msg_id is None:
        return Response(status_code=202)

    if method == "initialize":
        resp = JSONResponse(_result(msg_id, {
            "protocolVersion": "2025-06-18",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "mock-downstream", "version": "1.0"},
        }))
        resp.headers["Mcp-Session-Id"] = uuid.uuid4().hex
        return resp
    if method == "ping":
        return JSONResponse(_result(msg_id, {}))
    if method == "tools/list":
        return JSONResponse(_result(msg_id, {"tools": TOOLS}))
    if method == "tools/call":
        name = msg["params"]["name"]
        args = msg["params"].get("arguments", {})
        if name == "get_weather":
            text = f"Sunny, 24C in {args.get('city', '?')}"
        elif name == "admin_reset_key":
            text = f"Key for tenant {args.get('tenant')} rotated: sk-new-{uuid.uuid4().hex[:8]}"
        elif name == "admin_delete_user":
            text = f"User {args.get('user_id')} deleted"
        else:
            return JSONResponse(_error(msg_id, -32602, f"Unknown tool: {name}"))
        return JSONResponse(_result(msg_id, {"content": [{"type": "text", "text": text}], "isError": False}))
    return JSONResponse(_error(msg_id, -32601, "Method not found"))


async def stream_endpoint(request: Request) -> Response:
    msg = json.loads(await request.body())
    received.append(msg)
    body = f"event: message\ndata: {json.dumps(_result(msg['id'], {'tools': TOOLS}))}\n\n"
    return Response(body, media_type="text/event-stream")


app = Starlette(routes=[
    Route("/mcp", mcp_endpoint, methods=["POST"]),
    Route("/mcp-sse", stream_endpoint, methods=["POST"]),
])
