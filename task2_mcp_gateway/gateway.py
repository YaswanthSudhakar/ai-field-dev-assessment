"""MCP gateway for Task 2.

Checks the bearer JWT, parses the JSON-RPC body, blocks admin_* tool calls
for non-admins and forwards everything else to the downstream MCP server.

Run with: uvicorn gateway:app --port 8080
"""

from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
import os
import sys
from dataclasses import dataclass
from typing import Any

import httpx
import jwt
from starlette.applications import Starlette
from starlette.background import BackgroundTask
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route

logging.basicConfig(stream=sys.stderr, level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("mcp-gateway")

PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
UNAUTHORIZED = -32001
UPSTREAM_ERROR = -32002

ROLES = {"admin", "viewer"}
ADMIN_TOOL_PREFIX = "admin_"
PASSTHROUGH_METHODS = {"initialize", "ping", "tools/list", "tools/call"}
MAX_BODY_BYTES = 1_000_000

FORWARD_REQ_HEADERS = {"content-type", "accept", "mcp-session-id", "mcp-protocol-version", "last-event-id"}
FORWARD_RESP_HEADERS = {"content-type", "mcp-session-id", "cache-control"}


@dataclass(frozen=True)
class Settings:
    downstream_url: str
    jwt_secret: str
    jwt_algorithms: tuple[str, ...] = ("HS256",)
    upstream_timeout_s: float = 30.0
    filter_tools_list: bool = False

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            downstream_url=os.environ.get("DOWNSTREAM_URL", "http://127.0.0.1:9000/mcp"),
            jwt_secret=os.environ.get("GATEWAY_JWT_SECRET", "dev-secret-change-me-0123456789abcdef"),
            filter_tools_list=os.environ.get("FILTER_TOOLS_LIST", "0") == "1",
        )


@dataclass(frozen=True)
class Principal:
    subject: str
    role: str


class GatewayError(Exception):
    def __init__(self, code: int, message: str, http_status: int = 200, msg_id: Any = None):
        super().__init__(message)
        self.code, self.message, self.http_status, self.msg_id = code, message, http_status, msg_id


def jsonrpc_error(msg_id: Any, code: int, message: str, http_status: int = 200, headers=None) -> JSONResponse:
    return JSONResponse({"jsonrpc": "2.0", "id": msg_id, "error": {"code": code, "message": message}},
                        status_code=http_status, headers=headers)


def authenticate(auth_header: str | None, settings: Settings) -> Principal:
    if not auth_header:
        raise GatewayError(UNAUTHORIZED, "Missing bearer token", 401)
    scheme, _, token = auth_header.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise GatewayError(UNAUTHORIZED, "Authorization header must be 'Bearer <token>'", 401)
    try:
        claims = jwt.decode(token.strip(), settings.jwt_secret, algorithms=list(settings.jwt_algorithms),
                            options={"require": ["exp", "sub", "role"]})
    except jwt.ExpiredSignatureError:
        raise GatewayError(UNAUTHORIZED, "Token expired", 401)
    except jwt.PyJWTError:
        raise GatewayError(UNAUTHORIZED, "Invalid token", 401)
    role = claims.get("role")
    if role not in ROLES:
        raise GatewayError(UNAUTHORIZED, "Token has no recognised role", 401)
    return Principal(subject=str(claims["sub"]), role=role)


def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in pairs:
        if k in out:
            raise ValueError(f"duplicate key {k!r}")
        out[k] = v
    return out


def parse_jsonrpc(body: bytes) -> dict[str, Any]:
    try:
        msg = json.loads(body, object_pairs_hook=_no_duplicate_keys)
    except (ValueError, UnicodeDecodeError) as exc:
        raise GatewayError(PARSE_ERROR, f"Parse error: {exc}" if "duplicate" in str(exc) else "Parse error")
    if isinstance(msg, list):
        raise GatewayError(INVALID_REQUEST, "JSON-RPC batching is not supported by this gateway")
    if not isinstance(msg, dict):
        raise GatewayError(INVALID_REQUEST, "Request must be a JSON object")
    msg_id = msg.get("id")
    if msg.get("jsonrpc") != "2.0":
        raise GatewayError(INVALID_REQUEST, "jsonrpc must be '2.0'", msg_id=msg_id)
    if not isinstance(msg.get("method"), str):
        if "result" in msg or "error" in msg:
            return msg
        raise GatewayError(INVALID_REQUEST, "method must be a string", msg_id=msg_id)
    if "id" in msg and not (msg_id is None or isinstance(msg_id, (str, int)) and not isinstance(msg_id, bool)):
        raise GatewayError(INVALID_REQUEST, "id must be a string or integer", msg_id=None)
    return msg


def is_admin_tool(name: str) -> bool:
    return name.strip().lower().startswith(ADMIN_TOOL_PREFIX)


def authorize(msg: dict[str, Any], principal: Principal) -> None:
    method, msg_id = msg.get("method"), msg.get("id")
    if method is None:
        return
    if method.startswith("notifications/"):
        return
    if method not in PASSTHROUGH_METHODS:
        raise GatewayError(METHOD_NOT_FOUND, f"Method '{method}' is not allowed through this gateway",
                           msg_id=msg_id)
    if method != "tools/call":
        return

    if "id" not in msg:
        raise GatewayError(INVALID_REQUEST, "tools/call must be a request with an id")
    params = msg.get("params")
    if not isinstance(params, dict) or not isinstance(params.get("name"), str) or not params["name"].strip():
        raise GatewayError(INVALID_PARAMS, "tools/call requires params.name (string)", msg_id=msg_id)

    if is_admin_tool(params["name"]) and principal.role != "admin":
        log.warning("BLOCKED tools/call %r for sub=%s role=%s", params["name"], principal.subject, principal.role)
        raise GatewayError(UNAUTHORIZED, "Unauthorized Tool Call", msg_id=msg_id)


async def forward(request: Request, msg: dict[str, Any], principal: Principal,
                  client: httpx.AsyncClient, settings: Settings) -> Response:
    headers = {k: v for k, v in request.headers.items() if k.lower() in FORWARD_REQ_HEADERS}
    headers["content-type"] = "application/json"
    headers.setdefault("accept", "application/json, text/event-stream")
    headers["x-gateway-user"] = principal.subject
    headers["x-gateway-role"] = principal.role

    upstream_req = client.build_request("POST", settings.downstream_url,
                                        content=json.dumps(msg).encode(), headers=headers)
    try:
        upstream = await client.send(upstream_req, stream=True)
    except httpx.TimeoutException:
        log.error("downstream timeout for %s", msg.get("method"))
        return jsonrpc_error(msg.get("id"), UPSTREAM_ERROR, "Downstream MCP server timed out", 504)
    except httpx.HTTPError as exc:
        log.error("downstream unreachable: %s", type(exc).__name__)
        return jsonrpc_error(msg.get("id"), UPSTREAM_ERROR, "Downstream MCP server unavailable", 502)

    resp_headers = {k: v for k, v in upstream.headers.items() if k.lower() in FORWARD_RESP_HEADERS}

    if settings.filter_tools_list and msg.get("method") == "tools/list" and principal.role != "admin" \
            and upstream.headers.get("content-type", "").startswith("application/json"):
        payload = json.loads(await upstream.aread())
        await upstream.aclose()
        tools = payload.get("result", {}).get("tools")
        if isinstance(tools, list):
            payload["result"]["tools"] = [t for t in tools if not is_admin_tool(str(t.get("name", "")))]
        return JSONResponse(payload, status_code=upstream.status_code, headers=resp_headers)

    return StreamingResponse(upstream.aiter_raw(), status_code=upstream.status_code,
                             headers=resp_headers, background=BackgroundTask(upstream.aclose))


def create_app(settings: Settings | None = None, client: httpx.AsyncClient | None = None) -> Starlette:
    settings = settings or Settings.from_env()
    state: dict[str, httpx.AsyncClient] = {"client": client} if client else {}

    async def mcp_endpoint(request: Request) -> Response:
        msg_id = None
        try:
            principal = authenticate(request.headers.get("authorization"), settings)
            body = await request.body()
            if len(body) > MAX_BODY_BYTES:
                raise GatewayError(INVALID_REQUEST, "Request body too large", 413)
            msg = parse_jsonrpc(body)
            msg_id = msg.get("id")
            authorize(msg, principal)
        except GatewayError as e:
            headers = {"WWW-Authenticate": 'Bearer realm="mcp-gateway"'} if e.http_status == 401 else None
            return jsonrpc_error(e.msg_id if e.msg_id is not None else msg_id, e.code, e.message,
                                 e.http_status, headers)

        log.info("forward %s %s sub=%s role=%s", msg.get("method"),
                 (msg.get("params") or {}).get("name", ""), principal.subject, principal.role)
        return await forward(request, msg, principal, state["client"], settings)

    async def health(_: Request) -> Response:
        return JSONResponse({"status": "ok"})

    @asynccontextmanager
    async def lifespan(_app):
        if client is None:
            state["client"] = httpx.AsyncClient(timeout=settings.upstream_timeout_s)
        try:
            yield
        finally:
            if client is None:
                await state["client"].aclose()

    return Starlette(routes=[Route("/mcp", mcp_endpoint, methods=["POST"]),
                             Route("/healthz", health, methods=["GET"])],
                     lifespan=lifespan)


app = create_app()
