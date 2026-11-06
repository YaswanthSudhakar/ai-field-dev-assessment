from __future__ import annotations

import json
import time

import httpx
import jwt
import pytest

import mock_mcp_server as mock
from gateway import Settings, create_app
from make_token import make_token

SECRET = "test-secret-0123456789abcdef0123456789"
ADMIN = make_token("admin", "alice", secret=SECRET)
VIEWER = make_token("viewer", "bob", secret=SECRET)


def rpc(method, params=None, id=1):
    m = {"jsonrpc": "2.0", "id": id, "method": method}
    if params is not None:
        m["params"] = params
    return m


def build(filter_tools_list=False, downstream_path="/mcp", transport=None):
    upstream = httpx.AsyncClient(transport=transport or httpx.ASGITransport(app=mock.app),
                                 base_url="http://downstream")
    settings = Settings(downstream_url=f"http://downstream{downstream_path}", jwt_secret=SECRET,
                        filter_tools_list=filter_tools_list)
    app = create_app(settings, client=upstream)
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw")


@pytest.fixture(autouse=True)
def reset_mock():
    mock.received.clear()
    mock.received_headers.clear()


async def post(gw, body, token=VIEWER, raw=False, headers=None):
    h = {"content-type": "application/json", **(headers or {})}
    if token is not None:
        h["authorization"] = f"Bearer {token}"
    return await gw.post("/mcp", content=body if raw else json.dumps(body), headers=h)


async def test_tools_list_forwarded_transparently():
    async with build() as gw:
        r = await post(gw, rpc("tools/list", id=7))
    assert r.status_code == 200
    body = r.json()
    assert body["id"] == 7
    assert {t["name"] for t in body["result"]["tools"]} == {"get_weather", "admin_reset_key", "admin_delete_user"}
    assert mock.received == [rpc("tools/list", id=7)]


async def test_optional_tools_list_filtering_for_viewer():
    async with build(filter_tools_list=True) as gw:
        viewer = (await post(gw, rpc("tools/list"))).json()
        admin = (await post(gw, rpc("tools/list"), token=ADMIN)).json()
    assert [t["name"] for t in viewer["result"]["tools"]] == ["get_weather"]
    assert len(admin["result"]["tools"]) == 3


async def test_sse_response_streamed_through():
    async with build(downstream_path="/mcp-sse") as gw:
        r = await post(gw, rpc("tools/list", id=3))
    assert r.headers["content-type"].startswith("text/event-stream")
    data_line = next(l for l in r.text.splitlines() if l.startswith("data: "))
    assert json.loads(data_line[6:])["id"] == 3


async def test_viewer_can_call_regular_tool():
    async with build() as gw:
        r = await post(gw, rpc("tools/call", {"name": "get_weather", "arguments": {"city": "Austin"}}))
    assert "Austin" in r.json()["result"]["content"][0]["text"]


@pytest.mark.parametrize("name", ["admin_reset_key", "admin_delete_user", "ADMIN_reset_key",
                                  "Admin_Reset_Key", "  admin_reset_key", "admin_"])
async def test_viewer_blocked_from_admin_tools_and_downstream_never_called(name):
    async with build() as gw:
        r = await post(gw, rpc("tools/call", {"name": name, "arguments": {"tenant": "t1"}}, id="abc"))
    assert r.status_code == 200
    assert r.json() == {"jsonrpc": "2.0", "id": "abc",
                        "error": {"code": -32001, "message": "Unauthorized Tool Call"}}
    assert mock.received == []


async def test_admin_can_call_admin_tool():
    async with build() as gw:
        r = await post(gw, rpc("tools/call", {"name": "admin_reset_key", "arguments": {"tenant": "t1"}}),
                       token=ADMIN)
    assert "rotated" in r.json()["result"]["content"][0]["text"]
    assert len(mock.received) == 1


async def test_tool_named_like_admin_but_not_prefixed_is_allowed():
    async with build() as gw:
        r = await post(gw, rpc("tools/call", {"name": "get_admin_contact"}))
    assert r.json()["error"]["code"] == -32602
    assert len(mock.received) == 1


@pytest.mark.parametrize("auth", [None, "", "Basic abc", "Bearer ", "Bearer not-a-jwt",
                                  "Bearer " + jwt.encode({"sub": "x", "role": "admin", "exp": int(time.time()) + 60},
                                                         "WRONG-SECRET-0123456789abcdef0123456789", algorithm="HS256")])
async def test_bad_or_missing_token_is_401(auth):
    async with build() as gw:
        headers = {"content-type": "application/json"}
        if auth is not None:
            headers["authorization"] = auth
        r = await gw.post("/mcp", content=json.dumps(rpc("tools/list")), headers=headers)
    assert r.status_code == 401
    assert r.json()["error"]["code"] == -32001
    assert "Bearer" in r.headers["www-authenticate"]
    assert mock.received == []


async def test_expired_token_401():
    expired = jwt.encode({"sub": "x", "role": "admin", "exp": int(time.time()) - 10}, SECRET, algorithm="HS256")
    async with build() as gw:
        r = await post(gw, rpc("tools/list"), token=expired)
    assert r.status_code == 401 and r.json()["error"]["message"] == "Token expired"


async def test_unknown_role_401():
    tok = jwt.encode({"sub": "x", "role": "superuser", "exp": int(time.time()) + 60}, SECRET, algorithm="HS256")
    async with build() as gw:
        r = await post(gw, rpc("tools/list"), token=tok)
    assert r.status_code == 401


async def test_alg_none_token_rejected():
    tok = jwt.encode({"sub": "x", "role": "admin", "exp": int(time.time()) + 60}, None, algorithm="none")
    async with build() as gw:
        r = await post(gw, rpc("tools/call", {"name": "admin_reset_key"}), token=tok)
    assert r.status_code == 401
    assert mock.received == []


async def test_parse_error():
    async with build() as gw:
        r = await post(gw, b"{not json", raw=True)
    assert r.json() == {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}}


async def test_duplicate_key_smuggling_rejected():
    body = b'{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_weather","name":"admin_reset_key"}}'
    async with build() as gw:
        r = await post(gw, body, raw=True)
    assert r.json()["error"]["code"] == -32700
    assert mock.received == []


async def test_batch_rejected_so_admin_call_cannot_hide_in_it():
    batch = [rpc("tools/list", id=1), rpc("tools/call", {"name": "admin_reset_key"}, id=2)]
    async with build() as gw:
        r = await post(gw, batch)
    assert r.json()["error"]["code"] == -32600
    assert mock.received == []


@pytest.mark.parametrize("msg, code", [
    ({"id": 1, "method": "tools/list"}, -32600),
    ({"jsonrpc": "1.0", "id": 1, "method": "tools/list"}, -32600),
    ({"jsonrpc": "2.0", "id": 1, "method": 5}, -32600),
    ({"jsonrpc": "2.0", "id": {"x": 1}, "method": "tools/list"}, -32600),
    ({"jsonrpc": "2.0", "id": 1, "method": "tools/call"}, -32602),
    ({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": 5}}, -32602),
    ({"jsonrpc": "2.0", "method": "tools/call", "params": {"name": "admin_reset_key"}}, -32600),
    ({"jsonrpc": "2.0", "id": 1, "method": "resources/delete_all"}, -32601),
    ("just a string", -32600),
])
async def test_malformed_requests(msg, code):
    async with build() as gw:
        r = await post(gw, msg)
    assert r.json()["error"]["code"] == code
    assert mock.received == []


async def test_error_preserves_request_id():
    async with build() as gw:
        r = await post(gw, rpc("resources/delete_all", id=42))
    assert r.json()["id"] == 42


async def test_lifecycle_and_headers():
    async with build() as gw:
        init = await post(gw, rpc("initialize", {"protocolVersion": "2025-06-18", "capabilities": {},
                                                 "clientInfo": {"name": "t", "version": "1"}}))
        sid = init.headers["mcp-session-id"]
        note = await post(gw, {"jsonrpc": "2.0", "method": "notifications/initialized"},
                          headers={"mcp-session-id": sid, "cookie": "secret=1"})
    assert note.status_code == 202
    h = mock.received_headers[-1]
    assert h["mcp-session-id"] == sid
    assert "authorization" not in h and "cookie" not in h
    assert h["x-gateway-user"] == "bob" and h["x-gateway-role"] == "viewer"


async def test_downstream_down_returns_clean_jsonrpc_error():
    def boom(request):
        raise httpx.ConnectError("connection refused")
    async with build(transport=httpx.MockTransport(boom)) as gw:
        r = await post(gw, rpc("tools/list", id=9))
    assert r.status_code == 502
    assert r.json() == {"jsonrpc": "2.0", "id": 9,
                        "error": {"code": -32002, "message": "Downstream MCP server unavailable"}}


async def test_downstream_timeout():
    def slow(request):
        raise httpx.ReadTimeout("timed out")
    async with build(transport=httpx.MockTransport(slow)) as gw:
        r = await post(gw, rpc("tools/list"))
    assert r.status_code == 504
