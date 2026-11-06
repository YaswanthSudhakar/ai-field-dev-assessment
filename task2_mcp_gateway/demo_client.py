"""Calls the gateway on :8080 as a viewer and as an admin.
Start the mock server (:9000) and the gateway first.
"""

import json
import time

import httpx

from make_token import make_token

GW = "http://127.0.0.1:8080/mcp"


def wait_for_gateway():
    for _ in range(100):
        try:
            httpx.get("http://127.0.0.1:8080/healthz", timeout=0.5)
            return
        except httpx.HTTPError:
            time.sleep(0.2)
    raise SystemExit("Gateway not reachable on :8080 - is it running?")


def call(title, token, method, params=None):
    body = {"jsonrpc": "2.0", "id": 1, "method": method}
    if params:
        body["params"] = params
    headers = {"authorization": f"Bearer {token}"} if token else {}
    r = httpx.post(GW, json=body, headers=headers)
    print(f"\n=== {title}  [HTTP {r.status_code}] ===")
    print(json.dumps(r.json(), indent=2)[:800])


wait_for_gateway()
viewer, admin = make_token("viewer", "bob"), make_token("admin", "alice")

call("viewer: tools/list (forwarded)", viewer, "tools/list")
call("viewer: get_weather (allowed)", viewer, "tools/call", {"name": "get_weather", "arguments": {"city": "Austin"}})
call("viewer: admin_reset_key (BLOCKED -32001)", viewer, "tools/call",
     {"name": "admin_reset_key", "arguments": {"tenant": "acme"}})
call("admin: admin_reset_key (allowed)", admin, "tools/call",
     {"name": "admin_reset_key", "arguments": {"tenant": "acme"}})
call("no token (401)", None, "tools/list")
