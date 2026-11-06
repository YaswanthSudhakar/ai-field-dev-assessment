"""Runs server.py as a real subprocess and talks raw JSON-RPC to it, like an MCP host would."""

from __future__ import annotations

import ast
import json
import pathlib
import subprocess
import sys
import threading

import pytest

SERVER = pathlib.Path(__file__).with_name("server.py")


class StdioClient:
    def __init__(self) -> None:
        self.proc = subprocess.Popen(
            [sys.executable, str(SERVER)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        self._id = 0
        self.stdout_lines: list[str] = []
        # Read stderr in the background. On Windows the pipe fills up quickly and the server would block.
        self._err: list[str] = []
        self._err_thread = threading.Thread(target=lambda: self._err.extend(self.proc.stderr), daemon=True)
        self._err_thread.start()

    def _send(self, msg: dict) -> None:
        self.proc.stdin.write(json.dumps(msg) + "\n")
        self.proc.stdin.flush()

    def _read(self) -> dict:
        line = self.proc.stdout.readline()
        assert line, "server closed stdout unexpectedly"
        self.stdout_lines.append(line)
        frame = json.loads(line)
        assert frame.get("jsonrpc") == "2.0", f"not a JSON-RPC frame: {line!r}"
        return frame

    def request(self, method: str, params: dict | None = None) -> dict:
        self._id += 1
        msg = {"jsonrpc": "2.0", "id": self._id, "method": method}
        if params is not None:
            msg["params"] = params
        self._send(msg)
        frame = self._read()
        assert frame["id"] == self._id
        return frame

    def notify(self, method: str) -> None:
        self._send({"jsonrpc": "2.0", "method": method})

    def call(self, name: str, arguments) -> dict:
        return self.request("tools/call", {"name": name, "arguments": arguments})

    def close(self) -> tuple[str, str]:
        self.proc.stdin.close()
        rest_out = self.proc.stdout.read()
        self.proc.wait(timeout=10)
        self._err_thread.join(timeout=5)
        return rest_out, "".join(self._err)


@pytest.fixture(scope="module")
def client():
    c = StdioClient()
    init = c.request(
        "initialize",
        {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "pytest", "version": "0"}},
    )
    assert init["result"]["serverInfo"]["name"] == "customer-support-mcp"
    c.notify("notifications/initialized")
    yield c
    c.close()


def test_stdout_is_pure_jsonrpc_and_logs_go_to_stderr():
    c = StdioClient()
    c.request("initialize", {"protocolVersion": "2025-06-18", "capabilities": {},
                             "clientInfo": {"name": "pytest", "version": "0"}})
    c.notify("notifications/initialized")
    c.request("tools/list")
    c.call("get_customer_record", {"customer_id": "CUST-00042"})
    c.call("trigger_refund", {"customer_id": "bad", "amount": -1, "reason": "x"})
    rest_out, err = c.close()

    for line in c.stdout_lines + rest_out.splitlines():
        if line.strip():
            assert json.loads(line)["jsonrpc"] == "2.0"
    assert "calling get_customer_record" in err
    assert "calling get_customer_record" not in "".join(c.stdout_lines)


def test_source_has_no_stdout_writes():
    tree = ast.parse(SERVER.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            assert node.func.id != "print", f"print() at line {node.lineno}"
        if isinstance(node, ast.Attribute) and node.attr == "stdout":
            assert False, f"sys.stdout referenced at line {node.lineno}"


def test_tools_list_exposes_strict_schemas(client):
    tools = {t["name"]: t for t in client.request("tools/list")["result"]["tools"]}
    assert set(tools) == {"get_customer_record", "trigger_refund"}
    refund = tools["trigger_refund"]["inputSchema"]
    assert refund["additionalProperties"] is False
    assert set(refund["required"]) == {"customer_id", "amount", "reason"}
    assert refund["properties"]["reason"]["minLength"] == 10
    assert refund["properties"]["amount"]["exclusiveMinimum"] == 0


def test_get_customer_record_ok(client):
    res = client.call("get_customer_record", {"customer_id": "CUST-00042"})["result"]
    assert res["isError"] is False
    assert res["structuredContent"]["name"] == "Alan Turing"


def test_trigger_refund_ok(client):
    res = client.call("trigger_refund", {"customer_id": "CUST-00001", "amount": 19.99,
                                         "reason": "Item arrived damaged"})["result"]
    assert res["isError"] is False
    assert res["structuredContent"]["status"] == "processed"
    assert res["structuredContent"]["remaining_balance"] == 230.01


def test_whitespace_is_stripped_before_validation(client):
    res = client.call("get_customer_record", {"customer_id": "  CUST-00042  "})["result"]
    assert res["isError"] is False


def test_unknown_customer_is_tool_error_not_protocol_error(client):
    res = client.call("get_customer_record", {"customer_id": "CUST-99999"})["result"]
    assert res["isError"] is True
    assert "not found" in res["content"][0]["text"]


def test_refund_over_balance_is_tool_error(client):
    res = client.call("trigger_refund", {"customer_id": "CUST-00042", "amount": 500.0,
                                         "reason": "Customer requested full refund"})["result"]
    assert res["isError"] is True


BAD_CUSTOMER_IDS = ["CUST-1234", "CUST-123456", "cust-12345", "CUST_12345", "CUST-1234a",
                    "", "CUST-12345 X", "CUST-\uff11\uff12\uff13\uff14\uff15", "XCUST-12345", 12345, None, ["CUST-12345"]]


@pytest.mark.parametrize("cid", BAD_CUSTOMER_IDS)
def test_bad_customer_id_rejected(client, cid):
    frame = client.call("get_customer_record", {"customer_id": cid})
    assert frame["error"]["code"] == -32602
    assert frame["error"]["data"]["errors"][0]["field"] == "customer_id"


@pytest.mark.parametrize("amount", [0, -10, "10.00", True, None, 1.234, 1e309, 2_000_000])
def test_bad_amount_rejected(client, amount):
    frame = client.call("trigger_refund", {"customer_id": "CUST-00001", "amount": amount,
                                           "reason": "valid reason text"})
    assert frame["error"]["code"] == -32602, frame
    assert any(e["field"] == "amount" for e in frame["error"]["data"]["errors"])


@pytest.mark.parametrize("reason", ["too short", "          short     ", "", 1234567890, "bad\x00reason here"])
def test_bad_reason_rejected(client, reason):
    frame = client.call("trigger_refund", {"customer_id": "CUST-00001", "amount": 5.0, "reason": reason})
    assert frame["error"]["code"] == -32602, frame


def test_missing_and_extra_fields_rejected(client):
    frame = client.call("trigger_refund", {"customer_id": "CUST-00001"})
    fields = {e["field"] for e in frame["error"]["data"]["errors"]}
    assert {"amount", "reason"} <= fields

    frame = client.call("get_customer_record", {"customer_id": "CUST-00001", "admin": True})
    assert frame["error"]["code"] == -32602
    assert frame["error"]["data"]["errors"][0]["type"] == "extra_forbidden"


def test_non_object_arguments_rejected(client):
    frame = client.call("get_customer_record", ["CUST-00001"])
    assert "error" in frame and frame["error"]["code"] in (-32602, -32600)


def test_unknown_tool(client):
    frame = client.call("delete_everything", {})
    assert frame["error"]["code"] == -32602
    assert "Unknown tool" in frame["error"]["message"]


def test_unknown_method(client):
    frame = client.request("tools/nonexistent")
    assert frame["error"]["code"] == -32601


def test_server_survives_malformed_line(client):
    client.proc.stdin.write("this is not json\n")
    client.proc.stdin.flush()
    frame = client.call("get_customer_record", {"customer_id": "CUST-00042"})
    assert frame["result"]["isError"] is False
