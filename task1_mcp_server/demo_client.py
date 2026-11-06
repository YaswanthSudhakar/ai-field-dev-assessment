"""Starts server.py and sends it a few JSON-RPC calls over stdio."""

import json
import pathlib
import subprocess
import sys

SERVER = pathlib.Path(__file__).with_name("server.py")

proc = subprocess.Popen([sys.executable, str(SERVER)], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                        stderr=subprocess.DEVNULL, text=True, bufsize=1)
_id = 0


def send(method, params=None, notify=False):
    global _id
    msg = {"jsonrpc": "2.0", "method": method}
    if params is not None:
        msg["params"] = params
    if not notify:
        _id += 1
        msg["id"] = _id
    proc.stdin.write(json.dumps(msg) + "\n")
    proc.stdin.flush()
    if notify:
        return None
    return json.loads(proc.stdout.readline())


def show(title, resp):
    print(f"\n=== {title} ===")
    print(json.dumps(resp, indent=2))


send("initialize", {"protocolVersion": "2025-06-18", "capabilities": {},
                    "clientInfo": {"name": "demo", "version": "1"}})
send("notifications/initialized", notify=True)

show("tools/list", send("tools/list"))
show("get_customer_record (valid)", send("tools/call", {"name": "get_customer_record",
                                                        "arguments": {"customer_id": "CUST-00042"}}))
show("get_customer_record (bad ID -> -32602)", send("tools/call", {"name": "get_customer_record",
                                                                   "arguments": {"customer_id": "CUST-12"}}))
show("trigger_refund (valid)", send("tools/call", {"name": "trigger_refund", "arguments": {
    "customer_id": "CUST-00001", "amount": 19.99, "reason": "Item arrived damaged"}}))
show("trigger_refund (amount<0, short reason -> -32602)", send("tools/call", {"name": "trigger_refund", "arguments": {
    "customer_id": "CUST-00001", "amount": -5, "reason": "short"}}))
show("unknown tool -> -32602", send("tools/call", {"name": "delete_everything", "arguments": {}}))

proc.stdin.close()
proc.wait(5)
