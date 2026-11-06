# AI Field Developer Assessment

My solutions to the four tasks. Everything is in Python 3.11+, and each task sits in its own folder with its own tests.

| Task | Folder | Tests |
|---|---|---|
| 1. MCP server with strict validation over stdio | `task1_mcp_server/` | 38 |
| 2. MCP security gateway (auth and tool filtering) | `task2_mcp_gateway/` | 37 |
| 3. Streaming PII redaction for an LLM gateway | `task3_pii_stream_guardrail/` | 243 |
| 4. Token rate limiter and model fallback (SQLite) | `task4_rate_limit_fallback_router/` | 34 |

## Setup

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Running the tests

Linux / macOS / Git Bash:

```bash
./run_tests.sh
```

Windows PowerShell:

```powershell
foreach ($d in "task1_mcp_server","task2_mcp_gateway","task3_pii_stream_guardrail","task4_rate_limit_fallback_router") {
  Push-Location $d; python -m pytest -q -c ..\pytest.ini --rootdir . -p no:cacheprovider; Pop-Location
}
```

All 352 tests pass on both Linux and Windows. In VS Code they also show up in the Testing panel.

## Running the demos

The easiest way is VS Code. Open the folder, select the `.venv` interpreter, then pick one of these in Run and Debug:

- Task 1: demo client
- ▶ Task 2 demo
- ▶ Task 3 demo
- ▶ Task 4 demo

Each one starts the mock servers, the gateway and a small client that walks through the main cases. The commands to run each task by hand are in the sections below.

---

## Task 1: MCP server

`task1_mcp_server/server.py` uses the official `mcp` SDK (the low-level `Server`) over stdio and exposes `get_customer_record` and `trigger_refund`.

**Keeping stdout clean.** Nothing in the server prints. Logging is set up to go to stderr before anything else runs, and the SDK's stdio transport also redirects fd 1 while it's running, so a stray print from a library can't break the stream. The tests check this in three ways:

- every line on stdout parses as JSON-RPC
- the debug logs show up on stderr and not on stdout
- an AST check fails if `print()` or `sys.stdout` ever appears in the server

**Validation.** The Pydantic models run in strict mode, so `"10"` doesn't quietly become `10`. They also reject unknown fields and trim whitespace.

- `customer_id` has to match `^CUST-[0-9]{5}$`. I used `[0-9]` rather than `\d`, because a test showed `\d` accepts full-width digits like `１２３４５`.
- `amount` has to be greater than 0, finite, at most 1,000,000, and have no more than 2 decimal places.
- `reason` has to be 10–500 characters after trimming, with no control characters.

**Errors.**

| Case | Response |
|---|---|
| Invalid arguments | `-32602`, with a list of `{field, type, message}` in `error.data`. The raw input isn't echoed back. |
| Unknown tool | `-32602`, as the MCP spec says |
| Unknown method | `-32601` |
| Customer not found, refund above balance | A normal result with `isError: true`, so the model can read it and recover |
| Unexpected exception | `-32603`, with no traceback |

```bash
python task1_mcp_server/server.py              # point an MCP host like Claude Desktop at this
python task1_mcp_server/demo_client.py         # or run the demo
```

## Task 2: MCP security gateway

`task2_mcp_gateway/gateway.py` handles each request in four steps: authenticate, parse, authorize, forward.

- **Auth.** The `Bearer` token must be a valid HS256 JWT with a signature, `exp`, `sub`, and a `role` of `admin` or `viewer`. If it isn't, the gateway returns HTTP 401 and nothing is forwarded. Tokens using `alg=none` are rejected.
- **tools/list** is forwarded unchanged. If you set `FILTER_TOOLS_LIST=1`, the `admin_*` tools are hidden from viewers.
- **tools/call** for an `admin_*` tool without the admin role gets `{"code": -32001, "message": "Unauthorized Tool Call"}` straight from the gateway. The tests confirm the downstream server gets zero requests in that case.

I also closed a few ways around the admin check:

- The prefix check ignores case and surrounding spaces, so `ADMIN_x` and `" admin_x"` are blocked too.
- Duplicate JSON keys are rejected, so `{"name":"ok","name":"admin_x"}` can't trick the parser.
- Batches are rejected. MCP 2025-06-18 dropped batching, and a batch could hide an admin call.
- Methods the gateway doesn't recognise are denied by default.
- The gateway forwards the message it re-serialized after validating it, not the original bytes.

When forwarding, it strips the caller's `Authorization` header and cookies and adds `x-gateway-user` and `x-gateway-role` instead. `Mcp-Session-Id` passes through, and JSON and SSE responses are streamed back as they are. If the downstream server is down or times out, the client gets a clean JSON-RPC error (502/504) with the original request `id`.

```bash
cd task2_mcp_gateway
uvicorn mock_mcp_server:app --port 9000
DOWNSTREAM_URL=http://127.0.0.1:9000/mcp uvicorn gateway:app --port 8080
python demo_client.py
```

To test by hand with curl:

```bash
curl -s localhost:8080/mcp -H "Authorization: Bearer $(python make_token.py viewer bob)" \
  -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"admin_reset_key","arguments":{}}}'
# {"jsonrpc":"2.0","id":1,"error":{"code":-32001,"message":"Unauthorized Tool Call"}}
```

The JWT secret comes from `GATEWAY_JWT_SECRET`. There's a dev default so the demo works without any setup.

## Task 3: Streaming PII redaction

The tricky part is that PII gets split across deltas, for example `"jo" | "hn@exa" | "mple.com"`. Running a regex on each chunk misses it, and buffering the whole response wrecks time to first token.

My approach in `redactor.py` is to send everything up to the last safe point. All the patterns are made only of letters, digits, `. _ % + - @`, and spaces between digit groups. Any other character can't be part of a match, so I treat it as a boundary. On each delta the redactor:

1. redacts and sends everything up to the last boundary
2. holds back only what comes after it, which is usually the word currently being generated

That adds about one token of latency, and memory depends on the longest word rather than the whole response.

If no boundary appears for a long time (a long base64 string, for example), the held text is capped at `max_hold`. Every pattern has a maximum length, so keeping the last `MAX_MATCH_LEN` characters is enough to never cut a match in half.

**What it catches.**

- emails
- SSNs: dashed, spaced or plain, with consistent separators, skipping invalid ones like `000-`, `666-`, `9xx-`
- card numbers: 13–19 digits with optional spaces or dashes, Luhn-checked so order IDs aren't redacted by mistake

**How I checked it.**

- The streamed output matches one-shot `redact()` at every possible split point, character by character, and over 200 random chunkings.
- No emitted piece ever contains part of an email.
- Streaming 1.4 MB keeps fewer than 40 characters pending and stays under 200 KB of peak memory.
- Over real HTTP, the first text arrives in under 250 ms while the upstream takes more than 2 s, so the gateway isn't buffering.
- It costs about 2 µs per delta.

**The gateway** (`gateway.py`) is OpenAI-compatible (`/v1/chat/completions`) and uses one redactor per choice. It flushes on `finish_reason`, on `[DONE]`, or if the upstream drops mid-stream. It drops chunks that end up empty and closes the upstream connection if the client disconnects. Non-streaming responses are redacted too.

```bash
cd task3_pii_stream_guardrail
uvicorn mock_llm:app --port 9100
UPSTREAM_BASE_URL=http://127.0.0.1:9100 uvicorn gateway:app --port 8100
python demo_client.py
```

## Task 4: Rate limiter and fallback router

**Rate limiter** (`rate_limiter.py`). This is a sliding-window log in an on-disk SQLite file. Each request is one row `(tenant, ts, tokens)`, and usage is the sum over the last 60 seconds. That makes it exact, with no burst at the edge of a fixed window. Old rows are deleted on every check, and `purge_expired()` cleans up tenants that have gone quiet.

The flow is reserve, then commit:

1. Before the call, the estimate (prompt plus `max_tokens`) is reserved inside a `BEGIN IMMEDIATE` transaction, so two requests can't both slip under the limit.
2. After the call, the row is updated to the provider's real `usage.total_tokens`.
3. If the call fails, the reservation is removed, so tenants aren't charged for an outage.

I use WAL mode and `busy_timeout`, and the database calls run in `asyncio.to_thread` so they don't block the event loop. One test runs 4 processes against the same DB file and checks that exactly 50 × 1000 = 50,000 tokens get through. `Retry-After` is worked out from when enough of the oldest rows will expire. Tenants are stored by the SHA-256 of their API key, not the key itself.

**Router** (`router.py`).

- It fails over to the secondary on a 429, a 3000 ms timeout, a 5xx, or a connection error.
- It doesn't fail over on other 4xx errors, because the backup would just reject the same request and you'd pay twice.
- `asyncio.timeout()` cancels the slow call and httpx closes the socket, so a late reply from the primary can't be used or billed twice. If the reply lands just before the deadline, it wins cleanly. The tests cover both cases.
- If the primary sends a 429 with `Retry-After`, it's skipped for that long instead of being hit again.

**Errors.** Every failure goes out in the same format:

```json
{"error": {"type": "gateway_error", "code": "all_providers_unavailable",
           "message": "All model providers are currently unavailable. Please retry shortly.",
           "request_id": "req_…", "retry_after_seconds": 1.0}}
```

Upstream bodies, stack traces, hostnames and provider names are only logged on the server, under the `request_id`. One test makes the mock provider return a 500 containing a traceback, an internal IP and a password, and checks that none of it reaches the client.

```bash
cd task4_rate_limit_fallback_router
PROVIDER_NAME=primary MODE=slow SLOW_S=6 uvicorn mock_provider:app --port 9201
PROVIDER_NAME=secondary uvicorn mock_provider:app --port 9202
TENANT_KEYS="sk-a:acme" TOKENS_PER_MINUTE=2000 uvicorn app:create_app_from_env --factory --port 8200
python demo_client.py
```

---

## Assumptions and trade-offs

- I read `CUST-XXXXX` as exactly 5 ASCII digits.
- In Task 1, bad input returns a JSON-RPC error as the brief asks, while business failures (unknown customer, refund too high) use `isError`, which is what the MCP spec recommends for tool errors.
- In Task 2, a blocked tool call returns HTTP 200 with the JSON-RPC error, following JSON-RPC convention, and 401 is kept for auth failures. Switching to 403 would be a one-line change.
- In Task 3, I'd rather over-redact than leak. An email stuck to other text still gets its last 64 local-part characters redacted. `tool_calls` arguments aren't scanned yet; the next step would be running the same redactor over them.
- In Task 4, the token estimate is a simple ~4 characters per token, corrected to real usage after each call. In production I'd use the provider's tokenizer. The Task 4 endpoint doesn't support streaming, because failing over after tokens have already been sent needs a different design.
