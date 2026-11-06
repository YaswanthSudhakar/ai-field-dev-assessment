"""MCP server for Task 1: get_customer_record and trigger_refund over stdio.

All logging goes to stderr so stdout only ever has JSON-RPC on it.
Run with: python server.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import re
import sys
import uuid
from typing import Any, Callable

import mcp_types as types
from mcp.server.lowlevel import Server
from mcp.server.stdio import stdio_server
from mcp.shared.exceptions import MCPError
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

logging.basicConfig(
    stream=sys.stderr,
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    force=True,
)
log = logging.getLogger("customer-mcp")

INVALID_PARAMS = types.INVALID_PARAMS
INTERNAL_ERROR = types.INTERNAL_ERROR

CUSTOMER_ID_RE = r"^CUST-[0-9]{5}$"  # not \d, which also matches full-width digits


class _StrictInput(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", str_strip_whitespace=True)


class GetCustomerRecordInput(_StrictInput):
    customer_id: str = Field(
        pattern=CUSTOMER_ID_RE,
        description="Customer identifier in the form CUST-XXXXX (5 digits), e.g. CUST-00042",
        examples=["CUST-00042"],
    )


class TriggerRefundInput(_StrictInput):
    customer_id: str = Field(pattern=CUSTOMER_ID_RE, description="Customer identifier, CUST-XXXXX")
    amount: float = Field(gt=0, le=1_000_000, description="Refund amount, positive, max 2 decimal places")
    reason: str = Field(min_length=10, max_length=500, description="Why the refund is issued (>= 10 chars)")

    @field_validator("amount")
    @classmethod
    def _money(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError("amount must be a finite number")
        if round(v, 2) != v:
            raise ValueError("amount must have at most 2 decimal places")
        return v

    @field_validator("reason")
    @classmethod
    def _printable(cls, v: str) -> str:
        if re.search(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", v):
            raise ValueError("reason contains control characters")
        return v


CUSTOMERS: dict[str, dict[str, Any]] = {
    "CUST-00001": {"name": "Ada Lovelace", "tier": "gold", "balance": 250.00, "status": "active"},
    "CUST-00042": {"name": "Alan Turing", "tier": "silver", "balance": 80.50, "status": "active"},
    "CUST-12345": {"name": "Grace Hopper", "tier": "platinum", "balance": 1200.00, "status": "active"},
}


def _text(payload: str) -> list[types.TextContent]:
    return [types.TextContent(type="text", text=payload)]


async def get_customer_record(args: GetCustomerRecordInput) -> types.CallToolResult:
    record = CUSTOMERS.get(args.customer_id)
    if record is None:
        return types.CallToolResult(content=_text(f"Customer {args.customer_id} not found"), is_error=True)
    data = {"customer_id": args.customer_id, **record}
    return types.CallToolResult(content=_text(json.dumps(data)), structured_content=data)


async def trigger_refund(args: TriggerRefundInput) -> types.CallToolResult:
    record = CUSTOMERS.get(args.customer_id)
    if record is None:
        return types.CallToolResult(content=_text(f"Customer {args.customer_id} not found"), is_error=True)
    if args.amount > record["balance"]:
        return types.CallToolResult(
            content=_text(f"Refund {args.amount:.2f} exceeds refundable balance {record['balance']:.2f}"),
            is_error=True,
        )
    record["balance"] = round(record["balance"] - args.amount, 2)
    refund = {
        "refund_id": f"RF-{uuid.uuid4().hex[:10].upper()}",
        "customer_id": args.customer_id,
        "amount": args.amount,
        "reason": args.reason,
        "status": "processed",
        "remaining_balance": record["balance"],
    }
    log.info("refund issued %s for %s amount=%.2f", refund["refund_id"], args.customer_id, args.amount)
    return types.CallToolResult(content=_text(json.dumps(refund)), structured_content=refund)


ToolHandler = Callable[[Any], Any]
TOOLS: dict[str, tuple[type[BaseModel], ToolHandler, str]] = {
    "get_customer_record": (
        GetCustomerRecordInput,
        get_customer_record,
        "Fetch a customer's profile and refundable balance by customer ID.",
    ),
    "trigger_refund": (
        TriggerRefundInput,
        trigger_refund,
        "Issue a refund to a customer. Amount must not exceed their refundable balance.",
    ),
}


def _format_validation_errors(exc: ValidationError) -> list[dict[str, Any]]:
    return [
        {"field": ".".join(str(p) for p in e["loc"]) or "<root>", "type": e["type"], "message": e["msg"]}
        for e in exc.errors(include_url=False, include_input=False)
    ]


async def handle_list_tools(ctx, params) -> types.ListToolsResult:
    return types.ListToolsResult(
        tools=[
            types.Tool(name=name, description=desc, input_schema=model.model_json_schema())
            for name, (model, _, desc) in TOOLS.items()
        ]
    )


async def handle_call_tool(ctx, params: types.CallToolRequestParams) -> types.CallToolResult:
    entry = TOOLS.get(params.name)
    if entry is None:
        log.warning("unknown tool requested: %r", params.name)
        raise MCPError(INVALID_PARAMS, f"Unknown tool: {params.name}")

    model, handler, _ = entry
    raw_args = params.arguments if params.arguments is not None else {}
    if not isinstance(raw_args, dict):
        raise MCPError(INVALID_PARAMS, "Tool arguments must be a JSON object")

    try:
        args = model.model_validate(raw_args)
    except ValidationError as exc:
        errors = _format_validation_errors(exc)
        log.info("rejected %s: %s", params.name, errors)
        raise MCPError(INVALID_PARAMS, f"Invalid arguments for tool '{params.name}'", data={"errors": errors})

    log.debug("calling %s with %s", params.name, args.model_dump())
    try:
        return await handler(args)
    except MCPError:
        raise
    except Exception:  # don't send tracebacks back to the client
        log.exception("tool %s crashed", params.name)
        raise MCPError(INTERNAL_ERROR, "Internal error while executing tool")


server = Server(
    "customer-support-mcp",
    version="1.0.0",
    on_list_tools=handle_list_tools,
    on_call_tool=handle_call_tool,
)


async def main() -> None:
    log.info("starting customer-support-mcp on stdio")
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
