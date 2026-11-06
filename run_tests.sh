#!/usr/bin/env bash
# Runs each task separately (tasks 2 and 3 both have a gateway.py).
set -e
cd "$(dirname "$0")"
for d in task1_mcp_server task2_mcp_gateway task3_pii_stream_guardrail task4_rate_limit_fallback_router; do
  echo "=== $d ==="
  (cd "$d" && python3 -m pytest -q -c ../pytest.ini --rootdir . -p no:cacheprovider)
done
