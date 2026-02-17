#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request


def req(url: str, key: str, tenant: str):
    r = urllib.request.Request(url, headers={"X-API-Key": key, "X-Tenant-Id": tenant})
    with urllib.request.urlopen(r, timeout=20) as resp:
        return resp.status, json.loads(resp.read().decode("utf-8"))


def main() -> int:
    base = os.getenv("BASE_URL", "http://localhost:8001")
    key = os.getenv("HE_API_KEY", "")
    tenant = os.getenv("TENANT_ID", "demo")

    if not key:
        print("Set HE_API_KEY in env for smoke test")
        return 2

    tests = [
        "/api/connectors/health",
        "/api/connectors/stripe/status",
        "/api/templates/revenue-leaks/contracts",
        "/api/templates/revenue-leaks/trend?limit=5",
        "/api/templates/revenue-leaks/runs?limit=5",
    ]

    for t in tests:
        url = base.rstrip("/") + t
        try:
            status, payload = req(url, key, tenant)
            if isinstance(payload, dict):
                preview = f"keys={list(payload.keys())[:5]}"
            elif isinstance(payload, list):
                preview = f"list_len={len(payload)}"
            else:
                preview = f"type={type(payload).__name__}"
            print(f"PASS {t} status={status} {preview}")
        except urllib.error.HTTPError as e:
            print(f"FAIL {t} http={e.code} body={e.read().decode('utf-8', errors='ignore')}")
            return 1
        except Exception as e:
            print(f"FAIL {t} err={e}")
            return 1

    print("SMOKE_TEST_PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
