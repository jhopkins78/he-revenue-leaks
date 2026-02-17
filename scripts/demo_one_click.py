#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.seed_demo_tenant import seed_demo


def post_json(url: str, payload: dict, headers: dict):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={**headers, "Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def get_json(url: str, headers: dict):
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main() -> int:
    base = os.getenv("BASE_URL", "http://localhost:8001").rstrip("/")
    api_key = os.getenv("HE_API_KEY", "replace_me")
    tenant = os.getenv("TENANT_ID", "demo")

    files = seed_demo(tenant)

    headers = {
        "X-API-Key": api_key,
        "X-Tenant-Id": tenant,
    }

    payload = {
        "ordersPath": files["orders"],
        "orderLinesPath": files["order_lines"],
        "refundsPath": files["refunds"],
        "paymentsPath": files["payments"],
        "ticketsPath": files["tickets"],
        "discountsPath": files["discounts"],
    }

    run = post_json(f"{base}/api/templates/revenue-leaks/run", payload, headers)
    trend = get_json(f"{base}/api/templates/revenue-leaks/trend?limit=12", headers)

    screenshot_notes = Path(f"reports/{tenant}_demo_notes.md")
    screenshot_notes.parent.mkdir(parents=True, exist_ok=True)
    screenshot_notes.write_text(
        "\n".join(
            [
                f"# Demo Notes ({tenant})",
                "",
                f"- Dashboard URL: {base}/dashboard",
                f"- API docs: {base}/docs",
                f"- Latest runTs: {run.get('runTs')}",
                f"- Total Estimated Leak USD: {run.get('dashboard', {}).get('summaryCards', {}).get('totalEstimatedLeakUsd')}",
                f"- High Severity Count: {run.get('dashboard', {}).get('summaryCards', {}).get('highSeverityCount')}",
                f"- Trend points: {trend.get('count')}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    print("DEMO_READY")
    print(json.dumps({"runTs": run.get("runTs"), "summary": run.get("dashboard", {}).get("summaryCards", {}), "trendCount": trend.get("count")}, indent=2))
    print(f"Dashboard: {base}/dashboard")
    print(f"Notes: {screenshot_notes}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
