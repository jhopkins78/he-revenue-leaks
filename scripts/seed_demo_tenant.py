#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import pandas as pd


def seed_demo(tenant_id: str = "demo") -> dict:
    base = Path(f"data/normalized/{tenant_id}")
    base.mkdir(parents=True, exist_ok=True)

    orders = pd.DataFrame([
        {"order_id": "o1", "customer_id": "c1", "order_ts": "2026-01-05T10:00:00Z", "gross_revenue": 500, "discount_amount": 90, "net_revenue": 410, "shipping_cost": 40, "cogs_total": 220},
        {"order_id": "o2", "customer_id": "c2", "order_ts": "2026-01-12T11:00:00Z", "gross_revenue": 600, "discount_amount": 120, "net_revenue": 480, "shipping_cost": 55, "cogs_total": 280},
        {"order_id": "o3", "customer_id": "c3", "order_ts": "2026-01-17T13:00:00Z", "gross_revenue": 450, "discount_amount": 80, "net_revenue": 370, "shipping_cost": 42, "cogs_total": 210},
        {"order_id": "o4", "customer_id": "c1", "order_ts": "2025-11-20T10:00:00Z", "gross_revenue": 520, "discount_amount": 40, "net_revenue": 480, "shipping_cost": 28, "cogs_total": 230},
        {"order_id": "o5", "customer_id": "c4", "order_ts": "2025-12-02T10:00:00Z", "gross_revenue": 510, "discount_amount": 35, "net_revenue": 475, "shipping_cost": 26, "cogs_total": 220},
        {"order_id": "o6", "customer_id": "c5", "order_ts": "2025-12-22T10:00:00Z", "gross_revenue": 500, "discount_amount": 30, "net_revenue": 470, "shipping_cost": 25, "cogs_total": 215},
    ])

    order_lines = pd.DataFrame([
        {"order_id": "o1", "line_id": "l1", "sku_id": "sku_a", "qty": 1, "line_net": 410},
        {"order_id": "o2", "line_id": "l2", "sku_id": "sku_a", "qty": 1, "line_net": 480},
        {"order_id": "o3", "line_id": "l3", "sku_id": "sku_b", "qty": 1, "line_net": 370},
    ])

    refunds = pd.DataFrame([
        {"refund_id": "r1", "order_id": "o1", "refund_ts": "2026-01-20T10:00:00Z", "refund_amount": 140, "refund_reason": "quality"},
        {"refund_id": "r2", "order_id": "o2", "refund_ts": "2026-01-21T10:00:00Z", "refund_amount": 120, "refund_reason": "late_delivery"},
        {"refund_id": "r3", "order_id": "o4", "refund_ts": "2025-11-25T10:00:00Z", "refund_amount": 40, "refund_reason": "other"},
    ])

    payments = pd.DataFrame([
        {"payment_id": "p1", "order_id": "o1", "payment_ts": "2026-01-05T10:00:00Z", "amount": 410, "status": "succeeded", "dispute_amount": 0},
        {"payment_id": "p2", "order_id": "o2", "payment_ts": "2026-01-12T10:00:00Z", "amount": 480, "status": "failed", "dispute_amount": 0},
        {"payment_id": "p3", "order_id": "o3", "payment_ts": "2026-01-17T10:00:00Z", "amount": 370, "status": "disputed", "dispute_amount": 70},
        {"payment_id": "p4", "order_id": "o4", "payment_ts": "2025-11-20T10:00:00Z", "amount": 480, "status": "succeeded", "dispute_amount": 0},
        {"payment_id": "p5", "order_id": "o5", "payment_ts": "2025-12-02T10:00:00Z", "amount": 475, "status": "succeeded", "dispute_amount": 0},
    ])

    tickets = pd.DataFrame([
        {"ticket_id": "t1", "customer_id": "c1", "created_ts": "2026-01-18T10:00:00Z", "topic": "quality"},
        {"ticket_id": "t2", "customer_id": "c2", "created_ts": "2026-01-19T10:00:00Z", "topic": "delivery"},
        {"ticket_id": "t3", "customer_id": "c4", "created_ts": "2025-11-21T10:00:00Z", "topic": "billing"},
    ])

    discounts = pd.DataFrame([
        {"discount_event_id": "d1", "order_id": "o1", "coupon_code": "WELCOME", "customer_id": "c1", "discount_value": 90},
        {"discount_event_id": "d2", "order_id": "o2", "coupon_code": "WELCOME", "customer_id": "c2", "discount_value": 120},
        {"discount_event_id": "d3", "order_id": "o3", "coupon_code": "WELCOME", "customer_id": "c3", "discount_value": 80},
        {"discount_event_id": "d4", "order_id": "o4", "coupon_code": "WELCOME", "customer_id": "c1", "discount_value": 40},
    ])

    files = {
        "orders": base / "fact_orders.csv",
        "order_lines": base / "fact_order_lines.csv",
        "refunds": base / "fact_refunds.csv",
        "payments": base / "fact_payments.csv",
        "tickets": base / "fact_support_tickets.csv",
        "discounts": base / "fact_discounts.csv",
    }

    orders.to_csv(files["orders"], index=False)
    order_lines.to_csv(files["order_lines"], index=False)
    refunds.to_csv(files["refunds"], index=False)
    payments.to_csv(files["payments"], index=False)
    tickets.to_csv(files["tickets"], index=False)
    discounts.to_csv(files["discounts"], index=False)

    return {k: str(v) for k, v in files.items()}


if __name__ == "__main__":
    import argparse
    import json

    ap = argparse.ArgumentParser(description="Seed deterministic demo tenant data")
    ap.add_argument("--tenant-id", default="demo", help="Tenant id folder to write into")
    args = ap.parse_args()

    out = seed_demo(args.tenant_id)
    print(json.dumps(out, indent=2))
