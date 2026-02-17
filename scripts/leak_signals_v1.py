#!/usr/bin/env python3
"""Deterministic leak signal evaluator (v1).

Consumes normalized tables and emits top-10 revenue leak signals.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd


@dataclass
class LeakSignal:
    signal_id: str
    estimated_loss_usd: float
    severity: str
    confidence: float
    reason_code: str
    metrics: Dict


def _read_table(path: Optional[str]) -> pd.DataFrame:
    if not path:
        return pd.DataFrame()
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    if p.suffix == ".csv":
        return pd.read_csv(p)
    if p.suffix == ".jsonl":
        return pd.read_json(p, lines=True)
    if p.suffix == ".json":
        return pd.read_json(p)
    if p.suffix in {".parquet", ".pq"}:
        return pd.read_parquet(p)
    raise ValueError(f"Unsupported file type: {p}")


def _to_dt(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if col in df.columns and not df.empty:
        df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
    return df


def _safe_div(a: float, b: float) -> float:
    return float(a / b) if b not in (0, None) else 0.0


def _severity(loss: float, revenue_window: float) -> str:
    ratio = _safe_div(loss, max(revenue_window, 1.0))
    if ratio >= 0.08 or loss >= 10000:
        return "high"
    if ratio >= 0.03 or loss >= 2500:
        return "medium"
    return "low"


def _confidence(sample_size: int, completeness: float = 1.0) -> float:
    sample_score = min(1.0, _safe_div(sample_size, 1000) + 0.2)
    return round(max(0.1, min(1.0, 0.6 * completeness + 0.4 * sample_score)), 2)


def evaluate(
    orders: pd.DataFrame,
    order_lines: pd.DataFrame,
    refunds: pd.DataFrame,
    payments: pd.DataFrame,
    tickets: pd.DataFrame,
    discounts: pd.DataFrame,
) -> Dict:
    orders = _to_dt(orders, "order_ts")
    refunds = _to_dt(refunds, "refund_ts")
    payments = _to_dt(payments, "payment_ts")
    tickets = _to_dt(tickets, "created_ts")

    # derive anchor timestamp
    candidates = []
    for df, col in [(orders, "order_ts"), (refunds, "refund_ts"), (payments, "payment_ts")]:
        if not df.empty and col in df.columns:
            v = df[col].max()
            if pd.notna(v):
                candidates.append(v)
    anchor = max(candidates) if candidates else pd.Timestamp.utcnow().tz_localize("UTC")

    w_start = anchor - pd.Timedelta(days=28)
    b_start = anchor - pd.Timedelta(days=112)
    b_end = w_start

    ow = orders[(orders["order_ts"] >= w_start) & (orders["order_ts"] < anchor)] if "order_ts" in orders else pd.DataFrame()
    ob = orders[(orders["order_ts"] >= b_start) & (orders["order_ts"] < b_end)] if "order_ts" in orders else pd.DataFrame()

    rw = refunds[(refunds["refund_ts"] >= w_start) & (refunds["refund_ts"] < anchor)] if "refund_ts" in refunds else pd.DataFrame()
    rb = refunds[(refunds["refund_ts"] >= b_start) & (refunds["refund_ts"] < b_end)] if "refund_ts" in refunds else pd.DataFrame()

    pw = payments[(payments["payment_ts"] >= w_start) & (payments["payment_ts"] < anchor)] if "payment_ts" in payments else pd.DataFrame()
    pb = payments[(payments["payment_ts"] >= b_start) & (payments["payment_ts"] < b_end)] if "payment_ts" in payments else pd.DataFrame()

    tw = tickets[(tickets["created_ts"] >= w_start) & (tickets["created_ts"] < anchor)] if "created_ts" in tickets else pd.DataFrame()
    tb = tickets[(tickets["created_ts"] >= b_start) & (tickets["created_ts"] < b_end)] if "created_ts" in tickets else pd.DataFrame()

    net_w = float(ow.get("net_revenue", pd.Series(dtype=float)).sum())
    net_b = float(ob.get("net_revenue", pd.Series(dtype=float)).sum())
    gross_w = float(ow.get("gross_revenue", pd.Series(dtype=float)).sum())

    refund_w = float(rw.get("refund_amount", pd.Series(dtype=float)).sum())
    refund_b = float(rb.get("refund_amount", pd.Series(dtype=float)).sum())
    refund_rate_w = _safe_div(refund_w, net_w)
    refund_rate_b = _safe_div(refund_b, net_b)

    discount_w = float(ow.get("discount_amount", pd.Series(dtype=float)).sum())
    discount_b = float(ob.get("discount_amount", pd.Series(dtype=float)).sum())
    discount_rate_w = _safe_div(discount_w, gross_w)
    discount_rate_b = _safe_div(discount_b, float(ob.get("gross_revenue", pd.Series(dtype=float)).sum()))

    shipping_w = float(ow.get("shipping_cost", pd.Series(dtype=float)).sum())
    shipping_b = float(ob.get("shipping_cost", pd.Series(dtype=float)).sum())
    shipping_ratio_w = _safe_div(shipping_w, net_w)
    shipping_ratio_b = _safe_div(shipping_b, net_b)

    failed_w = pw[pw.get("status", pd.Series(dtype=str)).eq("failed")] if not pw.empty else pd.DataFrame()
    failed_b = pb[pb.get("status", pd.Series(dtype=str)).eq("failed")] if not pb.empty else pd.DataFrame()
    fail_rate_w = _safe_div(len(failed_w), len(pw))
    fail_rate_b = _safe_div(len(failed_b), len(pb))

    cogs_w = float(ow.get("cogs_total", pd.Series(dtype=float)).sum())
    cogs_b = float(ob.get("cogs_total", pd.Series(dtype=float)).sum())
    margin_w = _safe_div(net_w - cogs_w - shipping_w, net_w)
    margin_b = _safe_div(net_b - cogs_b - shipping_b, net_b)

    signals: List[LeakSignal] = []

    # 1 refund spike
    loss1 = max(0.0, refund_w - refund_rate_b * net_w) if refund_rate_w > refund_rate_b * 1.2 and refund_w >= 500 else 0.0
    signals.append(LeakSignal("refund_spike", round(loss1, 2), _severity(loss1, net_w), _confidence(len(rw)), "refund_rate_20pct_above_baseline", {"refund_rate_w": refund_rate_w, "refund_rate_b": refund_rate_b}))

    # 2 sku concentration
    loss2 = 0.0
    if not order_lines.empty and not rw.empty and "order_id" in order_lines and "order_id" in rw:
        merged = order_lines.merge(rw[["order_id", "refund_amount"]], on="order_id", how="inner")
        if "sku_id" in merged:
            grp = merged.groupby("sku_id", dropna=False)["refund_amount"].sum().sort_values(ascending=False).head(5)
            loss2 = float(grp.sum())
    signals.append(LeakSignal("sku_refund_concentration", round(loss2, 2), _severity(loss2, net_w), _confidence(len(order_lines)), "top_sku_refund_concentration", {}))

    # 3 discount overuse
    target_discount = max(discount_rate_b, 0.10)
    loss3 = max(0.0, (discount_rate_w - target_discount) * gross_w) if discount_rate_w > discount_rate_b + 0.03 else 0.0
    signals.append(LeakSignal("discount_overuse", round(loss3, 2), _severity(loss3, net_w), _confidence(len(ow)), "discount_rate_above_baseline_plus_3pp", {"discount_rate_w": discount_rate_w, "discount_rate_b": discount_rate_b}))

    # 4 coupon abuse
    loss4 = 0.0
    if not discounts.empty and {"coupon_code", "customer_id", "discount_value"}.issubset(discounts.columns):
        g = discounts.groupby("coupon_code", dropna=False).agg(uses=("coupon_code", "count"), users=("customer_id", "nunique"), val=("discount_value", "sum")).reset_index()
        abusive = g[g["uses"] / g["users"].clip(lower=1) > 3]
        loss4 = float(abusive["val"].sum())
    signals.append(LeakSignal("coupon_abuse", round(loss4, 2), _severity(loss4, net_w), _confidence(len(discounts)), "high_redemption_per_user", {}))

    # 5 shipping creep
    loss5 = max(0.0, shipping_w - shipping_ratio_b * net_w) if shipping_ratio_w > shipping_ratio_b * 1.15 else 0.0
    signals.append(LeakSignal("shipping_cost_creep", round(loss5, 2), _severity(loss5, net_w), _confidence(len(ow)), "shipping_ratio_15pct_above_baseline", {"shipping_ratio_w": shipping_ratio_w, "shipping_ratio_b": shipping_ratio_b}))

    # 6 failed payments
    failed_amount_w = float(failed_w.get("amount", pd.Series(dtype=float)).sum()) if not failed_w.empty else 0.0
    loss6 = failed_amount_w if fail_rate_w > fail_rate_b + 0.02 else 0.0
    signals.append(LeakSignal("failed_payment_recovery", round(loss6, 2), _severity(loss6, net_w), _confidence(len(pw)), "failed_payment_rate_above_baseline_plus_2pp", {"fail_rate_w": fail_rate_w, "fail_rate_b": fail_rate_b}))

    # 7 disputes
    disp_w = pw[pw.get("status", pd.Series(dtype=str)).eq("disputed")] if not pw.empty else pd.DataFrame()
    disp_b = pb[pb.get("status", pd.Series(dtype=str)).eq("disputed")] if not pb.empty else pd.DataFrame()
    dispute_amount_w = float(disp_w.get("dispute_amount", pd.Series(dtype=float)).sum())
    loss7 = dispute_amount_w + len(disp_w) * 15 if len(disp_w) > len(disp_b) * 1.2 else 0.0
    signals.append(LeakSignal("dispute_chargeback", round(loss7, 2), _severity(loss7, net_w), _confidence(len(pw)), "dispute_count_20pct_above_baseline", {}))

    # 8 margin compression
    loss8 = max(0.0, (margin_b - margin_w) * net_w) if margin_w < margin_b - 0.03 else 0.0
    signals.append(LeakSignal("margin_compression", round(loss8, 2), _severity(loss8, net_w), _confidence(len(ow)), "margin_drop_3pp", {"margin_w": margin_w, "margin_b": margin_b}))

    # 9 support linked refunds
    loss9 = 0.0
    if len(tw) > len(tb) * 1.2 and refund_w > refund_b * 1.1:
        expected_w = refund_b * (28.0 / 84.0)
        loss9 = max(0.0, refund_w - expected_w)
    signals.append(LeakSignal("support_linked_refunds", round(loss9, 2), _severity(loss9, net_w), _confidence(len(tw)), "support_growth_with_refund_growth", {"tickets_w": len(tw), "tickets_b": len(tb)}))

    # 10 repeat customer churned revenue
    loss10 = 0.0
    if "customer_id" in ow.columns and "customer_id" in ob.columns:
        rep_w = ow.groupby("customer_id").size()
        rep_b = ob.groupby("customer_id").size()
        repeaters_w = int((rep_w > 0).sum())
        repeaters_b = int((rep_b > 0).sum())
        aov_w = _safe_div(net_w, max(len(ow), 1))
        if repeaters_b > repeaters_w:
            loss10 = (repeaters_b - repeaters_w) * aov_w
    signals.append(LeakSignal("repeat_customer_churn", round(loss10, 2), _severity(loss10, net_w), _confidence(len(ow)), "repeat_customer_decline", {}))

    total = round(sum(s.estimated_loss_usd for s in signals), 2)
    return {
        "window": {
            "start": str(w_start),
            "end": str(anchor),
            "baseline_start": str(b_start),
            "baseline_end": str(b_end),
        },
        "summary": {
            "signals": len(signals),
            "total_estimated_loss_usd": total,
            "net_revenue_window": round(net_w, 2),
        },
        "signals": [asdict(s) for s in signals],
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Compute deterministic v1 leak signals")
    ap.add_argument("--orders", required=True)
    ap.add_argument("--order-lines", required=False, default=None)
    ap.add_argument("--refunds", required=False, default=None)
    ap.add_argument("--payments", required=False, default=None)
    ap.add_argument("--tickets", required=False, default=None)
    ap.add_argument("--discounts", required=False, default=None)
    ap.add_argument("--out", required=False, default="reports/leak_signals_v1.json")
    args = ap.parse_args()

    result = evaluate(
        orders=_read_table(args.orders),
        order_lines=_read_table(args.order_lines),
        refunds=_read_table(args.refunds),
        payments=_read_table(args.payments),
        tickets=_read_table(args.tickets),
        discounts=_read_table(args.discounts),
    )

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"wrote {out}")
    print(json.dumps(result["summary"], ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
