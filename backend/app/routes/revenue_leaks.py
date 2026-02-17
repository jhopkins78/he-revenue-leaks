from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from scripts.leak_signals_v1 import evaluate, _read_table

router = APIRouter()

RUNS_PATH = Path("logs/revenue_leaks_runs.jsonl")


class RevenueLeaksRunRequest(BaseModel):
    orders_path: str = Field(..., alias="ordersPath")
    order_lines_path: Optional[str] = Field(default=None, alias="orderLinesPath")
    refunds_path: Optional[str] = Field(default=None, alias="refundsPath")
    payments_path: Optional[str] = Field(default=None, alias="paymentsPath")
    tickets_path: Optional[str] = Field(default=None, alias="ticketsPath")
    discounts_path: Optional[str] = Field(default=None, alias="discountsPath")

    model_config = {"populate_by_name": True}


def _persist_run(entry: dict) -> None:
    RUNS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with RUNS_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def _load_runs(limit: int = 50) -> list:
    if not RUNS_PATH.exists():
        return []
    rows = []
    for line in RUNS_PATH.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            continue
    rows.sort(key=lambda r: r.get("runTs", ""), reverse=True)
    return rows[:limit]


def _delta_from_previous(current: dict, previous: Optional[dict]) -> dict:
    if not previous:
        return {
            "totalEstimatedLeakUsdDelta": None,
            "highSeverityCountDelta": None,
            "signalsDetectedDelta": None,
        }

    cur_cards = current.get("summaryCards", {})
    prev_cards = previous.get("summaryCards", {})

    def d(key: str):
        return (cur_cards.get(key, 0) or 0) - (prev_cards.get(key, 0) or 0)

    return {
        "totalEstimatedLeakUsdDelta": round(d("totalEstimatedLeakUsd"), 2),
        "highSeverityCountDelta": d("highSeverityCount"),
        "signalsDetectedDelta": d("signalsDetected"),
    }


@router.post("/templates/revenue-leaks/run")
def run_revenue_leaks(req: RevenueLeaksRunRequest):
    try:
        result = evaluate(
            orders=_read_table(req.orders_path),
            order_lines=_read_table(req.order_lines_path),
            refunds=_read_table(req.refunds_path),
            payments=_read_table(req.payments_path),
            tickets=_read_table(req.tickets_path),
            discounts=_read_table(req.discounts_path),
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail={"code": "revenue_leaks_eval_failed", "message": str(e)})

    signals = result.get("signals", [])
    ranked = sorted(signals, key=lambda s: float(s.get("estimated_loss_usd", 0.0)), reverse=True)
    high = [s for s in ranked if s.get("severity") == "high"]

    dashboard = {
        "window": result.get("window", {}),
        "summaryCards": {
            "totalEstimatedLeakUsd": result.get("summary", {}).get("total_estimated_loss_usd", 0),
            "signalsDetected": result.get("summary", {}).get("signals", 0),
            "highSeverityCount": len(high),
            "netRevenueWindow": result.get("summary", {}).get("net_revenue_window", 0),
        },
        "topLeaks": ranked[:10],
        "allSignals": ranked,
    }

    run_ts = datetime.utcnow().isoformat() + "Z"
    run_entry = {
        "runTs": run_ts,
        "template": "revenue_leaks_v1",
        "summaryCards": dashboard.get("summaryCards", {}),
        "window": dashboard.get("window", {}),
        "topLeaks": dashboard.get("topLeaks", []),
    }
    _persist_run(run_entry)

    recent = _load_runs(limit=2)
    previous = recent[1] if len(recent) > 1 else None

    return {
        "status": "success",
        "template": "revenue_leaks_v1",
        "runTs": run_ts,
        "dashboard": dashboard,
        "deltas": _delta_from_previous(dashboard, previous),
    }


@router.get("/templates/revenue-leaks/runs")
def get_revenue_leaks_runs(limit: int = 30):
    limit = max(1, min(int(limit), 200))
    runs = _load_runs(limit=limit)

    trend = []
    for r in reversed(runs):
        cards = r.get("summaryCards", {})
        trend.append(
            {
                "runTs": r.get("runTs"),
                "totalEstimatedLeakUsd": cards.get("totalEstimatedLeakUsd", 0),
                "highSeverityCount": cards.get("highSeverityCount", 0),
                "signalsDetected": cards.get("signalsDetected", 0),
            }
        )

    last = runs[0] if runs else None
    prev = runs[1] if len(runs) > 1 else None

    deltas = _delta_from_previous(last or {"summaryCards": {}}, prev)

    return {
        "status": "success",
        "template": "revenue_leaks_v1",
        "count": len(runs),
        "latest": last,
        "deltas": deltas,
        "trend": trend,
        "runs": runs,
    }


@router.get("/templates/revenue-leaks/trend")
def get_revenue_leaks_trend(limit: int = 60):
    """Compact trend payload optimized for chart rendering."""
    limit = max(1, min(int(limit), 365))
    runs = _load_runs(limit=limit)

    points = []
    for r in reversed(runs):
        cards = r.get("summaryCards", {})
        points.append(
            {
                "t": r.get("runTs"),
                "leakUsd": cards.get("totalEstimatedLeakUsd", 0),
                "high": cards.get("highSeverityCount", 0),
                "signals": cards.get("signalsDetected", 0),
            }
        )

    return {
        "status": "success",
        "template": "revenue_leaks_v1",
        "count": len(points),
        "points": points,
    }
