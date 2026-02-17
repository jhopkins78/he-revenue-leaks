from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.app.contracts import RevenueLeaksRunResponse, RevenueLeaksTrendResponse
from backend.app.security import get_tenant_id
from scripts.leak_signals_v1 import _read_table, evaluate

router = APIRouter()


def _runs_path(tenant_id: str) -> Path:
    return Path(f"logs/tenants/{tenant_id}/revenue_leaks_runs.jsonl")


class RevenueLeaksRunRequest(BaseModel):
    orders_path: str = Field(..., alias="ordersPath")
    order_lines_path: Optional[str] = Field(default=None, alias="orderLinesPath")
    refunds_path: Optional[str] = Field(default=None, alias="refundsPath")
    payments_path: Optional[str] = Field(default=None, alias="paymentsPath")
    tickets_path: Optional[str] = Field(default=None, alias="ticketsPath")
    discounts_path: Optional[str] = Field(default=None, alias="discountsPath")

    model_config = {"populate_by_name": True}


def _tenant_scoped_path(path: Optional[str], tenant_id: str) -> Optional[str]:
    if not path:
        return path
    p = Path(path)
    # hard tenant isolation guard
    if tenant_id not in p.as_posix().split("/"):
        raise HTTPException(status_code=400, detail={"code": "path_not_tenant_scoped", "message": f"Path must include tenant id '{tenant_id}'"})
    return path


def _persist_run(path: Path, entry: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def _load_runs(path: Path, limit: int = 50) -> list:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
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


@router.post("/templates/revenue-leaks/run", response_model=RevenueLeaksRunResponse, response_model_by_alias=True)
def run_revenue_leaks(req: RevenueLeaksRunRequest, tenant_id: str = Depends(get_tenant_id)):
    run_store = _runs_path(tenant_id)
    try:
        result = evaluate(
            orders=_read_table(_tenant_scoped_path(req.orders_path, tenant_id)),
            order_lines=_read_table(_tenant_scoped_path(req.order_lines_path, tenant_id)),
            refunds=_read_table(_tenant_scoped_path(req.refunds_path, tenant_id)),
            payments=_read_table(_tenant_scoped_path(req.payments_path, tenant_id)),
            tickets=_read_table(_tenant_scoped_path(req.tickets_path, tenant_id)),
            discounts=_read_table(_tenant_scoped_path(req.discounts_path, tenant_id)),
        )
    except HTTPException:
        raise
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
        "tenantId": tenant_id,
        "template": "revenue_leaks_v1",
        "summaryCards": dashboard.get("summaryCards", {}),
        "window": dashboard.get("window", {}),
        "topLeaks": dashboard.get("topLeaks", []),
    }
    _persist_run(run_store, run_entry)

    recent = _load_runs(run_store, limit=2)
    previous = recent[1] if len(recent) > 1 else None

    return {
        "status": "success",
        "template": "revenue_leaks_v1",
        "tenantId": tenant_id,
        "runTs": run_ts,
        "dashboard": dashboard,
        "deltas": _delta_from_previous(dashboard, previous),
    }


@router.get("/templates/revenue-leaks/contracts")
def get_revenue_leaks_contracts(tenant_id: str = Depends(get_tenant_id)):
    return {
        "status": "success",
        "tenantId": tenant_id,
        "template": "revenue_leaks_v1",
        "contracts": {
            "dashboard": {
                "summaryCards": [
                    "totalEstimatedLeakUsd",
                    "signalsDetected",
                    "highSeverityCount",
                    "netRevenueWindow",
                ],
                "topLeaks": "array<signal>",
                "allSignals": "array<signal>",
            },
            "trend": {
                "point": {"t": "iso8601", "leakUsd": "number", "high": "int", "signals": "int"}
            },
        },
    }


@router.get("/templates/revenue-leaks/runs")
def get_revenue_leaks_runs(limit: int = 30, tenant_id: str = Depends(get_tenant_id)):
    limit = max(1, min(int(limit), 200))
    run_store = _runs_path(tenant_id)
    runs = _load_runs(run_store, limit=limit)

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
        "tenantId": tenant_id,
        "count": len(runs),
        "latest": last,
        "deltas": deltas,
        "trend": trend,
        "runs": runs,
    }


@router.get("/templates/revenue-leaks/trend", response_model=RevenueLeaksTrendResponse, response_model_by_alias=True)
def get_revenue_leaks_trend(limit: int = 60, tenant_id: str = Depends(get_tenant_id)):
    """Compact trend payload optimized for chart rendering."""
    limit = max(1, min(int(limit), 365))
    run_store = _runs_path(tenant_id)
    runs = _load_runs(run_store, limit=limit)

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
        "tenantId": tenant_id,
        "count": len(points),
        "points": points,
    }
