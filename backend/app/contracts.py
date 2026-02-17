from __future__ import annotations

from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class SummaryCards(BaseModel):
    total_estimated_leak_usd: float = Field(..., alias="totalEstimatedLeakUsd")
    signals_detected: int = Field(..., alias="signalsDetected")
    high_severity_count: int = Field(..., alias="highSeverityCount")
    net_revenue_window: float = Field(..., alias="netRevenueWindow")

    model_config = {"populate_by_name": True}


class RevenueLeaksDashboard(BaseModel):
    window: Dict
    summary_cards: SummaryCards = Field(..., alias="summaryCards")
    top_leaks: List[Dict] = Field(default_factory=list, alias="topLeaks")
    all_signals: List[Dict] = Field(default_factory=list, alias="allSignals")

    model_config = {"populate_by_name": True}


class RevenueLeaksRunResponse(BaseModel):
    status: str
    template: str
    tenant_id: str = Field(..., alias="tenantId")
    run_ts: str = Field(..., alias="runTs")
    dashboard: RevenueLeaksDashboard
    deltas: Dict

    model_config = {"populate_by_name": True}


class RevenueLeaksTrendPoint(BaseModel):
    t: str
    leak_usd: float = Field(..., alias="leakUsd")
    high: int
    signals: int

    model_config = {"populate_by_name": True}


class RevenueLeaksTrendResponse(BaseModel):
    status: str
    template: str
    tenant_id: str = Field(..., alias="tenantId")
    count: int
    points: List[RevenueLeaksTrendPoint]

    model_config = {"populate_by_name": True}


class ConnectorHealth(BaseModel):
    name: str
    status: str
    configured: bool
    last_run_ts: Optional[str] = Field(default=None, alias="lastRunTs")
    last_error: Optional[str] = Field(default=None, alias="lastError")

    model_config = {"populate_by_name": True}
