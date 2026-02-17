from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from connectors.stripe_adapter import StripeConnector, serialize_result

router = APIRouter()


class StripeSyncRequest(BaseModel):
    entities: Optional[List[str]] = None
    since_epoch: Optional[int] = Field(default=None, alias="sinceEpoch")
    page_limit: int = Field(default=100, ge=1, le=100, alias="pageLimit")

    model_config = {"populate_by_name": True}


class StripeSyncSummary(BaseModel):
    run_id: str = Field(..., alias="runId")
    started_at: str = Field(..., alias="startedAt")
    finished_at: str = Field(..., alias="finishedAt")
    duration_seconds: float = Field(..., alias="durationSeconds")
    connector_status: str = Field(..., alias="connectorStatus")
    records_synced: int = Field(..., alias="recordsSynced")
    entities: dict
    cursor_path: str = Field(..., alias="cursorPath")

    model_config = {"populate_by_name": True}


class StripeStatusSummary(BaseModel):
    configured: bool
    connector_status: str = Field(..., alias="connectorStatus")
    cursor_path: str = Field(..., alias="cursorPath")
    cursor: dict
    last_raw_artifact: Optional[str] = Field(default=None, alias="lastRawArtifact")
    last_normalized_artifact: Optional[str] = Field(default=None, alias="lastNormalizedArtifact")

    model_config = {"populate_by_name": True}


@router.post("/connectors/stripe/sync", response_model=StripeSyncSummary, response_model_by_alias=True)
def sync_stripe(req: StripeSyncRequest):
    # Safe auth handling: API key must be configured on server environment.
    # We intentionally do not accept API key in request body/query to avoid accidental leaks.
    api_key_present = bool(os.getenv("STRIPE_API_KEY"))
    if not api_key_present:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "stripe_not_configured",
                "message": "Stripe connector is not configured. Set STRIPE_API_KEY on the server.",
            },
        )

    run_started = datetime.now()
    run_id = f"stripe_sync_{run_started.strftime('%Y%m%d_%H%M%S')}"

    try:
        connector = StripeConnector()
        result = connector.sync(
            entities=req.entities,
            since_epoch=req.since_epoch,
            page_limit=req.page_limit,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail={"code": "invalid_configuration", "message": str(e)})
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail={
                "code": "stripe_sync_failed",
                "message": "Stripe sync failed",
                "reason": str(e),
            },
        )

    payload = serialize_result(result)
    started_at = datetime.fromisoformat(payload["started_at"])
    finished_at = datetime.fromisoformat(payload["finished_at"]) if payload.get("finished_at") else datetime.now()

    details = payload.get("details") or {}
    return {
        "runId": run_id,
        "startedAt": payload["started_at"],
        "finishedAt": payload.get("finished_at") or finished_at.isoformat(),
        "durationSeconds": round((finished_at - started_at).total_seconds(), 3),
        "connectorStatus": payload.get("status", "unknown"),
        "recordsSynced": payload.get("records_synced", 0),
        "entities": details.get("entities", {}),
        "cursorPath": details.get("cursor_path", "runtime/connectors/stripe_cursor.json"),
    }


def _latest_file_path(dir_path: Path) -> Optional[str]:
    if not dir_path.exists() or not dir_path.is_dir():
        return None
    files = [p for p in dir_path.iterdir() if p.is_file()]
    if not files:
        return None
    latest = max(files, key=lambda p: p.stat().st_mtime)
    return str(latest)


@router.get("/connectors/stripe/status", response_model=StripeStatusSummary, response_model_by_alias=True)
def stripe_status():
    configured = bool(os.getenv("STRIPE_API_KEY"))
    cursor_path = Path("runtime/connectors/stripe_cursor.json")
    cursor = {}

    if cursor_path.exists():
        try:
            cursor = json.loads(cursor_path.read_text(encoding="utf-8"))
        except Exception:
            cursor = {}

    last_raw = _latest_file_path(Path("data/raw/stripe"))
    last_norm = _latest_file_path(Path("data/normalized/stripe"))

    if not configured:
        status = "not_configured"
    elif not cursor and not last_raw and not last_norm:
        status = "configured_never_synced"
    else:
        status = "configured"

    return {
        "configured": configured,
        "connectorStatus": status,
        "cursorPath": str(cursor_path),
        "cursor": cursor,
        "lastRawArtifact": last_raw,
        "lastNormalizedArtifact": last_norm,
    }
