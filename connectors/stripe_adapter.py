from __future__ import annotations

import json
import os
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from connectors.base import ConnectorResult

STRIPE_API_BASE = "https://api.stripe.com/v1"
DEFAULT_ENTITIES = ["charges", "customers", "invoices", "refunds"]
CURSOR_PATH = Path("runtime/connectors/stripe_cursor.json")
RAW_DIR = Path("data/raw/stripe")
NORMALIZED_DIR = Path("data/normalized/stripe")


class StripeConnector:
    """Stripe connector with incremental-sync cursor tracking.

    Cursor strategy:
    - Per-entity watermark at max(created) epoch seconds.
    - Next sync uses `created[gte]=watermark`.
    """

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("STRIPE_API_KEY")
        if not self.api_key:
            raise ValueError("STRIPE_API_KEY is required for Stripe connector")

    def sync(
        self,
        entities: Optional[Iterable[str]] = None,
        since_epoch: Optional[int] = None,
        page_limit: int = 100,
    ) -> ConnectorResult:
        started = datetime.now().isoformat()
        entities = list(entities or DEFAULT_ENTITIES)
        cursor = self._load_cursor()

        total = 0
        per_entity: Dict[str, Dict] = {}
        for entity in entities:
            start_epoch = since_epoch if since_epoch is not None else int(cursor.get(entity, 0) or 0)
            records, max_created = self._fetch_entity(entity, start_epoch, page_limit=page_limit)
            self._write_outputs(entity, records)
            total += len(records)
            per_entity[entity] = {
                "records": len(records),
                "from_epoch": start_epoch,
                "to_epoch": max_created,
            }
            if max_created and max_created > int(cursor.get(entity, 0) or 0):
                cursor[entity] = max_created

        self._save_cursor(cursor)
        finished = datetime.now().isoformat()
        return ConnectorResult(
            connector="stripe",
            status="success",
            records_synced=total,
            started_at=started,
            finished_at=finished,
            details={"entities": per_entity, "cursor_path": str(CURSOR_PATH)},
        )

    def _fetch_entity(self, entity: str, start_epoch: int, page_limit: int = 100) -> Tuple[List[Dict], int]:
        all_records: List[Dict] = []
        max_created = start_epoch
        starting_after: Optional[str] = None

        while True:
            payload = {
                "limit": max(1, min(page_limit, 100)),
                "created[gte]": max(0, int(start_epoch)),
            }
            if starting_after:
                payload["starting_after"] = starting_after

            response = self._stripe_get(f"/{entity}", payload)
            page_records = response.get("data", []) or []
            all_records.extend(page_records)

            for rec in page_records:
                created = int(rec.get("created") or 0)
                if created > max_created:
                    max_created = created

            if not response.get("has_more") or not page_records:
                break
            starting_after = page_records[-1].get("id")
            if not starting_after:
                break

        return all_records, max_created

    def _stripe_get(self, path: str, params: Dict) -> Dict:
        qs = urlencode(params)
        req = Request(
            f"{STRIPE_API_BASE}{path}?{qs}",
            method="GET",
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        with urlopen(req, timeout=30) as res:
            body = res.read().decode("utf-8")
            return json.loads(body)

    @staticmethod
    def _load_cursor() -> Dict:
        if CURSOR_PATH.exists():
            try:
                return json.loads(CURSOR_PATH.read_text(encoding="utf-8"))
            except Exception:
                return {}
        return {}

    @staticmethod
    def _save_cursor(cursor: Dict) -> None:
        CURSOR_PATH.parent.mkdir(parents=True, exist_ok=True)
        CURSOR_PATH.write_text(json.dumps(cursor, indent=2), encoding="utf-8")

    @staticmethod
    def _write_outputs(entity: str, records: List[Dict]) -> None:
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        NORMALIZED_DIR.mkdir(parents=True, exist_ok=True)

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_path = RAW_DIR / f"{entity}_{ts}.json"
        normalized_path = NORMALIZED_DIR / f"{entity}_{ts}.jsonl"

        raw_path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")

        with normalized_path.open("w", encoding="utf-8") as f:
            for record in records:
                normalized = {
                    "connector": "stripe",
                    "entity": entity,
                    "id": record.get("id"),
                    "created": record.get("created"),
                    "livemode": record.get("livemode"),
                    "payload": record,
                }
                f.write(json.dumps(normalized, ensure_ascii=False) + "\n")


def serialize_result(result: ConnectorResult) -> Dict:
    return asdict(result)
