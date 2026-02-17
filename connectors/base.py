from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional


@dataclass
class ConnectorResult:
    connector: str
    status: str
    records_synced: int = 0
    started_at: str = field(default_factory=lambda: datetime.now().isoformat())
    finished_at: Optional[str] = None
    details: Dict = field(default_factory=dict)


@dataclass
class ConnectorSpec:
    name: str
    auth_mode: str
    entities: List[str]
    supports_incremental: bool = True
