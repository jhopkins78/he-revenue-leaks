from __future__ import annotations

import json
import os
import re
from typing import Set

from fastapi import Header, HTTPException


_TENANT_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_-]{1,63}$")


def _allowed_keys() -> Set[str]:
    single = os.getenv("HE_API_KEY", "").strip()
    multi = os.getenv("HE_API_KEYS_JSON", "").strip()
    keys: Set[str] = set()
    if single:
        keys.add(single)
    if multi:
        try:
            arr = json.loads(multi)
            if isinstance(arr, list):
                keys.update([str(x) for x in arr if str(x).strip()])
        except Exception:
            pass
    return keys


def require_api_key(x_api_key: str = Header(default="", alias="X-API-Key")) -> None:
    keys = _allowed_keys()
    if not keys:
        # fail closed in production-like environments
        if os.getenv("HE_ALLOW_NO_AUTH", "0") == "1":
            return
        raise HTTPException(status_code=503, detail={"code": "auth_not_configured", "message": "Server auth is not configured"})

    if x_api_key not in keys:
        raise HTTPException(status_code=401, detail={"code": "unauthorized", "message": "Invalid API key"})


def get_tenant_id(x_tenant_id: str = Header(default="", alias="X-Tenant-Id")) -> str:
    tenant = (x_tenant_id or "").strip()
    if not tenant:
        raise HTTPException(status_code=400, detail={"code": "tenant_missing", "message": "X-Tenant-Id header is required"})
    if not _TENANT_RE.match(tenant):
        raise HTTPException(status_code=400, detail={"code": "tenant_invalid", "message": "Invalid tenant id format"})
    return tenant
