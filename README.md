# HE Revenue Leaks

Focused product extraction from Harmony Engine for the SMB Revenue Leak Program.

## Included
- Stripe connector + status/sync endpoints
- Revenue leak evaluator (10 deterministic signals)
- Revenue leak run/history/trend APIs
- SQL/dbt-style metric definitions

## Run
```bash
cd /Users/joshuahopkins/Documents/he-revenue-leaks
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn backend.app.main:app --host 0.0.0.0 --port 8001
```

## Endpoints
- `GET /api/connectors/health`
- `POST /api/connectors/stripe/sync`
- `GET /api/connectors/stripe/status`
- `POST /api/templates/revenue-leaks/run`
- `GET /api/templates/revenue-leaks/contracts`
- `GET /api/templates/revenue-leaks/runs`
- `GET /api/templates/revenue-leaks/trend`

## Security + Tenant isolation
All `/api/*` endpoints require headers:
- `X-API-Key: <server key>`
- `X-Tenant-Id: <tenant_slug>`

Set auth env vars:
- `HE_API_KEY` (single key) or
- `HE_API_KEYS_JSON` (JSON array of keys)

Each tenant is isolated in storage paths:
- `runtime/connectors/<tenant>/...`
- `data/raw/<tenant>/...`
- `data/normalized/<tenant>/...`
- `logs/tenants/<tenant>/...`

## Notes
Set `STRIPE_API_KEY` in the server environment before running Stripe sync.
