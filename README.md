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
- `POST /api/connectors/stripe/sync`
- `GET /api/connectors/stripe/status`
- `POST /api/templates/revenue-leaks/run`
- `GET /api/templates/revenue-leaks/runs`
- `GET /api/templates/revenue-leaks/trend`

## Notes
Set `STRIPE_API_KEY` in the server environment before running Stripe sync.
