#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from connectors.stripe_adapter import StripeConnector, serialize_result


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync Stripe data into Harmony Engine")
    parser.add_argument("--tenant-id", required=True, help="Tenant id for isolated sync storage")
    parser.add_argument("--entities", nargs="*", default=None, help="Subset entities (charges customers invoices refunds disputes payment_intents balance_transactions)")
    parser.add_argument("--since-epoch", type=int, default=None, help="Override incremental cursor start (unix epoch)")
    parser.add_argument("--page-limit", type=int, default=100)
    args = parser.parse_args()

    connector = StripeConnector(tenant_id=args.tenant_id)
    result = connector.sync(entities=args.entities, since_epoch=args.since_epoch, page_limit=args.page_limit)
    print(json.dumps(serialize_result(result), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
