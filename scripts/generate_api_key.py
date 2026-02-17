#!/usr/bin/env python3
from __future__ import annotations

import argparse
import secrets


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate HE API key")
    ap.add_argument("--bytes", type=int, default=32, help="Entropy bytes (default: 32)")
    ap.add_argument("--export", action="store_true", help="Print as shell export line")
    args = ap.parse_args()

    key = secrets.token_urlsafe(max(16, args.bytes))
    if args.export:
        print(f'export HE_API_KEY="{key}"')
    else:
        print(key)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
