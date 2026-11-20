"""Simulate an operational (non-code) failure and start a production run.

    uv run python scripts/break_ops.py upstream_outage
    uv run python scripts/break_ops.py ok              # heal

The warehouse outage is driven by the Prefect Variable `harborline_invoicing_ops`,
so the code is untouched and the autofix agent should end with `not_a_code_bug`.
"""

from __future__ import annotations

import argparse

from prefect.deployments import run_deployment
from prefect.variables import Variable


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["upstream_outage", "ok"])
    ap.add_argument("--no-run", action="store_true")
    args = ap.parse_args()
    Variable.set("harborline_invoicing_ops", args.mode, overwrite=True)
    print(f"harborline_invoicing_ops = {args.mode}")
    if args.mode != "ok" and not args.no_run:
        run = run_deployment("harborline-carrier-invoicing/production", timeout=0)
        print(f"Started production run {run.name} ({run.id})")


if __name__ == "__main__":
    main()
