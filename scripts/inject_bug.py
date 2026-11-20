"""Commit one bug from the catalog to `main`, push it, and start a production run.

    uv run python scripts/inject_bug.py rate_field          # main demo bug
    uv run python scripts/inject_bug.py zero_shipments --no-run
    uv run python scripts/inject_bug.py --list

Each bug is a real commit with a plausible upstream message, so the autofix
agent has to find and fix it in code. Undo with scripts/reset_code.py.
"""

from __future__ import annotations

import argparse
import csv
import io
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _git import ROOT, git, push_main, sync_main  # noqa: E402

FLOW = ROOT / "flows" / "invoicing.py"
RATES = ROOT / "config" / "rates.yaml"
SCANS = ROOT / "data" / "scans.csv"


def rate_field() -> str:
    RATES.write_text(RATES.read_text().replace("rate_per_kg:", "rate_per_kilo:"))
    return "carrier-ops: rename rate_per_kg to rate_per_kilo to match the 2027 contract schema"


def zero_shipments() -> str:
    src = FLOW.read_text()
    new = src.replace("avg_cost = round(amount / count, 2) if count else 0.0", "avg_cost = round(amount / count, 2)")
    if new == src:
        raise SystemExit("zero_shipments: guard line not found (already injected?)")
    FLOW.write_text(new)
    RATES.write_text(
        RATES.read_text().rstrip("\n")
        + "\n  northline:\n    rate_per_kg: 0.47\n    fee_per_shipment: 3.10\n"
    )
    return "Onboard Northline carrier; simplify average cost calculation"


def date_format() -> str:
    rows = list(csv.reader(io.StringIO(SCANS.read_text())))
    for r in rows[1:]:
        m, d, y = r[3].split("/")
        r[3] = f"{d}/{m}/{y}"
    out = io.StringIO()
    csv.writer(out, lineterminator="\n").writerows(rows)
    SCANS.write_text(out.getvalue())
    return "ingest: carrier scan feed switched to DD/MM/YYYY dates"


def bad_config() -> str:
    text = RATES.read_text()
    new = re.sub(r"(ridgeway:\n\s+rate_per_kg:) 0\.38", r"\1 -0.38", text)
    if new == text:
        raise SystemExit("bad_config: ridgeway rate not found (already injected?)")
    RATES.write_text(new)
    return "carrier-ops: apply Q4 Ridgeway volume credit"


BUGS = {
    "rate_field": (rate_field, "KeyError: 'rate_per_kg' (config key renamed upstream; fix in code)"),
    "zero_shipments": (zero_shipments, "ZeroDivisionError for a carrier with no shipments (guard removed; fix in code)"),
    "date_format": (date_format, "ValueError parsing DD/MM dates with %m/%d/%Y (fix in code)"),
    "bad_config": (bad_config, "ValueError: negative rate (fix belongs in config/, which the agent may NOT edit)"),
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("bug", nargs="?", choices=sorted(BUGS))
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--no-run", action="store_true", help="Commit and push only; don't start a production run")
    args = ap.parse_args()
    if args.list or not args.bug:
        for name, (_, desc) in BUGS.items():
            print(f"{name:15} {desc}")
        return

    sync_main()
    message = BUGS[args.bug][0]()
    git("add", "-A", "flows", "config", "data")
    git("commit", "-m", message)
    push_main()
    sha = git("rev-parse", "--short", "HEAD")
    print(f"Injected {args.bug} as {sha}: {message}")

    if not args.no_run:
        from prefect.deployments import run_deployment

        run = run_deployment("harborline-carrier-invoicing/production", timeout=0)
        print(f"Started production run {run.name} ({run.id})")


if __name__ == "__main__":
    main()
