"""harborline-carrier-invoicing: monthly invoice totals per carrier.

Reads shipment scans (data/scans.csv) and carrier rates (config/rates.yaml),
validates the rates, computes each carrier's invoice for the month, and
publishes a summary table as a Prefect artifact.

The pure functions (load_*, validate_rates, compute_invoices) are tested in
tests/test_invoicing.py. The flow wraps them in tasks.
"""

from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path

import yaml
from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact
from prefect.variables import Variable

ROOT = Path(__file__).resolve().parent.parent
SCANS_PATH = ROOT / "data" / "scans.csv"
RATES_PATH = ROOT / "config" / "rates.yaml"
DATE_FORMAT = "%m/%d/%Y"


# ---------------------------------------------------------------- pure logic --


def load_rates(path: Path = RATES_PATH) -> dict[str, dict]:
    """Carrier -> {rate_per_kg, fee_per_shipment} from the rates file.

    Accepts the 2027 contract key `rate_per_kilo` and maps it to `rate_per_kg`.
    """
    with open(path) as f:
        carriers = yaml.safe_load(f)["carriers"]
    for rate in carriers.values():
        if "rate_per_kilo" in rate:
            rate["rate_per_kg"] = rate.pop("rate_per_kilo")
    return carriers


def load_scans(path: Path = SCANS_PATH) -> list[dict]:
    """Delivered shipments, with `delivered_on` parsed to a date."""
    rows = []
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            rows.append(
                {
                    "shipment_id": row["shipment_id"],
                    "carrier": row["carrier"],
                    "weight_kg": float(row["weight_kg"]),
                    "delivered_on": datetime.strptime(row["delivered_on"], DATE_FORMAT).date(),
                }
            )
    return rows


def validate_rates(rates: dict[str, dict]) -> None:
    """Every carrier needs a positive per-kg rate and a non-negative per-shipment fee."""
    for carrier, rate in rates.items():
        per_kg = float(rate["rate_per_kg"])
        fee = float(rate["fee_per_shipment"])
        if per_kg <= 0:
            raise ValueError(f"rate_per_kg for {carrier} must be positive, got {per_kg}")
        if fee < 0:
            raise ValueError(f"fee_per_shipment for {carrier} must not be negative, got {fee}")


def compute_invoices(scans: list[dict], rates: dict[str, dict], month: str) -> list[dict]:
    """One invoice line per carrier in `rates` for `month` (YYYY-MM)."""
    year, mon = (int(x) for x in month.split("-"))
    lines = []
    for carrier, rate in sorted(rates.items()):
        shipments = [
            s for s in scans
            if s["carrier"] == carrier and s["delivered_on"].year == year and s["delivered_on"].month == mon
        ]
        count = len(shipments)
        total_kg = round(sum(s["weight_kg"] for s in shipments), 2)
        amount = round(total_kg * float(rate["rate_per_kg"]) + count * float(rate["fee_per_shipment"]), 2)
        avg_cost = round(amount / count, 2) if count else 0.0
        lines.append(
            {
                "carrier": carrier,
                "shipments": count,
                "total_kg": total_kg,
                "amount_usd": amount,
                "avg_cost_per_shipment": avg_cost,
            }
        )
    return lines


# ---------------------------------------------------------------- the flow ----


@task
def load_inputs() -> tuple[list[dict], dict[str, dict]]:
    log = get_run_logger()
    rates = load_rates()
    scans = load_scans()
    log.info("Loaded %d scans and rates for %d carriers", len(scans), len(rates))
    return scans, rates


@task
def check_rates(rates: dict[str, dict]) -> None:
    validate_rates(rates)
    get_run_logger().info("Rates valid for %s", ", ".join(sorted(rates)))


@task
def build_invoices(scans: list[dict], rates: dict[str, dict], month: str) -> list[dict]:
    lines = compute_invoices(scans, rates, month)
    get_run_logger().info("Computed %d invoice lines for %s", len(lines), month)
    return lines


@task
def publish(lines: list[dict], month: str) -> float:
    log = get_run_logger()
    # Operational dependency: the finance warehouse. A Prefect Variable lets the
    # demo simulate an outage (scripts/break_ops.py) without touching this code.
    if (Variable.get("harborline_invoicing_ops", default="ok") or "ok") == "upstream_outage":
        raise ConnectionError(
            "could not connect to warehouse finance.harborline.internal:5432: "
            "timeout after 30s (connection refused by upstream load balancer)"
        )
    table = "| Carrier | Shipments | Total kg | Amount (USD) | Avg cost |\n|---|---:|---:|---:|---:|\n" + "\n".join(
        f"| {l['carrier']} | {l['shipments']} | {l['total_kg']:,} | {l['amount_usd']:,.2f} | {l['avg_cost_per_shipment']:,.2f} |"
        for l in lines
    )
    total = round(sum(l["amount_usd"] for l in lines), 2)
    create_markdown_artifact(
        key="carrier-invoices", markdown=f"# Carrier invoices {month}\n\n{table}\n\n**Total: ${total:,.2f}**"
    )
    log.info("Published invoices for %s: total $%s", month, f"{total:,.2f}")
    return total


@flow(name="harborline-carrier-invoicing")
def carrier_invoicing(month: str = "2026-10", triggered_by: str = "schedule", source_commit: str = "") -> float:
    """Monthly carrier invoices for Harborline finance.

    `triggered_by` and `source_commit` are provenance only (who started this run and
    which commit it is meant to test); they don't change the computation.
    """
    if triggered_by != "schedule" or source_commit:
        get_run_logger().info("Triggered by %s (commit %s)", triggered_by, source_commit or "n/a")
    scans, rates = load_inputs()
    check_rates(rates)
    lines = build_invoices(scans, rates, month)
    return publish(lines, month)


if __name__ == "__main__":
    carrier_invoicing()
