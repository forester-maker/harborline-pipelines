"""Expected behavior of the invoicing logic. Run: uv run pytest -q"""

from datetime import date

import pytest

from flows.invoicing import compute_invoices, load_rates, load_scans, validate_rates

RATES = {
    "swiftbay": {"rate_per_kg": 0.5, "fee_per_shipment": 2.0},
    "quietline": {"rate_per_kg": 0.4, "fee_per_shipment": 1.0},
}
SCANS = [
    {"shipment_id": "HL-1", "carrier": "swiftbay", "weight_kg": 10.0, "delivered_on": date(2026, 10, 3)},
    {"shipment_id": "HL-2", "carrier": "swiftbay", "weight_kg": 30.0, "delivered_on": date(2026, 10, 21)},
    {"shipment_id": "HL-3", "carrier": "swiftbay", "weight_kg": 99.0, "delivered_on": date(2026, 9, 30)},
]


def test_amount_is_weight_times_rate_plus_fees():
    line = compute_invoices(SCANS, RATES, "2026-10")[1]  # sorted: quietline, swiftbay
    assert line["carrier"] == "swiftbay"
    assert line["shipments"] == 2
    assert line["amount_usd"] == pytest.approx(40 * 0.5 + 2 * 2.0)
    assert line["avg_cost_per_shipment"] == pytest.approx(12.0)


def test_carrier_with_no_shipments_bills_zero():
    line = compute_invoices(SCANS, RATES, "2026-10")[0]
    assert line == {"carrier": "quietline", "shipments": 0, "total_kg": 0, "amount_usd": 0.0, "avg_cost_per_shipment": 0.0}


def test_rates_must_be_positive():
    with pytest.raises(ValueError, match="must be positive"):
        validate_rates({"x": {"rate_per_kg": -0.1, "fee_per_shipment": 1}})


def test_repo_data_loads_and_validates():
    rates = load_rates()
    validate_rates(rates)
    scans = load_scans()
    assert scans and all(s["delivered_on"].year == 2026 for s in scans)
