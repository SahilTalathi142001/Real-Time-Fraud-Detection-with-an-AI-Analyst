from datetime import datetime, timezone

import pytest

from ui import data

ALERTS = [
    {"txn_id": "a", "event_time": "2026-10-22T10:58:00.000Z", "payer_id": "U1", "payer_vpa": "u1@ybl", "payee_vpa": "m@ibl",
     "amount": 1200.5, "city": "Pune", "merchant_category": "p2p", "score": 0.99, "scored_by": "model",
     "reasons": ["velocity_burst: 7 payments in the previous minute", "p2p_fanout: new payee after 9 payments in 10 min"]},
    {"txn_id": "b", "event_time": "2026-10-22T10:30:00.000Z", "payer_id": "U1", "payer_vpa": "u1@ybl", "payee_vpa": "n@ibl",
     "amount": 800.0, "city": "Delhi", "merchant_category": "electronics", "score": 0.9, "scored_by": "model",
     "reasons": ["velocity_burst: 4 payments in the previous minute"]},
    {"txn_id": "c", "event_time": "2026-10-22T10:55:00.000Z", "payer_id": "U2", "payer_vpa": "u2@ybl", "payee_vpa": "o@ibl",
     "amount": 50000.0, "city": "Delhi", "merchant_category": "jewellery", "score": 0.97, "scored_by": "rules", "reasons": []},
]


def test_kpis():
    now = datetime(2026, 10, 22, 11, 0, tzinfo=timezone.utc)
    assert data.alert_kpis(ALERTS, window_minutes=10, now=now) == {
        "alerts": 3, "money_at_risk": 52000.5, "payers": 2, "critical": 2, "last_window": 2,
    }


def test_reason_counts_count_each_code_once_per_alert():
    assert data.reason_counts(ALERTS) == {"velocity_burst": 2, "p2p_fanout": 1}


def test_table_rows():
    row = data.table_rows(ALERTS[:1])[0]
    assert row["time (UTC)"] == "10:58:00"
    assert row["reasons"] == "p2p_fanout, velocity_burst"


def test_ui_sql_is_read_only():
    with pytest.raises(ValueError):
        data.run_sql(object(), "DELETE FROM silver_transactions")
