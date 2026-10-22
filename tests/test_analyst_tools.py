from decimal import Decimal
from types import SimpleNamespace

import boto3
import pytest
from moto import mock_aws

from analyst import tools
from analyst.config import AnalystConfig
from common.athena import QueryResult

REGION = "us-east-1"
DAY = "2026-10-22"


def _alert(i: int, payer: str, score: float, reasons: list[str], day: str = DAY, city: str = "Pune") -> dict:
    return {
        "alert_id": f"t{i}", "txn_id": f"t{i}", "alert_day": day, "event_time": f"{day}T10:{i:02d}:00.000Z",
        "event_ts_ms": 1_792_670_000_000 + i * 60_000, "payer_id": payer, "payer_vpa": f"{payer.lower()}@ybl",
        "payee_vpa": f"mule{i}@ibl", "payee_type": "p2p", "amount": Decimal(str(1000 + i)), "merchant_category": "p2p",
        "channel": "UPI_INTENT", "city": city, "device_id": "dev-1", "score": Decimal(str(score)),
        "scored_by": "model", "reasons": reasons, "status": "OPEN", "sim_label_pattern": "velocity_burst",
        "features": {"txns_last_1m": Decimal("7"), "amount_ratio": Decimal("1.25")}, "expires_at": 1_800_000_000,
    }


class FakeAthena:
    def __init__(self):
        self.sql = []
        self.fail = None

    def run(self, sql, max_rows=200, timeout_s=120):
        self.sql.append(sql)
        if self.fail:
            raise RuntimeError(self.fail)
        return QueryResult(["city", "alerts"], [{"city": "Pune", "alerts": "3"}], "q-1", 2048, 900, False)


class FakeGlue:
    def get_paginator(self, name):
        assert name == "get_tables"
        pages = [{"TableList": [
            {"Name": "landing_payments", "StorageDescriptor": {"Columns": [{"Name": "txn_id", "Type": "string"}]}},
            {"Name": "silver_transactions", "TableType": "EXTERNAL_TABLE", "StorageDescriptor": {"Columns": [{"Name": "txn_id", "Type": "string"}, {"Name": "amount", "Type": "double"}]}},
            {"Name": "v_fraud_by_city", "TableType": "VIRTUAL_VIEW", "StorageDescriptor": {"Columns": [{"Name": "city", "Type": "string"}]}},
        ]}]
        return SimpleNamespace(paginate=lambda **kw: pages)


class FakeKb:
    def __init__(self):
        self.calls = []

    def retrieve(self, **kw):
        self.calls.append(kw)
        return {"retrievalResults": [{
            "content": {"text": "A velocity burst is many payments in a minute."},
            "location": {"s3Location": {"uri": "s3://bucket/kb/fraud-patterns.md"}}, "score": 0.6812,
        }]}


@pytest.fixture
def backends(monkeypatch):
    for k, v in {"AWS_DEFAULT_REGION": REGION, "AWS_ACCESS_KEY_ID": "testing", "AWS_SECRET_ACCESS_KEY": "testing"}.items():
        monkeypatch.setenv(k, v)
    with mock_aws():
        ddb = boto3.client("dynamodb", region_name=REGION)
        gsi = lambda name, pk: {  # noqa: E731
            "IndexName": name, "Projection": {"ProjectionType": "ALL"},
            "KeySchema": [{"AttributeName": pk, "KeyType": "HASH"}, {"AttributeName": "event_ts_ms", "KeyType": "RANGE"}],
        }
        ddb.create_table(
            TableName="alerts", BillingMode="PAY_PER_REQUEST",
            KeySchema=[{"AttributeName": "alert_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": n, "AttributeType": t} for n, t in
                                  [("alert_id", "S"), ("alert_day", "S"), ("payer_id", "S"), ("event_ts_ms", "N")]],
            GlobalSecondaryIndexes=[gsi("by_day", "alert_day"), gsi("by_payer", "payer_id")],
        )
        table = boto3.resource("dynamodb", region_name=REGION).Table("alerts")
        burst = ["velocity_burst: 7 payments in the previous minute", "p2p_fanout: new payee after 9 payments in 10 min"]
        for i in range(6):
            table.put_item(Item=_alert(i, "U00001", 0.99 - i * 0.05, burst))
        table.put_item(Item=_alert(10, "U00002", 0.7, ["impossible_travel: 1200 km at 9000 km/h"], city="Delhi"))
        table.put_item(Item=_alert(11, "U00003", 0.9, [], day="2026-10-21"))
        config = AnalystConfig("model", "wg", "db", "alerts", "KB123", max_rows=50)
        b = tools.Backends(config, boto3.Session(region_name=REGION))
        b.athena, b.glue, b.kb = FakeAthena(), FakeGlue(), FakeKb()
        yield b


def test_sql_tool_refuses_writes_and_multiple_statements(backends):
    for sql in ["DROP TABLE silver_transactions", "SELECT 1; DELETE FROM x", "INSERT INTO t SELECT 1"]:
        assert "error" in tools.query_lakehouse(backends, sql)
    assert backends.athena.sql == []


def test_sql_tool_returns_rows_and_turns_failures_into_messages(backends):
    ok = tools.query_lakehouse(backends, "SELECT city, count(*) AS alerts FROM silver_transactions GROUP BY 1")
    assert ok["rows"] == [{"city": "Pune", "alerts": "3"}] and ok["bytes_scanned"] == 2048
    backends.athena.fail = "COLUMN_NOT_FOUND: line 1:8: Column 'cty' cannot be resolved"
    bad = tools.query_lakehouse(backends, "SELECT cty FROM silver_transactions")
    assert "cty" in bad["error"]


def test_describe_tables_hides_landing_and_marks_views(backends):
    schema = tools.describe_tables(backends)
    assert set(schema) == {"silver_transactions", "v_fraud_by_city"}
    assert schema["v_fraud_by_city"]["type"] == "view"
    assert schema["silver_transactions"]["columns"] == ["txn_id string", "amount double"]
    assert "error" in tools.describe_tables(backends, "nope")


def test_recent_alerts_newest_first_with_score_filter(backends):
    r = tools.recent_alerts(backends, DAY, limit=3)
    assert [a["txn_id"] for a in r["alerts"]] == ["t10", "t5", "t4"]
    assert r["more_available"] is True
    high = tools.recent_alerts(backends, DAY, min_score=0.9, limit=50)
    assert {a["txn_id"] for a in high["alerts"]} == {"t0", "t1"}
    assert isinstance(high["alerts"][0]["amount"], int) and isinstance(high["alerts"][0]["score"], float)
    assert "error" in tools.recent_alerts(backends, "22/10/2026")


def test_alerts_carry_runbook_severity_and_ist_time(backends):
    by_id = {a["txn_id"]: a for a in tools.recent_alerts(backends, DAY, limit=50)["alerts"]}
    assert by_id["t0"]["severity"] == "CRITICAL"  # 0.99
    assert by_id["t1"]["severity"] == "HIGH"  # 0.94
    assert by_id["t10"]["severity"] == "MEDIUM"  # 0.70
    assert by_id["t0"]["event_time_ist"] == "2026-10-22 15:30:00 IST"  # 10:00 UTC
    assert tools.get_alert(backends, "t0")["severity"] == "CRITICAL"


def test_summarize_counts_in_code(backends):
    s = tools.summarize_alerts(backends, DAY)
    assert s["total_alerts"] == 7 and s["distinct_payers"] == 2
    assert s["money_at_risk_inr"] == sum(1000 + i for i in range(6)) + 1010
    assert s["top_payers"][0] == {"payer_id": "U00001", "payer_vpa": "u00001@ybl", "alerts": 6, "amount_inr": 6015.0}
    assert {"key": "velocity_burst", "alerts": 6} in s["alerts_with_reason"]
    assert s["critical_alerts_score_0_95_plus"] == 1
    assert tools.summarize_alerts(backends, "2020-01-01") == {"day": "2020-01-01", "total_alerts": 0}


def test_payer_alerts_and_alert_details(backends):
    r = tools.payer_alerts(backends, "U00001", limit=2)
    assert r["total_alerts"] == 6 and r["returned"] == 2 and r["alerts"][0]["txn_id"] == "t5"
    detail = tools.get_alert(backends, "t10")
    assert detail["features"] == {"txns_last_1m": 7, "amount_ratio": 1.25} and "expires_at" not in detail
    assert "error" in tools.get_alert(backends, "missing")


def test_playbook_search(backends):
    r = tools.search_playbook(backends, "velocity burst", max_results=20)
    assert r["passages"][0] == {"text": "A velocity burst is many payments in a minute.", "source": "fraud-patterns.md", "score": 0.681}
    assert backends.kb.calls[0]["retrievalConfiguration"]["vectorSearchConfiguration"]["numberOfResults"] == 8


def test_strands_tool_specs(backends):
    specs = {t.tool_name: t.tool_spec for t in tools.make_tools(backends)}
    assert set(specs) == {
        "run_lakehouse_sql", "list_lakehouse_tables", "summarize_todays_alerts", "get_recent_alerts",
        "get_payer_alerts", "get_alert_details", "search_fraud_playbook",
    }
    sql_schema = specs["run_lakehouse_sql"]["inputSchema"]["json"]
    assert sql_schema["required"] == ["sql"]
    assert "read-only" in specs["run_lakehouse_sql"]["description"]
