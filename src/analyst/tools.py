"""The analyst's tools: lakehouse SQL (Athena), live alert lookups (DynamoDB), and the fraud playbook (Bedrock KB).

Each tool is a plain function over a `Backends` object so tests can swap in fakes; `make_tools`
wraps them as Strands tools. Tools return errors as data ({"error": ...}) instead of raising, so the
model can read the message and correct its next call (e.g. fix a typo in a column name).
"""

from __future__ import annotations

import re
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr, Key
from strands import tool

from analyst.config import AnalystConfig
from common.athena import Athena, is_read_only

ALERT_SUMMARY_FIELDS = (
    "txn_id", "event_time", "payer_id", "payer_vpa", "payee_vpa", "payee_type", "amount", "merchant_category",
    "channel", "city", "device_id", "score", "scored_by", "reasons", "status", "sim_label_pattern",
)
DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
IST = timezone(timedelta(hours=5, minutes=30))
MAX_ALERTS = 50
MAX_SUMMARY_ITEMS = 5000
SUMMARY_PROJECTION = "payer_id, payer_vpa, amount, reasons, score, scored_by, sim_label_pattern, city, event_time"


class Backends:
    def __init__(self, config: AnalystConfig, session: boto3.Session | None = None) -> None:
        session = session or boto3.Session()
        self.config = config
        self.athena = Athena(config.athena_workgroup, config.glue_database, session)
        self.glue = session.client("glue")
        self.alerts = session.resource("dynamodb").Table(config.alerts_table)
        self.kb = session.client("bedrock-agent-runtime")
        self._schema: dict | None = None


def plain(value):
    """DynamoDB returns Decimals and sets; make them JSON friendly."""
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() and "." not in str(value) else float(value)
    if isinstance(value, dict):
        return {k: plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [plain(v) for v in value]
    return value


def severity(score: float) -> str:
    """Runbook severity bands (kb/alert-triage-runbook.md); computed here so the model never has to."""
    return "CRITICAL" if score >= 0.95 else "HIGH" if score >= 0.85 else "MEDIUM"


def to_ist(event_time: str) -> str:
    ts = datetime.fromisoformat(event_time.replace("Z", "+00:00")).astimezone(IST)
    return ts.strftime("%Y-%m-%d %H:%M:%S IST")


def _enrich(item: dict) -> dict:
    if "score" in item:
        item["severity"] = severity(float(item["score"]))
    if "event_time" in item:
        item["event_time_ist"] = to_ist(item["event_time"])
    return item


def _summary(item: dict) -> dict:
    return _enrich({k: plain(item[k]) for k in ALERT_SUMMARY_FIELDS if k in item})


def query_lakehouse(b: Backends, sql: str) -> dict:
    if not is_read_only(sql):
        return {"error": "Only one read-only SELECT / WITH / SHOW / DESCRIBE statement is allowed."}
    try:
        r = b.athena.run(sql, max_rows=b.config.max_rows, timeout_s=90)
    except Exception as e:  # noqa: BLE001 - the model needs the message to fix its SQL
        return {"error": str(e)[:600]}
    return {
        "columns": r.columns,
        "rows": r.rows,
        "row_count": len(r.rows),
        "truncated": r.truncated,
        "bytes_scanned": r.bytes_scanned,
        "query_id": r.query_id,
    }


def describe_tables(b: Backends, table: str = "") -> dict:
    if b._schema is None:
        schema = {}
        for page in b.glue.get_paginator("get_tables").paginate(DatabaseName=b.config.glue_database):
            for t in page["TableList"]:
                if t["Name"].startswith("landing_"):
                    continue  # transient files the loader writes; never query them
                sd = t.get("StorageDescriptor", {})
                schema[t["Name"]] = {
                    "type": "view" if t.get("TableType") == "VIRTUAL_VIEW" else "table",
                    "columns": [f"{c['Name']} {c['Type']}" for c in sd.get("Columns", [])],
                }
        b._schema = schema
    if table:
        return {table: b._schema[table]} if table in b._schema else {"error": f"no table {table!r}", "tables": sorted(b._schema)}
    return b._schema


def recent_alerts(b: Backends, day: str = "", min_score: float = 0.0, limit: int = 20) -> dict:
    day = day or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if not DAY_RE.match(day):
        return {"error": "day must look like YYYY-MM-DD"}
    limit = max(1, min(int(limit), MAX_ALERTS))
    kwargs = {"IndexName": "by_day", "KeyConditionExpression": Key("alert_day").eq(day), "ScanIndexForward": False}
    if min_score:
        kwargs["FilterExpression"] = Attr("score").gte(Decimal(str(min_score)))
    items, scanned = [], 0
    while len(items) < limit:
        resp = b.alerts.query(**kwargs)
        items += resp["Items"]
        scanned += resp.get("ScannedCount", len(resp["Items"]))
        if "LastEvaluatedKey" not in resp or scanned >= 2000:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return {"day": day, "alerts": [_summary(i) for i in items[:limit]], "returned": min(len(items), limit), "more_available": len(items) > limit}


def _top(counter: Counter, n: int = 10) -> list[dict]:
    return [{"key": k, "alerts": v} for k, v in counter.most_common(n)]


def summarize_alerts(b: Backends, day: str = "") -> dict:
    """Count a day's alerts in code: the model shouldn't have to count rows itself."""
    day = day or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if not DAY_RE.match(day):
        return {"error": "day must look like YYYY-MM-DD"}
    kwargs = {"IndexName": "by_day", "KeyConditionExpression": Key("alert_day").eq(day), "ProjectionExpression": SUMMARY_PROJECTION}
    items: list[dict] = []
    while len(items) < MAX_SUMMARY_ITEMS:
        resp = b.alerts.query(**kwargs)
        items += resp["Items"]
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    if not items:
        return {"day": day, "total_alerts": 0}

    payers, payer_amount, payer_vpa = Counter(), Counter(), {}
    reasons, patterns, cities, scorers = Counter(), Counter(), Counter(), Counter()
    for it in items:
        payers[it["payer_id"]] += 1
        payer_amount[it["payer_id"]] += float(it["amount"])
        payer_vpa[it["payer_id"]] = it.get("payer_vpa")
        reasons.update({r.split(":", 1)[0] for r in it.get("reasons", [])})
        patterns[it.get("sim_label_pattern", "unknown")] += 1
        cities[it.get("city", "unknown")] += 1
        scorers[it.get("scored_by", "unknown")] += 1
    scores = [float(it["score"]) for it in items]
    return {
        "day": day,
        "total_alerts": len(items),
        "capped": len(items) >= MAX_SUMMARY_ITEMS,
        "money_at_risk_inr": round(sum(float(it["amount"]) for it in items), 2),
        "distinct_payers": len(payers),
        "first_alert": min(it["event_time"] for it in items),
        "last_alert": max(it["event_time"] for it in items),
        "avg_score": round(sum(scores) / len(scores), 3),
        "critical_alerts_score_0_95_plus": sum(s >= 0.95 for s in scores),
        "top_payers": [
            {"payer_id": p, "payer_vpa": payer_vpa[p], "alerts": n, "amount_inr": round(payer_amount[p], 2)}
            for p, n in payers.most_common(10)
        ],
        "alerts_with_reason": _top(reasons),
        "alerts_by_city": _top(cities),
        "alerts_by_sim_pattern": _top(patterns),
        "alerts_by_scorer": _top(scorers),
    }


def payer_alerts(b: Backends, payer_id: str, limit: int = 20) -> dict:
    limit = max(1, min(int(limit), MAX_ALERTS))
    key = Key("payer_id").eq(payer_id)
    resp = b.alerts.query(IndexName="by_payer", KeyConditionExpression=key, ScanIndexForward=False, Limit=limit)
    total = b.alerts.query(IndexName="by_payer", KeyConditionExpression=key, Select="COUNT")["Count"]
    return {"payer_id": payer_id, "total_alerts": total, "alerts": [_summary(i) for i in resp["Items"]], "returned": len(resp["Items"])}


def get_alert(b: Backends, txn_id: str) -> dict:
    item = b.alerts.get_item(Key={"alert_id": txn_id}).get("Item")
    if not item:
        return {"error": f"no alert for transaction {txn_id!r} (it may not have been flagged)"}
    item = plain(item)
    item.pop("expires_at", None)
    return _enrich(item)


def search_playbook(b: Backends, query: str, max_results: int = 4) -> dict:
    if not b.config.knowledge_base_id:
        return {"error": "knowledge base is not configured"}
    try:
        resp = b.kb.retrieve(
            knowledgeBaseId=b.config.knowledge_base_id,
            retrievalQuery={"text": query},
            retrievalConfiguration={"vectorSearchConfiguration": {"numberOfResults": max(1, min(int(max_results), 8))}},
        )
    except Exception as e:  # noqa: BLE001
        return {"error": str(e)[:400]}
    return {
        "passages": [
            {
                "text": r["content"]["text"],
                "source": r.get("location", {}).get("s3Location", {}).get("uri", "").rsplit("/", 1)[-1],
                "score": round(float(r.get("score", 0.0)), 3),
            }
            for r in resp.get("retrievalResults", [])
        ]
    }


def make_tools(b: Backends) -> list:
    @tool
    def run_lakehouse_sql(sql: str) -> dict:
        """Run one read-only Athena (Trino SQL) query against the fraud lakehouse and return the rows.

        Use for history, trends, aggregates and anything older than today: silver_transactions (one row
        per payment), gold_fraud_daily, gold_payer_risk, gold_detection_quality, and the v_* views.
        Always filter silver_transactions by event_date when you can and add a LIMIT.

        Args:
            sql: A single SELECT or WITH query. No DDL/DML, no semicolons.
        """
        return query_lakehouse(b, sql)

    @tool
    def list_lakehouse_tables(table: str = "") -> dict:
        """List lakehouse tables and views with their columns and types (from the Glue Data Catalog).

        Args:
            table: Optional table name to describe just that table.
        """
        return describe_tables(b, table)

    @tool
    def summarize_todays_alerts(day: str = "") -> dict:
        """Exact counts for one day's real-time alerts (DynamoDB): total, money at risk, top payers, reasons, cities.

        Use this for any "how many" / "which payer has the most" / "what's happening today" question
        instead of counting rows from get_recent_alerts.

        Args:
            day: UTC date YYYY-MM-DD; defaults to today.
        """
        return summarize_alerts(b, day)

    @tool
    def get_recent_alerts(day: str = "", min_score: float = 0.0, limit: int = 10) -> dict:
        """List the newest real-time fraud alerts for one day from DynamoDB (seconds old, newest first).

        Args:
            day: UTC date YYYY-MM-DD; defaults to today.
            min_score: Only alerts with score >= this (0-1).
            limit: Max alerts to return (1-50).
        """
        return recent_alerts(b, day, min_score, limit)

    @tool
    def get_payer_alerts(payer_id: str, limit: int = 20) -> dict:
        """Fetch the alerts raised for one payer (payer_id like 'U00022'), newest first.

        Args:
            payer_id: The payer_id. If you only have a VPA, look up payer_id in gold_payer_risk first.
            limit: Max alerts to return (1-50).
        """
        return payer_alerts(b, payer_id, limit)

    @tool
    def get_alert_details(txn_id: str) -> dict:
        """Fetch one alert with its full feature vector and rule reasons, to explain why it was flagged.

        Args:
            txn_id: The transaction id of the alert.
        """
        return get_alert(b, txn_id)

    @tool
    def search_fraud_playbook(query: str, max_results: int = 4) -> dict:
        """Search the fraud team's playbook (fraud patterns, triage runbook, model card, data dictionary).

        Use it for "what does X mean", "what should we do about Y", and policy questions.

        Args:
            query: What to look up, in plain English.
            max_results: Passages to return (1-8).
        """
        return search_playbook(b, query, max_results)

    return [
        run_lakehouse_sql, list_lakehouse_tables, summarize_todays_alerts, get_recent_alerts,
        get_payer_alerts, get_alert_details, search_fraud_playbook,
    ]
