"""Data access for the Streamlit UI (kept separate from the page code so it can be unit tested)."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Key

from analyst.tools import ALERT_SUMMARY_FIELDS, plain
from common.athena import Athena, QueryResult, is_read_only


def today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def fetch_alerts(table, day: str, limit: int = 500) -> list[dict]:
    """Newest-first alerts for one UTC day from the by_day index."""
    kwargs = {"IndexName": "by_day", "KeyConditionExpression": Key("alert_day").eq(day), "ScanIndexForward": False}
    items: list[dict] = []
    while len(items) < limit:
        resp = table.query(**kwargs)
        items += resp["Items"]
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return [{k: plain(i[k]) for k in ALERT_SUMMARY_FIELDS if k in i} for i in items[:limit]]


def reason_code(reason: str) -> str:
    return reason.split(":", 1)[0]


def alert_kpis(alerts: list[dict], window_minutes: int = 10, now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    cutoff = now.timestamp() - window_minutes * 60
    recent = sum(
        1 for a in alerts if datetime.fromisoformat(a["event_time"].replace("Z", "+00:00")).timestamp() >= cutoff
    )
    return {
        "alerts": len(alerts),
        "money_at_risk": round(sum(a["amount"] for a in alerts), 2),
        "payers": len({a["payer_id"] for a in alerts}),
        "critical": sum(1 for a in alerts if a["score"] >= 0.95),
        "last_window": recent,
    }


def reason_counts(alerts: list[dict]) -> Counter:
    return Counter(code for a in alerts for code in {reason_code(r) for r in a.get("reasons", [])})


def table_rows(alerts: list[dict]) -> list[dict]:
    return [
        {
            "time (UTC)": a["event_time"][11:19],
            "score": a["score"],
            "amount_inr": a["amount"],
            "payer": a["payer_vpa"],
            "payee": a["payee_vpa"],
            "city": a["city"],
            "category": a.get("merchant_category"),
            "reasons": ", ".join(sorted({reason_code(r) for r in a.get("reasons", [])})),
            "scored_by": a.get("scored_by"),
            "txn_id": a["txn_id"],
        }
        for a in alerts
    ]


def saved_queries(session: boto3.Session, workgroup: str) -> dict[str, tuple[str, str]]:
    client = session.client("athena")
    ids = client.list_named_queries(WorkGroup=workgroup).get("NamedQueryIds", [])
    if not ids:
        return {}
    queries = client.batch_get_named_query(NamedQueryIds=ids)["NamedQueries"]
    return {q["Name"]: (q.get("Description", ""), q["QueryString"]) for q in sorted(queries, key=lambda q: q["Name"])}


def run_sql(athena: Athena, sql: str, max_rows: int = 500) -> QueryResult:
    if not is_read_only(sql):
        raise ValueError("Only read-only SELECT / WITH / SHOW / DESCRIBE queries can run from the UI.")
    return athena.run(sql, max_rows=max_rows)
