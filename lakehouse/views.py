"""Athena views over the silver/gold tables. Created after the first Glue run (views need their tables to exist)."""

VIEWS: dict[str, str] = {
    "v_recent_alerts": """
SELECT txn_id, event_ts, payer_id, payer_vpa, payee_vpa, amount, merchant_category, channel, city,
       device_id, round(score, 3) AS score, scored_by, reasons, sim_fraud_pattern
FROM silver_transactions
WHERE is_alert
""",
    "v_fraud_by_city": """
SELECT city,
       sum(txn_count) AS txn_count,
       sum(alert_count) AS alert_count,
       round(sum(alert_amount), 2) AS alert_amount_inr,
       round(100.0 * sum(alert_count) / sum(txn_count), 2) AS alert_rate_pct
FROM gold_fraud_daily
GROUP BY city
""",
    "v_fraud_by_pattern": """
SELECT sim_fraud_pattern AS pattern,
       count(*) AS txns,
       sum(CASE WHEN is_alert THEN 1 ELSE 0 END) AS flagged,
       round(100.0 * sum(CASE WHEN is_alert THEN 1 ELSE 0 END) / count(*), 1) AS flagged_pct,
       round(sum(amount), 2) AS amount_inr
FROM silver_transactions
GROUP BY sim_fraud_pattern
""",
}


def create_view_sql(name: str) -> str:
    return f"CREATE OR REPLACE VIEW {name} AS {VIEWS[name].strip()}"
