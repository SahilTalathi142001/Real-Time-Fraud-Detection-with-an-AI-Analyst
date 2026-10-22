"""Column definitions for the bronze layer, shared by the CDK stack and the loader Lambda."""

PAYMENT_COLUMNS: list[tuple[str, str]] = [
    ("txn_id", "string"), ("event_time", "string"), ("event_ts_ms", "bigint"),
    ("payer_id", "string"), ("payer_vpa", "string"), ("payee_vpa", "string"), ("payee_type", "string"),
    ("merchant_id", "string"), ("merchant_category", "string"), ("amount", "double"), ("currency", "string"),
    ("channel", "string"), ("device_id", "string"), ("device_os", "string"), ("ip_address", "string"),
    ("city", "string"), ("lat", "double"), ("lon", "double"),
    ("label_is_fraud", "boolean"), ("label_pattern", "string"), ("ingested_at_ms", "bigint"),
]

SCORE_COLUMNS: list[tuple[str, str]] = [
    ("txn_id", "string"), ("event_time", "string"), ("event_ts_ms", "bigint"), ("payer_id", "string"),
    ("score", "double"), ("rule_score", "double"), ("model_score", "double"), ("is_alert", "boolean"),
    ("scored_by", "string"), ("reasons", "string"), ("scored_at_ms", "bigint"),
]

# kind -> (landing JSON table, bronze Iceberg table, columns)
LAKE_TABLES: dict[str, tuple[str, str, list[tuple[str, str]]]] = {
    "payments": ("landing_payments", "bronze_payments", PAYMENT_COLUMNS),
    "scores": ("landing_scores", "bronze_scores", SCORE_COLUMNS),
}
