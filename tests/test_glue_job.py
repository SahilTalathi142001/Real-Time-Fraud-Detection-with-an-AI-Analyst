"""Runs the Glue job's transformations on local Spark (no Glue, no Iceberg)."""

import sys
import types

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from common.lake_schema import PAYMENT_COLUMNS, SCORE_COLUMNS  # noqa: E402
from generator.simulator import PaymentSimulator  # noqa: E402
from scorer.handler import score_record  # noqa: E402
from scorer.scoring import score_transactions  # noqa: E402

sys.modules.setdefault("awsglue", types.ModuleType("awsglue"))
sys.modules.setdefault("awsglue.utils", types.SimpleNamespace(getResolvedOptions=None))
from glue_jobs import silver_gold  # noqa: E402

SPARK_TYPES = {"string": "string", "bigint": "bigint", "double": "double", "boolean": "boolean"}


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[1]").appName("test").config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false").getOrCreate()
    yield s
    s.stop()


@pytest.fixture(scope="module")
def bronze(spark):
    events = PaymentSimulator(seed=9, fraud_rate=0.03, start_ts_ms=1_790_000_000_000).generate(3_000)
    for i, e in enumerate(events):
        e["ingested_at_ms"] = e["event_ts_ms"] + 100
    results = score_transactions(events, {})
    scores = [score_record(r, r.txn["event_ts_ms"] + 1500) for r in results]
    duplicated = events + events[:50]  # at-least-once delivery: some payments arrive twice

    def df(rows, columns):
        schema = ", ".join(f"{n} {SPARK_TYPES[t]}" for n, t in columns)
        return spark.createDataFrame([tuple(r.get(n) for n, _ in columns) for r in rows], schema)

    return df(duplicated, PAYMENT_COLUMNS), df(scores, SCORE_COLUMNS), events


def test_silver_dedupes_and_joins_scores(bronze):
    payments, scores, events = bronze
    silver = silver_gold.build_silver(payments, scores)
    rows = silver.collect()
    assert len(rows) == len(events)
    assert all(r.score is not None for r in rows)
    first = next(r for r in rows if r.txn_id == events[0]["txn_id"])
    assert str(first.event_ts).startswith(events[0]["event_time"][:19].replace("T", " "))
    assert first.score_latency_ms == 1500
    assert {r.sim_fraud_pattern for r in rows} >= {"none", "velocity_burst"}


def test_gold_tables(bronze):
    payments, scores, events = bronze
    silver = silver_gold.build_silver(payments, scores).cache()
    daily = silver_gold.build_gold_daily(silver).collect()
    assert sum(r.txn_count for r in daily) == len(events)
    assert all(0 <= r.alert_rate <= 1 for r in daily)

    payers = silver_gold.build_gold_payer_risk(silver).collect()
    assert sum(r.txn_count for r in payers) == len(events)
    assert all(r.home_city for r in payers)

    quality = silver_gold.build_gold_quality(silver).collect()
    assert len(quality) >= 1
    q = quality[0]
    assert q.tp + q.fp + q.fn + q.tn == len(events)
    assert 0 < q.precision <= 1 and 0 < q.recall <= 1
