"""Glue 5.0 PySpark job: bronze -> silver -> gold Iceberg tables.

silver_transactions     one clean row per payment (deduplicated, typed) joined with its score
gold_fraud_daily        per day x city x category: volume, alerts, money at risk
gold_payer_risk         per payer: activity, alerts, devices, cities, worst score
gold_detection_quality  per day x scorer: precision/recall vs simulator ground truth

The tables are small, so each run rebuilds them (createOrReplace). Iceberg keeps every
previous version as a snapshot, so you can still time-travel to an older build.
"""

import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

CATALOG = "glue_catalog"


def build_silver(payments, scores):
    latest_payment = Window.partitionBy("txn_id").orderBy(F.col("ingested_at_ms").desc_nulls_last())
    payments = payments.withColumn("_rn", F.row_number().over(latest_payment)).filter("_rn = 1").drop("_rn")
    latest_score = Window.partitionBy("txn_id").orderBy(F.col("scored_at_ms").desc_nulls_last())
    scores = (
        scores.withColumn("_rn", F.row_number().over(latest_score))
        .filter("_rn = 1")
        .select("txn_id", "score", "rule_score", "model_score", "is_alert", "scored_by", "reasons", "scored_at_ms")
    )
    event_ts = F.to_timestamp(F.regexp_replace("event_time", "Z$", ""), "yyyy-MM-dd'T'HH:mm:ss.SSS").cast("timestamp_ntz")
    return (
        payments.join(scores, "txn_id", "left")
        .withColumn("event_ts", event_ts)
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("event_hour_ist", F.hour(F.from_utc_timestamp(event_ts.cast("timestamp"), "Asia/Kolkata")))
        .withColumn("merchant_category", F.coalesce("merchant_category", F.lit("p2p")))
        .withColumn("is_alert", F.coalesce("is_alert", F.lit(False)))
        .withColumn("score_latency_ms", F.col("scored_at_ms") - F.col("event_ts_ms"))
        .select(
            "txn_id", "event_ts", "event_date", "event_hour_ist", "payer_id", "payer_vpa", "payee_vpa", "payee_type",
            "merchant_id", "merchant_category", "amount", "currency", "channel", "device_id", "device_os",
            "ip_address", "city", "lat", "lon", "score", "rule_score", "model_score", "is_alert", "scored_by",
            "reasons", "score_latency_ms", F.col("label_is_fraud").alias("sim_is_fraud"),
            F.col("label_pattern").alias("sim_fraud_pattern"),
        )
    )


def build_gold_daily(silver):
    return (
        silver.groupBy("event_date", "city", "merchant_category")
        .agg(
            F.count("*").alias("txn_count"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
            F.sum(F.col("is_alert").cast("int")).alias("alert_count"),
            F.round(F.sum(F.when(F.col("is_alert"), F.col("amount")).otherwise(0.0)), 2).alias("alert_amount"),
            F.sum(F.col("sim_is_fraud").cast("int")).alias("sim_fraud_count"),
        )
        .withColumn("alert_rate", F.col("alert_count") / F.col("txn_count"))
    )


def build_gold_payer_risk(silver):
    home_city = (
        silver.groupBy("payer_id", "city").count()
        .withColumn("_rn", F.row_number().over(Window.partitionBy("payer_id").orderBy(F.col("count").desc())))
        .filter("_rn = 1")
        .select("payer_id", F.col("city").alias("home_city"))
    )
    return (
        silver.groupBy("payer_id", "payer_vpa")
        .agg(
            F.count("*").alias("txn_count"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
            F.round(F.avg("amount"), 2).alias("avg_amount"),
            F.sum(F.col("is_alert").cast("int")).alias("alert_count"),
            F.max("score").alias("max_score"),
            F.max(F.when(F.col("is_alert"), F.col("event_ts"))).alias("last_alert_ts"),
            F.countDistinct("device_id").alias("distinct_devices"),
            F.countDistinct("city").alias("distinct_cities"),
            F.sum(F.col("sim_is_fraud").cast("int")).alias("sim_fraud_count"),
        )
        .join(home_city, "payer_id", "left")
    )


def build_gold_quality(silver):
    scored = silver.filter(F.col("scored_by").isNotNull())
    flag, truth = F.col("is_alert"), F.col("sim_is_fraud")
    return (
        scored.groupBy("event_date", "scored_by")
        .agg(
            F.sum((flag & truth).cast("int")).alias("tp"),
            F.sum((flag & ~truth).cast("int")).alias("fp"),
            F.sum((~flag & truth).cast("int")).alias("fn"),
            F.sum((~flag & ~truth).cast("int")).alias("tn"),
            F.round(F.avg("score_latency_ms")).alias("avg_score_latency_ms"),
        )
        .withColumn("precision", F.col("tp") / F.greatest(F.col("tp") + F.col("fp"), F.lit(1)))
        .withColumn("recall", F.col("tp") / F.greatest(F.col("tp") + F.col("fn"), F.lit(1)))
    )


def main() -> None:
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "database"])
    db = f"{CATALOG}.{args['database']}"
    spark = SparkSession.builder.getOrCreate()

    silver = build_silver(spark.table(f"{db}.bronze_payments"), spark.table(f"{db}.bronze_scores")).cache()
    outputs = {
        "silver_transactions": (silver, ["event_date"]),
        "gold_fraud_daily": (build_gold_daily(silver), ["event_date"]),
        "gold_payer_risk": (build_gold_payer_risk(silver), []),
        "gold_detection_quality": (build_gold_quality(silver), []),
    }
    for table, (df, partitions) in outputs.items():
        writer = df.writeTo(f"{db}.{table}").using("iceberg").tableProperty("format-version", "2")
        if partitions:
            writer = writer.partitionedBy(*[F.col(c) for c in partitions])
        writer.createOrReplace()
        print(f"wrote {db}.{table}: {spark.table(f'{db}.{table}').count()} rows")


if __name__ == "__main__":
    main()
