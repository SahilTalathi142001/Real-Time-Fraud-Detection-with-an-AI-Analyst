"""SQS FIFO -> scorer Lambda.

For each batch of payment messages (up to 10; payments of one payer always arrive in order):
1. load payer profiles from DynamoDB,
2. compute features and score (rules now, ML model from phase 3),
3. save updated profiles, write alerts to DynamoDB,
4. email the most severe alerts through SNS (throttled so a demo doesn't flood the inbox),
5. send every score to Firehose, which appends it to the bronze_scores Iceberg table,
6. emit CloudWatch metrics using the Embedded Metric Format (structured logs, no extra API calls).
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from scorer.scoring import ScoredTxn, score_transactions

PROFILES_TABLE = os.environ.get("PROFILES_TABLE", "")
ALERTS_TABLE = os.environ.get("ALERTS_TABLE", "")
ALERT_TOPIC_ARN = os.environ.get("ALERT_TOPIC_ARN", "")
ALERT_THRESHOLD = float(os.environ.get("ALERT_THRESHOLD", "0.6"))
MODEL_ENDPOINT = os.environ.get("MODEL_ENDPOINT", "")
SCORES_FIREHOSE = os.environ.get("SCORES_FIREHOSE", "")
EMAIL_MIN_SCORE = float(os.environ.get("EMAIL_MIN_SCORE", "0.9"))
MAX_EMAILS_PER_HOUR = int(os.environ.get("MAX_EMAILS_PER_HOUR", "10"))
PROFILE_TTL_DAYS = 30
ALERT_TTL_DAYS = 90
THROTTLE_KEY_PREFIX = "__email_throttle__#"

dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")
firehose = boto3.client("firehose")
# Serverless endpoints can take ~10-20s on a cold start; past that we fall back to rules.
sagemaker_runtime = boto3.client(
    "sagemaker-runtime", config=Config(read_timeout=25, connect_timeout=5, retries={"max_attempts": 2})
)


def _decode(records: list[dict]) -> list[dict]:
    return [json.loads(r["body"]) for r in records]


def _load_profiles(payer_ids: set[str]) -> dict[str, dict]:
    profiles: dict[str, dict] = {}
    keys = [{"payer_id": p} for p in payer_ids]
    for i in range(0, len(keys), 100):
        request = {PROFILES_TABLE: {"Keys": keys[i : i + 100], "ProjectionExpression": "payer_id, profile"}}
        while request:
            resp = dynamodb.batch_get_item(RequestItems=request)
            for item in resp["Responses"].get(PROFILES_TABLE, []):
                profiles[item["payer_id"]] = json.loads(item["profile"])
            request = resp.get("UnprocessedKeys") or None
    return profiles


def _save_profiles(profiles: dict[str, dict], payer_ids: set[str]) -> None:
    expires = int(time.time()) + PROFILE_TTL_DAYS * 86_400
    with dynamodb.Table(PROFILES_TABLE).batch_writer() as batch:
        for payer in payer_ids:
            batch.put_item(Item={"payer_id": payer, "profile": json.dumps(profiles[payer]), "expires_at": expires})


def _num(x: float) -> Decimal:
    return Decimal(str(round(float(x), 4)))


def alert_item(r: ScoredTxn, now: datetime) -> dict:
    t = r.txn
    item = {
        "alert_id": t["txn_id"],
        "txn_id": t["txn_id"],
        "alert_day": t["event_time"][:10],
        "event_time": t["event_time"],
        "event_ts_ms": int(t["event_ts_ms"]),
        "created_at": now.isoformat(timespec="seconds"),
        "status": "OPEN",
        "payer_id": t["payer_id"],
        "payer_vpa": t["payer_vpa"],
        "payee_vpa": t["payee_vpa"],
        "payee_type": t["payee_type"],
        "merchant_category": t.get("merchant_category") or "p2p",
        "channel": t["channel"],
        "amount": _num(t["amount"]),
        "city": t["city"],
        "device_id": t["device_id"],
        "score": _num(r.score),
        "rule_score": _num(r.rule_score),
        "scored_by": r.scored_by,
        "reasons": r.reasons or ["model_only"],
        "features": {k: _num(v) for k, v in r.features.items()},
        # Simulator ground truth, kept only so we can measure precision.
        "sim_label_pattern": t.get("label_pattern", "unknown"),
        "expires_at": int(now.timestamp()) + ALERT_TTL_DAYS * 86_400,
    }
    if r.model_score is not None:
        item["model_score"] = _num(r.model_score)
    return item


def score_record(r: ScoredTxn, scored_at_ms: int) -> dict:
    t = r.txn
    return {
        "txn_id": t["txn_id"],
        "event_time": t["event_time"],
        "event_ts_ms": int(t["event_ts_ms"]),
        "payer_id": t["payer_id"],
        "score": round(r.score, 5),
        "rule_score": round(r.rule_score, 5),
        "model_score": None if r.model_score is None else round(r.model_score, 5),
        "is_alert": r.is_alert,
        "scored_by": r.scored_by,
        "reasons": "; ".join(r.reasons),
        "scored_at_ms": scored_at_ms,
    }


def _ship_scores(results: list[ScoredTxn]) -> int:
    """Best effort: a lake hiccup must never block real-time scoring."""
    if not SCORES_FIREHOSE or not results:
        return 0
    now_ms = int(time.time() * 1000)
    records = [{"Data": (json.dumps(score_record(r, now_ms)) + "\n").encode()} for r in results]
    failed = 0
    for i in range(0, len(records), 500):
        try:
            failed += firehose.put_record_batch(DeliveryStreamName=SCORES_FIREHOSE, Records=records[i : i + 500])["FailedPutCount"]
        except ClientError as e:
            print(json.dumps({"level": "WARN", "msg": "firehose put failed", "error": str(e)[:200]}))
            failed += len(records[i : i + 500])
    return failed


def _reserve_email_slot(now: datetime) -> bool:
    """Atomically count emails per hour; refuse once MAX_EMAILS_PER_HOUR is reached."""
    try:
        dynamodb.Table(PROFILES_TABLE).update_item(
            Key={"payer_id": THROTTLE_KEY_PREFIX + now.strftime("%Y%m%d%H")},
            UpdateExpression="ADD sent :one SET expires_at = :exp",
            ConditionExpression="attribute_not_exists(sent) OR sent < :max",
            ExpressionAttributeValues={":one": 1, ":max": MAX_EMAILS_PER_HOUR, ":exp": int(now.timestamp()) + 7200},
        )
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


def format_email(r: ScoredTxn) -> tuple[str, str]:
    t = r.txn
    subject = f"[Fraud alert] Rs {t['amount']:,.0f} from {t['payer_vpa']} (score {r.score:.2f})"
    lines = [
        f"Transaction {t['txn_id']} was flagged as likely fraud.",
        "",
        f"Payer:    {t['payer_vpa']} ({t['payer_id']})",
        f"Payee:    {t['payee_vpa']} ({t['payee_type']})",
        f"Amount:   Rs {t['amount']:,.2f} via {t['channel']} ({t.get('merchant_category') or 'p2p'})",
        f"Where:    {t['city']} on device {t['device_id']}",
        f"When:     {t['event_time']}",
        f"Score:    {r.score:.3f} (scored by {r.scored_by})",
        "",
        "Why:",
        *[f"  - {reason}" for reason in (r.reasons or ["model score above threshold"])],
        "",
        "Look it up in DynamoDB table fraud-analyst-alerts, or ask the AI analyst.",
    ]
    return subject[:100], "\n".join(lines)


def _emit_metrics(results: list[ScoredTxn], emails: int, duration_ms: float, lake_failures: int = 0) -> None:
    now_ms = int(time.time() * 1000)
    latencies = [now_ms - int(r.txn["event_ts_ms"]) for r in results] or [0]
    print(
        json.dumps(
            {
                "_aws": {
                    "Timestamp": now_ms,
                    "CloudWatchMetrics": [
                        {
                            "Namespace": "FraudAnalyst",
                            "Dimensions": [["Service"]],
                            "Metrics": [
                                {"Name": "TransactionsScored", "Unit": "Count"},
                                {"Name": "AlertsRaised", "Unit": "Count"},
                                {"Name": "AlertEmailsSent", "Unit": "Count"},
                                {"Name": "ModelFallbacks", "Unit": "Count"},
                                {"Name": "LakeWriteFailures", "Unit": "Count"},
                                {"Name": "EventToScoreLatencyMs", "Unit": "Milliseconds"},
                                {"Name": "BatchDurationMs", "Unit": "Milliseconds"},
                            ],
                        }
                    ],
                },
                "Service": "scorer",
                "TransactionsScored": len(results),
                "AlertsRaised": sum(r.is_alert for r in results),
                "AlertEmailsSent": emails,
                "ModelFallbacks": sum(1 for r in results if "model_error" in r.extra),
                "LakeWriteFailures": lake_failures,
                "EventToScoreLatencyMs": max(latencies),
                "BatchDurationMs": duration_ms,
            }
        )
    )


def invoke_model(rows) -> list[float]:
    body = "\n".join(",".join(f"{v:.5f}" for v in row) for row in rows)
    resp = sagemaker_runtime.invoke_endpoint(EndpointName=MODEL_ENDPOINT, ContentType="text/csv", Body=body)
    scores = [float(x) for x in resp["Body"].read().decode().replace("\n", ",").split(",") if x.strip()]
    if len(scores) != len(rows):
        raise ValueError(f"endpoint returned {len(scores)} scores for {len(rows)} rows")
    return scores


def get_model_fn():
    return invoke_model if MODEL_ENDPOINT else None


def handler(event, context):
    started = time.time()
    txns = _decode(event.get("Records", []))
    payer_ids = {t["payer_id"] for t in txns}
    profiles = _load_profiles(payer_ids)

    results = score_transactions(txns, profiles, model_fn=get_model_fn(), threshold=ALERT_THRESHOLD)
    _save_profiles(profiles, payer_ids)

    now = datetime.now(timezone.utc)
    alerts = [r for r in results if r.is_alert]
    if alerts:
        with dynamodb.Table(ALERTS_TABLE).batch_writer(overwrite_by_pkeys=["alert_id"]) as batch:
            for r in alerts:
                batch.put_item(Item=alert_item(r, now))

    emails, emailed_payers = 0, set()
    for r in sorted(alerts, key=lambda r: -r.score):
        if r.score < EMAIL_MIN_SCORE or r.txn["payer_id"] in emailed_payers or not ALERT_TOPIC_ARN:
            continue
        if not _reserve_email_slot(now):
            break
        subject, body = format_email(r)
        sns.publish(TopicArn=ALERT_TOPIC_ARN, Subject=subject, Message=body)
        emailed_payers.add(r.txn["payer_id"])
        emails += 1

    for r in alerts:
        print(json.dumps({"level": "ALERT", "txn_id": r.txn["txn_id"], "payer_id": r.txn["payer_id"],
                          "score": round(r.score, 3), "reasons": r.reasons}))
    lake_failures = _ship_scores(results)
    _emit_metrics(results, emails, (time.time() - started) * 1000, lake_failures)
    return {"scored": len(results), "alerts": len(alerts), "emails": emails}
