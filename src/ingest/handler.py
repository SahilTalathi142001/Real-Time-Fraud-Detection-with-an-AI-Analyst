"""API Gateway (HTTP API) -> ingest Lambda -> SQS FIFO queue.

POST /payments with either one payment object or {"events": [ ... up to 500 ... ]}.
Valid payments are sent to a FIFO queue with MessageGroupId = payer_id, so one payer's
payments are delivered to the scorer strictly in order (it relies on that for velocity
features), while different payers are processed in parallel. MessageDeduplicationId =
txn_id, so a retried request inside the 5-minute dedup window is not scored twice.
"""

from __future__ import annotations

import json
import os
import time

import boto3

QUEUE_URL = os.environ.get("QUEUE_URL", "")
MAX_EVENTS = 500
SQS_BATCH = 10

REQUIRED: dict[str, type | tuple[type, ...]] = {
    "txn_id": str,
    "event_time": str,
    "event_ts_ms": int,
    "payer_id": str,
    "payer_vpa": str,
    "payee_vpa": str,
    "payee_type": str,
    "amount": (int, float),
    "channel": str,
    "device_id": str,
    "city": str,
    "lat": (int, float),
    "lon": (int, float),
}

sqs = boto3.client("sqs")


def validate(event: object) -> str | None:
    if not isinstance(event, dict):
        return "event must be a JSON object"
    for name, typ in REQUIRED.items():
        if name not in event:
            return f"missing field {name}"
        if isinstance(event[name], bool) or not isinstance(event[name], typ):
            return f"field {name} has wrong type"
    if event["amount"] <= 0:
        return "amount must be positive"
    return None


def _response(status: int, body: dict) -> dict:
    return {"statusCode": status, "headers": {"Content-Type": "application/json"}, "body": json.dumps(body)}


def _send(events: list[dict]) -> int:
    """Send in batches of 10 (the SQS maximum), retrying failed entries; returns the number still failing."""
    failed_total = 0
    for i in range(0, len(events), SQS_BATCH):
        entries = [
            {
                "Id": str(j),
                "MessageBody": json.dumps(e, separators=(",", ":")),
                "MessageGroupId": e["payer_id"],
                "MessageDeduplicationId": e["txn_id"],
            }
            for j, e in enumerate(events[i : i + SQS_BATCH])
        ]
        for attempt in range(3):
            resp = sqs.send_message_batch(QueueUrl=QUEUE_URL, Entries=entries)
            failed_ids = {f["Id"] for f in resp.get("Failed", [])}
            entries = [e for e in entries if e["Id"] in failed_ids]
            if not entries:
                break
            time.sleep(0.1 * 2**attempt)
        failed_total += len(entries)
    return failed_total


def handler(event, context):
    try:
        payload = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        return _response(400, {"error": "body is not valid JSON"})

    events = payload["events"] if isinstance(payload, dict) and "events" in payload else [payload]
    if not isinstance(events, list) or not events:
        return _response(400, {"error": "expected a payment object or {\"events\": [...]}"})
    if len(events) > MAX_EVENTS:
        return _response(413, {"error": f"at most {MAX_EVENTS} events per request"})

    valid, rejected = [], []
    for i, e in enumerate(events):
        error = validate(e)
        if error:
            rejected.append({"index": i, "error": error})
        else:
            e.setdefault("ingested_at_ms", int(time.time() * 1000))
            valid.append(e)

    failed = _send(valid) if valid else 0
    if failed:
        return _response(503, {"error": f"{failed} records could not be written, retry", "rejected": rejected})
    return _response(202 if valid else 400, {"accepted": len(valid), "rejected": rejected})
