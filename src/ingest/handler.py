"""API Gateway (HTTP API) -> ingest Lambda -> Kinesis.

POST /payments with either one payment object or {"events": [ ... up to 500 ... ]}.
Valid payments are written to Kinesis, partitioned by payer_id so that one payer's
payments stay in order on the same shard (the scorer relies on that for velocity features).
"""

from __future__ import annotations

import json
import os
import time

import boto3

STREAM_NAME = os.environ.get("STREAM_NAME", "")
MAX_EVENTS = 500

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

kinesis = boto3.client("kinesis")


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


def _put_records(events: list[dict]) -> int:
    records = [
        {"Data": json.dumps(e, separators=(",", ":")).encode(), "PartitionKey": e["payer_id"]} for e in events
    ]
    failed = 0
    for attempt in range(3):
        resp = kinesis.put_records(StreamName=STREAM_NAME, Records=records)
        failed = resp["FailedRecordCount"]
        if not failed:
            return 0
        records = [rec for rec, res in zip(records, resp["Records"]) if "ErrorCode" in res]
        time.sleep(0.1 * 2**attempt)
    return failed


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

    failed = _put_records(valid) if valid else 0
    if failed:
        return _response(503, {"error": f"{failed} records could not be written, retry", "rejected": rejected})
    return _response(202 if valid else 400, {"accepted": len(valid), "rejected": rejected})
