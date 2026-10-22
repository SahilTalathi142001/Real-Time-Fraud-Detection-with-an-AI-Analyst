"""Lake loader Lambda: SQS lake buffer -> S3 landing JSON -> Athena INSERT INTO bronze Iceberg tables.

This does the job Amazon Data Firehose would do in production (Firehose isn't available on this
account's Free plan; see docs/production-version.md). An EventBridge rule runs it every 5 minutes:

1. drain the lake buffer queue (each message = one scored payment: {"payment": {...}, "score": {...}}),
2. write one JSON Lines file per table to s3://<lake>/landing/<kind>/<run_id>.json,
3. for each table run `INSERT INTO bronze_<kind> SELECT ... FROM landing_<kind> WHERE "$path" = <file>`,
   which appends one Iceberg snapshot (commit) per table per run,
4. move the landing files to archive/ and only then delete the SQS messages.

If anything fails before step 4 the messages become visible again and the next run retries them,
so delivery is at-least-once; silver_transactions de-duplicates by txn_id.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone

import boto3

from common.athena import Athena
from common.lake_schema import LAKE_TABLES

QUEUE_URL = os.environ.get("LAKE_QUEUE_URL", "")
LAKE_BUCKET = os.environ.get("LAKE_BUCKET", "")
DATABASE = os.environ.get("GLUE_DATABASE", "")
WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "")
MAX_MESSAGES = int(os.environ.get("MAX_MESSAGES", "20000"))
MAX_DRAIN_SECONDS = float(os.environ.get("MAX_DRAIN_SECONDS", "60"))
SAFETY_MS = 90_000  # stop draining while there is still time for the Athena inserts

sqs = boto3.client("sqs")
s3 = boto3.client("s3")


def drain(remaining_ms) -> list[dict]:
    messages: list[dict] = []
    empty_polls = 0
    deadline = time.time() + MAX_DRAIN_SECONDS
    while len(messages) < MAX_MESSAGES and remaining_ms() > SAFETY_MS and empty_polls < 2 and time.time() < deadline:
        batch = sqs.receive_message(QueueUrl=QUEUE_URL, MaxNumberOfMessages=10, WaitTimeSeconds=1).get("Messages", [])
        empty_polls = 0 if batch else empty_polls + 1
        messages.extend(batch)
    return messages


def split(messages: list[dict]) -> dict[str, list[dict]]:
    rows: dict[str, list[dict]] = {kind: [] for kind in LAKE_TABLES}
    for m in messages:
        body = json.loads(m["Body"])
        if "payment" in body:
            rows["payments"].append(body["payment"])
        if "score" in body:
            rows["scores"].append(body["score"])
    return rows


def insert_sql(kind: str, path: str) -> str:
    landing, bronze, columns = LAKE_TABLES[kind]
    cols = ", ".join(name for name, _ in columns)
    return f'INSERT INTO {bronze} ({cols}) SELECT {cols} FROM {landing} WHERE "$path" = \'{path}\''


def delete_messages(messages: list[dict]) -> None:
    for i in range(0, len(messages), 10):
        entries = [{"Id": str(j), "ReceiptHandle": m["ReceiptHandle"]} for j, m in enumerate(messages[i : i + 10])]
        sqs.delete_message_batch(QueueUrl=QUEUE_URL, Entries=entries)


def handler(event, context):
    remaining_ms = context.get_remaining_time_in_millis if context else (lambda: 300_000)
    started = time.time()
    messages = drain(remaining_ms)
    if not messages:
        return {"messages": 0}

    now = datetime.now(timezone.utc)
    run_id = f"{now:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"
    athena = Athena(WORKGROUP, DATABASE)
    result: dict = {"messages": len(messages), "run_id": run_id}
    written: list[tuple[str, str]] = []

    for kind, rows in split(messages).items():
        if not rows:
            continue
        key = f"landing/{kind}/{run_id}.json"
        s3.put_object(Bucket=LAKE_BUCKET, Key=key, Body="\n".join(json.dumps(r) for r in rows).encode())
        q = athena.run(insert_sql(kind, f"s3://{LAKE_BUCKET}/{key}"), timeout_s=180)
        written.append((kind, key))
        result[kind] = len(rows)
        result[f"{kind}_athena_ms"] = q.elapsed_ms

    for kind, key in written:
        s3.copy_object(Bucket=LAKE_BUCKET, Key=f"archive/{kind}/dt={now:%Y-%m-%d}/{run_id}.json", CopySource={"Bucket": LAKE_BUCKET, "Key": key})
        s3.delete_object(Bucket=LAKE_BUCKET, Key=key)
    delete_messages(messages)

    result["duration_ms"] = int((time.time() - started) * 1000)
    print(json.dumps({"level": "INFO", "msg": "lake load complete", **result}))
    return result
