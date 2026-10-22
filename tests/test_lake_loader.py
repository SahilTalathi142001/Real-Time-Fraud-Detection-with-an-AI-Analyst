import importlib
import json

import boto3
import pytest
from moto import mock_aws

from generator.simulator import PaymentSimulator

REGION = "us-east-1"


class FakeAthena:
    queries: list[str] = []
    fail = False

    def __init__(self, workgroup, database):
        pass

    def run(self, sql, timeout_s=120):
        if FakeAthena.fail:
            raise RuntimeError("Athena query FAILED")
        FakeAthena.queries.append(sql)
        return type("R", (), {"elapsed_ms": 1234})()


@pytest.fixture
def loader(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("LAKE_BUCKET", "lake")
    monkeypatch.setenv("GLUE_DATABASE", "db")
    monkeypatch.setenv("ATHENA_WORKGROUP", "wg")
    with mock_aws():
        boto3.client("s3", region_name=REGION).create_bucket(Bucket="lake")
        url = boto3.client("sqs", region_name=REGION).create_queue(QueueName="lake-buffer", Attributes={"VisibilityTimeout": "900"})["QueueUrl"]
        monkeypatch.setenv("LAKE_QUEUE_URL", url)
        module = importlib.reload(importlib.import_module("lake_loader.handler"))
        monkeypatch.setattr(module, "Athena", FakeAthena)
        FakeAthena.queries, FakeAthena.fail = [], False
        yield module, url


def _fill(url, n=35):
    sqs = boto3.client("sqs", region_name=REGION)
    events = PaymentSimulator(seed=4, start_ts_ms=1_790_000_000_000).generate(n)
    for i in range(0, n, 10):
        sqs.send_message_batch(
            QueueUrl=url,
            Entries=[{"Id": str(j), "MessageBody": json.dumps({"payment": e, "score": {"txn_id": e["txn_id"], "score": 0.1}})} for j, e in enumerate(events[i : i + 10])],
        )
    return events


def _queue_depth(url):
    attrs = boto3.client("sqs", region_name=REGION).get_queue_attributes(
        QueueUrl=url, AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"]
    )["Attributes"]
    return int(attrs["ApproximateNumberOfMessages"]) + int(attrs["ApproximateNumberOfMessagesNotVisible"])


def test_empty_queue_is_a_cheap_no_op(loader):
    module, _ = loader
    assert module.handler({}, None) == {"messages": 0}
    assert FakeAthena.queries == []


def test_loads_landing_files_inserts_and_deletes_messages(loader):
    module, url = loader
    _fill(url)
    out = module.handler({}, None)
    assert out["messages"] == 35 and out["payments"] == 35 and out["scores"] == 35

    assert len(FakeAthena.queries) == 2
    assert FakeAthena.queries[0].startswith("INSERT INTO bronze_payments (txn_id, event_time")
    assert '"$path" = \'s3://lake/landing/payments/' in FakeAthena.queries[0]

    s3 = boto3.client("s3", region_name=REGION)
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket="lake").get("Contents", [])]
    assert not [k for k in keys if k.startswith("landing/")]
    archived = [k for k in keys if k.startswith("archive/payments/")]
    assert len(archived) == 1
    lines = s3.get_object(Bucket="lake", Key=archived[0])["Body"].read().decode().splitlines()
    assert len(lines) == 35 and "txn_id" in json.loads(lines[0])
    assert _queue_depth(url) == 0


def test_failed_insert_keeps_messages_for_retry(loader):
    module, url = loader
    _fill(url, 12)
    FakeAthena.fail = True
    with pytest.raises(RuntimeError):
        module.handler({}, None)
    assert _queue_depth(url) == 12


def test_insert_sql_lists_every_bronze_column():
    from common.lake_schema import SCORE_COLUMNS
    from lake_loader.handler import insert_sql

    sql = insert_sql("scores", "s3://b/k.json")
    for name, _ in SCORE_COLUMNS:
        assert name in sql
    assert sql.endswith("WHERE \"$path\" = 's3://b/k.json'")
