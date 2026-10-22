import base64
import importlib
import json

import boto3
import pytest
from moto import mock_aws

from generator.simulator import PaymentSimulator

REGION = "us-east-1"


@pytest.fixture
def aws(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("STREAM_NAME", "payments")
    monkeypatch.setenv("PROFILES_TABLE", "profiles")
    monkeypatch.setenv("ALERTS_TABLE", "alerts")
    monkeypatch.setenv("MAX_EMAILS_PER_HOUR", "2")
    with mock_aws():
        ddb = boto3.client("dynamodb", region_name=REGION)
        ddb.create_table(
            TableName="profiles", BillingMode="PAY_PER_REQUEST",
            KeySchema=[{"AttributeName": "payer_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "payer_id", "AttributeType": "S"}],
        )
        ddb.create_table(
            TableName="alerts", BillingMode="PAY_PER_REQUEST",
            KeySchema=[{"AttributeName": "alert_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "alert_id", "AttributeType": "S"}],
        )
        boto3.client("kinesis", region_name=REGION).create_stream(StreamName="payments", ShardCount=1)
        topic = boto3.client("sns", region_name=REGION).create_topic(Name="alerts")["TopicArn"]
        monkeypatch.setenv("ALERT_TOPIC_ARN", topic)
        yield


def _api_event(body):
    return {"body": json.dumps(body)}


def _events(n=3000, seed=11):
    return PaymentSimulator(seed=seed, fraud_rate=0.03, start_ts_ms=1_790_000_000_000).generate(n)


def test_ingest_writes_valid_events_and_rejects_bad_ones(aws):
    ingest = importlib.reload(importlib.import_module("ingest.handler"))
    good = _events(5)
    bad = dict(good[0], amount=-5)
    resp = ingest.handler(_api_event({"events": good + [bad, {"hello": 1}]}), None)
    body = json.loads(resp["body"])
    assert resp["statusCode"] == 202
    assert body["accepted"] == 5 and len(body["rejected"]) == 2

    kinesis = boto3.client("kinesis", region_name=REGION)
    shard = kinesis.describe_stream(StreamName="payments")["StreamDescription"]["Shards"][0]["ShardId"]
    it = kinesis.get_shard_iterator(StreamName="payments", ShardId=shard, ShardIteratorType="TRIM_HORIZON")["ShardIterator"]
    records = kinesis.get_records(ShardIterator=it)["Records"]
    assert len(records) == 5
    assert {r["PartitionKey"] for r in records} == {e["payer_id"] for e in good}


def test_ingest_accepts_single_event_and_rejects_garbage(aws):
    ingest = importlib.reload(importlib.import_module("ingest.handler"))
    assert ingest.handler(_api_event(_events(1)[0]), None)["statusCode"] == 202
    assert ingest.handler({"body": "not json"}, None)["statusCode"] == 400
    assert ingest.handler(_api_event({"events": []}), None)["statusCode"] == 400


def _kinesis_event(txns):
    return {"Records": [{"kinesis": {"data": base64.b64encode(json.dumps(t).encode()).decode()}} for t in txns]}


def test_scorer_writes_profiles_alerts_and_throttles_email(aws, capsys):
    scorer = importlib.reload(importlib.import_module("scorer.handler"))
    events = _events()
    totals = {"scored": 0, "alerts": 0, "emails": 0}
    for i in range(0, len(events), 100):
        out = scorer.handler(_kinesis_event(events[i : i + 100]), None)
        for k in totals:
            totals[k] += out[k]

    assert totals["scored"] == len(events)
    assert totals["alerts"] > 0
    assert totals["emails"] == 2  # MAX_EMAILS_PER_HOUR

    ddb = boto3.resource("dynamodb", region_name=REGION)
    alerts = ddb.Table("alerts").scan()["Items"]
    assert len(alerts) == totals["alerts"]
    a = alerts[0]
    assert a["status"] == "OPEN" and a["reasons"] and float(a["score"]) >= 0.6
    profile = ddb.Table("profiles").get_item(Key={"payer_id": events[0]["payer_id"]})["Item"]
    assert json.loads(profile["profile"])["txn_count"] >= 1

    logs = capsys.readouterr().out
    assert '"Namespace": "FraudAnalyst"' in logs


def test_format_email_mentions_amount_and_reasons(aws):
    scorer = importlib.reload(importlib.import_module("scorer.handler"))
    from scorer.scoring import ScoredTxn

    t = _events(1)[0]
    subject, body = scorer.format_email(ScoredTxn(txn=t, features={}, rule_score=0.95, reasons=["velocity_burst: 7"], score=0.95))
    assert "Fraud alert" in subject and len(subject) <= 100
    assert "velocity_burst" in body and t["txn_id"] in body
