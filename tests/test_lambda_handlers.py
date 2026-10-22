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
        queue = boto3.client("sqs", region_name=REGION).create_queue(
            QueueName="payments.fifo", Attributes={"FifoQueue": "true"}
        )["QueueUrl"]
        monkeypatch.setenv("QUEUE_URL", queue)
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

    sqs = boto3.client("sqs", region_name=REGION)
    queue = sqs.get_queue_url(QueueName="payments.fifo")["QueueUrl"]
    messages = []
    while True:
        batch = sqs.receive_message(QueueUrl=queue, MaxNumberOfMessages=10, AttributeNames=["MessageGroupId"]).get("Messages", [])
        if not batch:
            break
        messages += batch
        sqs.delete_message_batch(QueueUrl=queue, Entries=[{"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]} for i, m in enumerate(batch)])
    assert len(messages) == 5
    assert {m["Attributes"]["MessageGroupId"] for m in messages} == {e["payer_id"] for e in good}
    assert {json.loads(m["Body"])["txn_id"] for m in messages} == {e["txn_id"] for e in good}


def test_ingest_dedups_retried_payments(aws):
    ingest = importlib.reload(importlib.import_module("ingest.handler"))
    events = _events(3)
    ingest.handler(_api_event({"events": events}), None)
    ingest.handler(_api_event({"events": events}), None)
    sqs = boto3.client("sqs", region_name=REGION)
    queue = sqs.get_queue_url(QueueName="payments.fifo")["QueueUrl"]
    attrs = sqs.get_queue_attributes(QueueUrl=queue, AttributeNames=["ApproximateNumberOfMessages"])["Attributes"]
    assert attrs["ApproximateNumberOfMessages"] == "3"


def test_ingest_accepts_single_event_and_rejects_garbage(aws):
    ingest = importlib.reload(importlib.import_module("ingest.handler"))
    assert ingest.handler(_api_event(_events(1)[0]), None)["statusCode"] == 202
    assert ingest.handler({"body": "not json"}, None)["statusCode"] == 400
    assert ingest.handler(_api_event({"events": []}), None)["statusCode"] == 400


def _sqs_event(txns):
    return {"Records": [{"messageId": t["txn_id"], "body": json.dumps(t)} for t in txns]}


def test_scorer_writes_profiles_alerts_and_throttles_email(aws, capsys):
    scorer = importlib.reload(importlib.import_module("scorer.handler"))
    events = _events()
    totals = {"scored": 0, "alerts": 0, "emails": 0}
    for i in range(0, len(events), 10):
        out = scorer.handler(_sqs_event(events[i : i + 10]), None)
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
