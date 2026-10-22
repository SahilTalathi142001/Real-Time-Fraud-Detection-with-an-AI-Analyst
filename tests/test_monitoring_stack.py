import json

from aws_cdk.assertions import Match

ALARMS = {
    "fraud-analyst-payments-dlq-not-empty",
    "fraud-analyst-lake-dlq-not-empty",
    "fraud-analyst-scorer-errors",
    "fraud-analyst-model-fallbacks",
}


def _dashboard_body(t) -> str:
    (dashboard,) = t.find_resources("AWS::CloudWatch::Dashboard").values()
    return json.dumps(dashboard["Properties"]["DashboardBody"])


def test_ops_topic_emails_the_owner(template):
    t = template("FraudMonitoring")
    t.has_resource_properties("AWS::SNS::Topic", {"TopicName": "fraud-analyst-ops-alarms"})
    t.has_resource_properties("AWS::SNS::Subscription", {"Protocol": "email", "Endpoint": Match.string_like_regexp("@")})


def test_four_alarms_notify_the_ops_topic(template):
    t = template("FraudMonitoring")
    alarms = t.find_resources("AWS::CloudWatch::Alarm")
    assert {a["Properties"]["AlarmName"] for a in alarms.values()} == ALARMS
    for alarm in alarms.values():
        props = alarm["Properties"]
        assert props["TreatMissingData"] == "notBreaching"
        (action,) = props["AlarmActions"]
        assert action["Ref"].startswith("OpsTopic")


def test_model_fallback_alarm_needs_a_sustained_failure(template):
    template("FraudMonitoring").has_resource_properties("AWS::CloudWatch::Alarm", {
        "AlarmName": "fraud-analyst-model-fallbacks", "Threshold": 10, "EvaluationPeriods": 2,
        "Metrics": [Match.object_like({"MetricStat": Match.object_like({
            "Metric": Match.object_like({"Namespace": "FraudAnalyst", "MetricName": "ModelFallbacks"}), "Period": 300,
        })})],
    })


def test_dashboard_covers_every_stage(template):
    t = template("FraudMonitoring")
    t.has_resource_properties("AWS::CloudWatch::Dashboard", {"DashboardName": "fraud-analyst"})
    body = _dashboard_body(t)
    for title in ("Event -> score latency", "Queue depth", "Dead-letter queues", "Lake loader",
                  "Athena data scanned", "Bedrock tokens", "AgentCore: questions answered", "Alarms"):
        assert title in body, title
    assert "fraud_analyst::DEFAULT" in body


def test_dashboard_shows_sagemaker_only_with_a_model(template, model_template):
    assert "AWS/SageMaker" not in _dashboard_body(template("FraudMonitoring"))
    assert "AWS/SageMaker" in _dashboard_body(model_template("FraudMonitoring"))
