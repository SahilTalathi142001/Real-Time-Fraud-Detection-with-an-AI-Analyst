from aws_cdk.assertions import Match


def test_single_provisioned_shard(template):
    template("FraudStreaming").has_resource_properties(
        "AWS::Kinesis::Stream",
        {"ShardCount": 1, "StreamModeDetails": {"StreamMode": "PROVISIONED"}, "RetentionPeriodHours": 24},
    )


def test_tables_are_on_demand_with_ttl_and_alert_indexes(template):
    t = template("FraudStreaming")
    t.resource_count_is("AWS::DynamoDB::GlobalTable", 2)
    alerts = t.find_resources("AWS::DynamoDB::GlobalTable", {"Properties": {"TableName": "fraud-analyst-alerts"}})
    props = next(iter(alerts.values()))["Properties"]
    assert props["BillingMode"] == "PAY_PER_REQUEST"
    assert props["TimeToLiveSpecification"] == {"AttributeName": "expires_at", "Enabled": True}
    assert {g["IndexName"] for g in props["GlobalSecondaryIndexes"]} == {"by_day", "by_payer"}


def test_api_requires_iam_auth_and_is_throttled(template):
    t = template("FraudStreaming")
    t.has_resource_properties("AWS::ApiGatewayV2::Route", {"RouteKey": "POST /payments", "AuthorizationType": "AWS_IAM"})
    t.has_resource_properties(
        "AWS::ApiGatewayV2::Stage",
        {"StageName": "$default", "DefaultRouteSettings": Match.object_like({"ThrottlingRateLimit": 20, "ThrottlingBurstLimit": 50})},
    )


def test_scorer_reads_stream_in_order_with_dlq(template):
    template("FraudStreaming").has_resource_properties(
        "AWS::Lambda::EventSourceMapping",
        {
            "BatchSize": 100,
            "ParallelizationFactor": 1,
            "BisectBatchOnFunctionError": True,
            "MaximumRetryAttempts": 2,
            "DestinationConfig": Match.object_like({"OnFailure": Match.any_value()}),
        },
    )


def test_alert_email_subscription(template):
    template("FraudStreaming").has_resource_properties(
        "AWS::SNS::Subscription", {"Protocol": "email", "Endpoint": "sahilcsgo69@gmail.com"}
    )


def test_lambdas_are_arm64_python312(template):
    fns = template("FraudStreaming").find_resources("AWS::Lambda::Function")
    app_fns = [f for f in fns.values() if f["Properties"].get("FunctionName", "").startswith("fraud-analyst-")]
    assert len(app_fns) == 2
    for f in app_fns:
        assert f["Properties"]["Runtime"] == "python3.12"
        assert f["Properties"]["Architectures"] == ["arm64"]
