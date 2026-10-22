from aws_cdk.assertions import Match

from common.athena import is_read_only


def test_no_firehose_or_kinesis(template):
    t = template("FraudLakehouse")
    assert not t.find_resources("AWS::KinesisFirehose::DeliveryStream")
    assert not t.find_resources("AWS::Kinesis::Stream")


def test_bronze_iceberg_and_landing_json_tables(template):
    t = template("FraudLakehouse")
    for table in ("bronze_payments", "bronze_scores"):
        t.has_resource_properties(
            "AWS::Glue::Table",
            {
                "OpenTableFormatInput": {"IcebergInput": {"MetadataOperation": "CREATE", "Version": "2"}},
                "TableInput": Match.object_like({"Name": table}),
            },
        )
    for table in ("landing_payments", "landing_scores"):
        t.has_resource_properties(
            "AWS::Glue::Table",
            {
                "TableInput": Match.object_like(
                    {
                        "Name": table,
                        "StorageDescriptor": Match.object_like(
                            {"SerdeInfo": Match.object_like({"SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe"})}
                        ),
                    }
                )
            },
        )


def test_loader_runs_every_5_minutes(template):
    t = template("FraudLakehouse")
    t.has_resource_properties("AWS::Events::Rule", {"ScheduleExpression": "rate(5 minutes)", "State": "ENABLED"})
    t.has_resource_properties(
        "AWS::Lambda::Function",
        {"FunctionName": "fraud-analyst-lake-loader", "Timeout": 300},
    )
    t.has_resource_properties("AWS::SQS::Queue", {"QueueName": "fraud-analyst-lake-buffer", "VisibilityTimeout": 900})


def test_glue_job_is_small_and_on_demand(template):
    t = template("FraudLakehouse")
    t.has_resource_properties(
        "AWS::Glue::Job",
        {
            "GlueVersion": "5.0",
            "WorkerType": "G.1X",
            "NumberOfWorkers": 2,
            "DefaultArguments": Match.object_like({"--datalake-formats": "iceberg"}),
        },
    )
    assert not t.find_resources("AWS::Glue::Trigger")


def test_athena_workgroup_caps_scan_size(template):
    template("FraudLakehouse").has_resource_properties(
        "AWS::Athena::WorkGroup",
        {"WorkGroupConfiguration": Match.object_like({"BytesScannedCutoffPerQuery": 1_000_000_000, "EnforceWorkGroupConfiguration": True})},
    )


def test_scorer_sends_to_lake_buffer(template):
    fns = template("FraudStreaming").find_resources("AWS::Lambda::Function", {"Properties": {"FunctionName": "fraud-analyst-scorer"}})
    env = next(iter(fns.values()))["Properties"]["Environment"]["Variables"]
    assert "LAKE_QUEUE_URL" in env


def test_read_only_sql_guard():
    assert is_read_only("SELECT * FROM gold_payer_risk LIMIT 5")
    assert is_read_only("WITH x AS (SELECT 1) SELECT * FROM x")
    assert is_read_only("-- top payers\nSELECT 1")
    assert is_read_only("SELECT 'drop table' AS s")
    assert not is_read_only("DROP TABLE bronze_payments")
    assert not is_read_only("SELECT 1; DROP TABLE x")
    assert not is_read_only("INSERT INTO bronze_payments SELECT * FROM landing_payments")
    assert not is_read_only("CREATE TABLE x AS SELECT 1")
