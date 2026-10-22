"""Phase 2: real-time streaming path.

generator --(SigV4-signed HTTPS)--> API Gateway HTTP API --> ingest Lambda --> Kinesis
Kinesis --> scorer Lambda --> DynamoDB (profiles, alerts) + SNS email for severe alerts
"""

from aws_cdk import (
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
    aws_apigatewayv2 as apigw,
    aws_apigatewayv2_authorizers as authorizers,
    aws_apigatewayv2_integrations as integrations,
    aws_dynamodb as dynamodb,
    aws_kinesis as kinesis,
    aws_lambda as lambda_,
    aws_lambda_event_sources as sources,
    aws_logs as logs,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    aws_sqs as sqs,
)
from constructs import Construct

from infra.config import ProjectConfig

LAMBDA_CODE_EXCLUDES = ["glue_jobs", "**/__pycache__", "**/*.pyc"]


class StreamingStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *, config: ProjectConfig, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        name = config.project_name

        # One provisioned shard: ~$0.015/hour and 1 MB/s or 1,000 records/s of writes, far above what we need.
        self.stream = kinesis.Stream(
            self,
            "PaymentsStream",
            stream_name=f"{name}-payments",
            stream_mode=kinesis.StreamMode.PROVISIONED,
            shard_count=1,
            retention_period=Duration.hours(24),
            encryption=kinesis.StreamEncryption.MANAGED,
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.profiles_table = dynamodb.TableV2(
            self,
            "ProfilesTable",
            table_name=f"{name}-profiles",
            partition_key=dynamodb.Attribute(name="payer_id", type=dynamodb.AttributeType.STRING),
            billing=dynamodb.Billing.on_demand(),
            time_to_live_attribute="expires_at",
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.alerts_table = dynamodb.TableV2(
            self,
            "AlertsTable",
            table_name=f"{name}-alerts",
            partition_key=dynamodb.Attribute(name="alert_id", type=dynamodb.AttributeType.STRING),
            billing=dynamodb.Billing.on_demand(),
            time_to_live_attribute="expires_at",
            removal_policy=RemovalPolicy.DESTROY,
            global_secondary_indexes=[
                dynamodb.GlobalSecondaryIndexPropsV2(
                    index_name="by_day",
                    partition_key=dynamodb.Attribute(name="alert_day", type=dynamodb.AttributeType.STRING),
                    sort_key=dynamodb.Attribute(name="event_ts_ms", type=dynamodb.AttributeType.NUMBER),
                ),
                dynamodb.GlobalSecondaryIndexPropsV2(
                    index_name="by_payer",
                    partition_key=dynamodb.Attribute(name="payer_id", type=dynamodb.AttributeType.STRING),
                    sort_key=dynamodb.Attribute(name="event_ts_ms", type=dynamodb.AttributeType.NUMBER),
                ),
            ],
        )

        self.alert_topic = sns.Topic(self, "AlertTopic", topic_name=f"{name}-alerts", display_name="Fraud alerts")
        if config.alert_email:
            self.alert_topic.add_subscription(subs.EmailSubscription(config.alert_email))

        code = lambda_.Code.from_asset("src", exclude=LAMBDA_CODE_EXCLUDES)

        self.ingest_fn = lambda_.Function(
            self,
            "IngestFn",
            function_name=f"{name}-ingest",
            log_group=self._log_group("IngestLogs", f"/aws/lambda/{name}-ingest"),
            runtime=lambda_.Runtime.PYTHON_3_12,
            architecture=lambda_.Architecture.ARM_64,
            handler="ingest.handler.handler",
            code=code,
            memory_size=256,
            timeout=Duration.seconds(10),
            environment={"STREAM_NAME": self.stream.stream_name},
        )
        self.stream.grant_write(self.ingest_fn)

        self.scorer_fn = lambda_.Function(
            self,
            "ScorerFn",
            function_name=f"{name}-scorer",
            log_group=self._log_group("ScorerLogs", f"/aws/lambda/{name}-scorer"),
            runtime=lambda_.Runtime.PYTHON_3_12,
            architecture=lambda_.Architecture.ARM_64,
            handler="scorer.handler.handler",
            code=code,
            memory_size=512,
            timeout=Duration.seconds(60),
            environment={
                "PROFILES_TABLE": self.profiles_table.table_name,
                "ALERTS_TABLE": self.alerts_table.table_name,
                "ALERT_TOPIC_ARN": self.alert_topic.topic_arn,
                "ALERT_THRESHOLD": "0.6",
                "EMAIL_MIN_SCORE": "0.9",
                "MAX_EMAILS_PER_HOUR": "10",
            },
        )
        self.profiles_table.grant_read_write_data(self.scorer_fn)
        self.alerts_table.grant_read_write_data(self.scorer_fn)
        self.alert_topic.grant_publish(self.scorer_fn)

        self.scorer_dlq = sqs.Queue(self, "ScorerDlq", queue_name=f"{name}-scorer-dlq", retention_period=Duration.days(4))
        self.scorer_fn.add_event_source(
            sources.KinesisEventSource(
                self.stream,
                starting_position=lambda_.StartingPosition.LATEST,
                batch_size=100,
                max_batching_window=Duration.seconds(1),
                parallelization_factor=1,  # keep each payer's events strictly ordered
                retry_attempts=2,
                bisect_batch_on_error=True,
                on_failure=sources.SqsDlq(self.scorer_dlq),
            )
        )

        self.api = apigw.HttpApi(
            self,
            "PaymentsApi",
            api_name=f"{name}-payments-api",
            create_default_stage=False,
        )
        apigw.HttpStage(
            self,
            "DefaultStage",
            http_api=self.api,
            stage_name="$default",
            auto_deploy=True,
            # Caps how fast anyone (including a runaway generator) can push data in, and so caps cost.
            throttle=apigw.ThrottleSettings(rate_limit=20, burst_limit=50),
        )
        self.api.add_routes(
            path="/payments",
            methods=[apigw.HttpMethod.POST],
            integration=integrations.HttpLambdaIntegration("IngestIntegration", self.ingest_fn),
            authorizer=authorizers.HttpIamAuthorizer(),
        )

        CfnOutput(self, "PaymentsApiUrl", value=f"{self.api.api_endpoint}/payments")
        CfnOutput(self, "StreamName", value=self.stream.stream_name)
        CfnOutput(self, "AlertsTableName", value=self.alerts_table.table_name)
        CfnOutput(self, "ProfilesTableName", value=self.profiles_table.table_name)
        CfnOutput(self, "AlertTopicArn", value=self.alert_topic.topic_arn)

    def _log_group(self, construct_id: str, log_group_name: str) -> logs.LogGroup:
        return logs.LogGroup(
            self,
            construct_id,
            log_group_name=log_group_name,
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )
