"""Phase 2: real-time streaming path.

generator --(SigV4-signed HTTPS)--> API Gateway HTTP API --> ingest Lambda --> SQS FIFO
SQS FIFO --> scorer Lambda --> DynamoDB (profiles, alerts) + SNS email for severe alerts

SQS FIFO stands in for Kinesis Data Streams, which this AWS account (Free plan) cannot use.
MessageGroupId = payer_id gives the same per-payer ordering a Kinesis partition key would.
See docs/production-version.md.
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
    aws_iam as iam,
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
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        config: ProjectConfig,
        model_endpoint_name: str | None = None,
        alert_threshold: float = 0.6,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        name = config.project_name

        self.payments_dlq = sqs.Queue(
            self,
            "PaymentsDlq",
            queue_name=f"{name}-payments-dlq.fifo",
            fifo=True,
            retention_period=Duration.days(14),
        )
        self.payments_queue = sqs.Queue(
            self,
            "PaymentsQueue",
            queue_name=f"{name}-payments.fifo",
            fifo=True,
            # High-throughput FIFO: ordering and dedup are per payer, not per queue.
            deduplication_scope=sqs.DeduplicationScope.MESSAGE_GROUP,
            fifo_throughput_limit=sqs.FifoThroughputLimit.PER_MESSAGE_GROUP_ID,
            retention_period=Duration.days(1),
            visibility_timeout=Duration.seconds(360),  # 6x the scorer timeout, as AWS recommends
            encryption=sqs.QueueEncryption.SQS_MANAGED,
            dead_letter_queue=sqs.DeadLetterQueue(queue=self.payments_dlq, max_receive_count=3),
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
            environment={"QUEUE_URL": self.payments_queue.queue_url},
        )
        self.payments_queue.grant_send_messages(self.ingest_fn)

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
                "ALERT_THRESHOLD": str(alert_threshold),
                "MODEL_ENDPOINT": model_endpoint_name or "",
                "EMAIL_MIN_SCORE": "0.9",
                "MAX_EMAILS_PER_HOUR": "10",
            },
        )
        self.profiles_table.grant_read_write_data(self.scorer_fn)
        self.alerts_table.grant_read_write_data(self.scorer_fn)
        self.alert_topic.grant_publish(self.scorer_fn)
        if model_endpoint_name:
            self.scorer_fn.add_to_role_policy(
                iam.PolicyStatement(
                    actions=["sagemaker:InvokeEndpoint"],
                    resources=[f"arn:aws:sagemaker:{self.region}:{self.account}:endpoint/{model_endpoint_name}"],
                )
            )

        self.scorer_fn.add_event_source(
            sources.SqsEventSource(
                self.payments_queue,
                batch_size=10,  # FIFO maximum
                max_concurrency=5,  # caps parallel scorers (and DynamoDB/SageMaker load)
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
        CfnOutput(self, "PaymentsQueueUrl", value=self.payments_queue.queue_url)
        CfnOutput(self, "PaymentsDlqUrl", value=self.payments_dlq.queue_url)
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
