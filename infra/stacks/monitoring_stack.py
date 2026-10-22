"""Phase 6: one CloudWatch dashboard for the whole pipeline, plus a few alarms.

Rows follow a payment through the system: business KPIs -> latency -> queues -> Lambda health ->
model endpoint -> lakehouse -> AI analyst. Alarms email the ops topic (separate from fraud alerts)
when something needs a human: messages in a dead-letter queue, scorer errors, or the model falling
back to rules.
"""

from __future__ import annotations

from aws_cdk import CfnOutput, Duration, Stack
from aws_cdk import aws_apigatewayv2 as apigw
from aws_cdk import aws_cloudwatch as cw
from aws_cdk import aws_cloudwatch_actions as cw_actions
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as subs
from aws_cdk import aws_sqs as sqs
from constructs import Construct

from infra.config import ProjectConfig

APP_NAMESPACE = "FraudAnalyst"
FIVE_MIN = Duration.minutes(5)
ONE_MIN = Duration.minutes(1)


def app_metric(name: str, statistic: str = "Sum", label: str | None = None, period: Duration = ONE_MIN) -> cw.Metric:
    return cw.Metric(
        namespace=APP_NAMESPACE, metric_name=name, dimensions_map={"Service": "scorer"},
        statistic=statistic, period=period, label=label or name,
    )


class MonitoringStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        config: ProjectConfig,
        api: apigw.HttpApi,
        payments_queue: sqs.IQueue,
        payments_dlq: sqs.IQueue,
        lake_buffer: sqs.IQueue,
        lake_buffer_dlq: sqs.IQueue,
        ingest_fn: lambda_.IFunction,
        scorer_fn: lambda_.IFunction,
        loader_fn: lambda_.IFunction,
        endpoint_name: str | None,
        glue_job_name: str,
        athena_workgroup: str,
        runtime_name: str | None,
        model_id: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        name = config.project_name

        self.ops_topic = sns.Topic(self, "OpsTopic", topic_name=f"{name}-ops-alarms", display_name="Fraud pipeline ops alarms")
        if config.alert_email:
            self.ops_topic.add_subscription(subs.EmailSubscription(config.alert_email))

        dash = cw.Dashboard(
            self, "Dashboard", dashboard_name=name, default_interval=Duration.hours(3),
            period_override=cw.PeriodOverride.INHERIT,
        )
        dash.add_widgets(cw.TextWidget(width=24, height=2, markdown=(
            "# Real-time UPI fraud detection\n"
            "Payments: generator -> HTTP API -> ingest Lambda -> SQS FIFO -> scorer Lambda (XGBoost on SageMaker serverless) "
            "-> DynamoDB alerts + SNS email -> SQS lake buffer -> loader Lambda -> Iceberg (Athena). "
            "The AI analyst (AgentCore + Claude) reads alerts, the lakehouse and the playbook."
        )))

        # ---- Business KPIs --------------------------------------------------------------------
        scored, alerts = app_metric("TransactionsScored", label="scored"), app_metric("AlertsRaised", label="alerts")
        dash.add_widgets(
            cw.SingleValueWidget(
                title="Totals for the selected time range", width=12, height=4, set_period_to_time_range=True,
                metrics=[scored, alerts, app_metric("AlertEmailsSent", label="emails sent"), app_metric("ModelFallbacks", label="model fallbacks")],
            ),
            cw.GraphWidget(
                title="Payments scored and alerts raised (per minute)", width=12, height=4,
                left=[scored], right=[alerts],
                left_y_axis=cw.YAxisProps(min=0, show_units=False), right_y_axis=cw.YAxisProps(min=0, show_units=False),
            ),
        )

        # ---- Latency ----------------------------------------------------------------------------
        dash.add_widgets(
            cw.GraphWidget(
                title="Event -> score latency (ms, worst payment per batch)", width=8, height=6,
                left=[app_metric("EventToScoreLatencyMs", s, s, ONE_MIN) for s in ("p50", "p95", "Maximum")],
                left_y_axis=cw.YAxisProps(min=0, show_units=False),
            ),
            cw.GraphWidget(
                title="HTTP API: requests and errors", width=8, height=6,
                left=[api.metric_count(period=ONE_MIN, label="requests")],
                right=[api.metric_client_error(period=ONE_MIN, label="4xx"), api.metric_server_error(period=ONE_MIN, label="5xx")],
            ),
            cw.GraphWidget(
                title="Lambda duration p95 (ms)", width=8, height=6,
                left=[fn.metric_duration(statistic="p95", period=ONE_MIN, label=label)
                      for fn, label in ((ingest_fn, "ingest"), (scorer_fn, "scorer"), (loader_fn, "lake loader"))],
            ),
        )

        # ---- Queues ------------------------------------------------------------------------------
        dash.add_widgets(
            cw.GraphWidget(
                title="Queue depth (messages waiting)", width=8, height=6,
                left=[payments_queue.metric_approximate_number_of_messages_visible(period=ONE_MIN, statistic="Maximum", label="payments FIFO"),
                      lake_buffer.metric_approximate_number_of_messages_visible(period=ONE_MIN, statistic="Maximum", label="lake buffer")],
            ),
            cw.GraphWidget(
                title="Age of oldest message (s)", width=8, height=6,
                left=[payments_queue.metric_approximate_age_of_oldest_message(period=ONE_MIN, statistic="Maximum", label="payments FIFO"),
                      lake_buffer.metric_approximate_age_of_oldest_message(period=ONE_MIN, statistic="Maximum", label="lake buffer")],
            ),
            cw.GraphWidget(
                title="Dead-letter queues (should be 0)", width=8, height=6,
                left=[payments_dlq.metric_approximate_number_of_messages_visible(period=ONE_MIN, statistic="Maximum", label="payments DLQ"),
                      lake_buffer_dlq.metric_approximate_number_of_messages_visible(period=ONE_MIN, statistic="Maximum", label="lake buffer DLQ")],
            ),
        )

        # ---- Lambda health + model ------------------------------------------------------------------
        lambda_health = []
        for fn, label in ((ingest_fn, "ingest"), (scorer_fn, "scorer"), (loader_fn, "lake loader")):
            lambda_health += [fn.metric_errors(period=FIVE_MIN, label=f"{label} errors"), fn.metric_throttles(period=FIVE_MIN, label=f"{label} throttles")]
        row = [cw.GraphWidget(title="Lambda errors and throttles", width=8, height=6, left=lambda_health)]
        if endpoint_name:
            ep = {"EndpointName": endpoint_name, "VariantName": "AllTraffic"}
            sm = lambda metric, stat, label: cw.Metric(namespace="AWS/SageMaker", metric_name=metric, dimensions_map=ep, statistic=stat, period=ONE_MIN, label=label)  # noqa: E731
            row += [
                cw.GraphWidget(
                    title=f"SageMaker {endpoint_name}: invocations and errors", width=8, height=6,
                    left=[sm("Invocations", "Sum", "invocations")],
                    right=[sm("Invocation4XXErrors", "Sum", "4xx"), sm("Invocation5XXErrors", "Sum", "5xx"), app_metric("ModelFallbacks", label="scorer fallbacks to rules")],
                ),
                cw.GraphWidget(
                    title="Model latency and cold starts (microseconds)", width=8, height=6,
                    left=[sm("ModelLatency", "p95", "model latency p95")], right=[sm("ModelSetupTime", "Maximum", "cold start setup time")],
                ),
            ]
        dash.add_widgets(*row)

        # ---- Lakehouse --------------------------------------------------------------------------
        glue_dims = {"JobName": glue_job_name, "JobRunId": "ALL", "Type": "count"}
        dash.add_widgets(
            cw.GraphWidget(
                title="Lake loader: invocations and errors (every 5 min)", width=8, height=6,
                left=[loader_fn.metric_invocations(period=FIVE_MIN, label="runs"), loader_fn.metric_errors(period=FIVE_MIN, label="errors")],
                right=[app_metric("LakeWriteFailures", label="scorer -> buffer send failures", period=FIVE_MIN)],
            ),
            cw.GraphWidget(
                title="Athena data scanned (bytes, workgroup)", width=8, height=6,
                left=[cw.Metric(namespace="AWS/Athena", metric_name="ProcessedBytes", dimensions_map={"WorkGroup": athena_workgroup, "QueryState": "SUCCEEDED", "QueryType": "DML"}, statistic="Sum", period=FIVE_MIN, label="bytes scanned")],
            ),
            cw.GraphWidget(
                title="Glue silver/gold job: completed tasks", width=8, height=6,
                left=[cw.Metric(namespace="Glue", metric_name="glue.driver.aggregate.numCompletedTasks", dimensions_map=glue_dims, statistic="Sum", period=FIVE_MIN, label="completed tasks")],
            ),
        )

        # ---- AI analyst ----------------------------------------------------------------------------
        bedrock = lambda metric, label: cw.Metric(namespace="AWS/Bedrock", metric_name=metric, dimensions_map={"ModelId": model_id}, statistic="Sum", period=FIVE_MIN, label=label)  # noqa: E731
        analyst_row = [
            cw.GraphWidget(
                title=f"Bedrock tokens ({model_id})", width=8, height=6,
                left=[bedrock("InputTokenCount", "input tokens"), bedrock("OutputTokenCount", "output tokens")],
            )
        ]
        if runtime_name:
            # The Resource dimension is the runtime ARN (random suffix), so match on the endpoint Name instead.
            endpoint = f"{runtime_name}::DEFAULT"
            ac_metric = lambda metric, stat, label: cw.MathExpression(  # noqa: E731
                expression=f"SEARCH('{{AWS/Bedrock-AgentCore,Name,Operation,Resource}} MetricName=\"{metric}\" Name=\"{endpoint}\"', '{stat}', 300)",
                using_metrics={}, label=label, period=FIVE_MIN,
            )
            analyst_row += [
                cw.GraphWidget(title="AgentCore: questions answered and errors", width=8, height=6,
                               left=[ac_metric("Invocations", "Sum", "invocations")],
                               right=[ac_metric("UserErrors", "Sum", "user errors"), ac_metric("SystemErrors", "Sum", "system errors")]),
                cw.GraphWidget(title="AgentCore: answer latency (ms)", width=8, height=6,
                               left=[ac_metric("Latency", "Average", "average latency")]),
            ]
        dash.add_widgets(*analyst_row)

        # ---- Alarms -------------------------------------------------------------------------------
        alarms = [
            payments_dlq.metric_approximate_number_of_messages_visible(period=FIVE_MIN, statistic="Maximum").create_alarm(
                self, "PaymentsDlqAlarm", alarm_name=f"{name}-payments-dlq-not-empty", threshold=1, evaluation_periods=1,
                comparison_operator=cw.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
                alarm_description="Payments failed scoring 3 times and were parked in the DLQ. Check the scorer logs.",
                treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
            ),
            lake_buffer_dlq.metric_approximate_number_of_messages_visible(period=FIVE_MIN, statistic="Maximum").create_alarm(
                self, "LakeDlqAlarm", alarm_name=f"{name}-lake-dlq-not-empty", threshold=1, evaluation_periods=1,
                comparison_operator=cw.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
                alarm_description="Lake loader failed 5 times on some messages. Check the loader logs.",
                treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
            ),
            scorer_fn.metric_errors(period=FIVE_MIN).create_alarm(
                self, "ScorerErrorsAlarm", alarm_name=f"{name}-scorer-errors", threshold=1, evaluation_periods=1,
                comparison_operator=cw.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
                alarm_description="The scorer Lambda raised errors (SQS will retry, then use the DLQ).",
                treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
            ),
            app_metric("ModelFallbacks", period=FIVE_MIN).create_alarm(
                self, "ModelFallbackAlarm", alarm_name=f"{name}-model-fallbacks", threshold=10, evaluation_periods=2,
                comparison_operator=cw.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
                alarm_description="10+ payments per 5 min for 10 min were scored by rules because the SageMaker endpoint failed.",
                treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
            ),
        ]
        for alarm in alarms:
            alarm.add_alarm_action(cw_actions.SnsAction(self.ops_topic))
        dash.add_widgets(cw.AlarmStatusWidget(title="Alarms", width=24, height=3, alarms=alarms))

        CfnOutput(self, "DashboardName", value=name)
        CfnOutput(self, "OpsTopicArn", value=self.ops_topic.topic_arn)
