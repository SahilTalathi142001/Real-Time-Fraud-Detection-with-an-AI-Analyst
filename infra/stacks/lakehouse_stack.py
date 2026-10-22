"""Phase 4: the Iceberg lakehouse.

Bronze (raw, append-only):
    scorer Lambda --SQS lake buffer--> loader Lambda (every 5 min) --Athena INSERT--> bronze_payments, bronze_scores
Silver/gold (curated, rebuilt on demand by a Glue PySpark job):
    silver_transactions, gold_fraud_daily, gold_payer_risk, gold_detection_quality
Athena (workgroup with a per-query scan limit) queries all of it.

The buffer + loader stands in for Amazon Data Firehose, which this account's Free plan can't use.
See docs/production-version.md.
"""

from aws_cdk import (
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
    aws_athena as athena,
    aws_events as events,
    aws_events_targets as targets,
    aws_glue as glue,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_logs as logs,
    aws_s3 as s3,
    aws_s3_assets as assets,
    aws_sqs as sqs,
)
from constructs import Construct

from common.lake_schema import LAKE_TABLES
from infra.config import ProjectConfig

LAMBDA_CODE_EXCLUDES = ["glue_jobs", "analyst", "**/__pycache__", "**/*.pyc"]


class LakehouseStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        config: ProjectConfig,
        lake_bucket: s3.IBucket,
        athena_results_bucket: s3.IBucket,
        database_name: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        name = config.project_name
        self.database_name = database_name
        warehouse = f"s3://{lake_bucket.bucket_name}/warehouse"
        catalog_arns = [
            f"arn:aws:glue:{self.region}:{self.account}:catalog",
            f"arn:aws:glue:{self.region}:{self.account}:database/{database_name}",
            f"arn:aws:glue:{self.region}:{self.account}:table/{database_name}/*",
        ]

        for kind, (landing, bronze, columns) in LAKE_TABLES.items():
            self._landing_table(f"Landing{kind.title()}", landing, columns, f"s3://{lake_bucket.bucket_name}/landing/{kind}/")
            self._iceberg_table(f"Bronze{kind.title()}", bronze, columns, f"{warehouse}/{bronze}")

        # ---- Athena ------------------------------------------------------------------------
        self.workgroup_name = name
        workgroup = athena.CfnWorkGroup(
            self,
            "Workgroup",
            name=self.workgroup_name,
            description="Fraud analyst queries (1 GB scan limit per query)",
            recursive_delete_option=True,
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                enforce_work_group_configuration=True,
                publish_cloud_watch_metrics_enabled=True,
                bytes_scanned_cutoff_per_query=1_000_000_000,
                engine_version=athena.CfnWorkGroup.EngineVersionProperty(selected_engine_version="Athena engine version 3"),
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{athena_results_bucket.bucket_name}/results/",
                    encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(encryption_option="SSE_S3"),
                ),
            ),
        )
        for query_name, description, sql in SAMPLE_QUERIES:
            q = athena.CfnNamedQuery(
                self, f"Query{query_name.title().replace('_', '')}", name=query_name, description=description,
                database=database_name, work_group=self.workgroup_name, query_string=sql,
            )
            q.node.add_dependency(workgroup)

        # ---- Bronze ingestion: SQS lake buffer + scheduled loader -------------------------------
        self.lake_buffer_dlq = sqs.Queue(self, "LakeBufferDlq", queue_name=f"{name}-lake-buffer-dlq", retention_period=Duration.days(14))
        self.lake_buffer = sqs.Queue(
            self,
            "LakeBuffer",
            queue_name=f"{name}-lake-buffer",
            retention_period=Duration.days(4),
            visibility_timeout=Duration.minutes(15),  # longer than one loader run
            encryption=sqs.QueueEncryption.SQS_MANAGED,
            dead_letter_queue=sqs.DeadLetterQueue(queue=self.lake_buffer_dlq, max_receive_count=5),
        )

        self.loader_fn = lambda_.Function(
            self,
            "LakeLoaderFn",
            function_name=f"{name}-lake-loader",
            runtime=lambda_.Runtime.PYTHON_3_12,
            architecture=lambda_.Architecture.ARM_64,
            handler="lake_loader.handler.handler",
            code=lambda_.Code.from_asset("src", exclude=LAMBDA_CODE_EXCLUDES),
            memory_size=512,
            timeout=Duration.minutes(5),
            environment={
                "LAKE_QUEUE_URL": self.lake_buffer.queue_url,
                "LAKE_BUCKET": lake_bucket.bucket_name,
                "GLUE_DATABASE": database_name,
                "ATHENA_WORKGROUP": self.workgroup_name,
            },
            log_group=logs.LogGroup(
                self, "LakeLoaderLogs", log_group_name=f"/aws/lambda/{name}-lake-loader",
                retention=logs.RetentionDays.ONE_WEEK, removal_policy=RemovalPolicy.DESTROY,
            ),
        )
        self.lake_buffer.grant_consume_messages(self.loader_fn)
        lake_bucket.grant_read_write(self.loader_fn)
        lake_bucket.grant_delete(self.loader_fn)
        athena_results_bucket.grant_read_write(self.loader_fn)
        self.loader_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults", "athena:StopQueryExecution"],
                resources=[f"arn:aws:athena:{self.region}:{self.account}:workgroup/{self.workgroup_name}"],
            )
        )
        self.loader_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:GetPartitions", "glue:UpdateTable"],
                resources=catalog_arns,
            )
        )
        events.Rule(
            self,
            "LakeLoaderSchedule",
            rule_name=f"{name}-lake-loader-every-5-min",
            schedule=events.Schedule.rate(Duration.minutes(5)),
            targets=[targets.LambdaFunction(self.loader_fn, retry_attempts=0)],
        )

        # ---- Glue PySpark job: silver + gold -----------------------------------------------
        script = assets.Asset(self, "SilverGoldScript", path="src/glue_jobs/silver_gold.py")
        glue_role = iam.Role(
            self,
            "GlueJobRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")],
        )
        lake_bucket.grant_read_write(glue_role)
        script.grant_read(glue_role)
        glue_role.add_to_policy(
            iam.PolicyStatement(
                actions=["glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable"],
                resources=catalog_arns,
            )
        )
        iceberg_conf = " --conf ".join(
            [
                "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog",
                f"spark.sql.catalog.glue_catalog.warehouse={warehouse}",
                "spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
                "spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
            ]
        )
        self.glue_job_name = f"{name}-silver-gold"
        glue.CfnJob(
            self,
            "SilverGoldJob",
            name=self.glue_job_name,
            role=glue_role.role_arn,
            glue_version="5.0",
            worker_type="G.1X",
            number_of_workers=2,
            timeout=20,
            max_retries=0,
            execution_property=glue.CfnJob.ExecutionPropertyProperty(max_concurrent_runs=1),
            command=glue.CfnJob.JobCommandProperty(name="glueetl", python_version="3", script_location=script.s3_object_url),
            default_arguments={
                "--datalake-formats": "iceberg",
                "--conf": iceberg_conf,
                "--database": database_name,
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--job-language": "python",
            },
        )

        CfnOutput(self, "AthenaWorkgroup", value=self.workgroup_name)
        CfnOutput(self, "GlueJobName", value=self.glue_job_name)
        CfnOutput(self, "LakeBufferUrl", value=self.lake_buffer.queue_url)
        CfnOutput(self, "LakeLoaderFunction", value=self.loader_fn.function_name)

    def _landing_table(self, construct_id: str, table: str, columns: list[tuple[str, str]], location: str) -> glue.CfnTable:
        return glue.CfnTable(
            self,
            construct_id,
            catalog_id=self.account,
            database_name=self.database_name,
            table_input=glue.CfnTable.TableInputProperty(
                name=table,
                table_type="EXTERNAL_TABLE",
                description="Landing zone: JSON Lines files the loader writes and immediately inserts into bronze",
                parameters={"classification": "json"},
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    location=location,
                    columns=[glue.CfnTable.ColumnProperty(name=n, type=t) for n, t in columns],
                    input_format="org.apache.hadoop.mapred.TextInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.openx.data.jsonserde.JsonSerDe",
                        parameters={"ignore.malformed.json": "true"},
                    ),
                ),
            ),
        )

    def _iceberg_table(self, construct_id: str, table: str, columns: list[tuple[str, str]], location: str) -> glue.CfnTable:
        return glue.CfnTable(
            self,
            construct_id,
            catalog_id=self.account,
            database_name=self.database_name,
            open_table_format_input=glue.CfnTable.OpenTableFormatInputProperty(
                iceberg_input=glue.CfnTable.IcebergInputProperty(metadata_operation="CREATE", version="2")
            ),
            table_input=glue.CfnTable.TableInputProperty(
                name=table,
                table_type="EXTERNAL_TABLE",
                description=f"Bronze (raw) {table.split('_', 1)[1]}, appended by the lake loader",
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    location=location,
                    columns=[glue.CfnTable.ColumnProperty(name=n, type=t) for n, t in columns],
                ),
            ),
        )


SAMPLE_QUERIES = [
    (
        "fraud_by_city_today",
        "Alert rate and money at risk by city for the latest day",
        """SELECT city, sum(txn_count) AS txn_count, sum(alert_count) AS alert_count,
       round(100.0 * sum(alert_count) / sum(txn_count), 2) AS alert_rate_pct, round(sum(alert_amount)) AS alert_amount_inr
FROM gold_fraud_daily
WHERE event_date = (SELECT max(event_date) FROM gold_fraud_daily)
GROUP BY city
ORDER BY alert_count DESC""",
    ),
    (
        "top_risky_payers",
        "Payers with the most alerts",
        """SELECT payer_id, payer_vpa, home_city, txn_count, alert_count, round(max_score, 3) AS max_score, distinct_devices, distinct_cities
FROM gold_payer_risk
WHERE alert_count > 0
ORDER BY alert_count DESC, max_score DESC
LIMIT 20""",
    ),
    (
        "detection_quality",
        "Precision and recall of the live scorer against simulator ground truth",
        """SELECT event_date, scored_by, tp, fp, fn, round(precision, 3) AS precision, round(recall, 3) AS recall
FROM gold_detection_quality
ORDER BY event_date DESC, scored_by""",
    ),
    (
        "bronze_freshness",
        "How recent is the newest raw payment in the lake",
        """SELECT count(*) AS payments, max(event_time) AS newest_event FROM bronze_payments""",
    ),
]
