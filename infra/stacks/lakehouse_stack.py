"""Phase 4: the Iceberg lakehouse.

Bronze (raw, append-only, written by Firehose):
    Kinesis payments stream --Firehose--> bronze_payments
    scorer Lambda          --Firehose--> bronze_scores
Silver/gold (curated, rebuilt on demand by a Glue PySpark job):
    silver_transactions, gold_fraud_daily, gold_payer_risk, gold_detection_quality
Athena (workgroup with a per-query scan limit) queries all of it.
"""

from aws_cdk import (
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
    aws_athena as athena,
    aws_glue as glue,
    aws_iam as iam,
    aws_kinesis as kinesis,
    aws_kinesisfirehose as firehose,
    aws_logs as logs,
    aws_s3 as s3,
    aws_s3_assets as assets,
)
from constructs import Construct

from infra.config import ProjectConfig

PAYMENT_COLUMNS = [
    ("txn_id", "string"), ("event_time", "string"), ("event_ts_ms", "bigint"),
    ("payer_id", "string"), ("payer_vpa", "string"), ("payee_vpa", "string"), ("payee_type", "string"),
    ("merchant_id", "string"), ("merchant_category", "string"), ("amount", "double"), ("currency", "string"),
    ("channel", "string"), ("device_id", "string"), ("device_os", "string"), ("ip_address", "string"),
    ("city", "string"), ("lat", "double"), ("lon", "double"),
    ("label_is_fraud", "boolean"), ("label_pattern", "string"), ("ingested_at_ms", "bigint"),
]
SCORE_COLUMNS = [
    ("txn_id", "string"), ("event_time", "string"), ("event_ts_ms", "bigint"), ("payer_id", "string"),
    ("score", "double"), ("rule_score", "double"), ("model_score", "double"), ("is_alert", "boolean"),
    ("scored_by", "string"), ("reasons", "string"), ("scored_at_ms", "bigint"),
]


def scores_firehose_name(config: ProjectConfig) -> str:
    return f"{config.project_name}-scores-to-iceberg"


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
        payments_stream: kinesis.IStream,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        name = config.project_name
        self.database_name = database_name
        warehouse = f"s3://{lake_bucket.bucket_name}/warehouse"

        payments_table = self._iceberg_table("BronzePayments", "bronze_payments", PAYMENT_COLUMNS, f"{warehouse}/bronze_payments")
        scores_table = self._iceberg_table("BronzeScores", "bronze_scores", SCORE_COLUMNS, f"{warehouse}/bronze_scores")

        # ---- Firehose: bronze ingestion ----------------------------------------------------
        firehose_role = iam.Role(self, "FirehoseRole", assumed_by=iam.ServicePrincipal("firehose.amazonaws.com"))
        lake_bucket.grant_read_write(firehose_role)
        payments_stream.grant_read(firehose_role)
        firehose_role.add_to_policy(
            iam.PolicyStatement(
                actions=["glue:GetDatabase", "glue:GetTable", "glue:UpdateTable"],
                resources=[
                    f"arn:aws:glue:{self.region}:{self.account}:catalog",
                    f"arn:aws:glue:{self.region}:{self.account}:database/{database_name}",
                    f"arn:aws:glue:{self.region}:{self.account}:table/{database_name}/*",
                ],
            )
        )
        firehose_logs = logs.LogGroup(
            self, "FirehoseLogs", log_group_name=f"/aws/firehose/{name}",
            retention=logs.RetentionDays.ONE_WEEK, removal_policy=RemovalPolicy.DESTROY,
        )
        firehose_logs.grant_write(firehose_role)

        def iceberg_destination(table: str, stream_name: str):
            log_stream = logs.LogStream(self, f"{stream_name}LogStream", log_group=firehose_logs, log_stream_name=stream_name)
            return firehose.CfnDeliveryStream.IcebergDestinationConfigurationProperty(
                catalog_configuration=firehose.CfnDeliveryStream.CatalogConfigurationProperty(
                    catalog_arn=f"arn:aws:glue:{self.region}:{self.account}:catalog"
                ),
                role_arn=firehose_role.role_arn,
                destination_table_configuration_list=[
                    firehose.CfnDeliveryStream.DestinationTableConfigurationProperty(
                        destination_database_name=database_name, destination_table_name=table
                    )
                ],
                append_only=True,
                # Larger buffers mean fewer, bigger files; 60s keeps the demo feeling "live".
                buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(interval_in_seconds=60, size_in_m_bs=16),
                s3_backup_mode="FailedDataOnly",
                s3_configuration=firehose.CfnDeliveryStream.S3DestinationConfigurationProperty(
                    bucket_arn=lake_bucket.bucket_arn,
                    role_arn=firehose_role.role_arn,
                    error_output_prefix=f"firehose-errors/{table}/",
                ),
                cloud_watch_logging_options=firehose.CfnDeliveryStream.CloudWatchLoggingOptionsProperty(
                    enabled=True, log_group_name=firehose_logs.log_group_name, log_stream_name=log_stream.log_stream_name
                ),
            )

        payments_firehose = firehose.CfnDeliveryStream(
            self,
            "PaymentsToIceberg",
            delivery_stream_name=f"{name}-payments-to-iceberg",
            delivery_stream_type="KinesisStreamAsSource",
            kinesis_stream_source_configuration=firehose.CfnDeliveryStream.KinesisStreamSourceConfigurationProperty(
                kinesis_stream_arn=payments_stream.stream_arn, role_arn=firehose_role.role_arn
            ),
            iceberg_destination_configuration=iceberg_destination("bronze_payments", "payments"),
        )
        scores_firehose = firehose.CfnDeliveryStream(
            self,
            "ScoresToIceberg",
            delivery_stream_name=scores_firehose_name(config),
            delivery_stream_type="DirectPut",
            iceberg_destination_configuration=iceberg_destination("bronze_scores", "scores"),
        )
        for stream, table in ((payments_firehose, payments_table), (scores_firehose, scores_table)):
            stream.node.add_dependency(table)
            stream.node.add_dependency(firehose_role)

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
                resources=[
                    f"arn:aws:glue:{self.region}:{self.account}:catalog",
                    f"arn:aws:glue:{self.region}:{self.account}:database/{database_name}",
                    f"arn:aws:glue:{self.region}:{self.account}:table/{database_name}/*",
                ],
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

        CfnOutput(self, "AthenaWorkgroup", value=self.workgroup_name)
        CfnOutput(self, "GlueJobName", value=self.glue_job_name)
        CfnOutput(self, "PaymentsFirehose", value=payments_firehose.ref)
        CfnOutput(self, "ScoresFirehose", value=scores_firehose.ref)

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
                description=f"Bronze (raw) {table.split('_', 1)[1]} written by Firehose",
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
        """SELECT city, txn_count, alert_count, round(alert_rate * 100, 2) AS alert_rate_pct, round(alert_amount) AS alert_amount_inr
FROM gold_fraud_daily
WHERE event_date = (SELECT max(event_date) FROM gold_fraud_daily)
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
