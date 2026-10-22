"""Phase 1: shared foundation every later stack builds on.

- AWS Budgets cost alerts (email) so spend never surprises us
- S3 buckets: the data lake, Athena query results, and build artifacts
- The Glue Data Catalog database that holds the lakehouse tables
"""

from aws_cdk import (
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
    aws_budgets as budgets,
    aws_glue as glue,
    aws_s3 as s3,
)
from constructs import Construct

from infra.config import ProjectConfig


class FoundationStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *, config: ProjectConfig, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.config = config

        self._create_budget()

        self.lake_bucket = self._bucket("Lake", "lake")
        self.athena_results_bucket = self._bucket(
            "AthenaResults",
            "athena-results",
            lifecycle_rules=[s3.LifecycleRule(expiration=Duration.days(7))],
        )
        self.artifacts_bucket = self._bucket("Artifacts", "artifacts")

        self.glue_database_name = config.glue_database
        glue.CfnDatabase(
            self,
            "LakeDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=self.glue_database_name,
                description="Fraud analyst lakehouse: bronze, silver and gold Iceberg tables",
                location_uri=f"s3://{self.lake_bucket.bucket_name}/warehouse/",
            ),
        )

        CfnOutput(self, "LakeBucketName", value=self.lake_bucket.bucket_name)
        CfnOutput(self, "AthenaResultsBucketName", value=self.athena_results_bucket.bucket_name)
        CfnOutput(self, "ArtifactsBucketName", value=self.artifacts_bucket.bucket_name)
        CfnOutput(self, "GlueDatabaseName", value=self.glue_database_name)

    def _bucket(self, construct_id: str, suffix: str, **kwargs) -> s3.Bucket:
        return s3.Bucket(
            self,
            construct_id,
            bucket_name=f"{self.config.project_name}-{suffix}-{self.account}-{self.region}",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            **kwargs,
        )

    def _create_budget(self) -> None:
        if not self.config.alert_email:
            raise ValueError("Set the alertEmail context value (cdk.json or -c alertEmail=...)")

        subscribers = [
            budgets.CfnBudget.SubscriberProperty(subscription_type="EMAIL", address=self.config.alert_email)
        ]
        notifications = [
            budgets.CfnBudget.NotificationWithSubscribersProperty(
                notification=budgets.CfnBudget.NotificationProperty(
                    notification_type="ACTUAL",
                    comparison_operator="GREATER_THAN",
                    threshold=threshold,
                    threshold_type="ABSOLUTE_VALUE",
                ),
                subscribers=subscribers,
            )
            for threshold in self.config.budget_alert_thresholds_usd
        ]

        budgets.CfnBudget(
            self,
            "CostBudget",
            budget=budgets.CfnBudget.BudgetDataProperty(
                budget_name=f"{self.config.project_name}-total-spend",
                budget_type="COST",
                # Annual (calendar year-to-date) so the running total keeps counting
                # across the month boundary instead of resetting on the 1st.
                time_unit="ANNUALLY",
                budget_limit=budgets.CfnBudget.SpendProperty(
                    amount=self.config.budget_limit_usd, unit="USD"
                ),
                # Credits would otherwise net spend to ~$0 and no alert would ever fire.
                cost_types=budgets.CfnBudget.CostTypesProperty(
                    include_credit=False,
                    include_refund=False,
                    include_tax=True,
                    include_support=True,
                    include_subscription=True,
                    include_upfront=True,
                    include_recurring=True,
                    include_other_subscription=True,
                    include_discount=True,
                    use_amortized=False,
                    use_blended=False,
                ),
            ),
            notifications_with_subscribers=notifications,
        )
