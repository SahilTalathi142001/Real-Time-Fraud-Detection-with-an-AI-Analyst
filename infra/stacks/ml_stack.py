"""Phase 3: the ML model.

- An IAM role SageMaker uses for training/processing jobs and for the endpoint
- When a trained model exists (ml/latest_model.json or -c modelDataUrl=...):
  a SageMaker Model, a *serverless* EndpointConfig, and the Endpoint.
  Serverless inference bills per request and per GB-second of compute, so an idle endpoint costs nothing.
"""

from aws_cdk import CfnOutput, Stack, aws_iam as iam, aws_s3 as s3, aws_sagemaker as sagemaker
from constructs import Construct

from infra.config import ProjectConfig


class MlStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        config: ProjectConfig,
        artifacts_bucket: s3.IBucket,
        model_data_url: str | None,
        model_image: str | None,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.endpoint_name = f"{config.project_name}-xgb"

        self.role = iam.Role(
            self,
            "SageMakerRole",
            role_name=f"{config.project_name}-sagemaker",
            assumed_by=iam.ServicePrincipal("sagemaker.amazonaws.com"),
        )
        artifacts_bucket.grant_read_write(self.role, "ml/*")
        self.role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "ecr:GetAuthorizationToken",
                    "ecr:BatchCheckLayerAvailability",
                    "ecr:GetDownloadUrlForLayer",
                    "ecr:BatchGetImage",
                    "cloudwatch:PutMetricData",
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                    "logs:DescribeLogStreams",
                ],
                resources=["*"],
            )
        )
        CfnOutput(self, "SageMakerRoleArn", value=self.role.role_arn)

        self.has_endpoint = bool(model_data_url and model_image)
        if not self.has_endpoint:
            return

        model = sagemaker.CfnModel(
            self,
            "XgbModel",
            execution_role_arn=self.role.role_arn,
            primary_container=sagemaker.CfnModel.ContainerDefinitionProperty(
                image=model_image, model_data_url=model_data_url
            ),
        )
        endpoint_config = sagemaker.CfnEndpointConfig(
            self,
            "XgbServerlessConfig",
            production_variants=[
                sagemaker.CfnEndpointConfig.ProductionVariantProperty(
                    variant_name="AllTraffic",
                    model_name=model.attr_model_name,
                    serverless_config=sagemaker.CfnEndpointConfig.ServerlessConfigProperty(
                        memory_size_in_mb=2048, max_concurrency=5
                    ),
                )
            ],
        )
        sagemaker.CfnEndpoint(
            self,
            "XgbEndpoint",
            endpoint_name=self.endpoint_name,
            endpoint_config_name=endpoint_config.attr_endpoint_config_name,
        )
        CfnOutput(self, "EndpointName", value=self.endpoint_name)
        CfnOutput(self, "ModelDataUrl", value=model_data_url)
