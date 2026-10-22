"""Phase 5: the AI fraud analyst.

    kb/*.md --BucketDeployment--> S3 (artifacts/kb/) --Bedrock KB ingestion (Titan embeddings)--> S3 Vectors index
    Streamlit / CLI --InvokeAgentRuntime (IAM SigV4)--> AgentCore Runtime (Strands agent, Claude on Bedrock)
        tools: Athena SQL over the lakehouse, DynamoDB alert lookups, Bedrock KB retrieve

Everything here is pay-per-use: AgentCore bills only while a session is active, S3 Vectors bills
storage + queries, and the knowledge base has no always-on cluster (unlike OpenSearch Serverless).
"""

from __future__ import annotations

from aws_cdk import (
    Annotations,
    CfnOutput,
    Fn,
    RemovalPolicy,
    Stack,
    aws_bedrock as bedrock,
    aws_bedrockagentcore as agentcore,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_logs as logs,
    aws_s3 as s3,
    aws_s3_assets as assets,
    aws_s3_deployment as s3deploy,
    aws_s3vectors as s3vectors,
)
from constructs import Construct

from infra.config import ProjectConfig

EMBEDDING_MODEL = "amazon.titan-embed-text-v2:0"
EMBEDDING_DIMENSIONS = 1024
KB_PREFIX = "kb/"


class AnalystStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        config: ProjectConfig,
        artifacts_bucket: s3.IBucket,
        lake_bucket: s3.IBucket,
        athena_results_bucket: s3.IBucket,
        database_name: str,
        athena_workgroup: str,
        alerts_table: dynamodb.ITableV2,
        model_id: str,
        runtime_zip: str | None,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        name = config.project_name

        # ---- Knowledge base: playbook docs -> Titan embeddings -> S3 Vectors -------------------
        vector_bucket = s3vectors.CfnVectorBucket(self, "KbVectors", vector_bucket_name=f"{name}-kb-{self.account}")
        vector_index = s3vectors.CfnIndex(
            self,
            "KbIndex",
            vector_bucket_arn=vector_bucket.attr_vector_bucket_arn,
            index_name="playbook",
            data_type="float32",
            dimension=EMBEDDING_DIMENSIONS,
            distance_metric="cosine",
            # Bedrock stores the chunk text in these keys; non-filterable keys aren't limited to 2 KB.
            metadata_configuration=s3vectors.CfnIndex.MetadataConfigurationProperty(
                non_filterable_metadata_keys=["AMAZON_BEDROCK_TEXT", "AMAZON_BEDROCK_METADATA"]
            ),
        )
        vector_index.apply_removal_policy(RemovalPolicy.DESTROY)
        vector_bucket.apply_removal_policy(RemovalPolicy.DESTROY)

        docs = s3deploy.BucketDeployment(
            self,
            "PlaybookDocs",
            sources=[s3deploy.Source.asset("kb")],
            destination_bucket=artifacts_bucket,
            destination_key_prefix=KB_PREFIX,
        )

        embedding_arn = f"arn:aws:bedrock:{self.region}::foundation-model/{EMBEDDING_MODEL}"
        kb_role = iam.Role(
            self,
            "KnowledgeBaseRole",
            assumed_by=iam.ServicePrincipal(
                "bedrock.amazonaws.com",
                conditions={"StringEquals": {"aws:SourceAccount": self.account}},
            ),
        )
        kb_role.add_to_policy(iam.PolicyStatement(actions=["bedrock:InvokeModel"], resources=[embedding_arn]))
        kb_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3vectors:PutVectors", "s3vectors:GetVectors", "s3vectors:DeleteVectors", "s3vectors:QueryVectors", "s3vectors:GetIndex"],
                resources=[vector_index.attr_index_arn],
            )
        )
        kb_role.add_to_policy(iam.PolicyStatement(actions=["s3:ListBucket"], resources=[artifacts_bucket.bucket_arn]))
        kb_role.add_to_policy(iam.PolicyStatement(actions=["s3:GetObject"], resources=[artifacts_bucket.arn_for_objects(f"{KB_PREFIX}*")]))

        kb = bedrock.CfnKnowledgeBase(
            self,
            "KnowledgeBase",
            name=f"{name}-playbook",
            description="Fraud playbook: patterns, triage runbook, model card, data dictionary",
            role_arn=kb_role.role_arn,
            knowledge_base_configuration=bedrock.CfnKnowledgeBase.KnowledgeBaseConfigurationProperty(
                type="VECTOR",
                vector_knowledge_base_configuration=bedrock.CfnKnowledgeBase.VectorKnowledgeBaseConfigurationProperty(
                    embedding_model_arn=embedding_arn,
                    embedding_model_configuration=bedrock.CfnKnowledgeBase.EmbeddingModelConfigurationProperty(
                        bedrock_embedding_model_configuration=bedrock.CfnKnowledgeBase.BedrockEmbeddingModelConfigurationProperty(
                            dimensions=EMBEDDING_DIMENSIONS, embedding_data_type="FLOAT32"
                        )
                    ),
                ),
            ),
            storage_configuration=bedrock.CfnKnowledgeBase.StorageConfigurationProperty(
                type="S3_VECTORS",
                s3_vectors_configuration=bedrock.CfnKnowledgeBase.S3VectorsConfigurationProperty(
                    index_arn=vector_index.attr_index_arn
                ),
            ),
        )
        kb.node.add_dependency(kb_role)
        data_source = bedrock.CfnDataSource(
            self,
            "PlaybookSource",
            knowledge_base_id=kb.attr_knowledge_base_id,
            name="playbook-docs",
            data_deletion_policy="DELETE",
            data_source_configuration=bedrock.CfnDataSource.DataSourceConfigurationProperty(
                type="S3",
                s3_configuration=bedrock.CfnDataSource.S3DataSourceConfigurationProperty(
                    bucket_arn=artifacts_bucket.bucket_arn, inclusion_prefixes=[KB_PREFIX]
                ),
            ),
            vector_ingestion_configuration=bedrock.CfnDataSource.VectorIngestionConfigurationProperty(
                chunking_configuration=bedrock.CfnDataSource.ChunkingConfigurationProperty(
                    chunking_strategy="FIXED_SIZE",
                    fixed_size_chunking_configuration=bedrock.CfnDataSource.FixedSizeChunkingConfigurationProperty(
                        max_tokens=300, overlap_percentage=15
                    ),
                )
            ),
        )
        data_source.node.add_dependency(docs)

        # ---- AgentCore Runtime: the Strands agent as a code zip -------------------------------
        runtime_role = iam.Role(
            self,
            "RuntimeRole",
            role_name=f"{name}-agentcore-runtime",
            assumed_by=iam.ServicePrincipal(
                "bedrock-agentcore.amazonaws.com",
                conditions={
                    "StringEquals": {"aws:SourceAccount": self.account},
                    "ArnLike": {"aws:SourceArn": f"arn:aws:bedrock-agentcore:{self.region}:{self.account}:*"},
                },
            ),
        )
        self._grant_runtime_basics(runtime_role)
        # Cross-region inference profiles (in.*, global.*) route to the model in several regions.
        runtime_role.add_to_policy(
            iam.PolicyStatement(
                actions=["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"],
                resources=[
                    "arn:aws:bedrock:*::foundation-model/anthropic.*",
                    f"arn:aws:bedrock:{self.region}:{self.account}:inference-profile/*",
                ],
            )
        )
        runtime_role.add_to_policy(iam.PolicyStatement(actions=["bedrock:Retrieve"], resources=[kb.attr_knowledge_base_arn]))
        runtime_role.add_to_policy(
            iam.PolicyStatement(
                actions=["athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults", "athena:StopQueryExecution"],
                resources=[f"arn:aws:athena:{self.region}:{self.account}:workgroup/{athena_workgroup}"],
            )
        )
        runtime_role.add_to_policy(
            iam.PolicyStatement(
                actions=["glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:GetPartitions"],
                resources=[
                    f"arn:aws:glue:{self.region}:{self.account}:catalog",
                    f"arn:aws:glue:{self.region}:{self.account}:database/{database_name}",
                    f"arn:aws:glue:{self.region}:{self.account}:table/{database_name}/*",
                ],
            )
        )
        lake_bucket.grant_read(runtime_role)
        athena_results_bucket.grant_read_write(runtime_role)
        alerts_table.grant_read_data(runtime_role)

        self.runtime_arn = None
        self.runtime_name = name.replace("-", "_") if runtime_zip else None
        if runtime_zip:
            code = assets.Asset(self, "RuntimeCode", path=runtime_zip)
            code.grant_read(runtime_role)
            runtime = agentcore.CfnRuntime(
                self,
                "Runtime",
                agent_runtime_name=self.runtime_name,
                description="Strands fraud analyst agent (Claude on Bedrock)",
                role_arn=runtime_role.role_arn,
                agent_runtime_artifact=agentcore.CfnRuntime.AgentRuntimeArtifactProperty(
                    code_configuration=agentcore.CfnRuntime.CodeConfigurationProperty(
                        code=agentcore.CfnRuntime.CodeProperty(
                            s3=agentcore.CfnRuntime.S3LocationProperty(bucket=code.s3_bucket_name, prefix=code.s3_object_key)
                        ),
                        entry_point=["main.py"],
                        runtime="PYTHON_3_12",
                    )
                ),
                network_configuration=agentcore.CfnRuntime.NetworkConfigurationProperty(network_mode="PUBLIC"),
                protocol_configuration="HTTP",
                lifecycle_configuration=agentcore.CfnRuntime.LifecycleConfigurationProperty(
                    idle_runtime_session_timeout=900, max_lifetime=3600
                ),
                environment_variables={
                    "MODEL_ID": model_id,
                    "KNOWLEDGE_BASE_ID": kb.attr_knowledge_base_id,
                    "ALERTS_TABLE": alerts_table.table_name,
                    "ATHENA_WORKGROUP": athena_workgroup,
                    "GLUE_DATABASE": database_name,
                },
            )
            runtime.node.add_dependency(runtime_role)
            # AgentCore creates this log group itself on first use, with no expiry.
            logs.LogRetention(
                self,
                "RuntimeLogRetention",
                log_group_name=Fn.join("", ["/aws/bedrock-agentcore/runtimes/", runtime.attr_agent_runtime_id, "-DEFAULT"]),
                retention=logs.RetentionDays.ONE_WEEK,
                removal_policy=RemovalPolicy.DESTROY,
            )
            self.runtime_arn = runtime.attr_agent_runtime_arn
            CfnOutput(self, "AgentRuntimeArn", value=runtime.attr_agent_runtime_arn)
        else:
            Annotations.of(self).add_warning("No agent code zip: run `python -m agent package` to deploy the AgentCore runtime.")

        CfnOutput(self, "KnowledgeBaseId", value=kb.attr_knowledge_base_id)
        CfnOutput(self, "DataSourceId", value=data_source.attr_data_source_id)
        CfnOutput(self, "ModelId", value=model_id)
        CfnOutput(self, "AlertsTableName", value=alerts_table.table_name)
        CfnOutput(self, "AthenaWorkgroup", value=athena_workgroup)
        CfnOutput(self, "GlueDatabase", value=database_name)
        CfnOutput(self, "RuntimeRoleArn", value=runtime_role.role_arn)

    def _grant_runtime_basics(self, role: iam.Role) -> None:
        """Logs, traces, metrics and workload identity tokens that every AgentCore runtime needs."""
        log_arn = f"arn:aws:logs:{self.region}:{self.account}:log-group:/aws/bedrock-agentcore/runtimes/*"
        role.add_to_policy(iam.PolicyStatement(actions=["logs:DescribeLogGroups"], resources=[f"arn:aws:logs:{self.region}:{self.account}:log-group:*"]))
        role.add_to_policy(
            iam.PolicyStatement(
                actions=["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogStreams"],
                resources=[log_arn, f"{log_arn}:log-stream:*"],
            )
        )
        role.add_to_policy(
            iam.PolicyStatement(
                actions=["xray:PutTraceSegments", "xray:PutTelemetryRecords", "xray:GetSamplingRules", "xray:GetSamplingTargets"],
                resources=["*"],
            )
        )
        role.add_to_policy(
            iam.PolicyStatement(
                actions=["cloudwatch:PutMetricData"],
                resources=["*"],
                conditions={"StringEquals": {"cloudwatch:namespace": "bedrock-agentcore"}},
            )
        )
        role.add_to_policy(
            iam.PolicyStatement(
                actions=["bedrock-agentcore:GetWorkloadAccessToken", "bedrock-agentcore:GetWorkloadAccessTokenForJWT", "bedrock-agentcore:GetWorkloadAccessTokenForUserId"],
                resources=[
                    f"arn:aws:bedrock-agentcore:{self.region}:{self.account}:workload-identity-directory/default",
                    f"arn:aws:bedrock-agentcore:{self.region}:{self.account}:workload-identity-directory/default/workload-identity/*",
                ],
            )
        )
