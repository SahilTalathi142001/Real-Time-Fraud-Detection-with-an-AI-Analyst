from aws_cdk.assertions import Match


def _statements(template, role_logical_prefix: str) -> list[dict]:
    policies = template("FraudAnalyst").find_resources("AWS::IAM::Policy")
    out = []
    for p in policies.values():
        roles = p["Properties"]["Roles"]
        if any(r.get("Ref", "").startswith(role_logical_prefix) for r in roles):
            out += p["Properties"]["PolicyDocument"]["Statement"]
    return out


def _actions(statements) -> set[str]:
    acts = set()
    for s in statements:
        acts |= set(s["Action"] if isinstance(s["Action"], list) else [s["Action"]])
    return acts


def test_vector_index_matches_titan_v2(template):
    t = template("FraudAnalyst")
    t.has_resource_properties("AWS::S3Vectors::Index", {
        "Dimension": 1024, "DistanceMetric": "cosine", "DataType": "float32",
        "MetadataConfiguration": {"NonFilterableMetadataKeys": ["AMAZON_BEDROCK_TEXT", "AMAZON_BEDROCK_METADATA"]},
    })
    t.has_resource_properties("AWS::Bedrock::KnowledgeBase", {
        "StorageConfiguration": {"Type": "S3_VECTORS", "S3VectorsConfiguration": {"IndexArn": Match.any_value()}},
        "KnowledgeBaseConfiguration": {"Type": "VECTOR", "VectorKnowledgeBaseConfiguration": Match.object_like({
            "EmbeddingModelArn": Match.string_like_regexp("amazon.titan-embed-text-v2:0$"),
        })},
    })
    t.has_resource_properties("AWS::Bedrock::DataSource", {
        "DataSourceConfiguration": {"Type": "S3", "S3Configuration": Match.object_like({"InclusionPrefixes": ["kb/"]})},
    })


def test_no_always_on_vector_store(template):
    t = template("FraudAnalyst")
    for always_on in ["AWS::OpenSearchServerless::Collection", "AWS::RDS::DBCluster", "AWS::EC2::Instance"]:
        t.resource_count_is(always_on, 0)


def test_agentcore_runtime_runs_code_zip(template):
    template("FraudAnalyst").has_resource_properties("AWS::BedrockAgentCore::Runtime", {
        "AgentRuntimeName": "fraud_analyst",
        "AgentRuntimeArtifact": {"CodeConfiguration": {
            "Code": {"S3": {"Bucket": Match.any_value(), "Prefix": Match.string_like_regexp(r"\.zip$")}},
            "EntryPoint": ["main.py"], "Runtime": "PYTHON_3_12",
        }},
        "NetworkConfiguration": {"NetworkMode": "PUBLIC"},
        "ProtocolConfiguration": "HTTP",
        "EnvironmentVariables": Match.object_like({
            "MODEL_ID": "in.anthropic.claude-haiku-4-5-20251001-v1:0",
            "ATHENA_WORKGROUP": "fraud-analyst",
            "GLUE_DATABASE": "fraud_analyst_lake",
            "KNOWLEDGE_BASE_ID": Match.any_value(),
        }),
    })


def test_runtime_role_is_read_only_on_data(template):
    actions = _actions(_statements(template, "RuntimeRole"))
    assert {"bedrock:InvokeModel", "bedrock:Retrieve", "athena:StartQueryExecution", "dynamodb:Query", "dynamodb:GetItem"} <= actions
    assert not {a for a in actions if a.startswith("dynamodb:") and any(w in a for w in ("Put", "Delete", "Update", "BatchWrite"))}
    assert not {a for a in actions if a.startswith("glue:") and any(w in a for w in ("Create", "Delete", "Update"))}


def test_knowledge_base_role_reads_only_the_playbook_prefix(template):
    statements = _statements(template, "KnowledgeBaseRole")
    get_object = [s for s in statements if s["Action"] == "s3:GetObject"]
    assert len(get_object) == 1
    resource = get_object[0]["Resource"]["Fn::Join"][1]
    assert resource[-1] == "/kb/*"
