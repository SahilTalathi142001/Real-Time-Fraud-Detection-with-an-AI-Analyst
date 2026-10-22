"""Runtime settings for the AI analyst, read from environment variables.

AgentCore Runtime sets these from the CDK stack; locally, `python -m agent` fills them in
from the deployed CloudFormation outputs.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

DEFAULT_MODEL_ID = "in.anthropic.claude-haiku-4-5-20251001-v1:0"


@dataclass(frozen=True)
class AnalystConfig:
    model_id: str
    athena_workgroup: str
    glue_database: str
    alerts_table: str
    knowledge_base_id: str
    max_rows: int = 50

    @classmethod
    def from_env(cls) -> "AnalystConfig":
        return cls(
            model_id=os.environ.get("MODEL_ID", DEFAULT_MODEL_ID),
            athena_workgroup=os.environ.get("ATHENA_WORKGROUP", "fraud-analyst"),
            glue_database=os.environ.get("GLUE_DATABASE", "fraud_analyst_lake"),
            alerts_table=os.environ.get("ALERTS_TABLE", "fraud-analyst-alerts"),
            knowledge_base_id=os.environ.get("KNOWLEDGE_BASE_ID", ""),
            max_rows=int(os.environ.get("MAX_SQL_ROWS", "50")),
        )
