"""Talk to the deployed analyst (AgentCore Runtime) or run it in-process; shared by the CLI and the UI."""

from __future__ import annotations

import json
import os
import uuid
from functools import lru_cache

import boto3
from botocore.config import Config

ANALYST_STACK = "FraudAnalyst"


@lru_cache(maxsize=None)
def stack_outputs(stack: str) -> dict[str, str]:
    resp = boto3.client("cloudformation").describe_stacks(StackName=stack)
    return {o["OutputKey"]: o["OutputValue"] for o in resp["Stacks"][0].get("Outputs", [])}


def new_session_id() -> str:
    return f"ui-{uuid.uuid4().hex}"  # AgentCore wants >= 33 characters


def use_deployed_settings() -> None:
    """Point the in-process agent at the same resources the deployed runtime uses."""
    out = stack_outputs(ANALYST_STACK)
    os.environ.setdefault("MODEL_ID", out["ModelId"])
    os.environ.setdefault("KNOWLEDGE_BASE_ID", out["KnowledgeBaseId"])
    os.environ.setdefault("ALERTS_TABLE", out["AlertsTableName"])
    os.environ.setdefault("ATHENA_WORKGROUP", out["AthenaWorkgroup"])
    os.environ.setdefault("GLUE_DATABASE", out["GlueDatabase"])


class RemoteAnalyst:
    """Calls InvokeAgentRuntime, signed with your IAM credentials (SigV4)."""

    def __init__(self, runtime_arn: str | None = None) -> None:
        self.runtime_arn = runtime_arn or stack_outputs(ANALYST_STACK)["AgentRuntimeArn"]
        self.client = boto3.client("bedrock-agentcore", config=Config(read_timeout=300, retries={"max_attempts": 2}))

    def ask(self, question: str, session_id: str, reset: bool = False) -> dict:
        resp = self.client.invoke_agent_runtime(
            agentRuntimeArn=self.runtime_arn,
            qualifier="DEFAULT",
            runtimeSessionId=session_id,
            contentType="application/json",
            accept="application/json",
            payload=json.dumps({"prompt": question, "reset": reset}).encode(),
        )
        return json.loads(resp["response"].read())


class LocalAnalyst:
    """Runs the same Strands agent in this process (handy for debugging prompts and tools)."""

    def __init__(self) -> None:
        use_deployed_settings()
        from analyst.agent import ask, build_agent
        from analyst.config import AnalystConfig

        self._ask, self._build, self._config = ask, build_agent, AnalystConfig.from_env()
        self._agents: dict = {}

    def ask(self, question: str, session_id: str, reset: bool = False) -> dict:
        if reset or session_id not in self._agents:
            self._agents[session_id] = self._build(self._config)
        return {**self._ask(self._agents[session_id], question), "model_id": self._config.model_id}
