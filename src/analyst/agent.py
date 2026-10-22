"""Build the Strands agent and run one question through it."""

from __future__ import annotations

import time

from botocore.config import Config
from strands import Agent
from strands.agent.conversation_manager import SlidingWindowConversationManager
from strands.models import BedrockModel

from analyst.config import AnalystConfig
from analyst.prompt import SYSTEM_PROMPT
from analyst.tools import Backends, make_tools


def build_agent(config: AnalystConfig, backends: Backends | None = None, model=None) -> Agent:
    backends = backends or Backends(config)
    model = model or BedrockModel(
        model_id=config.model_id,
        temperature=0.1,
        max_tokens=2048,
        boto_client_config=Config(read_timeout=120, retries={"max_attempts": 4, "mode": "adaptive"}),
    )
    return Agent(
        model=model,
        tools=make_tools(backends),
        system_prompt=SYSTEM_PROMPT,
        callback_handler=None,
        conversation_manager=SlidingWindowConversationManager(window_size=30),
    )


def tool_trace(messages: list[dict]) -> list[dict]:
    """Pair every toolUse in the new messages with its toolResult status, for the UI's "how I got this" panel."""
    calls, status = [], {}
    for message in messages:
        for block in message.get("content", []):
            if "toolUse" in block:
                calls.append({"id": block["toolUse"]["toolUseId"], "name": block["toolUse"]["name"], "input": block["toolUse"]["input"]})
            elif "toolResult" in block:
                status[block["toolResult"]["toolUseId"]] = block["toolResult"].get("status", "success")
    return [{"name": c["name"], "input": c["input"], "status": status.get(c["id"], "unknown")} for c in calls]


def ask(agent: Agent, question: str) -> dict:
    started = time.time()
    first_new = len(agent.messages)
    before = dict(agent.event_loop_metrics.accumulated_usage)  # metrics accumulate over the agent's lifetime
    result = agent(question)
    after = result.metrics.accumulated_usage
    return {
        "answer": str(result).strip(),
        "tool_calls": tool_trace(agent.messages[first_new:]),
        "usage": {k: after.get(f"{k}Tokens", 0) - before.get(f"{k}Tokens", 0) for k in ("input", "output")},
        "latency_ms": int((time.time() - started) * 1000),
    }
