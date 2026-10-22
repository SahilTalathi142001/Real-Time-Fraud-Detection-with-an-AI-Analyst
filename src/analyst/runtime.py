"""AgentCore Runtime entrypoint.

AgentCore runs `main.py` (see agent/main.py), which starts this app: an HTTP server on port 8080 with
`/ping` (health) and `/invocations` (one question). Each runtimeSessionId gets its own microVM, and we
keep that session's Agent in memory so follow-up questions ("and yesterday?") have context.
"""

from __future__ import annotations

import json
import logging
from collections import OrderedDict
from threading import Lock

from bedrock_agentcore.runtime import BedrockAgentCoreApp

from analyst.agent import ask, build_agent
from analyst.config import AnalystConfig
from analyst.tools import Backends

MAX_SESSIONS = 20

app = BedrockAgentCoreApp()
log = logging.getLogger("analyst")
logging.basicConfig(level=logging.INFO, format="%(message)s")

_config = AnalystConfig.from_env()
_backends: Backends | None = None
_agents: OrderedDict = OrderedDict()
_lock = Lock()


def agent_for(session_id: str, reset: bool = False):
    global _backends
    with _lock:
        if _backends is None:
            _backends = Backends(_config)
        if reset:
            _agents.pop(session_id, None)
        if session_id not in _agents:
            _agents[session_id] = build_agent(_config, _backends)
            while len(_agents) > MAX_SESSIONS:
                _agents.popitem(last=False)
        _agents.move_to_end(session_id)
        return _agents[session_id]


@app.entrypoint
def invoke(payload, context):
    payload = payload or {}
    question = str(payload.get("prompt", "")).strip()
    if not question:
        return {"error": 'Send a JSON body like {"prompt": "How many alerts today?"}'}
    session_id = getattr(context, "session_id", None) or "local"
    try:
        result = ask(agent_for(session_id, reset=bool(payload.get("reset"))), question)
    except Exception as e:  # noqa: BLE001 - return a readable error to the caller instead of a bare 500
        log.exception("analyst failed")
        return {"error": f"{type(e).__name__}: {str(e)[:500]}"}
    log.info(json.dumps({
        "msg": "answered", "session": session_id[-8:], "latency_ms": result["latency_ms"],
        "tools": [c["name"] for c in result["tool_calls"]], **result["usage"],
    }))
    return {**result, "model_id": _config.model_id}
