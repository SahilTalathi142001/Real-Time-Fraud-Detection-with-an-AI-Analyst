import importlib
import zipfile

import pytest

from agent import package
from analyst.agent import tool_trace


def test_tool_trace_pairs_calls_with_results():
    messages = [
        {"role": "assistant", "content": [{"text": "Let me check."}, {"toolUse": {"toolUseId": "a", "name": "run_lakehouse_sql", "input": {"sql": "SELECT 1"}}}]},
        {"role": "user", "content": [{"toolResult": {"toolUseId": "a", "status": "error", "content": []}}]},
        {"role": "assistant", "content": [{"toolUse": {"toolUseId": "b", "name": "summarize_todays_alerts", "input": {}}}]},
        {"role": "user", "content": [{"toolResult": {"toolUseId": "b", "status": "success", "content": []}}]},
    ]
    assert tool_trace(messages) == [
        {"name": "run_lakehouse_sql", "input": {"sql": "SELECT 1"}, "status": "error"},
        {"name": "summarize_todays_alerts", "input": {}, "status": "success"},
    ]


@pytest.fixture
def runtime(monkeypatch):
    module = importlib.reload(importlib.import_module("analyst.runtime"))
    built = []

    def fake_build(config, backends):
        built.append(object())
        return built[-1]

    monkeypatch.setattr(module, "Backends", lambda config: "backends")
    monkeypatch.setattr(module, "build_agent", fake_build)
    monkeypatch.setattr(module, "ask", lambda agent, q: {"answer": f"answer to {q}", "tool_calls": [], "usage": {"input": 1, "output": 2}, "latency_ms": 5, "agent": agent})
    return module, built


def test_runtime_keeps_one_agent_per_session(runtime):
    module, built = runtime
    ctx = type("Ctx", (), {"session_id": "s" * 40})()
    first = module.invoke({"prompt": "How many alerts?"}, ctx)
    second = module.invoke({"prompt": "And yesterday?"}, ctx)
    assert first["answer"] == "answer to How many alerts?" and first["model_id"]
    assert first["agent"] is second["agent"] and len(built) == 1
    module.invoke({"prompt": "Start over", "reset": True}, ctx)
    assert len(built) == 2


def test_runtime_rejects_empty_prompt_and_reports_errors(runtime, monkeypatch):
    module, _ = runtime
    assert "error" in module.invoke({}, None)

    def boom(agent, q):
        raise RuntimeError("ThrottlingException")

    monkeypatch.setattr(module, "ask", boom)
    assert module.invoke({"prompt": "hi"}, None)["error"] == "RuntimeError: ThrottlingException"


def test_runtime_zip_is_reproducible_and_has_entrypoint(tmp_path, monkeypatch):
    monkeypatch.setattr(package, "BUILD", tmp_path)
    staging = tmp_path / "analyst_pkg"
    (staging / "somelib").mkdir(parents=True)
    (staging / "somelib" / "__init__.py").write_text("x = 1\n")
    (staging / "somelib" / "__pycache__").mkdir()
    (staging / "somelib" / "__pycache__" / "junk.pyc").write_bytes(b"\0")
    a = package.build(tmp_path / "a.zip", skip_deps=True)
    b = package.build(tmp_path / "b.zip", skip_deps=True)
    assert a.read_bytes() == b.read_bytes()
    names = zipfile.ZipFile(a).namelist()
    assert "main.py" in names and "analyst/runtime.py" in names and "common/athena.py" in names
    assert "somelib/__init__.py" in names and not any("__pycache__" in n for n in names)
