"""AI analyst CLI.

    python -m agent package                 build build/analyst_runtime.zip (then `cdk deploy FraudAnalyst`)
    python -m agent ingest-kb               (re)index the playbook in kb/ into the Bedrock Knowledge Base
    python -m agent ask "question"          ask the deployed AgentCore runtime
    python -m agent ask --local "question"  run the same agent in this process
    python -m agent chat [--local]          interactive conversation (keeps context between questions)
    python -m agent eval [--local]          check answers against ground truth from DynamoDB/Athena
"""

from __future__ import annotations

import argparse
import json
import time

import boto3

from agent.client import ANALYST_STACK, LocalAnalyst, RemoteAnalyst, new_session_id, stack_outputs


def ingest_kb() -> None:
    out = stack_outputs(ANALYST_STACK)
    ba = boto3.client("bedrock-agent")
    kb_id, ds_id = out["KnowledgeBaseId"], out["DataSourceId"]
    job = ba.start_ingestion_job(knowledgeBaseId=kb_id, dataSourceId=ds_id)["ingestionJob"]
    print(f"ingestion job {job['ingestionJobId']} started")
    while True:
        job = ba.get_ingestion_job(knowledgeBaseId=kb_id, dataSourceId=ds_id, ingestionJobId=job["ingestionJobId"])["ingestionJob"]
        if job["status"] in ("COMPLETE", "FAILED", "STOPPED"):
            break
        time.sleep(5)
    print(job["status"], json.dumps(job.get("statistics", {})))
    if job.get("failureReasons"):
        raise SystemExit("\n".join(job["failureReasons"]))


def print_answer(result: dict, verbose: bool) -> None:
    if "error" in result:
        print("ERROR:", result["error"])
        return
    print(result["answer"])
    if verbose:
        print("\n--- tools used ---")
        for call in result["tool_calls"]:
            print(f"[{call['status']}] {call['name']}: {json.dumps(call['input'])[:400]}")
        print(f"--- {result['latency_ms']} ms, tokens {result['usage']} ---")


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m agent")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("package")
    p.add_argument("--skip-deps", action="store_true", help="reuse build/analyst_pkg, only refresh our code")
    sub.add_parser("ingest-kb")
    for name in ("ask", "chat", "eval"):
        p = sub.add_parser(name)
        p.add_argument("--local", action="store_true")
        p.add_argument("--quiet", action="store_true", help="hide the tool trace")
        if name == "ask":
            p.add_argument("question")
    args = parser.parse_args()

    if args.cmd == "package":
        from agent.package import build

        build(skip_deps=args.skip_deps)
    elif args.cmd == "ingest-kb":
        ingest_kb()
    else:
        analyst = LocalAnalyst() if args.local else RemoteAnalyst()
        session = new_session_id()
        if args.cmd == "eval":
            from agent.client import use_deployed_settings
            from agent.evals import run

            use_deployed_settings()
            raise SystemExit(1 if run(analyst, new_session_id) else 0)
        if args.cmd == "ask":
            print_answer(analyst.ask(args.question, session), not args.quiet)
            return
        print("Ask about fraud alerts, payers, trends. Ctrl-D to quit.")
        while True:
            try:
                question = input("\nyou> ").strip()
            except EOFError:
                break
            if question:
                print_answer(analyst.ask(question, session), not args.quiet)


if __name__ == "__main__":
    main()
