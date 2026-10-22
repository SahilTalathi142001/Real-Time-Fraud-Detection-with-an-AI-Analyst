"""Lakehouse CLI.

    python -m lakehouse glue              # run the silver/gold Glue job and wait
    python -m lakehouse views             # create/refresh the Athena views
    python -m lakehouse query "SELECT ..."
    python -m lakehouse demo              # run the saved sample queries
"""

from __future__ import annotations

import argparse
import time

import boto3

from lakehouse.athena import Athena
from lakehouse.views import VIEWS, create_view_sql


def outputs(session: boto3.Session, stack: str) -> dict[str, str]:
    desc = session.client("cloudformation").describe_stacks(StackName=stack)["Stacks"][0]
    return {o["OutputKey"]: o["OutputValue"] for o in desc.get("Outputs", [])}


def print_table(result, limit: int = 30) -> None:
    if not result.rows:
        print("(no rows)")
        return
    widths = {c: max(len(c), *(len(str(r.get(c))) for r in result.rows[:limit])) for c in result.columns}
    print("  ".join(c.ljust(widths[c]) for c in result.columns))
    for r in result.rows[:limit]:
        print("  ".join(str(r.get(c)).ljust(widths[c]) for c in result.columns))
    print(f"-- {len(result.rows)} rows, {result.bytes_scanned / 1e6:.2f} MB scanned, {result.elapsed_ms} ms")


def cmd_glue(session: boto3.Session, job: str) -> None:
    glue = session.client("glue")
    run_id = glue.start_job_run(JobName=job)["JobRunId"]
    print(f"Started Glue job {job} run {run_id}")
    started = time.time()
    while True:
        run = glue.get_job_run(JobName=job, RunId=run_id)["JobRun"]
        state = run["JobRunState"]
        print(f"  [{int(time.time() - started):>4}s] {state}")
        if state in ("SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT", "ERROR"):
            break
        time.sleep(15)
    if state != "SUCCEEDED":
        raise SystemExit(f"Glue job {state}: {run.get('ErrorMessage')}")
    print(f"Glue job done in {run.get('ExecutionTime')}s, {run.get('DPUSeconds', 0) / 3600:.3f} DPU-hours")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m lakehouse", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("glue")
    sub.add_parser("views")
    q = sub.add_parser("query")
    q.add_argument("sql")
    sub.add_parser("demo")
    args = parser.parse_args(argv)

    session = boto3.Session()
    out = outputs(session, "FraudLakehouse")
    database = outputs(session, "FraudFoundation")["GlueDatabaseName"]
    athena = Athena(out["AthenaWorkgroup"], database, session)

    if args.cmd == "glue":
        cmd_glue(session, out["GlueJobName"])
    elif args.cmd == "views":
        for name in VIEWS:
            athena.run(create_view_sql(name))
            print(f"created view {name}")
    elif args.cmd == "query":
        print_table(athena.run(args.sql))
    elif args.cmd == "demo":
        client = session.client("athena")
        ids = client.list_named_queries(WorkGroup=out["AthenaWorkgroup"])["NamedQueryIds"]
        for nq in client.batch_get_named_query(NamedQueryIds=ids)["NamedQueries"]:
            print(f"\n== {nq['Name']}: {nq.get('Description', '')}")
            print_table(athena.run(nq["QueryString"]), limit=10)


if __name__ == "__main__":
    main()
