"""Small Athena client: run a query in the project workgroup and return rows as dicts.

Shared by the lake loader Lambda, the lakehouse CLI, and the AI analyst's SQL tool.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass

import boto3

READ_ONLY = re.compile(r"^\s*(--[^\n]*\n\s*)*(select|with|show|describe|explain)\b", re.IGNORECASE)
FORBIDDEN = re.compile(r"\b(insert|update|delete|merge|drop|create|alter|truncate|grant|revoke|unload|msck|vacuum|optimize)\b", re.IGNORECASE)


@dataclass
class QueryResult:
    columns: list[str]
    rows: list[dict]
    query_id: str
    bytes_scanned: int
    elapsed_ms: int
    truncated: bool


def is_read_only(sql: str) -> bool:
    stripped = re.sub(r"'(?:[^']|'')*'", "''", sql)  # ignore keywords inside string literals
    return bool(READ_ONLY.match(stripped)) and not FORBIDDEN.search(stripped) and ";" not in stripped.strip().rstrip(";")


class Athena:
    def __init__(self, workgroup: str, database: str, session: boto3.Session | None = None) -> None:
        self.workgroup = workgroup
        self.database = database
        self.client = (session or boto3.Session()).client("athena")

    def run(self, sql: str, max_rows: int = 200, timeout_s: float = 120) -> QueryResult:
        qid = self.client.start_query_execution(
            QueryString=sql, WorkGroup=self.workgroup, QueryExecutionContext={"Database": self.database}
        )["QueryExecutionId"]
        started, delay = time.time(), 0.3
        while True:
            q = self.client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
            state = q["Status"]["State"]
            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            if time.time() - started > timeout_s:
                self.client.stop_query_execution(QueryExecutionId=qid)
                raise TimeoutError(f"Athena query {qid} took longer than {timeout_s}s")
            time.sleep(delay)
            delay = min(delay * 1.5, 2.0)
        if state != "SUCCEEDED":
            raise RuntimeError(f"Athena query {state}: {q['Status'].get('StateChangeReason', '')}")

        stats = q.get("Statistics", {})
        columns: list[str] = []
        rows: list[dict] = []
        truncated = False
        paginator = self.client.get_paginator("get_query_results")
        for page in paginator.paginate(QueryExecutionId=qid, PaginationConfig={"PageSize": 1000}):
            if not columns:
                columns = [c["Name"] for c in page["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
            for raw in page["ResultSet"]["Rows"]:
                values = [d.get("VarCharValue") for d in raw["Data"]]
                if not rows and values == columns and not truncated:
                    continue  # header row on the first page
                if len(rows) >= max_rows:
                    truncated = True
                    break
                rows.append(dict(zip(columns, values)))
            if truncated:
                break
        return QueryResult(
            columns=columns,
            rows=rows,
            query_id=qid,
            bytes_scanned=int(stats.get("DataScannedInBytes", 0)),
            elapsed_ms=int(stats.get("TotalExecutionTimeInMillis", 0)),
            truncated=truncated,
        )
