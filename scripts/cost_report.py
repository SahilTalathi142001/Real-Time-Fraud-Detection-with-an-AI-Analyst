"""Cost Explorer breakdown for this project, as a markdown table.

    python scripts/cost_report.py [--start 2026-10-01] [--end 2026-10-31]

Costs are shown before credits (what the usage is worth) and the Free plan credit balance is
printed at the end. Cost Explorer data lags by up to 24 hours.

Anything in a region other than this project's (e.g. an EC2 instance that existed before the
project) is listed separately under "Not this project".
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, timedelta

import boto3

GLOBAL_REGIONS = {"global", "NoRegion", ""}


def costs_by_service_region(ce, start: str, end: str) -> dict[tuple[str, str], float]:
    out: dict[tuple[str, str], float] = defaultdict(float)
    token = None
    while True:
        kwargs = dict(
            TimePeriod={"Start": start, "End": end}, Granularity="MONTHLY", Metrics=["UnblendedCost"],
            GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}, {"Type": "DIMENSION", "Key": "REGION"}],
            Filter={"Not": {"Dimensions": {"Key": "RECORD_TYPE", "Values": ["Credit", "Refund"]}}},
        )
        if token:
            kwargs["NextPageToken"] = token
        resp = ce.get_cost_and_usage(**kwargs)
        for period in resp["ResultsByTime"]:
            for g in period["Groups"]:
                service, region = g["Keys"]
                out[(service, region)] += float(g["Metrics"]["UnblendedCost"]["Amount"])
        token = resp.get("NextPageToken")
        if not token:
            return out


def daily_totals(ce, start: str, end: str) -> list[tuple[str, float]]:
    resp = ce.get_cost_and_usage(
        TimePeriod={"Start": start, "End": end}, Granularity="DAILY", Metrics=["UnblendedCost"],
        Filter={"Not": {"Dimensions": {"Key": "RECORD_TYPE", "Values": ["Credit", "Refund"]}}},
    )
    return [(p["TimePeriod"]["Start"], float(p["Total"]["UnblendedCost"]["Amount"])) for p in resp["ResultsByTime"]]


def main() -> None:
    today = date.today()
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default=today.replace(day=1).isoformat())
    parser.add_argument("--end", default=(today + timedelta(days=1)).isoformat())
    args = parser.parse_args()

    session = boto3.Session()
    project_region = session.region_name
    ce = session.client("ce", region_name="us-east-1")
    costs = costs_by_service_region(ce, args.start, args.end)

    ours = {k: v for k, v in costs.items() if k[1] in GLOBAL_REGIONS | {project_region}}
    other = {k: v for k, v in costs.items() if k not in ours}

    def table(rows: dict[tuple[str, str], float], title: str) -> float:
        total = sum(rows.values())
        print(f"\n### {title}: ${total:,.2f}\n")
        print("| Service | Region | Cost (USD, before credits) |\n| --- | --- | ---: |")
        for (service, region), amount in sorted(rows.items(), key=lambda kv: -kv[1]):
            if amount >= 0.005:
                print(f"| {service} | {region or 'global'} | ${amount:,.2f} |")
        small = sum(a for a in rows.values() if a < 0.005)
        if small:
            print(f"| (other services under $0.01 each) | | ${small:,.2f} |")
        return total

    print(f"## AWS costs {args.start} to {args.end} (Cost Explorer lags up to 24 h)")
    table(ours, f"This project ({project_region} + global)")
    if other:
        table(other, "Not this project (other regions)")
    print("\n### Daily total, all services\n")
    print("| Day | Cost |\n| --- | ---: |")
    for day, amount in daily_totals(ce, args.start, args.end):
        if amount >= 0.005:
            print(f"| {day} | ${amount:,.2f} |")
    try:
        state = session.client("freetier", region_name="us-east-1").get_account_plan_state()
        credits = state["accountPlanRemainingCredits"]
        print(f"\nFree plan: {state['accountPlanType']}, credits remaining ${credits['amount']:,.2f}, "
              f"expires {state['accountPlanExpirationDate']:%Y-%m-%d}")
    except Exception as e:  # noqa: BLE001 - paid accounts have no plan state
        print(f"\n(no Free plan state: {type(e).__name__})")


if __name__ == "__main__":
    main()
