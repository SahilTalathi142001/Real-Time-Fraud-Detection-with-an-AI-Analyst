"""Typed access to the CDK context values in cdk.json.

Override any value at deploy time, e.g.:
    cdk deploy -c alertEmail=you@example.com
"""

from dataclasses import dataclass

from constructs import Node


@dataclass(frozen=True)
class ProjectConfig:
    project_name: str
    alert_email: str
    budget_limit_usd: float
    budget_alert_thresholds_usd: tuple[float, ...]

    @property
    def glue_database(self) -> str:
        return self.project_name.replace("-", "_") + "_lake"

    @staticmethod
    def from_node(node: Node) -> "ProjectConfig":
        thresholds = node.try_get_context("budgetAlertThresholdsUsd") or [40, 80, 110]
        if isinstance(thresholds, str):
            thresholds = [t for t in thresholds.split(",") if t.strip()]
        return ProjectConfig(
            project_name=node.try_get_context("projectName") or "fraud-analyst",
            alert_email=node.try_get_context("alertEmail") or "",
            budget_limit_usd=float(node.try_get_context("budgetLimitUsd") or 134),
            budget_alert_thresholds_usd=tuple(float(t) for t in thresholds),
        )
