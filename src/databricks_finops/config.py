from __future__ import annotations

import argparse
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

ISSUE_TYPES = {
    "EXPENSIVE_WORKLOAD",
    "HEALTHY_EXPENSIVE",
    "HIGH_COST_LOW_UTILIZATION",
    "LOW_UTILIZATION",
    "MEMORY_BOUND",
    "HIGH_FAILURE_COST",
    "HIGH_FAILURE_RATE",
    "RETRY_HEAVY",
    "MISSING_TAGS",
    "UNKNOWN_ATTRIBUTION",
    "SHARED_CLUSTER_ATTRIBUTION",
    "INSUFFICIENT_DATA",
    "REVIEW_REQUIRED",
}

SUGGESTED_ACTIONS = {
    "REVIEW_WORKER_COUNT",
    "REVIEW_NODE_TYPE",
    "INVESTIGATE_MEMORY_PRESSURE",
    "INVESTIGATE_FAILURES",
    "INVESTIGATE_RETRIES",
    "IMPROVE_TAGGING",
    "REVIEW_ALL_PURPOSE_USAGE",
    "LOOKS_HEALTHY",
    "REVIEW_REQUIRED",
    "INSUFFICIENT_DATA",
}

HEALTH_SEVERITIES = {"INFO", "LOW", "MEDIUM", "HIGH", "CRITICAL"}

PRICE_SOURCES = {"LIST_PRICES", "FALLBACK_DBU_PRICE", "MIXED", "UNKNOWN"}

CONFIDENCE_VALUES = {"HIGH", "MEDIUM", "LOW"}

DEFAULT_THRESHOLDS = {
    "low_cpu_pct": 20.0,
    "very_low_cpu_pct": 10.0,
    "low_memory_pct": 40.0,
    "high_memory_pct": 80.0,
    "high_cost_low_utilization_min_cost": 10.0,
    "expensive_monthly_cost": 100.0,
    "high_failure_rate_pct": 20.0,
    "high_failed_cost": 10.0,
    "retry_heavy_count": 5.0,
}

DEFAULT_SCORING_WEIGHTS = {
    "cost": 0.45,
    "waste": 0.25,
    "reliability": 0.15,
    "tagging": 0.10,
    "frequency": 0.05,
}

DEFAULT_PRIORITY = {
    "high_priority_threshold": 80.0,
    "medium_priority_threshold": 50.0,
}

DEFAULT_REQUIRED_TAGS = ("project", "team", "owner", "environment", "cost_center")
DEFAULT_CRITICAL_TAGS = ("owner", "team", "cost_center")
DEFAULT_TAG_ALIASES = {
    "project": ("project", "Project", "PROJECT", "app", "application"),
    "team": ("team", "Team", "TEAM", "department", "group"),
    "owner": ("owner", "Owner", "OWNER", "email", "contact"),
    "environment": ("environment", "Environment", "env", "ENV"),
    "cost_center": (
        "cost_center",
        "CostCenter",
        "costCenter",
        "cost-centre",
        "cost_centre",
    ),
}

TABLE_CONTRACTS = {
    "daily_cost": [
        "usage_date",
        "workspace_id",
        "billing_origin_product",
        "sku_name",
        "cloud",
        "usage_unit",
        "workload_type",
        "workload_id",
        "job_id",
        "job_name",
        "cluster_id",
        "warehouse_id",
        "pipeline_id",
        "notebook_id",
        "run_as",
        "project",
        "team",
        "owner",
        "environment",
        "cost_center",
        "dbus",
        "estimated_cost",
        "currency_code",
        "display_currency",
        "price_source",
        "attribution_quality",
        "attribution_notes",
        "created_at",
        "run_id",
    ],
    "workload_cost_summary": [
        "workspace_id",
        "workload_type",
        "workload_id",
        "job_id",
        "job_name",
        "cluster_id",
        "warehouse_id",
        "pipeline_id",
        "run_as",
        "project",
        "team",
        "owner",
        "environment",
        "cost_center",
        "total_dbus",
        "estimated_cost",
        "active_days",
        "first_seen_date",
        "last_seen_date",
        "avg_daily_cost",
        "cost_rank",
        "attribution_quality",
        "attribution_notes",
        "created_at",
        "run_id",
    ],
    "compute_utilization_summary": [
        "workspace_id",
        "cluster_id",
        "job_id",
        "job_name",
        "avg_cpu_pct",
        "peak_cpu_pct",
        "avg_memory_pct",
        "peak_memory_pct",
        "avg_network_sent_bytes",
        "avg_network_received_bytes",
        "dbus",
        "estimated_cost",
        "dbus_per_cpu_pct",
        "utilization_category",
        "utilization_grain",
        "sizing_signal",
        "confidence",
        "created_at",
        "run_id",
    ],
    "job_reliability_summary": [
        "workspace_id",
        "job_id",
        "job_name",
        "run_count",
        "success_count",
        "failed_count",
        "cancelled_count",
        "skipped_count",
        "retry_count",
        "failure_rate_pct",
        "avg_duration_minutes",
        "total_duration_minutes",
        "estimated_failed_dbus",
        "estimated_failed_cost",
        "last_run_start_time",
        "last_run_result_state",
        "reliability_category",
        "confidence",
        "created_at",
        "run_id",
    ],
    "tagging_quality_summary": [
        "usage_date",
        "workspace_id",
        "workload_type",
        "workload_id",
        "run_as",
        "dbus",
        "estimated_cost",
        "project",
        "team",
        "owner",
        "environment",
        "cost_center",
        "missing_project_tag",
        "missing_team_tag",
        "missing_owner_tag",
        "missing_environment_tag",
        "missing_cost_center_tag",
        "missing_required_tag_count",
        "missing_critical_tag_count",
        "tagging_quality",
        "created_at",
        "run_id",
    ],
    "optimization_candidates": [
        "priority_rank",
        "workspace_id",
        "workload_type",
        "workload_id",
        "job_id",
        "job_name",
        "owner",
        "team",
        "project",
        "environment",
        "monthly_dbus",
        "estimated_monthly_cost",
        "avg_cpu_pct",
        "avg_memory_pct",
        "failure_rate_pct",
        "retry_count",
        "issue_type",
        "suggested_action",
        "waste_score",
        "frequency_score",
        "reliability_score",
        "tagging_score",
        "priority_score",
        "confidence",
        "evidence",
        "created_at",
        "run_id",
    ],
}

OUTPUT_TABLES = [
    *TABLE_CONTRACTS.keys(),
    "accelerator_summary",
    "accelerator_health",
    "accelerator_run_log",
]

OUTPUT_VIEWS = [
    "vw_executive_summary",
    "vw_cost_trend_daily",
    "vw_most_expensive_workloads",
    "vw_optimization_backlog",
    "vw_tagging_quality",
    "vw_architecture_review_candidates",
    "vw_system_table_coverage",
    "vw_attribution_quality",
    "vw_compute_sizing_signals",
    "vw_failed_jobs",
    "vw_wasteful_workloads",
]


@dataclass(frozen=True)
class ThresholdConfig:
    low_cpu_pct: float = DEFAULT_THRESHOLDS["low_cpu_pct"]
    very_low_cpu_pct: float = DEFAULT_THRESHOLDS["very_low_cpu_pct"]
    low_memory_pct: float = DEFAULT_THRESHOLDS["low_memory_pct"]
    high_memory_pct: float = DEFAULT_THRESHOLDS["high_memory_pct"]
    high_cost_low_utilization_min_cost: float = DEFAULT_THRESHOLDS[
        "high_cost_low_utilization_min_cost"
    ]
    expensive_monthly_cost: float = DEFAULT_THRESHOLDS["expensive_monthly_cost"]
    high_failure_rate_pct: float = DEFAULT_THRESHOLDS["high_failure_rate_pct"]
    high_failed_cost: float = DEFAULT_THRESHOLDS["high_failed_cost"]
    retry_heavy_count: float = DEFAULT_THRESHOLDS["retry_heavy_count"]


@dataclass(frozen=True)
class PriorityConfig:
    high_priority_threshold: float = DEFAULT_PRIORITY["high_priority_threshold"]
    medium_priority_threshold: float = DEFAULT_PRIORITY["medium_priority_threshold"]


@dataclass(frozen=True)
class ScoringConfig:
    cost: float = DEFAULT_SCORING_WEIGHTS["cost"]
    waste: float = DEFAULT_SCORING_WEIGHTS["waste"]
    reliability: float = DEFAULT_SCORING_WEIGHTS["reliability"]
    tagging: float = DEFAULT_SCORING_WEIGHTS["tagging"]
    frequency: float = DEFAULT_SCORING_WEIGHTS["frequency"]

    def as_dict(self) -> dict[str, float]:
        return {
            "cost": self.cost,
            "waste": self.waste,
            "reliability": self.reliability,
            "tagging": self.tagging,
            "frequency": self.frequency,
        }


@dataclass(frozen=True)
class TaggingRules:
    required_tags: tuple[str, ...] = DEFAULT_REQUIRED_TAGS
    critical_tags: tuple[str, ...] = DEFAULT_CRITICAL_TAGS
    tag_aliases: dict[str, tuple[str, ...]] = field(
        default_factory=lambda: dict(DEFAULT_TAG_ALIASES)
    )


@dataclass(frozen=True)
class AppConfig:
    catalog: str
    schema: str
    lookback_days: int
    currency_code: str
    display_currency: str
    fallback_dbu_price: float
    bundle_root: str
    thresholds: ThresholdConfig = field(default_factory=ThresholdConfig)
    scoring_weights: ScoringConfig = field(default_factory=ScoringConfig)
    priority: PriorityConfig = field(default_factory=PriorityConfig)
    tagging_rules: TaggingRules = field(default_factory=TaggingRules)
    config_warnings: tuple[str, ...] = ()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Databricks FinOps Accelerator using real Databricks System Tables."
    )
    parser.add_argument("--catalog", default="finops")
    parser.add_argument("--schema", default="accelerator")
    parser.add_argument("--lookback-days", type=int, default=30)
    parser.add_argument("--currency-code", default="USD")
    parser.add_argument("--fallback-dbu-price", type=float, default=0.55)
    parser.add_argument("--bundle-root", default=".")
    return parser.parse_args(argv)


def validate_identifier(value: str, label: str) -> str:
    if not VALID_IDENTIFIER.match(value):
        raise ValueError(
            f"Invalid {label}: {value}. Use a simple Unity Catalog identifier such as finops."
        )
    return value


def validate_currency(value: str, label: str) -> str:
    normalized = value.upper()
    if not re.match(r"^[A-Z]{3}$", normalized):
        raise ValueError(f"Invalid {label}: {value}. Use a three-letter code such as USD.")
    return normalized


def _parse_scalar(value: str) -> Any:
    value = value.strip().strip('"').strip("'")
    if value.lower() in {"true", "false"}:
        return value.lower() == "true"
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value


def _parse_simple_yaml(text: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    current_section: str | None = None
    current_child: str | None = None

    for raw_line in text.splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip():
            continue

        indent = len(line) - len(line.lstrip(" "))
        stripped = line.strip()

        if indent == 0 and stripped.endswith(":"):
            current_section = stripped[:-1]
            current_child = None
            if current_section in {"required_tags", "critical_tags"}:
                result[current_section] = []
            else:
                result[current_section] = {}
            continue

        if current_section is None:
            continue

        section = result[current_section]

        if indent == 2 and stripped.startswith("- ") and isinstance(section, list):
            section.append(stripped[2:].strip())
        elif (
            indent == 2
            and current_section == "tag_aliases"
            and stripped.endswith(":")
            and isinstance(section, dict)
        ):
            current_child = stripped[:-1]
            section[current_child] = []
        elif (
            indent == 4
            and current_section == "tag_aliases"
            and current_child
            and stripped.startswith("- ")
            and isinstance(section, dict)
        ):
            section[current_child].append(stripped[2:].strip())
        elif indent == 2 and ":" in stripped and isinstance(section, dict):
            key, value = stripped.split(":", 1)
            section[key.strip()] = _parse_scalar(value)

    return result


def _read_yaml_file(path: Path) -> tuple[dict[str, Any], str | None]:
    if not path.exists():
        return {}, f"{path.as_posix()} not found; built-in defaults were used."

    try:
        try:
            import yaml  # type: ignore

            data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except ImportError:
            data = _parse_simple_yaml(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return {}, f"{path.as_posix()} did not parse as a mapping; built-in defaults were used."
        return data, None
    except Exception as exc:
        return {}, f"{path.as_posix()} could not be parsed ({type(exc).__name__}: {exc}); built-in defaults were used."


def _numeric_section(
    data: dict[str, Any],
    section: str,
    defaults: dict[str, float],
) -> tuple[dict[str, float], list[str]]:
    values = dict(defaults)
    warnings: list[str] = []
    raw_section = data.get(section, {})
    if not isinstance(raw_section, dict):
        return values, [f"{section} config must be a mapping; built-in defaults were used."]

    for key in defaults:
        if key not in raw_section:
            continue
        try:
            values[key] = float(raw_section[key])
        except (TypeError, ValueError):
            warnings.append(f"{section}.{key} must be numeric; default {defaults[key]} was used.")
    return values, warnings


def _list_section(
    data: dict[str, Any],
    section: str,
    defaults: tuple[str, ...],
) -> tuple[tuple[str, ...], list[str]]:
    raw_section = data.get(section, list(defaults))
    if not isinstance(raw_section, list):
        return defaults, [f"{section} config must be a list; built-in defaults were used."]
    values = tuple(str(value) for value in raw_section if str(value).strip())
    return values or defaults, []


def _alias_section(data: dict[str, Any]) -> tuple[dict[str, tuple[str, ...]], list[str]]:
    raw_aliases = data.get("tag_aliases", DEFAULT_TAG_ALIASES)
    if not isinstance(raw_aliases, dict):
        return dict(DEFAULT_TAG_ALIASES), [
            "tag_aliases config must be a mapping; built-in defaults were used."
        ]

    aliases: dict[str, tuple[str, ...]] = dict(DEFAULT_TAG_ALIASES)
    warnings: list[str] = []
    for key, raw_values in raw_aliases.items():
        if not isinstance(raw_values, list):
            warnings.append(f"tag_aliases.{key} must be a list; default aliases were used.")
            continue
        values = tuple(str(value) for value in raw_values if str(value).strip())
        if values:
            aliases[str(key)] = values
    return aliases, warnings


def load_runtime_rules(bundle_root: str) -> tuple[
    ThresholdConfig,
    ScoringConfig,
    PriorityConfig,
    TaggingRules,
    tuple[str, ...],
]:
    root = Path(bundle_root)
    warnings: list[str] = []

    thresholds_data, warning = _read_yaml_file(root / "config" / "thresholds.yml")
    if warning:
        warnings.append(warning)

    threshold_values, section_warnings = _numeric_section(
        thresholds_data,
        "thresholds",
        DEFAULT_THRESHOLDS,
    )
    warnings.extend(section_warnings)

    scoring_values, section_warnings = _numeric_section(
        thresholds_data,
        "scoring_weights",
        DEFAULT_SCORING_WEIGHTS,
    )
    warnings.extend(section_warnings)
    weight_total = sum(scoring_values.values())
    if abs(weight_total - 1.0) > 0.000001:
        warnings.append(
            f"scoring_weights must add up to 1.0; built-in default weights were used instead of total {weight_total:.6f}."
        )
        scoring_values = dict(DEFAULT_SCORING_WEIGHTS)

    priority_values, section_warnings = _numeric_section(
        thresholds_data,
        "priority",
        DEFAULT_PRIORITY,
    )
    warnings.extend(section_warnings)

    tag_data, warning = _read_yaml_file(root / "config" / "tagging_rules.yml")
    if warning:
        warnings.append(warning)

    required_tags, section_warnings = _list_section(
        tag_data,
        "required_tags",
        DEFAULT_REQUIRED_TAGS,
    )
    warnings.extend(section_warnings)
    critical_tags, section_warnings = _list_section(
        tag_data,
        "critical_tags",
        DEFAULT_CRITICAL_TAGS,
    )
    warnings.extend(section_warnings)
    tag_aliases, section_warnings = _alias_section(tag_data)
    warnings.extend(section_warnings)

    return (
        ThresholdConfig(**threshold_values),
        ScoringConfig(**scoring_values),
        PriorityConfig(**priority_values),
        TaggingRules(
            required_tags=required_tags,
            critical_tags=critical_tags,
            tag_aliases=tag_aliases,
        ),
        tuple(warnings),
    )


def config_from_args(args: argparse.Namespace) -> AppConfig:
    thresholds, scoring_weights, priority, tagging_rules, warnings = load_runtime_rules(
        args.bundle_root
    )
    return AppConfig(
        catalog=validate_identifier(args.catalog, "catalog"),
        schema=validate_identifier(args.schema, "schema"),
        lookback_days=max(1, args.lookback_days),
        currency_code=validate_currency(args.currency_code, "currency code"),
        display_currency=validate_currency(args.currency_code, "currency code"),
        fallback_dbu_price=max(0.0, args.fallback_dbu_price),
        bundle_root=args.bundle_root,
        thresholds=thresholds,
        scoring_weights=scoring_weights,
        priority=priority,
        tagging_rules=tagging_rules,
        config_warnings=warnings,
    )
