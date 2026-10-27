from __future__ import annotations


SCORE_WEIGHTS: dict[str, float] = {
    "cost": 0.45,
    "waste": 0.25,
    "reliability": 0.15,
    "tagging": 0.10,
    "frequency": 0.05,
}


def clamp_score(value: float) -> float:
    return max(0.0, min(100.0, float(value)))


def priority_score_sql_expr(
    cost: str = "cost_score",
    waste: str = "waste_score",
    reliability: str = "reliability_score",
    tagging: str = "tagging_score",
    frequency: str = "frequency_score",
    weights: dict[str, float] | None = None,
) -> str:
    weights = weights or SCORE_WEIGHTS
    return (
        f"{cost} * {weights['cost']:.2f}\n"
        f"                    + {waste} * {weights['waste']:.2f}\n"
        f"                    + {reliability} * {weights['reliability']:.2f}\n"
        f"                    + {tagging} * {weights['tagging']:.2f}\n"
        f"                    + {frequency} * {weights['frequency']:.2f}"
    )


def weighted_priority_score(
    cost_score: float,
    waste_score: float,
    reliability_score: float,
    tagging_score: float,
    frequency_score: float,
    weights: dict[str, float] | None = None,
) -> float:
    """Transparent 0-100 priority score used by SQL and tests.

    Cost dominates, but utilization, reliability, tagging/attribution risk, and
    workload frequency all contribute so the backlog is useful across business
    and platform review conversations.
    """

    weights = weights or SCORE_WEIGHTS
    return round(
        clamp_score(cost_score) * weights["cost"]
        + clamp_score(waste_score) * weights["waste"]
        + clamp_score(reliability_score) * weights["reliability"]
        + clamp_score(tagging_score) * weights["tagging"]
        + clamp_score(frequency_score) * weights["frequency"],
        6,
    )
