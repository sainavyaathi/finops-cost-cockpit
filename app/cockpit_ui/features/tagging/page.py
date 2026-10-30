from __future__ import annotations

import streamlit as st

from cockpit_ui.core import AppContext
from cockpit_ui.core.components import Kpi, donut, kpi_row, money, section, stacked_bar, table
from cockpit_ui.core.theme import tone_colors

NO_TEAM = "No team tag"
TAG_QUALITY = {"GOOD": ("Good", "good"), "PARTIAL": ("Partial", "neutral"),
               "POOR": ("Poor", "warn"), "MISSING": ("Missing", "bad")}
ATTRIBUTION = {"HIGH": ("High", "good"), "MEDIUM": ("Medium", "neutral"),
               "LOW": ("Low", "warn"), "UNKNOWN": ("Unknown", "bad")}
WEAK_ATTRIBUTION = ("LOW", "UNKNOWN")


def _labels(codes, mapping: dict[str, tuple[str, str]]):
    return codes.fillna("UNKNOWN").map(lambda c: mapping.get(c, (str(c).capitalize(), "neutral"))[0])


def _rank(codes, mapping: dict[str, tuple[str, str]]):
    """Position of each code in ``mapping`` (best first) so legends read Good -> Missing."""
    order = {code: i for i, code in enumerate(mapping)}
    return codes.fillna("UNKNOWN").map(lambda c: order.get(c, len(order)))


def _colors(mapping: dict[str, tuple[str, str]]) -> dict[str, str]:
    return tone_colors(dict(mapping.values()))


def render(ctx: AppContext) -> None:
    tagging = ctx.table("vw_tagging_quality")
    tagging["team"] = tagging["team"].fillna(NO_TEAM)
    tagging["tag_quality"] = _labels(tagging["worst_tagging_quality"], TAG_QUALITY)
    tagging["_rank"] = _rank(tagging["worst_tagging_quality"], TAG_QUALITY)
    attribution = ctx.table("vw_attribution_quality")
    attribution["confidence"] = _labels(attribution["attribution_quality"], ATTRIBUTION)
    attribution["_rank"] = _rank(attribution["attribution_quality"], ATTRIBUTION)
    attribution = attribution.sort_values("_rank")

    total = tagging["estimated_cost"].sum()
    gaps = tagging[tagging["missing_required_tag_count"] > 0]
    missing = gaps["estimated_cost"].sum()
    weak = attribution.loc[attribution["attribution_quality"].isin(WEAK_ATTRIBUTION), "estimated_cost"].sum()
    kpi_row([
        Kpi("Spend", money(total)),
        Kpi("Missing a required tag", money(missing), delta=f"{missing / total:.0%} of spend" if total and missing else None,
            tone="warn" if missing > 0 else "good"),
        Kpi("Low-confidence attribution", money(weak), tone="warn" if weak > 0 else "good",
            help="Spend attributed with LOW or UNKNOWN confidence, e.g. shared clusters or missing run-as."),
        Kpi("Teams with gaps", str(gaps.loc[gaps["team"] != NO_TEAM, "team"].nunique()),
            help="Tagged teams that still own workloads missing a required tag."),
    ])

    left, right = st.columns([3, 2], gap="large")
    with left:
        by_team = (tagging.groupby(["_rank", "tag_quality", "team"], as_index=False)["estimated_cost"].sum()
                   .sort_values(["_rank", "estimated_cost"], ascending=[True, False]))
        stacked_bar(by_team, "team", "estimated_cost", "tag_quality", money=True,
                    colors=_colors(TAG_QUALITY), title="Spend by team and tag quality", height=340)
    with right:
        by_confidence = attribution.groupby("confidence", as_index=False, sort=False)["estimated_cost"].sum()
        donut(by_confidence, "confidence", "estimated_cost", money=True, colors=_colors(ATTRIBUTION),
              title="Spend by attribution confidence", height=340)

    only_gaps = st.toggle("Only rows missing a required tag", value=True)
    rows = gaps if only_gaps else tagging
    section("Tagging gaps" if only_gaps else "All teams, owners and projects",
            "Send each owner the list of workloads to tag. Sorted by spend.")
    view = rows.sort_values("estimated_cost", ascending=False)[
        ["team", "owner", "project", "tag_quality", "missing_required_tag_count", "estimated_cost"]
    ].fillna({"owner": "", "project": ""}).rename(columns={"missing_required_tag_count": "Missing tags"})
    table(view, money_columns=["estimated_cost"], download_name="tagging_gaps" if only_gaps else "tagging_quality")
