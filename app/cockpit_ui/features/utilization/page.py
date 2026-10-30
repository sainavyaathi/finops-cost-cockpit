from __future__ import annotations

import plotly.express as px
import streamlit as st

from cockpit_ui.core import AppContext
from cockpit_ui.core.components import Kpi, bar, chart, kpi_row, money, number, section, table
from cockpit_ui.core.theme import palette, tone_colors

# Display labels and semantic tones for the engine's sizing_signal values.
SIGNALS = {
    "REVIEW_WORKER_COUNT": ("Reduce workers", "bad"),
    "REVIEW_NODE_TYPE": ("Smaller node type", "warn"),
    "INVESTIGATE_MEMORY_PRESSURE": ("Memory-bound, keep size", "accent"),
    "LOOKS_HEALTHY": ("Healthy", "good"),
    "INSUFFICIENT_DATA": ("Not enough data", "neutral"),
}
DOWNSIZE = ("REVIEW_WORKER_COUNT", "REVIEW_NODE_TYPE")


def _label(signal: object) -> str:
    return SIGNALS.get(str(signal), (str(signal).replace("_", " ").capitalize(), "neutral"))[0]


def _signal_colors(labels) -> dict[str, str]:
    tones = {label: tone for label, tone in SIGNALS.values()}
    return tone_colors({label: tones.get(label, "neutral") for label in labels})


def _scatter(signals, colors: dict[str, str]):
    fig = px.scatter(
        signals, x="avg_cpu_pct", y="avg_memory_pct", size="estimated_cost", color="signal",
        color_discrete_map=colors, hover_name="compute", size_max=46,
        custom_data=["estimated_cost", "utilization"],
    )
    fig.update_traces(
        marker=dict(opacity=0.85, line=dict(width=1, color=palette().surface)),
        hovertemplate="<b>%{hovertext}</b><br>CPU %{x:.1f}% · memory %{y:.1f}%<br>"
                      "$%{customdata[0]:,.0f} · %{customdata[1]}<extra></extra>",
    )
    fig.update_xaxes(range=[0, 100], ticksuffix="%", title="Average CPU")
    fig.update_yaxes(range=[0, 100], ticksuffix="%", title="Average memory")
    return fig


def render(ctx: AppContext) -> None:
    signals = ctx.table("vw_compute_sizing_signals")
    signals["compute"] = signals["job_name"].fillna(signals["cluster_id"]).astype(str)
    signals["signal"] = signals["sizing_signal"].map(_label)
    signals["utilization"] = signals["utilization_category"].fillna("").str.replace("_", " ").str.capitalize()
    signals = signals.sort_values("estimated_cost", ascending=False)

    downsize = signals[signals["sizing_signal"].isin(DOWNSIZE)]
    memory_bound = int((signals["sizing_signal"] == "INVESTIGATE_MEMORY_PRESSURE").sum())
    kpi_row([
        Kpi("Compute reviewed", number(len(signals)), help="Clusters and job clusters with utilisation metrics."),
        Kpi("Spend reviewed", money(signals["estimated_cost"].sum())),
        Kpi("Spend on downsize candidates", money(downsize["estimated_cost"].sum()),
            delta=f"{len(downsize)} of {len(signals)} resources" if len(downsize) else None,
            tone="warn" if len(downsize) else "good",
            help="Current spend of compute the engine flags for fewer workers or a smaller node type."),
        Kpi("Memory-bound", number(memory_bound), tone="accent" if memory_bound else "neutral",
            help="Low CPU but high memory: do not downsize."),
    ])

    colors = _signal_colors(signals["signal"].unique())
    left, right = st.columns([3, 2], gap="large")
    with left:
        chart(_scatter(signals, colors), height=400, title="CPU vs memory, bubble size is spend")
    with right:
        bar(signals, "compute", "estimated_cost", color="signal", colors=colors, money=True,
            title="Spend by sizing signal", height=400)

    section("Sizing signals", "Act on 'Reduce workers' and 'Smaller node type'; leave memory-bound compute as is.")
    view = signals[["compute", "signal", "utilization", "avg_cpu_pct", "avg_memory_pct", "confidence",
                    "estimated_cost"]].rename(columns={"compute": "Cluster or job", "signal": "Sizing signal",
                                                       "avg_cpu_pct": "Avg CPU", "avg_memory_pct": "Avg memory"})
    table(view, money_columns=["estimated_cost"], percent_columns=["Avg CPU", "Avg memory"],
          download_name="compute_sizing_signals")
