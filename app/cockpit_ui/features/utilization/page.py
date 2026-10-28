from __future__ import annotations

import plotly.express as px
import streamlit as st

from cockpit_ui.core import AppContext
from cockpit_ui.core.components import PLOTLY_TEMPLATE, table


def render(ctx: AppContext) -> None:
    signals = ctx.table("vw_compute_sizing_signals")
    signals["cluster"] = signals["job_name"].fillna(signals["cluster_id"]).astype(str)

    fig = px.scatter(
        signals, x="avg_cpu_pct", y="avg_memory_pct", size="estimated_cost", color="utilization_category",
        hover_name="cluster", template=PLOTLY_TEMPLATE, size_max=50,
        labels={"avg_cpu_pct": "Average CPU %", "avg_memory_pct": "Average memory %"},
        title="CPU vs memory (bubble size = cost)",
    )
    fig.update_layout(height=460, legend_title=None, margin=dict(l=10, r=10, t=50, b=10))
    fig.update_xaxes(range=[0, 100])
    fig.update_yaxes(range=[0, 100])
    st.plotly_chart(fig, use_container_width=True)

    table(
        signals[["cluster", "avg_cpu_pct", "avg_memory_pct", "utilization_category", "sizing_signal",
                 "confidence", "estimated_cost"]].sort_values("estimated_cost", ascending=False),
        money_columns=["estimated_cost"], percent_columns=["avg_cpu_pct", "avg_memory_pct"],
        download_name="compute_sizing_signals",
    )
