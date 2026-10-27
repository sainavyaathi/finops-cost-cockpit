"""Shared UI building blocks so every feature page looks the same."""

from __future__ import annotations

from typing import Iterable, Sequence

import pandas as pd
import plotly.express as px
import streamlit as st

from .feature import Feature

PLOTLY_TEMPLATE = "plotly_white"


def money(value: float | int | None, decimals: int = 0) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"${value:,.{decimals}f}"


def number(value: float | int | None, decimals: int = 0) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{value:,.{decimals}f}"


def page_header(feature: Feature) -> None:
    st.title(feature.title)
    meta = " · ".join(part for part in [", ".join(feature.plan_refs), f"Owner: {feature.owner}" if feature.owner else ""] if part)
    if feature.description:
        st.caption(f"{feature.description}  \n{meta}" if meta else feature.description)
    elif meta:
        st.caption(meta)


def kpi_row(items: Sequence[tuple[str, str]]) -> None:
    columns = st.columns(len(items))
    for column, (label, value) in zip(columns, items):
        column.metric(label, value, border=True)


def not_available(missing: Iterable[str]) -> None:
    st.info(
        "This page needs tables that are not in the current data source yet: "
        + ", ".join(f"`{name}`" for name in missing)
        + ". Run the pipeline (locally: `.\\sandbox\\run_local.ps1`) and refresh.",
        icon=":material/info:",
    )


def bar(frame: pd.DataFrame, x: str, y: str, *, color: str | None = None, horizontal: bool = True,
        title: str | None = None, height: int = 420) -> None:
    if horizontal:
        fig = px.bar(frame, x=y, y=x, color=color, orientation="h", template=PLOTLY_TEMPLATE, title=title)
        fig.update_yaxes(categoryorder="total ascending", title=None)
    else:
        fig = px.bar(frame, x=x, y=y, color=color, template=PLOTLY_TEMPLATE, title=title)
    fig.update_layout(height=height, margin=dict(l=10, r=10, t=50 if title else 10, b=10), legend_title=None)
    st.plotly_chart(fig, use_container_width=True)


def line(frame: pd.DataFrame, x: str, ys: list[str], *, title: str | None = None, height: int = 360) -> None:
    fig = px.line(frame, x=x, y=ys, template=PLOTLY_TEMPLATE, title=title, markers=False)
    fig.update_layout(height=height, margin=dict(l=10, r=10, t=50 if title else 10, b=10), legend_title=None,
                      yaxis_title=None, xaxis_title=None)
    st.plotly_chart(fig, use_container_width=True)


def donut(frame: pd.DataFrame, names: str, values: str, *, title: str | None = None, height: int = 360) -> None:
    fig = px.pie(frame, names=names, values=values, hole=0.55, template=PLOTLY_TEMPLATE, title=title)
    fig.update_layout(height=height, margin=dict(l=10, r=10, t=50 if title else 10, b=10), legend_title=None)
    st.plotly_chart(fig, use_container_width=True)


def table(frame: pd.DataFrame, *, money_columns: Sequence[str] = (), percent_columns: Sequence[str] = (),
          download_name: str | None = None) -> None:
    config = {c: st.column_config.NumberColumn(format="$%.2f") for c in money_columns if c in frame}
    config.update({c: st.column_config.NumberColumn(format="%.1f%%") for c in percent_columns if c in frame})
    st.dataframe(frame, hide_index=True, width="stretch", column_config=config)
    if download_name:
        st.download_button("Download CSV", frame.to_csv(index=False).encode("utf-8"),
                           file_name=f"{download_name}.csv", mime="text/csv", icon=":material/download:")
