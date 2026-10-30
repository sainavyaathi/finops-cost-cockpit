"""Shared UI building blocks so every feature page looks the same.

Formatting helpers (``money``, ``number``, ``delta_text``, ``relative_time``) are pure and safe to
use anywhere. Render helpers write to the current Streamlit container and return ``None``;
``status_chip`` and ``status_badge`` return HTML strings for composing into other markup.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from html import escape
from typing import Iterable, Mapping, Sequence, Union

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from . import theme
from .feature import Feature

PLOTLY_TEMPLATE = theme.TEMPLATE_LIGHT
theme.register_plotly_templates()

_STATUS_TONES = {"live": "good", "preview": "warn", "planned": "neutral"}


# ------------------------------------------------------------------ formatting

def _missing(value: object) -> bool:
    return value is None or bool(pd.isna(value))


def _compact(value: float, decimals: int) -> str:
    for threshold, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "k")):
        if abs(value) >= threshold:
            return f"{value / threshold:,.{max(decimals, 1)}f}{suffix}"
    return f"{value:,.{decimals}f}"


def money(value: float | int | None, decimals: int = 0, *, compact: bool = False) -> str:
    """``$1,234`` (or ``$1.2k`` with ``compact``). Values are already in display currency."""
    if _missing(value):
        return "-"
    body = _compact(abs(value), decimals) if compact else f"{abs(value):,.{decimals}f}"
    return f"{'-' if value < 0 else ''}${body}"


_format_money = money  # chart helpers take a ``money`` flag that shadows the function


def number(value: float | int | None, decimals: int = 0, *, compact: bool = False) -> str:
    if _missing(value):
        return "-"
    return _compact(value, decimals) if compact else f"{value:,.{decimals}f}"


def _humanize(column: str) -> str:
    text = column.replace("_", " ").strip()
    return text[:1].upper() + text[1:]


def delta_text(current: float | int | None, previous: float | int | None, decimals: int = 1) -> str | None:
    """Signed percentage change, e.g. ``"+12.5%"``; ``None`` when there is no meaningful baseline."""
    if _missing(current) or _missing(previous) or previous == 0:
        return None
    change = (current - previous) / abs(previous) * 100
    rounded = round(change, decimals)
    if rounded == 0:
        return f"{0:.{decimals}f}%"
    return f"{rounded:+.{decimals}f}%"


def relative_time(moment: datetime, now: datetime | None = None) -> str:
    """``"just now"``, ``"14m ago"``, ``"3h ago"``, ``"2d ago"``; naive datetimes are treated as UTC."""
    now = now or datetime.now(timezone.utc)
    moment = moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)
    now = now if now.tzinfo else now.replace(tzinfo=timezone.utc)
    seconds = (now - moment).total_seconds()
    if seconds < 60:
        return "just now"
    size, unit = next((size, unit) for size, unit in ((86400, "d"), (3600, "h"), (60, "m")) if seconds >= size)
    return f"{int(seconds // size)}{unit} ago"


# ------------------------------------------------------------------ chips and headers

def status_chip(text: str, tone: str = "neutral", *, mono: bool = False) -> str:
    """HTML for a small chip. ``tone`` is one of neutral, accent, good, warn, bad."""
    if tone not in theme.TONES:
        raise ValueError(f"tone must be one of {theme.TONES}, got {tone!r}.")
    classes = f"ck-chip ck-chip--{tone}" + (" ck-chip--mono" if mono else "")
    return f'<span class="{classes}">{escape(text)}</span>'


def status_badge(status: str) -> str:
    """HTML chip for a feature status: live, preview or planned."""
    if status not in _STATUS_TONES:
        raise ValueError(f"Unknown status {status!r}.")
    return status_chip(status.capitalize(), _STATUS_TONES[status])


def _html(markup: str) -> None:
    st.markdown(markup, unsafe_allow_html=True)


def page_header(feature: Feature, *, subtitle: str | None = None, freshness: str | None = None) -> None:
    chips = []
    if feature.plan_label:
        chips.append(status_chip(feature.plan_label, "accent"))
    if feature.status != "live":
        chips.append(status_badge(feature.status))
    if chips:
        _html(f'<div class="ck-eyebrow">{"".join(chips)}</div>')

    st.title(feature.title, anchor=False)

    lede = subtitle or feature.description
    meta = [f"Owner {feature.owner}" if feature.owner else "", f"Data as of {freshness}" if freshness else ""]
    meta_html = "".join(f"<span>{escape(part)}</span>" for part in meta if part)
    if lede or meta_html:
        _html(
            '<div class="ck-header-sub">'
            + (f'<div class="ck-lede">{escape(lede)}</div>' if lede else "")
            + (f'<div class="ck-meta">{meta_html}</div>' if meta_html else "")
            + "</div>"
        )


def section(title: str, caption: str | None = None) -> None:
    caption_html = f'<div class="ck-section__caption">{escape(caption)}</div>' if caption else ""
    _html(f'<div class="ck-section"><div class="ck-section__title">{escape(title)}</div>{caption_html}</div>')


# ------------------------------------------------------------------ KPIs

@dataclass(frozen=True)
class Kpi:
    label: str
    value: str
    delta: str | None = None
    help: str | None = None
    tone: str = "neutral"

    def __post_init__(self) -> None:
        if self.tone not in theme.TONES:
            raise ValueError(f"tone must be one of {theme.TONES}, got {self.tone!r}.")


KpiLike = Union[Kpi, tuple[str, str], tuple[str, str, Union[str, None]]]


def as_kpi(item: KpiLike) -> Kpi:
    return item if isinstance(item, Kpi) else Kpi(*item)


def _kpi_html(kpi: Kpi) -> str:
    color = theme.tone_color(kpi.tone)
    dot = f'<span class="ck-kpi__dot" style="background:{color}"></span>' if kpi.tone != "neutral" and not kpi.delta else ""
    help_ = f'<span class="ck-kpi__help" title="{escape(kpi.help)}">ⓘ</span>' if kpi.help else ""
    delta = f'<div class="ck-kpi__delta" style="color:{color}">{escape(kpi.delta)}</div>' if kpi.delta else ""
    return (
        f'<div class="ck-kpi"><div class="ck-kpi__label">{dot}<span>{escape(kpi.label)}</span>{help_}</div>'
        f'<div class="ck-kpi__value" title="{escape(kpi.value)}">{escape(kpi.value)}</div>{delta}</div>'
    )


def kpi_row(items: Sequence[KpiLike]) -> None:
    """A responsive row of KPI cards. Items are ``(label, value)``, ``(label, value, delta)`` or ``Kpi``."""
    if items:
        _html(f'<div class="ck-kpis">{"".join(_kpi_html(as_kpi(item)) for item in items)}</div>')


# ------------------------------------------------------------------ states

def not_available(missing: Iterable[str]) -> None:
    st.info(
        "This page needs tables that are not in the current data source yet: "
        + ", ".join(f"`{name}`" for name in missing)
        + ". Run the pipeline (`sandbox/run_local.py`, or `.\\sandbox\\run_local.ps1` on Windows) and refresh.",
        icon=":material/hourglass_empty:",
    )


def empty_state(title: str, body: str, *, icon: str | None = None, key: str | None = None) -> None:
    """A quiet dashed panel for "nothing here" moments. ``icon`` is a ``:material/...:`` shortcode."""
    slug = key or re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
    with st.container(key=f"ck-empty-{slug}", gap="small"):
        st.markdown(f"{icon + ' ' if icon else ''}**{title}**")
        st.caption(body)


def planned_feature(feature: Feature, expected_tables: Sequence[str], what_it_will_show: Sequence[str]) -> None:
    """The panel shown in place of a page whose status is ``planned``."""
    with st.container(key=f"ck-empty-planned-{feature.id}", gap="medium"):
        st.markdown(":material/construction: **In development.** This page appears here as soon as it ships.")
        left, right = st.columns([3, 2], gap="large")
        with left:
            _html('<div class="ck-label">What it will show</div>')
            if what_it_will_show:
                st.markdown("\n".join(f"- {line}" for line in what_it_will_show))
            else:
                st.caption(feature.description or "Details to follow.")
        with right:
            _html('<div class="ck-label">Gold tables it will read</div>')
            if expected_tables:
                chips = "".join(status_chip(name, mono=True) for name in expected_tables)
                _html(f'<div class="ck-tables">{chips}</div>')
            else:
                st.caption("To be defined.")


# ------------------------------------------------------------------ charts

def _chart_title(title: str | None) -> None:
    if title:
        _html(f'<div class="ck-chart-title">{escape(title)}</div>')


def _hover_value(field: str, money_values: bool) -> str:
    """Plotly hover token for ``field``: ``$1,234.50`` for money, otherwise up to 2 decimals."""
    return f"$%{{{field}:,.2f}}" if money_values else f"%{{{field}:,.2~f}}"


def _format_value_axis(fig: go.Figure, axis: str, money_values: bool) -> None:
    update = fig.update_xaxes if axis == "x" else fig.update_yaxes
    update(title=None, tickprefix="$" if money_values else "", rangemode="tozero")


def chart(fig: go.Figure, *, height: int | None = None, title: str | None = None) -> None:
    """Render any Plotly figure with the cockpit template. Use for figures no helper covers."""
    _chart_title(title)
    # Backgrounds are set on the figure, not only the template: Streamlit fills a template-transparent
    # plot area with secondaryBackgroundColor.
    fig.update_layout(template=theme.template_name(), title=None, legend_title_text=None,
                      paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
    if height:
        fig.update_layout(height=height)
    st.plotly_chart(fig, theme=None, config=theme.PLOTLY_CONFIG)


def _bars(frame: pd.DataFrame, x: str, y: str, color: str | None, *, horizontal: bool, money_values: bool,
          colors: Mapping[str, str] | None) -> go.Figure:
    category, value = ("y", "x") if horizontal else ("x", "y")
    fig = px.bar(frame, **{category: x, value: y}, color=color, orientation="h" if horizontal else "v",
                 barmode="relative", color_discrete_map=colors)
    if horizontal:
        fig.update_yaxes(categoryorder="total ascending", ticksuffix="  ")
    fig.update_layout({f"{category}axis": dict(title=None, showgrid=False)})
    _format_value_axis(fig, value, money_values)
    fig.update_traces(hovertemplate=f"%{{{category}}}<br>{_hover_value(value, money_values)}<extra>%{{fullData.name}}</extra>")
    if color is None:
        fig.update_traces(marker_color=theme.palette().accent)
    return fig


def bar(frame: pd.DataFrame, x: str, y: str, *, color: str | None = None, horizontal: bool = True,
        title: str | None = None, height: int = 380, money: bool = False,
        colors: Mapping[str, str] | None = None) -> None:
    """``x`` is the category column and ``y`` the value column, whatever the orientation.

    With ``color`` the bars stack by that column. ``colors`` pins colours to its values
    (see ``theme.tone_colors``); other values use the palette.
    """
    chart(_bars(frame, x, y, color, horizontal=horizontal, money_values=money, colors=colors), height=height, title=title)


def stacked_bar(frame: pd.DataFrame, x: str, y: str, color: str, *, horizontal: bool = False,
                title: str | None = None, height: int = 380, money: bool = False,
                colors: Mapping[str, str] | None = None) -> None:
    """Bars of ``y`` per ``x`` category, stacked by ``color``; vertical by default (time on ``x``)."""
    chart(_bars(frame, x, y, color, horizontal=horizontal, money_values=money, colors=colors), height=height, title=title)


def _time_series(fig: go.Figure, money_values: bool) -> go.Figure:
    fig.for_each_trace(lambda trace: trace.update(name=_humanize(trace.name)))
    fig.update_xaxes(title=None, showgrid=False)
    _format_value_axis(fig, "y", money_values)
    fig.update_traces(hovertemplate=_hover_value("y", money_values) + "<extra>%{fullData.name}</extra>")
    fig.update_layout(hovermode="x unified")
    return fig


def line(frame: pd.DataFrame, x: str, ys: list[str], *, title: str | None = None, height: int = 340,
         money: bool = False) -> None:
    fig = px.line(frame, x=x, y=ys, markers=False)
    if len(ys) == 1:
        fig.update_layout(showlegend=False)
    chart(_time_series(fig, money), height=height, title=title)


def area(frame: pd.DataFrame, x: str, ys: list[str], *, title: str | None = None, height: int = 340,
         money: bool = False, stacked: bool = True) -> None:
    fig = px.area(frame, x=x, y=ys) if stacked else px.line(frame, x=x, y=ys)
    if not stacked:
        fig.update_traces(fill="tozeroy")
    fig.update_traces(line=dict(width=1.5))
    if len(ys) == 1:
        fig.update_layout(showlegend=False)
    chart(_time_series(fig, money), height=height, title=title)


def donut(frame: pd.DataFrame, names: str, values: str, *, title: str | None = None, height: int = 340,
          money: bool = False, colors: Mapping[str, str] | None = None) -> None:
    fig = px.pie(frame, names=names, values=values, hole=0.64, color=names if colors else None,
                 color_discrete_map=colors)
    fig.update_traces(
        textinfo="none",
        hovertemplate=f"%{{label}}<br>{_hover_value('value', money)} · %{{percent}}<extra></extra>",
    )
    total = frame[values].sum()
    p = theme.palette()
    fig.add_annotation(
        text=f"<span style='font-size:11px;color:{p.muted}'>Total</span><br>"
             f"<b>{_format_money(total, compact=True) if money else number(total, compact=True)}</b>",
        showarrow=False, font=dict(size=17, color=p.ink), x=0.5, y=0.5, xref="paper", yref="paper",
    )
    fig.update_layout(legend=dict(orientation="v", x=1.02, y=0.5, yanchor="middle"))
    chart(fig, height=height, title=title)


# ------------------------------------------------------------------ tables

def table(frame: pd.DataFrame, *, money_columns: Sequence[str] = (), percent_columns: Sequence[str] = (),
          download_name: str | None = None, height: int | None = None) -> None:
    """A dataframe with readable headers. ``percent_columns`` hold values already in percent (42.0 = 42%)."""
    config: dict[str, object] = {}
    for column in frame.columns:
        label = _humanize(str(column))
        if column in money_columns:
            config[column] = st.column_config.NumberColumn(label, format="dollar")
        elif column in percent_columns:
            config[column] = st.column_config.NumberColumn(label, format="%.1f%%")
        else:
            config[column] = st.column_config.Column(label)
    st.dataframe(frame, hide_index=True, width="stretch", column_config=config,
                 **({"height": height} if height else {}))
    if download_name:
        st.download_button("Download CSV", frame.to_csv(index=False).encode("utf-8"), type="tertiary",
                           file_name=f"{download_name}.csv", mime="text/csv", icon=":material/download:")
