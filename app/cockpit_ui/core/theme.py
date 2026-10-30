"""Design tokens, the Plotly template and the small amount of CSS the cockpit needs.

Streamlit's own colours, fonts and radii come from ``app/.streamlit/config.toml``; this module
mirrors the same tokens for the things config cannot reach (Plotly figures, KPI cards, chips).
Light is the designed mode. Set ``base = "dark"`` (plus the DARK values) in config.toml to switch
everything to dark; ``mode()`` follows ``theme.base``. It deliberately ignores
``st.context.theme.type``: in Streamlit 1.50 that reflects the browser's OS preference on the
first run, not the custom theme actually shown, so it would mix dark cards into a light app.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Mapping

import plotly.graph_objects as go
import plotly.io as pio
import streamlit as st

Mode = Literal["light", "dark"]

FONT_STACK = "Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', sans-serif"
MONO_STACK = "'JetBrains Mono', 'SFMono-Regular', Menlo, Consolas, monospace"


@dataclass(frozen=True)
class Palette:
    canvas: str
    surface: str
    subtle: str
    border: str
    ink: str
    muted: str
    faint: str
    grid: str
    accent: str
    accent_soft: str
    good: str
    warn: str
    bad: str


LIGHT = Palette(
    canvas="#FAFAF9", surface="#FFFFFF", subtle="#F3F3F1", border="#E5E4E0",
    ink="#1B1B1F", muted="#6B6A70", faint="#9C9BA1", grid="#EEEDEA",
    accent="#4B5BD6", accent_soft="#EBEDFB",
    good="#1F7A4D", warn="#A8650E", bad="#B4352B",
)

DARK = Palette(
    canvas="#111113", surface="#18181B", subtle="#1F1F23", border="#2C2C31",
    ink="#ECECEE", muted="#A1A1A8", faint="#6E6E76", grid="#26262B",
    accent="#8592F0", accent_soft="#252A4A",
    good="#4CC38A", warn="#E2A23B", bad="#F07067",
)

# Categorical series: the accent first, then tints/shades of it, then neutral slates.
# Mid-tones only, so the same sequence reads on both light and dark canvases.
CATEGORICAL = ("#4B5BD6", "#9AA4EC", "#2C3893", "#8A8F98", "#C3C8F3", "#5B616B", "#6F7DE0", "#B9BDC4")

TONES = ("neutral", "accent", "good", "warn", "bad")

TEMPLATE_LIGHT = "cockpit"
TEMPLATE_DARK = "cockpit_dark"


def mode() -> Mode:
    return "dark" if st.get_option("theme.base") == "dark" else "light"


def palette(current: Mode | None = None) -> Palette:
    return DARK if (current or mode()) == "dark" else LIGHT


def template_name(current: Mode | None = None) -> str:
    return TEMPLATE_DARK if (current or mode()) == "dark" else TEMPLATE_LIGHT


def tone_color(tone: str, current: Mode | None = None) -> str:
    p = palette(current)
    return {"neutral": p.muted, "accent": p.accent, "good": p.good, "warn": p.warn, "bad": p.bad}[tone]


def tone_colors(tones: Mapping[str, str], current: Mode | None = None) -> dict[str, str]:
    """Map category values to semantic colours, e.g. ``{"GOOD": "good", "POOR": "bad"}``."""
    return {value: tone_color(tone, current) for value, tone in tones.items()}


# ------------------------------------------------------------------ Plotly

def _template(p: Palette) -> go.layout.Template:
    axis = dict(
        showgrid=True, gridcolor=p.grid, gridwidth=1, zeroline=False, showline=False,
        ticks="", tickfont=dict(size=11, color=p.muted), title=dict(font=dict(size=11, color=p.muted)),
        automargin=True,
    )
    return go.layout.Template(
        layout=go.Layout(
            font=dict(family=FONT_STACK, size=12, color=p.ink),
            colorway=list(CATEGORICAL),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=4, r=8, t=8, b=4),
            xaxis=axis,
            yaxis=axis,
            legend=dict(
                orientation="h", yanchor="bottom", y=1.0, xanchor="left", x=0,
                font=dict(size=11, color=p.muted), title=dict(text=""), bgcolor="rgba(0,0,0,0)",
                itemsizing="constant",
            ),
            hoverlabel=dict(
                bgcolor=p.surface, bordercolor=p.border, align="left",
                font=dict(family=FONT_STACK, size=12, color=p.ink),
            ),
            hovermode="closest",
            bargap=0.35,
            barcornerradius=3,
            title=dict(text=""),
        ),
        data=dict(
            pie=[go.Pie(marker=dict(line=dict(color=p.canvas, width=2)), sort=False)],
            scatter=[go.Scatter(line=dict(width=2))],
        ),
    )


def register_plotly_templates() -> None:
    """Idempotent; safe to call on every rerun."""
    pio.templates[TEMPLATE_LIGHT] = _template(LIGHT)
    pio.templates[TEMPLATE_DARK] = _template(DARK)
    pio.templates.default = TEMPLATE_LIGHT


PLOTLY_CONFIG = {"displayModeBar": False, "displaylogo": False, "scrollZoom": False}


# ------------------------------------------------------------------ CSS

_CSS = """
:root {{
  --ck-surface: {surface}; --ck-subtle: {subtle}; --ck-border: {border};
  --ck-ink: {ink}; --ck-muted: {muted}; --ck-faint: {faint};
  --ck-accent: {accent}; --ck-accent-soft: {accent_soft};
  --ck-good: {good}; --ck-warn: {warn}; --ck-bad: {bad};
  --ck-mono: {mono};
}}
[data-testid="stMainBlockContainer"] {{ padding-top: 2.25rem; padding-bottom: 3rem; max-width: 1360px; }}
[data-testid="stHeader"] {{ background: transparent; }}
[data-testid="stMetricValue"], .js-plotly-plot text, .ck-num {{ font-variant-numeric: tabular-nums; }}
h1 {{ letter-spacing: -0.015em; }}

.ck-eyebrow {{ display: flex; gap: 6px; align-items: center; margin-bottom: -0.5rem; }}
.ck-header-sub {{ margin: -0.75rem 0 0.5rem; }}
.ck-lede {{ color: var(--ck-muted); font-size: 0.95rem; line-height: 1.5; }}
.ck-meta {{ color: var(--ck-faint); font-size: 0.8rem; margin-top: 2px; }}
.ck-meta span + span::before {{ content: "·"; margin: 0 7px; color: var(--ck-faint); }}

.ck-chip {{
  display: inline-flex; align-items: center; gap: 5px; height: 20px; padding: 0 7px;
  border-radius: 4px; font-size: 0.72rem; font-weight: 500; letter-spacing: 0.02em; white-space: nowrap;
  border: 1px solid var(--ck-border); color: var(--ck-muted); background: var(--ck-surface);
}}
.ck-chip::before {{ content: ""; width: 6px; height: 6px; border-radius: 50%; background: currentColor; display: none; }}
.ck-chip--accent {{ color: var(--ck-accent); border-color: transparent; background: var(--ck-accent-soft); }}
.ck-chip--good::before, .ck-chip--warn::before, .ck-chip--bad::before {{ display: block; }}
.ck-chip--good {{ color: var(--ck-good); }}
.ck-chip--warn {{ color: var(--ck-warn); }}
.ck-chip--bad  {{ color: var(--ck-bad); }}
.ck-chip--mono {{ font-family: var(--ck-mono); letter-spacing: 0; }}

.ck-kpis {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 10px; }}
.ck-kpi {{
  background: var(--ck-surface); border: 1px solid var(--ck-border); border-radius: 8px;
  padding: 12px 14px 11px; min-width: 0;
}}
.ck-kpi__label {{
  display: flex; align-items: center; gap: 6px; color: var(--ck-muted);
  font-size: 0.72rem; font-weight: 500; text-transform: uppercase; letter-spacing: 0.05em;
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}}
.ck-kpi__dot {{ width: 6px; height: 6px; border-radius: 50%; flex: none; }}
.ck-kpi__help {{ cursor: help; color: var(--ck-faint); text-transform: none; }}
.ck-kpi__value {{
  font-size: 1.55rem; font-weight: 600; letter-spacing: -0.02em; line-height: 1.25; margin-top: 4px;
  color: var(--ck-ink); font-variant-numeric: tabular-nums; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}}
.ck-kpi__delta {{ font-size: 0.78rem; font-weight: 500; margin-top: 2px; font-variant-numeric: tabular-nums; }}

.ck-section {{ margin: 0.75rem 0 -0.35rem; }}
.ck-section__title {{ font-size: 0.95rem; font-weight: 600; color: var(--ck-ink); }}
.ck-section__caption {{ font-size: 0.82rem; color: var(--ck-muted); margin-top: 1px; }}
.ck-chart-title {{ font-size: 0.82rem; font-weight: 600; color: var(--ck-ink); margin: 0.5rem 0 -0.6rem; }}

.ck-label {{ color: var(--ck-faint); font-size: 0.7rem; font-weight: 500; text-transform: uppercase; letter-spacing: 0.06em; }}
.ck-source {{ display: flex; flex-direction: column; gap: 6px; font-size: 0.8rem; color: var(--ck-muted); }}
.ck-source code {{ font-family: var(--ck-mono); font-size: 0.75rem; color: var(--ck-ink); background: none; padding: 0; }}
.ck-tables {{ display: flex; flex-wrap: wrap; gap: 6px; }}

[class*="st-key-ck-empty"] {{ border: 1px dashed var(--ck-border); border-radius: 8px; padding: 18px 20px; }}
"""


def css(current: Mode | None = None) -> str:
    p = palette(current)
    return _CSS.format(mono=MONO_STACK, **p.__dict__)


def apply_theme() -> None:
    """Register the Plotly templates and inject the stylesheet. Call once per script run."""
    register_plotly_templates()
    st.html(f"<style>{css()}</style>")
