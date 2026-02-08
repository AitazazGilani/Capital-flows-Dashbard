"""
Reusable Plotly chart functions for the macro dashboard.
"""

import plotly.graph_objects as go
import pandas as pd
import streamlit as st


CHART_TEMPLATE = "plotly_dark"
CHART_MARGINS = dict(l=50, r=30, t=50, b=30)
CHART_FONT = dict(family="Arial, sans-serif", size=12)

# Color palette for multi-line charts
COLORS = [
    "#636EFA", "#EF553B", "#00CC96", "#AB63FA", "#FFA15A",
    "#19D3F3", "#FF6692", "#B6E880", "#FF97FF", "#FECB52",
    "#1F77B4", "#FF7F0E", "#2CA02C",
]


def line_chart(df: pd.DataFrame, title: str, yaxis_title: str = None,
               height: int = 400, normalize: bool = False) -> go.Figure:
    """Multi-line chart from a df where columns are series.
    If normalize=True, rebase all to 100."""
    if df.empty:
        return _empty_figure(title, height)

    plot_df = df.copy()
    if normalize:
        first_valid = plot_df.apply(lambda s: s.dropna().iloc[0] if not s.dropna().empty else 1)
        plot_df = plot_df / first_valid * 100
        yaxis_title = yaxis_title or "Indexed (100)"

    fig = go.Figure()
    for i, col in enumerate(plot_df.columns):
        fig.add_trace(go.Scatter(
            x=plot_df.index, y=plot_df[col],
            name=str(col), mode="lines",
            line=dict(color=COLORS[i % len(COLORS)], width=2),
        ))

    fig.update_layout(
        title=title, height=height, template=CHART_TEMPLATE,
        margin=CHART_MARGINS, font=CHART_FONT,
        yaxis_title=yaxis_title,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="x unified",
    )
    return fig


def dual_axis_chart(series1: pd.Series, series2: pd.Series,
                    name1: str, name2: str, title: str,
                    height: int = 400) -> go.Figure:
    """Two series on different y-axes."""
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=series1.index, y=series1.values,
        name=name1, mode="lines",
        line=dict(color=COLORS[0], width=2),
    ))
    fig.add_trace(go.Scatter(
        x=series2.index, y=series2.values,
        name=name2, mode="lines",
        line=dict(color=COLORS[1], width=2),
        yaxis="y2",
    ))

    fig.update_layout(
        title=title, height=height, template=CHART_TEMPLATE,
        margin=CHART_MARGINS, font=CHART_FONT,
        yaxis=dict(title=name1, side="left"),
        yaxis2=dict(title=name2, side="right", overlaying="y"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="x unified",
    )
    return fig


def bar_chart(df: pd.DataFrame, title: str,
              color_positive: str = "#00CC96", color_negative: str = "#EF553B",
              height: int = 400) -> go.Figure:
    """Bar chart with conditional coloring for positive/negative values."""
    if df.empty:
        return _empty_figure(title, height)

    # Handle both Series and single-column DataFrame
    if isinstance(df, pd.Series):
        values = df.values
        labels = df.index
    elif df.shape[1] == 1:
        values = df.iloc[:, 0].values
        labels = df.index
    else:
        # Use last row for multiple columns
        values = df.iloc[-1].values
        labels = df.columns

    colors = [color_positive if v >= 0 else color_negative for v in values]

    fig = go.Figure(go.Bar(
        x=labels, y=values, marker_color=colors,
        text=[f"{v:.1f}" for v in values], textposition="outside",
    ))

    fig.update_layout(
        title=title, height=height, template=CHART_TEMPLATE,
        margin=CHART_MARGINS, font=CHART_FONT,
        hovermode="x",
    )
    return fig


def grouped_bar_chart(df: pd.DataFrame, title: str, height: int = 400) -> go.Figure:
    """Grouped bar chart - each column is a group, index is x-axis."""
    if df.empty:
        return _empty_figure(title, height)

    fig = go.Figure()
    for i, col in enumerate(df.columns):
        fig.add_trace(go.Bar(
            x=df.index, y=df[col], name=str(col),
            marker_color=COLORS[i % len(COLORS)],
        ))

    fig.update_layout(
        title=title, height=height, template=CHART_TEMPLATE,
        margin=CHART_MARGINS, font=CHART_FONT,
        barmode="group",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


def stacked_area(df: pd.DataFrame, title: str, height: int = 400) -> go.Figure:
    """Stacked area chart from df columns."""
    if df.empty:
        return _empty_figure(title, height)

    fig = go.Figure()
    for i, col in enumerate(df.columns):
        fig.add_trace(go.Scatter(
            x=df.index, y=df[col],
            name=str(col), mode="lines",
            stackgroup="one",
            line=dict(color=COLORS[i % len(COLORS)]),
        ))

    fig.update_layout(
        title=title, height=height, template=CHART_TEMPLATE,
        margin=CHART_MARGINS, font=CHART_FONT,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="x unified",
    )
    return fig


def heatmap(df: pd.DataFrame, title: str, colorscale: str = "RdYlGn",
            height: int = 400, fmt: str = ".2f") -> go.Figure:
    """Heatmap from df. Good for FX % changes, risk tables."""
    if df.empty:
        return _empty_figure(title, height)

    text = df.map(lambda x: f"{x:{fmt}}" if pd.notna(x) else "")

    fig = go.Figure(go.Heatmap(
        z=df.values, x=df.columns.tolist(), y=df.index.tolist(),
        colorscale=colorscale, text=text.values, texttemplate="%{text}",
        hovertemplate="Row: %{y}<br>Col: %{x}<br>Value: %{text}<extra></extra>",
    ))

    fig.update_layout(
        title=title, height=height, template=CHART_TEMPLATE,
        margin=CHART_MARGINS, font=CHART_FONT,
    )
    return fig


def metric_row(metrics: list):
    """Display a row of st.metric cards.
    metrics = [{"label": "S&P 500", "value": "5,842", "delta": "+0.3%"}, ...]"""
    cols = st.columns(len(metrics))
    for col, m in zip(cols, metrics):
        col.metric(
            label=m.get("label", ""),
            value=m.get("value", ""),
            delta=m.get("delta", None),
        )


def yield_curve_chart(curve_df: pd.DataFrame, height: int = 400) -> go.Figure:
    """Yield curve lines overlaid. curve_df has maturities as index, snapshots as columns."""
    if curve_df.empty:
        return _empty_figure("Yield Curve", height)

    fig = go.Figure()
    for i, col in enumerate(curve_df.columns):
        fig.add_trace(go.Scatter(
            x=curve_df.index, y=curve_df[col],
            name=str(col), mode="lines+markers",
            line=dict(color=COLORS[i % len(COLORS)], width=2),
        ))

    fig.update_layout(
        title="US Treasury Yield Curve", height=height, template=CHART_TEMPLATE,
        margin=CHART_MARGINS, font=CHART_FONT,
        yaxis_title="Yield (%)",
        xaxis_title="Maturity",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


def sortable_table(df: pd.DataFrame, title: str, color_columns: list = None):
    """Styled dataframe with color coding on specified columns."""
    st.subheader(title)
    if df.empty:
        st.info("No data available.")
        return

    if color_columns:
        def color_val(val):
            if isinstance(val, (int, float)):
                if val > 0:
                    return "color: #00CC96"
                elif val < 0:
                    return "color: #EF553B"
            elif isinstance(val, str):
                positive_words = ["Improving", "Accumulating", "Positive", "Strengthening", "Inflow"]
                negative_words = ["Deteriorating", "Depleting", "Negative", "Weakening", "Outflow"]
                if val in positive_words:
                    return "color: #00CC96"
                elif val in negative_words:
                    return "color: #EF553B"
            return ""

        styled = df.style.map(color_val, subset=[c for c in color_columns if c in df.columns])
        st.dataframe(styled, use_container_width=True)
    else:
        st.dataframe(df, use_container_width=True)


def step_chart(x, y, title: str, yaxis_title: str = None, height: int = 400) -> go.Figure:
    """Step chart for implied rate paths."""
    fig = go.Figure(go.Scatter(
        x=x, y=y, mode="lines+markers",
        line=dict(shape="hv", color=COLORS[0], width=2),
        marker=dict(size=8),
    ))

    fig.update_layout(
        title=title, height=height, template=CHART_TEMPLATE,
        margin=CHART_MARGINS, font=CHART_FONT,
        yaxis_title=yaxis_title,
        hovermode="x",
    )
    return fig


def _empty_figure(title: str, height: int = 400) -> go.Figure:
    """Return an empty figure with a message."""
    fig = go.Figure()
    fig.update_layout(
        title=title, height=height, template=CHART_TEMPLATE,
        margin=CHART_MARGINS, font=CHART_FONT,
        annotations=[dict(text="No data available", showarrow=False,
                          xref="paper", yref="paper", x=0.5, y=0.5,
                          font=dict(size=20, color="gray"))],
    )
    return fig
