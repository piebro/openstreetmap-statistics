import re
import uuid
from dataclasses import dataclass
from typing import Any

import duckdb
import plotly.graph_objects as go


def format_dataframe_for_display(df):
    """Format numeric columns in dataframe for better display."""
    df_display = df.copy()

    for col in df_display.columns:
        if df_display[col].dtype in ["int64", "float64", "int32", "float32"]:
            df_display[col] = df_display[col].map("{:,.0f}".format)

    return df_display


def get_tables_html(dataframes_dict, show_search=True, center_columns=[]):
    """Create a tabbed interface for DataFrames with pagination and search.

    Args:
        dataframes_dict: Dictionary of dataframes to display
        show_search: Whether to show search functionality
        center_columns: List of column names to center-align, or dict mapping table names to column lists
    """
    uid = str(uuid.uuid4())[:8]

    html = f'<div class="tabs-container" id="tabs-{uid}"><div class="tab-buttons">'

    for i, name in enumerate(dataframes_dict.keys()):
        active = "active" if i == 0 else ""
        html += f'<button id="btn-{uid}-{i}" class="tab-btn {active}" onclick="switchTab(\'{uid}\', {i})">{name}</button>'
    html += '</div><div class="tab-content">'

    for i, (name, df) in enumerate(dataframes_dict.items()):
        display_style = "block" if i == 0 else "none"
        html += f'<div id="pane-{uid}-{i}" class="tab-pane" style="display: {display_style};"><div class="controls-container">'

        if show_search:
            html += f'<input type="text" id="search-{uid}-{i}" class="search-box" placeholder="Search in {name}..." onkeyup="updateTable(\'{uid}\', {i})">'
        else:
            html += "<div></div>"

        html += f"""
        <div class="pagination-controls">
            <select id="pagesize-{uid}-{i}" class="page-size-select" onchange="updateTable('{uid}', {i})">
                <option value="10" selected>10</option>
                <option value="25">25</option>
                <option value="50">50</option>
                <option value="100">100</option>
            </select>
            <div class="pagination-nav">
                <button class="page-btn" onclick="changePage('{uid}', {i}, -1)" id="prev-{uid}-{i}">‹</button>
                <span id="pageinfo-{uid}-{i}" class="page-info"></span>
                <button class="page-btn" onclick="changePage('{uid}', {i}, 1)" id="next-{uid}-{i}">›</button>
            </div>
        </div>
        """  # noqa: RUF001
        html += '</div><div class="table-wrapper">'

        df_formatted = format_dataframe_for_display(df)
        table_html = df_formatted.to_html(index=False, classes="data-table", table_id=f"table-{uid}-{i}", escape=False)

        def make_sortable_header(match):
            header_content = match.group(1)
            return f'<th class="sortable sorting" onclick="sortTable(\'{uid}\', {i}, this)">{header_content}</th>'

        table_html = re.sub(r"<th>([^<]+)</th>", make_sortable_header, table_html)

        # Add inline CSS for centering specific columns
        center_css = ""
        if center_columns:
            for col_name in center_columns:
                if col_name in df_formatted.columns:
                    col_index = list(df_formatted.columns).index(col_name)
                    center_css += f"#table-{uid}-{i} td:nth-child({col_index + 1}) {{ text-align: center !important; }}"

        if center_css:
            table_html = f"<style>{center_css}</style>" + table_html
        html += table_html
        html += "</div></div>"
    html += "</div></div>"
    html += f"<script>initializeTableInstance('{uid}', {len(dataframes_dict)}, 10, {str(show_search).lower()});</script>"
    return html


DEFAULT_LAYOUT = dict(
    margin=dict(l=55, r=55, b=55, t=55),
    font=dict(family="Times", size=15),
    title_x=0.5,
    paper_bgcolor="#f5f2f0",
    plot_bgcolor="#f5f2f0",
    xaxis=dict(tickcolor="black", linecolor="black", showgrid=True, gridcolor="darkgray", zerolinecolor="darkgray"),
    yaxis=dict(
        tickcolor="black",
        linecolor="black",
        showgrid=True,
        gridcolor="darkgray",
        zerolinecolor="darkgray",
        rangemode="tozero",
    ),
)


@dataclass
class FigureConfig:
    """Configuration for a single figure/button."""

    title: str
    y_col: str
    group_col: str
    query: str
    label: str = None
    y_unit_hover_template: str = None


def execute_query_and_process(config):
    """Execute SQL query and process the data."""
    df = duckdb.sql(config.query).df()
    group_list = df[config.group_col].unique().tolist() if not df.empty else []
    return df, group_list


def add_traces_to_figure(fig, df, config, group_list, is_first=False):
    """Add traces to figure for a given configuration."""
    for group in group_list:
        group_data = df[df[config.group_col] == group]
        y_unit_hover_template = config.y_unit_hover_template if config.y_unit_hover_template else f"{config.y_col}"
        fig.add_trace(
            go.Scatter(
                x=group_data["months"],
                y=group_data[config.y_col],
                name=group,
                visible=is_first,
                hovertemplate=f"{group}" + f"<br>%{{x}}<br>%{{y:,}} {y_unit_hover_template}<extra></extra>",
            )
        )


def create_button_config(config: FigureConfig, trace_start_idx: int, trace_count: int, total_traces: int) -> dict[str, Any]:
    """Create button configuration for updatemenus."""
    visibility = [False] * total_traces
    for i in range(trace_start_idx, trace_start_idx + trace_count):
        visibility[i] = True

    return {
        "label": config.label,
        "args": [{"visible": visibility}, {"title.text": config.title, "yaxis.title.text": config.y_col}],
        "method": "update",
    }


def get_figure(figure_configs, xaxis_title):
    fig = go.Figure()
    buttons = []
    current_trace_idx = 0

    for i, config in enumerate(figure_configs):
        df, group_list = execute_query_and_process(config)
        add_traces_to_figure(fig, df, config, group_list, is_first=(i == 0))

        button_config = create_button_config(
            config, current_trace_idx, len(group_list), sum(len(execute_query_and_process(cfg)[1]) for cfg in figure_configs)
        )
        buttons.append(button_config)
        current_trace_idx += len(group_list)

    layout_config = {
        "title": buttons[0]["args"][1]["title.text"],
        "xaxis_title": xaxis_title,
        "yaxis_title": buttons[0]["args"][1]["yaxis.title.text"],
        **DEFAULT_LAYOUT,
    }
    if len(buttons) > 1:
        layout_config["updatemenus"] = [{"type": "buttons", "buttons": buttons}]

    fig.update_layout(layout_config)
    return fig
