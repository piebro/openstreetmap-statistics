import re
import uuid


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
        html += (
            f'<button id="btn-{uid}-{i}" class="tab-btn {active}" onclick="switchTab(\'{uid}\', {i})">{name}</button>'
        )
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
    html += (
        f"<script>initializeTableInstance('{uid}', {len(dataframes_dict)}, 10, {str(show_search).lower()});</script>"
    )
    return html
