#!/usr/bin/env python3
"""
Simple Jupyter notebook to HTML converter
"""

import argparse
import json
import os
import uuid
from pathlib import Path


def convert_notebook_to_html(notebook_path: str, output_dir: str = "stats"):
    """Convert a Jupyter notebook to a basic HTML page"""

    # Read the notebook file
    with open(notebook_path, encoding="utf-8") as f:
        notebook = json.load(f)

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Get notebook name for GitHub link
    notebook_name = Path(notebook_path).stem

    # Generate HTML content
    html_content = generate_html(notebook, notebook_name)

    # Write HTML file
    output_path = os.path.join(output_dir, f"{notebook_name}.html")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"Converted {notebook_path} to {output_path}")
    return output_path


def extract_code_label(source_text, is_first_code_cell=False):
    """Extract label for code block"""
    if is_first_code_cell:
        return "Imports and Default Layout"
    return "code"


def extract_title_from_notebook(notebook):
    """Extract title from the first markdown cell"""
    cells = notebook.get("cells", [])

    for cell in cells:
        if cell.get("cell_type") == "markdown":
            source = cell.get("source", [])
            if isinstance(source, list):
                source_text = "".join(source)
            else:
                source_text = str(source)

            # Look for the first header
            lines = source_text.strip().split("\n")
            for line in lines:
                line = line.strip()
                if line.startswith("# "):
                    return line[2:].strip()

            # If no header found, use first non-empty line
            for line in lines:
                line = line.strip()
                if line:
                    return line

    return "Converted Notebook"


def generate_html(notebook, notebook_name=""):
    """Generate basic HTML from notebook content"""

    # Extract title from notebook
    title = extract_title_from_notebook(notebook)

    # Generate header HTML
    header_html = f"""<header class='header'>
        <a href="index.html">go back</a>
        <nav>
            <a href="https://github.com/piebro/openstreetmap-statistics/blob/master/notebooks/{notebook_name}.ipynb">notebook</a>
            <a href="https://github.com/piebro/openstreetmap-statistics/">about</a>
        </nav>
    </header>"""

    # Generate footer HTML
    footer_html = """<footer class='footer'>
        <p><a href="https://github.com/piebro/openstreetmap-statistics/">View on GitHub</a></p>
    </footer>"""

    # <meta name="viewport" content="width=1024, initial-scale=1.0, user-scalable=yes">
    html_template = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    
    <title>{title}</title>
    <link rel="stylesheet" href="notebook_styles.css">
    <script src="https://cdn.plot.ly/plotly-3.0.1.min.js" charset="utf-8"></script>
    <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism.min.css" rel="stylesheet" />
    <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-core.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/plugins/autoloader/prism-autoloader.min.js"></script>
</head>
<body>
    {header}
    {content}
    {footer}
    <script>
        function toggleCode(button) {{
            const content = button.nextElementSibling;
            const isExpanded = content.classList.contains('expanded');
            
            if (isExpanded) {{
                content.classList.remove('expanded');
                button.classList.remove('expanded');
            }} else {{
                content.classList.add('expanded');
                button.classList.add('expanded');
            }}
        }}
    </script>
</body>
</html>"""

    content_parts = []
    first_code_cell = True

    # Process each cell
    for cell in notebook.get("cells", []):
        cell_type = cell.get("cell_type", "")
        source = cell.get("source", [])

        # Join source lines
        if isinstance(source, list):
            source_text = "".join(source)
        else:
            source_text = str(source)

        if cell_type == "markdown":
            # Simple markdown processing (just convert headers)
            processed_content = process_simple_markdown(source_text)
            content_parts.append(f'<div class="cell markdown-cell">{processed_content}</div>')

        elif cell_type == "code":
            # Skip empty code cells
            if not source_text.strip():
                continue

            # Extract label for the code block
            code_label = extract_code_label(source_text, first_code_cell)
            first_code_cell = False
            code_id = f"code-{uuid.uuid4().hex[:8]}"

            code_html = f'''<div class="code">
                <div class="code-toggle" onclick="toggleCode(this)">{escape_html(code_label)}</div>
                <div class="code-content" id="{code_id}">
                    <pre><code class="language-python">{escape_html(source_text)}</code></pre>
                </div>
            </div>'''

            # Add outputs if they exist
            outputs = cell.get("outputs", [])
            output_html = ""
            if outputs:
                for output in outputs:
                    output_html += process_output(output)

            content_parts.append(f'<div class="cell code-cell">{code_html}{output_html}</div>')

    return html_template.format(title=title, header=header_html, content="\n".join(content_parts), footer=footer_html)


def process_output(output):
    """Process notebook cell output and return HTML"""
    output_html = ""

    # Handle different output types
    if output.get("output_type") == "display_data" and "data" in output:
        data = output["data"]

        # Handle Plotly plots
        if "application/vnd.plotly.v1+json" in data:
            print("plotly")
            plotly_data = data["application/vnd.plotly.v1+json"]
            plot_id = f"plotly-div-{uuid.uuid4().hex[:8]}"

            # Extract the actual plot data and layout
            plot_data = plotly_data.get("data", [])
            plot_layout = plotly_data.get("layout", {})
            plot_config = plotly_data.get("config", {})

            # Create a div for the plot and JavaScript to render it
            # Configure for responsive behavior
            plot_config = plot_config or {}
            plot_config.update({"responsive": True, "displayModeBar": True, "displaylogo": False})

            output_html += f'''
<div id="{plot_id}" class="plotly-output"></div>
<script>
    Plotly.newPlot('{plot_id}', {json.dumps(plot_data)}, {json.dumps(plot_layout)}, {json.dumps(plot_config)});
</script>
'''

        # Handle HTML tables (pandas DataFrames)
        elif "text/html" in data:
            print("html")
            html_data = data["text/html"]
            if isinstance(html_data, list):
                html_data = "".join(html_data)

            # Check if it's a pandas DataFrame table
            if "<table" in html_data and "dataframe" in html_data:
                table_id = f"table-{uuid.uuid4().hex[:8]}"
                output_html += f'<div class="table-output" id="{table_id}">{html_data}</div>'
                output_html += f"""
<script>
    // Format numbers in table with commas
    document.addEventListener('DOMContentLoaded', function() {{
        const table = document.querySelector('#{table_id} table');
        if (table) {{
            const cells = table.querySelectorAll('td:not(:first-child)');
            cells.forEach(cell => {{
                const text = cell.textContent.trim();
                if (/^\\d+$/.test(text)) {{
                    const number = parseInt(text);
                    cell.textContent = number.toLocaleString();
                }}
            }});
        }}
    }});
</script>
"""
            else:
                output_html += f'<div class="output">{html_data}</div>'

    # Handle execute_result outputs (like DataFrame displays)
    elif output.get("output_type") == "execute_result" and "data" in output:
        print("execute_result")
        data = output["data"]

        # Handle HTML tables (pandas DataFrames)
        if "text/html" in data:
            html_data = data["text/html"]
            if isinstance(html_data, list):
                html_data = "".join(html_data)

            # Check if it's a pandas DataFrame table
            if "<table" in html_data and "dataframe" in html_data:
                table_id = f"table-{uuid.uuid4().hex[:8]}"
                output_html += f'<div class="table-output" id="{table_id}">{html_data}</div>'
                output_html += f"""
<script>
    // Format numbers in table with commas
    document.addEventListener('DOMContentLoaded', function() {{
        const table = document.querySelector('#{table_id} table');
        if (table) {{
            const cells = table.querySelectorAll('td:not(:first-child)');
            cells.forEach(cell => {{
                const text = cell.textContent.trim();
                if (/^\\d+$/.test(text)) {{
                    const number = parseInt(text);
                    cell.textContent = number.toLocaleString();
                }}
            }});
        }}
    }});
</script>
"""
            else:
                output_html += f'<div class="output">{html_data}</div>'

        # Handle text output as fallback
        elif "text/plain" in data:
            text_data = data["text/plain"]
            if isinstance(text_data, list):
                text_data = "".join(text_data)

            # Skip widget-related text outputs
            if not ("Progress(" in text_data or "Widget(" in text_data or "Layout(" in text_data):
                output_html += f'<div class="output">{escape_html(text_data)}</div>'

    # Handle text output from stream
    elif "text" in output:
        print("text")
        output_text = "".join(output["text"]) if isinstance(output["text"], list) else output["text"]
        output_html += f'<div class="output">{escape_html(output_text)}</div>'

    return output_html


def process_simple_markdown(text):
    """Basic markdown processing for headers"""
    lines = text.split("\n")
    processed_lines = []

    for line in lines:
        line = line.strip()
        if line.startswith("# "):
            processed_lines.append(f"<h1>{escape_html(line[2:])}</h1>")
        elif line.startswith("## "):
            processed_lines.append(f"<h2>{escape_html(line[3:])}</h2>")
        elif line.startswith("### "):
            processed_lines.append(f"<h3>{escape_html(line[4:])}</h3>")
        elif line:
            processed_lines.append(f"<p>{escape_html(line)}</p>")

    return "\n".join(processed_lines)


def escape_html(text):
    """Escape HTML special characters"""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#x27;")
    )


def generate_index_html(output_dir: str = "stats"):
    """Generate an index.html file listing all available statistics"""

    # Find all HTML files in the output directory (excluding index.html)
    html_files = []
    if os.path.exists(output_dir):
        for file in os.listdir(output_dir):
            if file.endswith(".html") and file != "index.html":
                html_files.append(file)

    # Sort files alphabetically
    html_files.sort()

    # Generate navigation links
    nav_links = []
    for html_file in html_files:
        # Create a display name from filename
        display_name = html_file.replace(".html", "").replace("_", " ").title()
        nav_links.append(f'            <li><a href="{html_file}">{display_name}</a></li>')

    # Generate header HTML
    header_html = """<header class='header'>
        <nav>
            <a href="https://github.com/piebro/openstreetmap-statistics/">GitHub</a>
            <a href="https://github.com/piebro/openstreetmap-statistics/blob/master/README.md">About</a>
        </nav>
    </header>"""

    # Generate footer HTML
    footer_html = """<footer class='footer'>
        <p>&copy; 2025 OpenStreetMap Statistics. Data from OpenStreetMap contributors.</p>
        <p><a href="https://github.com/piebro/openstreetmap-statistics/">View on GitHub</a></p>
    </footer>"""

    # Generate index content
    index_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=1024, initial-scale=1.0, user-scalable=yes">
    <title>OpenStreetMap Statistics</title>
    <link rel="stylesheet" href="notebook_styles.css">
</head>
<body>
    {header_html}
    
    <div class="cell markdown-cell">
        <h1>OpenStreetMap Statistics</h1>
        <p>Explore various statistics and analyses of OpenStreetMap data.</p>
    </div>
    
    <div class="cell markdown-cell">
        <h2>Available Statistics</h2>
        <ul>
{chr(10).join(nav_links)}
        </ul>
    </div>
    
    {footer_html}
</body>
</html>"""

    # Write index.html file
    index_path = os.path.join(output_dir, "index.html")
    with open(index_path, "w", encoding="utf-8") as f:
        f.write(index_html)

    print(f"Generated index file at {index_path}")
    return index_path


def main():
    """Main CLI function"""
    parser = argparse.ArgumentParser(description="Convert Jupyter notebook to HTML")
    parser.add_argument("notebook", help="Path to the Jupyter notebook file")
    parser.add_argument("--output-dir", "-o", default="stats", help="Output directory for HTML files (default: stats)")

    args = parser.parse_args()

    # Check if notebook file exists
    if not os.path.exists(args.notebook):
        print(f"Error: Notebook file '{args.notebook}' not found")
        return 1

    try:
        convert_notebook_to_html(args.notebook, args.output_dir)
        return 0
    except Exception as e:
        print(f"Error converting notebook: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
