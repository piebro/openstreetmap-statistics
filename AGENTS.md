# Creating New Statistics

The project analyzes OpenStreetMap data through preprocessed parquet files that provide detailed information about changesets, their comments, notes, and note comments.

## Available Datasets

The project includes 4 different datasets:

1. **changeset_data** - Main dataset containing all changesets with enriched metadata
   - Location: `../changeset_data/year=*/month=*/*.parquet`
   - Partitioned by year and month
   - Contains enriched columns like `created_by`, `device_type`, `imagery_used`, etc.

2. **changeset_comments_data** - Comments on changesets (changeset discussions)
   - Location: `../changeset_comments_data/*.parquet`
   - Not partitioned, stored as flat parquet files
   - Join with changeset_data using `changeset_id`

3. **notes_data** - Notes on the map
   - Location: `../notes_data/*.parquet`
   - Not partitioned, stored as flat parquet files
   - Contains information about map notes including their location and status

4. **notes_comments_data** - Comments on notes
   - Location: `../notes_comments_data/*.parquet`
   - Not partitioned, stored as flat parquet files
   - Join with notes_data using `note_id`

## Dataset Structure

### Changeset Data

The changeset dataset is located at `../changeset_data/year=*/month=*/*.parquet` and contains the following key columns:

### Base Columns
- `changeset_id` - ID of the changeset
- `edit_count` - Number of edits in the changeset
- `user_name` - OSM contributor username
- `year` - Year of the changeset
- `month` - Month of the changeset

### Enriched Columns (added by `scripts/changeset_raw_data_to_data.py`)
- `created_by` - Normalized editing software name
- `device_type` - Classification: `desktop_editor`, `mobile_editor`, `tool`, `other`
- `bot` - Boolean indicating if the changeset was made by a bot
- `mid_pos_x`, `mid_pos_y` - Discretized coordinates (0-360, 0-180)
- `imagery_used` - Array of imagery sources used
- `hashtags` - Array of hashtags from the changeset
- `source` - Array of data sources used
- `mobile_os` - Mobile OS detection (`Android`, `iOS`, or `NULL`)
- `streetcomplete_quest` - Normalized StreetComplete quest type
- `all_tags` - Array of all tag prefixes used
- `organised_team` - Organised team/corporation affiliation if applicable
- `for_profit` - Boolean indicating if the changeset was made by a for-profit organisation

### Changeset Comments Data

The changeset comments dataset is located at `../changeset_comments_data/*.parquet` and contains comments from OpenStreetMap changeset discussions:

**Columns:**
- `changeset_id` - ID of the changeset being commented on (join key with changeset_data)
- `date` - Timestamp when the comment was made (UTC)
- `user_name` - Username of the person who made the comment
- `text` - Content of the comment

### Notes Data

The notes dataset is located at `../notes_data/*.parquet` and contains information about map notes:

**Columns:**
- `note_id` - Unique identifier for the note
- `lat` - Latitude of the note location
- `lon` - Longitude of the note location
- `created_at` - Timestamp when the note was created (UTC)
- `closed_at` - Timestamp when the note was closed (NULL if still open)
- `mid_pos_x` - Discretized longitude coordinate (0-360)
- `mid_pos_y` - Discretized latitude coordinate (0-180)

### Notes Comments Data

The notes comments dataset is located at `../notes_comments_data/*.parquet` and contains comments on map notes:

**Columns:**
- `note_id` - ID of the note being commented on (join key with notes_data)
- `action` - Action type (e.g., "opened", "commented", "closed", "reopened")
- `timestamp` - Timestamp when the comment/action was made (UTC)
- `user_name` - Username of the person who made the comment/action
- `text` - Content of the comment

## Project Setup

### Standard Notebook Initialization

```python
import duckdb
import util

util.init()
```

## Core Patterns and Examples

### Pattern 1: Basic Time Series Analysis

**Example: Monthly Contributors/Edits/Changesets**

```python
df = duckdb.sql("""
WITH user_first_appearance AS (
    SELECT
        user_name,
        year,
        month,
        ROW_NUMBER() OVER (PARTITION BY user_name ORDER BY year, month) as rn
    FROM (
        SELECT DISTINCT user_name, year, month
        FROM '../changeset_data/year=*/month=*/*.parquet'
    )
),
first_appearances AS (
    SELECT user_name, year, month
    FROM user_first_appearance
    WHERE rn = 1
),
monthly_metrics AS (
    SELECT
        year,
        month,
        CONCAT(year, '-', LPAD(CAST(month as VARCHAR), 2, '0')) as months,
        COUNT(DISTINCT user_name) as Contributors,
        CAST(SUM(edit_count) as BIGINT) as Edits,
        CAST(COUNT(*) AS INTEGER) as Changesets
    FROM '../changeset_data/year=*/month=*/*.parquet'
    GROUP BY year, month
),
monthly_new_contributors AS (
    SELECT
        year,
        month,
        COUNT(DISTINCT user_name) as "New Contributors"
    FROM first_appearances
    GROUP BY year, month
),
combined_metrics AS (
    SELECT
        m.year,
        m.month,
        m.months,
        m.Contributors,
        COALESCE(n."New Contributors", 0) as "New Contributors",
        m.Edits,
        m.Changesets
    FROM monthly_metrics m
    LEFT JOIN monthly_new_contributors n ON m.year = n.year AND m.month = n.month
)
SELECT
    months,
    Contributors,
    "New Contributors",
    Edits,
    Changesets,
    SUM("New Contributors") OVER (ORDER BY year, month) as "Accumulated Contributors",
    SUM(Edits) OVER (ORDER BY year, month) as "Accumulated Edits",
    SUM(Changesets) OVER (ORDER BY year, month) as "Accumulated Changesets"
FROM combined_metrics
ORDER BY year, month
""").df()

util.show_figure(
    [
        util.FigureConfig(
            title="Monthly Contributors",
            label="Contributors",
            x_col="months",
            y_col="Contributors",
            query_or_df=df,
        ),
        util.FigureConfig(
            title="Monthly Edits",
            label="Edits", 
            x_col="months",
            y_col="Edits",
            query_or_df=df,
        )
    ]
)
```

### Pattern 2: Top N Analysis with Groups

**Example: Top 10 Editing Software**

```python
df = duckdb.sql("""
WITH top_software AS (
    SELECT created_by
    FROM (
        SELECT
            created_by,
            COUNT(DISTINCT user_name) as total_contributors
        FROM '../changeset_data/year=*/month=*/*.parquet'
        WHERE created_by IS NOT NULL
        GROUP BY created_by
        ORDER BY total_contributors DESC
        LIMIT 10
    )
),
monthly_contributors AS (
    SELECT 
        year,
        month,
        CONCAT(year, '-', LPAD(CAST(month as VARCHAR), 2, '0')) as months,
        created_by,
        COUNT(DISTINCT user_name) as "Contributors",
        SUM(edit_count) as "Edit Count"
    FROM '../changeset_data/year=*/month=*/*.parquet'
    WHERE created_by IN (SELECT created_by FROM top_software)
    GROUP BY year, month, created_by
)
SELECT * FROM monthly_contributors
ORDER BY year, month, created_by
""").df()

util.show_figure(
    [
        util.FigureConfig(
            title="Monthly Contributors by Top 10 Editing Software",
            label="Contributors",
            x_col="months",
            y_col="Contributors",
            group_col="created_by",
            query_or_df=df,
        )
    ]
)
```

### Pattern 3: Percentage Analysis

**Example: Percentage Contributors by Software**

```python
df = duckdb.sql("""
WITH top_software AS (
    SELECT created_by
    FROM (
        SELECT
            created_by,
            COUNT(DISTINCT user_name) as total_contributors
        FROM '../changeset_data/year=*/month=*/*.parquet'
        WHERE created_by IS NOT NULL
        GROUP BY created_by
        ORDER BY total_contributors DESC
        LIMIT 10
    )
),
monthly_software_contributors AS (
    SELECT 
        CONCAT(year, '-', LPAD(CAST(month as VARCHAR), 2, '0')) as months,
        created_by,
        COUNT(DISTINCT user_name) as contributors
    FROM '../changeset_data/year=*/month=*/*.parquet'
    WHERE created_by IN (SELECT created_by FROM top_software)
    GROUP BY year, month, created_by
),
monthly_total_contributors AS (
    SELECT 
        CONCAT(year, '-', LPAD(CAST(month as VARCHAR), 2, '0')) as months,
        COUNT(DISTINCT user_name) as total_contributors
    FROM '../changeset_data/year=*/month=*/*.parquet'
    WHERE created_by IS NOT NULL
    GROUP BY year, month
)
SELECT 
    msc.months,
    msc.created_by,
    ROUND((msc.contributors * 100.0) / mtc.total_contributors, 2) as 'Percentage of Contributors'
FROM monthly_software_contributors msc
JOIN monthly_total_contributors mtc ON msc.months = mtc.months
ORDER BY msc.months, msc.created_by""").df()

util.show_figure(
    [
        util.FigureConfig(
            title="Monthly Percentage of Contributors by Top 10 Editing Software",
            x_col="months",
            y_col="Percentage of Contributors",
            y_unit_hover_template="%",
            group_col="created_by",
            query_or_df=df,
        )
    ]
)
```

### Pattern 4: Geographical Analysis

**Example: Edit Distribution Map**

```python
df = duckdb.sql("""
SELECT
    mid_pos_x as x,
    mid_pos_y as y,
    SUM(edit_count) as z
FROM '../changeset_data/year=*/month=*/*.parquet'
WHERE mid_pos_x IS NOT NULL AND mid_pos_y IS NOT NULL
GROUP BY mid_pos_x, mid_pos_y
""").df()

util.show_figure(
    [
        util.FigureConfig(
            title="Edit Count",
            x_col="x",
            y_col="y",
            z_col="z",
            query_or_df=df,
            plot_type="map",
        )
    ]
)
```

### Pattern 5: Cohort Analysis

**Example: Contributor Attrition Rate**

```python
sql_query = """
WITH user_first_edit AS (
    SELECT 
        user_name,
        MIN(year) as first_edit_year
    FROM '../changeset_data/year=*/month=*/*.parquet'
    GROUP BY user_name
),
year_user_edits AS (
    SELECT 
        year,
        user_name,
        SUM(edit_count) as total_edits
    FROM '../changeset_data/year=*/month=*/*.parquet'
    GROUP BY year, user_name
),
merged_data AS (
    SELECT 
        y.year,
        y.user_name,
        y.total_edits,
        u.first_edit_year,
        CASE 
            WHEN u.first_edit_year % 2 = 1 THEN 
                CONCAT('First edit in: ', CAST(u.first_edit_year AS VARCHAR), '-', CAST(u.first_edit_year + 1 AS VARCHAR))
            ELSE 
                CONCAT('First edit in: ', CAST(u.first_edit_year - 1 AS VARCHAR), '-', CAST(u.first_edit_year AS VARCHAR))
        END as first_edit_period
    FROM year_user_edits y
    JOIN user_first_edit u ON y.user_name = u.user_name
),
grouped_data AS (
    SELECT 
        CAST(year AS VARCHAR) as years,
        first_edit_period,
        SUM(total_edits) as total_edits
    FROM merged_data
    GROUP BY year, first_edit_period
)
SELECT 
    years,
    first_edit_period,
    total_edits,
    ROUND(100.0 * total_edits / SUM(total_edits) OVER (PARTITION BY years), 2) as percentage
FROM grouped_data
ORDER BY years, first_edit_period
"""

df = duckdb.sql(sql_query).df()

util.show_figure(
    [
        util.FigureConfig(
            title="Percentage of Edits by First Edit Period",
            label="Percentage",
            x_col="years",
            y_col="percentage",
            query_or_df=df,
            group_col="first_edit_period",
            plot_type="bar",
        )
    ]
)
```

### Pattern 6: Interactive Tables

**Example: Top 100 Editing Software Table**

```python
query = """
WITH user_first_year AS (
	SELECT 
		user_name,
		created_by,
		MIN(year) as first_year
	FROM '../changeset_data/year=*/month=*/*.parquet'
	WHERE created_by IS NOT NULL
	GROUP BY user_name, created_by
),
software_totals AS (
	SELECT
		created_by as "Editing Software",
		CAST(SUM(edit_count) as BIGINT) as total_edits_all_time,
		CAST(SUM(CASE WHEN year >= 2021 THEN edit_count ELSE 0 END) as BIGINT) as total_edits_2021_now,
		CAST(COUNT(DISTINCT user_name) as BIGINT) as total_contributors_all_time,
		CAST(COUNT(DISTINCT CASE WHEN year >= 2021 THEN user_name END) as BIGINT) as total_contributors_2021_now
	FROM '../changeset_data/year=*/month=*/*.parquet'
	WHERE created_by IS NOT NULL
	GROUP BY created_by
),
yearly_metrics AS (
	SELECT
		d.year,
		d.created_by as "Editing Software",
		CAST(SUM(d.edit_count) as BIGINT) as "Edits",
		CAST(COUNT(DISTINCT d.user_name) as BIGINT) as "Contributors",
		CAST(COUNT(DISTINCT CASE WHEN ufy.first_year = d.year THEN d.user_name END) as BIGINT) as "New Contributors"
	FROM '../changeset_data/year=*/month=*/*.parquet' d
	LEFT JOIN user_first_year ufy 
		ON d.user_name = ufy.user_name AND d.created_by = ufy.created_by
	WHERE d.created_by IS NOT NULL
	GROUP BY d.year, d.created_by
)
SELECT 
	ym.year,
	ym."Editing Software",
	ym."Edits",
	ym."New Contributors",
	ym."Contributors",
	st.total_edits_all_time as "Total Edits",
	st.total_edits_2021_now as "Total Edits (2021 - Now)",
	st.total_contributors_all_time as "Total Contributors",
	st.total_contributors_2021_now as "Total Contributors (2021 - Now)"
FROM yearly_metrics ym
JOIN software_totals st
	ON ym."Editing Software" = st."Editing Software"
ORDER BY year DESC, "Edits" DESC
"""
df = duckdb.sql(query).df()

top_100_contributors = df.groupby("Editing Software")["Total Contributors"].first().nlargest(100)
top_100_contributors_2021_now = df.groupby("Editing Software")["Total Contributors (2021 - Now)"].first().nlargest(100)

table_configs = [
    util.TableConfig(
        title="Top 100 Contributors",
        query_or_df=df[df["Editing Software"].isin(top_100_contributors.index)],
        x_axis_col="year",
        y_axis_col="Editing Software", 
        value_col="Contributors",
        center_columns=["Rank", "Editing Software"],
        sum_col="Total Contributors",
    ),
    util.TableConfig(
        title="Top 100 Contributors 2021 - Now",
        query_or_df=df[(df["Editing Software"].isin(top_100_contributors_2021_now.index)) & (df["year"] >= 2021)],
        x_axis_col="year",
        y_axis_col="Editing Software",
        value_col="Contributors",
        center_columns=["Rank", "Editing Software"],
        sum_col="Total Contributors (2021 - Now)",
    ),
]

util.show_tables(table_configs)
```

### Pattern 7: Organised Teams Analysis

**Example: Corporate Contributors Analysis**

```python
df = duckdb.sql("""
WITH monthly_total AS (
    SELECT 
        year,
        month,
        CONCAT(year, '-', LPAD(CAST(month as VARCHAR), 2, '0')) as months,
        COUNT(DISTINCT user_name) as total_contributors,
        CAST(SUM(edit_count) as BIGINT) as total_edits
    FROM '../changeset_data/year=*/month=*/*.parquet'
    GROUP BY year, month
),
monthly_corporation AS (
    SELECT 
        year,
        month,
        CONCAT(year, '-', LPAD(CAST(month as VARCHAR), 2, '0')) as months,
        COUNT(DISTINCT user_name) as corp_contributors,
        CAST(SUM(edit_count) as BIGINT) as corp_edits
    FROM '../changeset_data/year=*/month=*/*.parquet'
    WHERE organised_team IS NOT NULL
    GROUP BY year, month
)
SELECT 
    mt.months,
    mt.total_contributors as "Total Contributors",
    COALESCE(mc.corp_contributors, 0) as "Corporation Contributors",
    mt.total_edits as "Total Edits",
    COALESCE(mc.corp_edits, 0) as "Corporation Edits",
    ROUND((COALESCE(mc.corp_contributors, 0) * 100.0) / mt.total_contributors, 2) as "Percent Contributors from Corporations",
    ROUND((COALESCE(mc.corp_edits, 0) * 100.0) / mt.total_edits, 2) as "Percent Edits from Corporations"
FROM monthly_total mt
LEFT JOIN monthly_corporation mc ON mt.year = mc.year AND mt.month = mc.month
ORDER BY mt.year, mt.month
""").df()

util.show_figure(
    [
        util.FigureConfig(
            title="Percentage of Edits from Corporations",
            label="Edits Percentage",
            x_col="months",
            y_col="Percent Edits from Corporations",
            y_unit_hover_template="%",
            query_or_df=df,
        ),
        util.FigureConfig(
            title="Percentage of Contributors from Corporations",
            label="Contributors Percentage",
            x_col="months",
            y_col="Percent Contributors from Corporations",
            y_unit_hover_template="%",
            query_or_df=df,
        ),
    ]
)
```

### Pattern 8: Source Tags and Data Quality Analysis

**Example: Source Tag Usage Trends**

```python
df = duckdb.sql("""
WITH source_expanded AS (
    SELECT 
        year,
        month,
        user_name,
        edit_count,
        unnest(source) as source_tag
    FROM '../changeset_data/year=*/month=*/*.parquet'
    WHERE source IS NOT NULL
),
top_sources AS (
    SELECT source_tag
    FROM (
        SELECT
            source_tag,
            SUM(edit_count) as total_edits
        FROM source_expanded
        GROUP BY source_tag
        ORDER BY total_edits DESC
        LIMIT 10
    )
),
monthly_source_data AS (
    SELECT 
        se.year,
        se.month,
        CONCAT(se.year, '-', LPAD(CAST(se.month as VARCHAR), 2, '0')) as months,
        se.source_tag,
        COUNT(DISTINCT se.user_name) as "Contributors",
        SUM(se.edit_count) as "Edits"
    FROM source_expanded se
    WHERE se.source_tag IN (SELECT source_tag FROM top_sources)
    GROUP BY se.year, se.month, se.source_tag
)
SELECT 
    months,
    source_tag,
    "Contributors",
    "Edits"
FROM monthly_source_data
ORDER BY year, month, source_tag
""").df()

util.show_figure(
    [
        util.FigureConfig(
            title="Monthly Edits by Top 10 Source Tags",
            label="Edits",
            x_col="months",
            y_col="Edits",
            group_col="source_tag",
            query_or_df=df,
        ),
        util.FigureConfig(
            title="Monthly Contributors by Top 10 Source Tags",
            label="Contributors",
            x_col="months",
            y_col="Contributors",
            group_col="source_tag",
            query_or_df=df,
        ),
    ]
)
```

## Utility Functions Reference

### Figure Configuration (`util.FigureConfig`)

```python
@dataclass
class FigureConfig:
    title: str                           # Chart title
    x_col: str                          # X-axis column name
    y_col: str                          # Y-axis column name
    z_col: str = None                   # Z-axis for maps
    query_or_df: str | pd.DataFrame     # SQL query or DataFrame
    group_col: str = None               # Column for grouping traces
    label: str = None                   # Label for button in multi-chart
    y_unit_hover_template: str = None   # Unit for hover template (e.g., "%")
    plot_type: str = "scatter"          # "scatter", "bar", or "map"
    trace_names: list[str] = None       # Custom ordering of traces
```

### Table Configuration (`util.TableConfig`)

```python
class TableConfig(NamedTuple):
    title: str                          # Table title
    query_or_df: str | pd.DataFrame     # SQL query or DataFrame  
    x_axis_col: str                     # Column for pivot table columns
    y_axis_col: str                     # Column for pivot table rows
    value_col: str                      # Column for pivot table values
    center_columns: tuple[str] = ()     # Columns to center-align
    sum_col: str = None                 # Column for sorting
```

### Data Reshaping with Pandas

When you need to plot multiple series from columns in wide format, use `df.melt()` to convert to long format:

```python
# Before melt (wide format):
# months    | series_1 | series_2 | series_3
# 2007-09   | 100      | 200      | 300

df_melted = df.melt(
    id_vars=['months'],           # Columns to keep as identifiers
    var_name='series_name',        # Name for new column with old column names
    value_name='value'             # Name for new column with values
)

# After melt (long format):
# months    | series_name | value
# 2007-09   | series_1    | 100
# 2007-09   | series_2    | 200
# 2007-09   | series_3    | 300

# Now can use with group_col in FigureConfig
util.show_figure([
    util.FigureConfig(
        title="Title",
        x_col="months",
        y_col="value",
        group_col="series_name",
        query_or_df=df_melted,
    )
])
```

## SQL Patterns and Best Practices

### Common SQL Patterns

1. Month Formatting: 
    ```sql
    CONCAT(year, '-', LPAD(CAST(month as VARCHAR), 2, '0')) as months
    ```

2. Window Functions for First Appearance:
   ```sql
   ROW_NUMBER() OVER (PARTITION BY user_name ORDER BY year, month) as rn
   ```

3. Accumulated Values:
   ```sql
   SUM(column) OVER (ORDER BY year, month) as "Accumulated Column"
   ```

4. Top N Selection:
   ```sql
   WITH top_n AS (
       SELECT column
       FROM table
       ORDER BY metric DESC
       LIMIT n
   )
   ```

5. Percentage Calculations:
   ```sql
   ROUND((subset_count * 100.0) / total_count, 2) as percentage
   ```

## Performance Considerations

### Query Optimization
- Use CTEs (Common Table Expressions) for complex queries
- Filter early in the query pipeline
- Use appropriate aggregations before JOINs
- Consider partitioning when accessing specific time ranges

## Advanced Features

### Dropdown Charts for Year-by-Year Analysis
```python
configs = []
for year in sorted(df["year"].unique()):
    configs.append(
        util.FigureConfig(
            title=f"Edit Count {year}",
            x_col="x",
            y_col="y",
            z_col="z",
            query_or_df=df[df["year"] == year],
            plot_type="map",
        )
    )
util.show_figure(configs, type="dropdown")
```

### Custom Trace Ordering
```python
# Get trace names ordered by last value, unique values, or first value
trace_names = util.get_trace_names(df, "group_col", "x_col", "y_col", "last")
# Options: "last", "first", "unique"

util.FigureConfig(
    # ... other parameters ...
    trace_names=trace_names
)
```

### Multi-Chart Figures with Button Navigation
```python
util.show_figure(
    [
        util.FigureConfig(title="Chart 1", label="Button 1", ...),
        util.FigureConfig(title="Chart 2", label="Button 2", ...),
        util.FigureConfig(title="Chart 3", label="Button 3", ...),
    ]
)
```

### Array Column Expansion (Unnesting)
```python
# For columns containing arrays (like source, hashtags, imagery_used)
WITH source_expanded AS (
    SELECT
        year,
        month,
        user_name,
        edit_count,
        unnest(source) as source_tag
    FROM '../changeset_data/year=*/month=*/*.parquet'
    WHERE source IS NOT NULL
)
```

### HTML Link Integration
```python
# Load replacement rules for clickable links
with open("../config/replace_rules_created_by.json") as f:
    editing_software_name_to_html_link = {
        name: f'<a href="{item["link"]}">{name}</a>' for name, item in json.load(f).items() if "link" in item
    }

df["Editing Software"] = df["Editing Software"].apply(
    lambda name: editing_software_name_to_html_link[name] if name in editing_software_name_to_html_link else name
)
# There are also ../config/replace_rules_imagery_and_source.json
```
