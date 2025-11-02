OpenStreetMap Dataset Information for Custom Query Generation

This data is available for querying using DuckDB WASM in the browser. All data is stored as parquet files on Hugging Face at: https://huggingface.co/datasets/piebro/osm-data

IMPORTANT FILE SIZE NOTES:
- changeset_data: Each monthly parquet file is approximately 20 MB
- changeset_comments_data: Total dataset is 70-150 MB across all files
- notes_data: Total dataset is 30-36 MB across all files
- notes_comments_data: Total dataset is 40-60 MB across all files

Keep these file sizes in mind when writing queries - downloading large files may take time in the browser.

AVAILABLE DATASETS:

1. CHANGESET DATA
Location: https://huggingface.co/datasets/piebro/osm-data/resolve/main/changeset_data/year=YYYY/month=M/data_0.parquet
Partitioned by: year and month

Key Columns:
- changeset_id: ID of the changeset
- edit_count: Number of edits in the changeset
- user_name: OSM contributor username
- year, month: Time partitioning columns
- created_by: Normalized editing software name (e.g., "iD", "JOSM", "StreetComplete")
- device_type: Classification (desktop_editor, mobile_editor, tool, other)
- bot: Boolean indicating if changeset was made by a bot
- mid_pos_x, mid_pos_y: Discretized coordinates (0-360, 0-180)
- imagery_used: Array of imagery sources
- hashtags: Array of hashtags from the changeset
- source: Array of data sources used
- mobile_os: Mobile OS (Android, iOS, or NULL)
- streetcomplete_quest: StreetComplete quest type (if applicable)
- organised_team: Team/corporation affiliation
- for_profit: Boolean for for-profit organisations

2. CHANGESET COMMENTS DATA
Location: https://huggingface.co/datasets/piebro/osm-data/resolve/main/changeset_comments_data/part-0.parquet (multiple parts)

Key Columns:
- changeset_id: ID of changeset (join key with changeset_data)
- date: Timestamp of comment (UTC)
- user_name: Username of commenter
- text: Comment content

3. NOTES DATA
Location: https://huggingface.co/datasets/piebro/osm-data/resolve/main/notes_data/part-0.parquet (multiple parts)

Key Columns:
- note_id: Unique note identifier
- lat, lon: Latitude and longitude
- created_at: Creation timestamp (UTC)
- closed_at: Close timestamp (NULL if still open)
- mid_pos_x, mid_pos_y: Discretized coordinates (0-360, 0-180)

4. NOTES COMMENTS DATA
Location: https://huggingface.co/datasets/piebro/osm-data/resolve/main/notes_comments_data/part-0.parquet (multiple parts)

Key Columns:
- note_id: Note ID (join key with notes_data)
- action: Action type (opened, commented, closed, reopened)
- timestamp: Action timestamp (UTC)
- user_name: Username
- text: Comment content

DUCKDB QUERY PATTERNS:

Reading Parquet Files:
IMPORTANT: Wildcards ("*") cannot be used in HTTPS URLs. You must explicitly list each file path.

```sql
-- Single file
FROM read_parquet('https://huggingface.co/datasets/piebro/osm-data/resolve/main/changeset_data/year=2025/month=1/data_0.parquet')

-- Multiple files (array) - each file must be listed explicitly
FROM read_parquet([
    'https://huggingface.co/datasets/piebro/osm-data/resolve/main/changeset_data/year=2025/month=1/data_0.parquet',
    'https://huggingface.co/datasets/piebro/osm-data/resolve/main/changeset_data/year=2025/month=2/data_0.parquet'
])

-- INVALID: This will NOT work with HTTPS URLs
-- FROM read_parquet('https://huggingface.co/datasets/piebro/osm-data/resolve/main/changeset_data/year=*/month=*/*.parquet')
```

Working with Array Columns (imagery_used, hashtags, source):
```sql
-- Expand arrays with unnest
SELECT unnest(hashtags) as hashtag, edit_count
FROM read_parquet('...')
WHERE hashtags IS NOT NULL
```

Common Aggregations:
```sql
-- Cast to appropriate types for cleaner output
CAST(SUM(edit_count) as BIGINT) as "Total Edits"
CAST(COUNT(DISTINCT user_name) as BIGINT) as "Contributors"
COUNT(*) as "Total Records"
```

Time Grouping:
```sql
-- Format year-month for display
CONCAT(year, '-', LPAD(CAST(month as VARCHAR), 2, '0')) as months

-- Filter by date
WHERE year >= 2024
WHERE year = 2025 AND month >= 1
```

EXAMPLE QUERIES:

Top Software by Contributors (2025):
```sql
SELECT
    created_by as "Software",
    CAST(COUNT(DISTINCT user_name) as BIGINT) as "Contributors"
FROM read_parquet('https://huggingface.co/datasets/piebro/osm-data/resolve/main/changeset_data/year=2025/month=1/data_0.parquet')
WHERE created_by IS NOT NULL
GROUP BY created_by
ORDER BY "Contributors" DESC
LIMIT 10
```

Monthly Edit Trend:
```sql
SELECT
    CONCAT(year, '-', LPAD(CAST(month as VARCHAR), 2, '0')) as "Month",
    CAST(SUM(edit_count) as BIGINT) as "Total Edits"
FROM read_parquet([
    'https://huggingface.co/datasets/piebro/osm-data/resolve/main/changeset_data/year=2024/month=12/data_0.parquet',
    'https://huggingface.co/datasets/piebro/osm-data/resolve/main/changeset_data/year=2025/month=1/data_0.parquet'
])
GROUP BY year, month
ORDER BY year, month
```

Bot vs Human Contributors:
```sql
SELECT
    bot,
    CAST(COUNT(DISTINCT user_name) as BIGINT) as "Contributors",
    CAST(SUM(edit_count) as BIGINT) as "Edits"
FROM read_parquet('https://huggingface.co/datasets/piebro/osm-data/resolve/main/changeset_data/year=2025/month=1/data_0.parquet')
GROUP BY bot
```

TIPS FOR QUERY GENERATION:
- Always use HTTPS URLs for parquet files
- CRITICAL: Wildcards ("*") do NOT work in HTTPS URLs - explicitly list each file path
- Remember that queries download data files - consider file sizes
- Use array syntax [...] for multiple files
- CAST aggregations to BIGINT or INTEGER for cleaner output
- Use meaningful column aliases with quotes: "Total Edits"
- Filter early (WHERE clauses) to reduce processing
- Use IS NOT NULL when filtering on nullable columns
- For recent data, focus on recent years/months to minimize download size
