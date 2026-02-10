import argparse
import json
import time
from pathlib import Path

import duckdb


def sql_case_statement_from_rules(rules_file, column_name):
    """Generate SQL CASE statement from JSON rules file."""
    with Path(rules_file).open(encoding="utf-8") as f:
        rules = json.load(f)

    escape = lambda s: s.replace("'", "''")

    patterns = {
        "aliases": lambda p: f"= '{escape(p)}'",
        "starts_with": lambda p: f"LIKE '{escape(p)}%'",
        "ends_with": lambda p: f"LIKE '%{escape(p)}'",
        "contains": lambda p: f"LIKE '%{escape(p)}%'",
    }

    conditions = []
    for name, info in rules.items():
        for rule_type, rule_patterns in info.items():
            if rule_type in patterns:
                for pattern in rule_patterns if isinstance(rule_patterns, list) else [rule_patterns]:
                    conditions.append(f"WHEN {column_name} {patterns[rule_type](pattern)} THEN '{escape(name)}'")

    if len(conditions) == 0:
        return column_name

    conditions_str = "\n".join(conditions)
    return f"CASE\n{conditions_str}\nELSE {column_name}\nEND"


def get_created_by_case_statement():
    """Generate SQL CASE statement for created_by normalization."""
    return sql_case_statement_from_rules("config/replace_rules_created_by.json", "main.tags['created_by']")


def get_device_type_case_statement():
    """Generate SQL CASE statement for device type classification."""
    with Path("config/replace_rules_created_by.json").open(encoding="utf-8") as f:
        rules = json.load(f)

    escape = lambda s: s.replace("'", "''")
    conditions = []

    for name, info in rules.items():
        if "type" in info:
            device_type = info["type"]
            if device_type in ["desktop_editor", "mobile_editor", "tool"]:
                conditions.append(f"WHEN created_by = '{escape(name)}' THEN '{device_type}'")

    conditions_str = "\n".join(conditions)
    return f"CASE\n{conditions_str}\nELSE 'other'\nEND"


def get_imagery_used_case_statement():
    # split on semicolon, clean URL encoding and apply rules to each element
    imagery_case_statement = sql_case_statement_from_rules("config/replace_rules_imagery_and_source.json", "x")
    return f"""
    CASE 
        WHEN main.tags['imagery_used'] IS NOT NULL AND main.tags['imagery_used'] != '' 
        THEN list_transform(
            list_filter(
                list_transform(
                    string_split(replace(replace(main.tags['imagery_used'], '%20%', ' '), '%2c%', ','), ';'),
                    x -> trim(x)
                ),
                x -> x != ''
            ),
            x -> {imagery_case_statement}
        )
        ELSE NULL
    END
    """


def get_hashtags_case_statement():
    return """
    CASE 
        WHEN main.tags['hashtags'] IS NOT NULL AND main.tags['hashtags'] != ''
        THEN string_split(lower(main.tags['hashtags']), ';')
        ELSE NULL
    END
    """


def get_source_case_statement():
    # split on multiple separators and apply rules to each element
    source_case_statement = sql_case_statement_from_rules("config/replace_rules_imagery_and_source.json", "x")
    return f"""
    CASE 
        WHEN main.tags['source'] IS NOT NULL AND main.tags['source'] != '' 
        THEN list_transform(
            list_filter(
                list_transform(
                    regexp_split_to_array(main.tags['source'], ';| / | & |, |\\||\\+'),
                    x -> trim(x)
                ),
                x -> x != ''
            ),
            x -> {source_case_statement}
        )
        ELSE NULL
    END
    """


def get_mobile_os_case_statement():
    return """
    CASE 
        WHEN lower(main.tags['created_by']) LIKE '%android%' THEN 'Android'
        WHEN lower(main.tags['created_by']) LIKE '%ios%' THEN 'iOS'
        ELSE NULL
    END
    """


def get_streetcomplete_quest_case_statement():
    return """
    CASE 
        WHEN main.tags['StreetComplete:quest_type'] IS NULL THEN NULL
        WHEN main.tags['StreetComplete:quest_type'] = 'AddAccessibleForPedestrians' THEN 'AddProhibitedForPedestrians'
        WHEN main.tags['StreetComplete:quest_type'] = 'AddWheelChairAccessPublicTransport' THEN 'AddWheelchairAccessPublicTransport'
        WHEN main.tags['StreetComplete:quest_type'] = 'AddWheelChairAccessToilets' THEN 'AddWheelchairAccessPublicTransport'
        WHEN main.tags['StreetComplete:quest_type'] = 'AddSidewalks' THEN 'AddSidewalk'
        ELSE main.tags['StreetComplete:quest_type']
    END
    """

def get_maproulette_challenge_case_statement():
    return f"""
    CASE 
        WHEN main.tags['comment'] IS NOT NULL AND main.tags['comment'] ~ 'mpr\.lt/c/\\d+/' 
        THEN (
            SELECT (regexp_matches(main.tags['comment'], 'mpr\.lt/c/(\\d+)/'))[1]
        )
        ELSE NULL
    END
    """

def create_organised_team_lookup_table():
    """Create a temporary table for efficient organised team user mapping."""
    with Path("config/organised_teams_contributors.json").open(encoding="utf-8") as f:
        team_data = json.load(f)

    # Build list of (user_name, team, for_profit) tuples
    user_team_mapping = []
    for team_name, team_info in team_data.items():
        for user in team_info["usernames"]:
            user_team_mapping.append((user, team_name, team_info["for_profit"]))

    if not user_team_mapping:
        return

    # Create temporary table with the mapping
    escape = lambda s: s.replace("'", "''")
    values = ", ".join(
        [f"('{escape(user)}', '{escape(team)}', {for_profit})" for user, team, for_profit in user_team_mapping]
    )

    create_table_sql = f"""
    CREATE OR REPLACE TEMPORARY TABLE organised_team_lookup (user_name VARCHAR, team VARCHAR, for_profit BOOLEAN);
    INSERT INTO organised_team_lookup VALUES {values};
    CREATE INDEX IF NOT EXISTS idx_team_user ON organised_team_lookup(user_name);
    """

    duckdb.sql(create_table_sql)


def get_column_expressions():
    """Get SQL expressions for all enrichment columns."""
    expressions = {}
    expressions["mid_pos_x"] = "CAST(ROUND(((main.bottom_left_lon + main.top_right_lon) / 2 + 180) % 360) AS INTEGER)"
    expressions["mid_pos_y"] = "CAST(ROUND(((main.bottom_left_lat + main.top_right_lat) / 2 + 90) % 180) AS INTEGER)"
    expressions["bot"] = "COALESCE(main.tags['bot'] = 'yes', false)"
    expressions["created_by"] = get_created_by_case_statement()
    expressions["device_type"] = get_device_type_case_statement()
    expressions["imagery_used"] = get_imagery_used_case_statement()
    expressions["hashtags"] = get_hashtags_case_statement()
    expressions["source"] = get_source_case_statement()
    expressions["mobile_os"] = get_mobile_os_case_statement()
    expressions["streetcomplete_quest"] = get_streetcomplete_quest_case_statement()
    expressions["maproulette_challenge"] = get_maproulette_challenge_case_statement()
    # split each tag name on ':' and take the first part
    expressions["all_tags"] = "array_distinct(list_transform(map_keys(main.tags), x -> split_part(x, ':', 1)))"
    expressions["organised_team"] = "team_lookup.team"
    expressions["for_profit"] = "team_lookup.for_profit"
    return expressions


def get_column_sql(expressions):
    base_columns = ["main.changeset_id", "main.edit_count", "main.user_name", "main.month", "main.year"]
    enriched_columns = [f"{expr} as {name}" for name, expr in expressions.items()]
    all_columns = base_columns + enriched_columns
    columns_sql = ",\n            ".join(all_columns)
    return columns_sql


def get_available_months(input_path, year):
    """Get all available months for a given year from the input data."""
    query = f"""
    SELECT DISTINCT month
    FROM '{input_path}/year={year}/month=*/*.parquet'
    ORDER BY month
    """
    result = duckdb.sql(query).fetchall()
    return [row[0] for row in result]


def get_all_available_year_months(input_path):
    """Get all available year-month combinations from the input data."""
    query = f"""
    SELECT DISTINCT year, month
    FROM '{input_path}/year=*/month=*/*.parquet'
    ORDER BY year, month
    """
    result = duckdb.sql(query).fetchall()
    return [(row[0], row[1]) for row in result]


def get_last_year_month(input_path, offset=0):
    """Get a recent year-month from the input data.

    Args:
        input_path: Path to the parquet data
        offset: How many months back from the latest (0 = latest, 1 = second-to-last, etc.)
    """
    query = f"""
    SELECT DISTINCT year, month
    FROM '{input_path}/year=*/month=*/*.parquet'
    ORDER BY year DESC, month DESC
    LIMIT 1 OFFSET {offset}
    """
    result = duckdb.sql(query).fetchone()
    return (result[0], result[1]) if result else None


def enrich_table_year_month(input_path, output_path, year, month, expressions):
    """Enrich parquet table with additional columns for a specific year-month."""
    print(f"Processing year-month: {year}-{month:02d}")
    sql_query = f"""
    COPY (
        SELECT
            {get_column_sql(expressions)}
        FROM '{input_path}/year=*/month=*/*.parquet' main
        LEFT JOIN organised_team_lookup team_lookup ON main.user_name = team_lookup.user_name
        WHERE main.year = {year} AND main.month = {month}
    ) TO '{output_path}'
    (FORMAT PARQUET, PARTITION_BY (year, month), OVERWRITE_OR_IGNORE true);
    """
    # Use single thread to create exactly 1 file per partition and preserve insertion order to create the row order for different runs
    duckdb.sql("SET preserve_insertion_order = true")
    duckdb.sql("SET threads = 1")
    duckdb.sql(sql_query)
    duckdb.sql("SET preserve_insertion_order TO DEFAULT")
    duckdb.sql("SET threads TO DEFAULT")


def main():
    parser = argparse.ArgumentParser(
        description="Enrich OSM changeset parquet tables. Can process specific year-month, all months in a year, or all available data."
    )
    parser.add_argument("input_path", help="Path to the input parquet dataset directory")
    parser.add_argument("output_path", help="Path to the output enriched dataset directory")
    parser.add_argument(
        "year", type=int, nargs="?", help="Year to process (optional, processes all years if not provided)"
    )
    parser.add_argument(
        "month", type=int, nargs="?", help="Month to process (optional, processes all months if not provided)"
    )
    parser.add_argument(
        "--last-complete-month",
        action="store_true",
        help="Process only the last complete month (skips the most recent potentially incomplete month)",
    )
    args = parser.parse_args()

    start_time = time.time()
    print("Creating organised team lookup table for efficient organised team mapping")
    create_organised_team_lookup_table()
    expressions = get_column_expressions()
    print(f"Adding columns: {', '.join(expressions.keys())}")

    # Determine which year-month combinations to process
    if args.last_complete_month:
        # Process the second-to-last month (skip the most recent incomplete month)
        last_ym = get_last_year_month(args.input_path, offset=1)
        year_months = [last_ym]
    elif args.year is None:
        year_months = get_all_available_year_months(args.input_path)
        print(f"Processing all available data: {len(year_months)} year-month combinations")
    elif args.month is None:
        months = get_available_months(args.input_path, args.year)
        year_months = [(args.year, month) for month in months]
        print(f"Processing year {args.year}: {len(months)} months")
    else:
        year_months = [(args.year, args.month)]

    for year, month in year_months:
        enrich_table_year_month(args.input_path, args.output_path, year, month, expressions)

    elapsed_time = time.time() - start_time
    print(f"Enrichment completed successfully in {int(elapsed_time // 60)}:{int(elapsed_time % 60):02d} minutes")


if __name__ == "__main__":
    main()
