import argparse
import json
import shutil
import time
from pathlib import Path

import duckdb


def sql_case_statement_from_rules(rules_file, column_name):
    """Generate SQL CASE statement from JSON rules file."""
    rules_path = Path(rules_file)
    if not rules_path.exists():
        return column_name

    with rules_path.open(encoding="utf-8") as f:
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


def get_corporation_case_statement(corp_file):
    """Generate SQL CASE statement for corporation mapping."""
    with Path(corp_file).open(encoding="utf-8") as f:
        corp_data = json.load(f)

    escape = lambda s: s.replace("'", "''")
    conditions = []

    for corp_name, (_, user_list) in corp_data.items():
        for user in user_list:
            conditions.append(f"WHEN user_name = '{escape(user)}' THEN '{escape(corp_name)}'")

    if not conditions:
        return "NULL"

    conditions_str = "\n".join(conditions)
    return f"CASE\n{conditions_str}\nELSE NULL\nEND"


def get_column_expressions():
    """Get SQL expressions for all enrichment columns."""
    expressions = {}

    # Coordinates
    expressions["mid_pos_x"] = "CAST(ROUND(((bottom_left_lon + top_right_lon) / 2 + 180) % 360) AS INTEGER)"
    expressions["mid_pos_y"] = "CAST(ROUND(((bottom_left_lat + top_right_lat) / 2 + 90) % 180) AS INTEGER)"

    # Bot detection
    expressions["bot"] = "COALESCE(tags['bot'] = 'yes', false)"

    # Created by
    expressions["created_by"] = sql_case_statement_from_rules(
        "config/replace_rules_created_by.json", "tags['created_by']"
    )

    # Imagery used - split on semicolon, clean URL encoding, apply rules to each element
    imagery_case_statement = sql_case_statement_from_rules("config/replace_rules_imagery_and_source.json", "x")
    expressions["imagery_used"] = f"""
    CASE 
        WHEN tags['imagery_used'] IS NOT NULL AND tags['imagery_used'] != '' 
        THEN list_transform(
            list_filter(
                list_transform(
                    string_split(replace(replace(tags['imagery_used'], '%20%', ' '), '%2c%', ','), ';'),
                    x -> trim(x)
                ),
                x -> x != ''
            ),
            x -> {imagery_case_statement}
        )
        ELSE NULL
    END
    """

    # Hashtags - split on semicolon, convert to lowercase, return as array
    expressions["hashtags"] = """
    CASE 
        WHEN tags['hashtags'] IS NOT NULL AND tags['hashtags'] != ''
        THEN string_split(lower(tags['hashtags']), ';')
        ELSE NULL
    END
    """

    # Source - split on multiple separators and apply rules
    source_case_statement = sql_case_statement_from_rules("config/replace_rules_imagery_and_source.json", "x")
    expressions["source"] = f"""
    CASE 
        WHEN tags['source'] IS NOT NULL AND tags['source'] != '' 
        THEN list_transform(
            list_filter(
                list_transform(
                    regexp_split_to_array(tags['source'], ';| / | & |, |\||\+'),
                    x -> trim(x)
                ),
                x -> x != ''
            ),
            x -> {source_case_statement}
        )
        ELSE NULL
    END
    """

    # Mobile OS detection
    expressions["mobile_os"] = """
    CASE 
        WHEN lower(tags['created_by']) LIKE '%android%' THEN 'Android'
        WHEN lower(tags['created_by']) LIKE '%ios%' THEN 'iOS'
        ELSE NULL
    END
    """

    # StreetComplete quest normalization
    expressions["streetcomplete_quest"] = """
    CASE 
        WHEN tags['created_by'] = 'StreetComplete' AND tags['StreetComplete:quest_type'] IS NOT NULL 
        THEN CASE tags['StreetComplete:quest_type']
            WHEN 'AddAccessibleForPedestrians' THEN 'AddProhibitedForPedestrians'
            WHEN 'AddWheelChairAccessPublicTransport' THEN 'AddWheelchairAccessPublicTransport'
            WHEN 'AddWheelChairAccessToilets' THEN 'AddWheelchairAccessPublicTransport'
            WHEN 'AddSidewalks' THEN 'AddSidewalk'
            ELSE tags['StreetComplete:quest_type']
        END
        ELSE NULL
    END
    """

    # All tags extraction - split each tag name on ':' and take the first part
    expressions["all_tags"] = "array_distinct(list_transform(map_keys(tags), x -> split_part(x, ':', 1)))"

    # Corporation mapping
    expressions["corporation"] = get_corporation_case_statement("config/corporation_contributors.json")

    return expressions


def enrich_table(input_path, output_path, overwrite):
    """Enrich parquet table with additional columns."""
    start_time = time.time()
    output_path_obj = Path(output_path)

    if output_path_obj.exists():
        if overwrite:
            print(f"Removing existing directory: {output_path}")
            shutil.rmtree(output_path_obj)
        else:
            raise FileExistsError(f"Output directory '{output_path}' already exists. Use --overwrite to delete it.")

    # Get column expressions
    expressions = get_column_expressions()

    # Build SQL
    base_columns = ["edit_count", "user_name", "month", "year"]
    enriched_columns = [f"{expr} as {name}" for name, expr in expressions.items()]
    all_columns = base_columns + enriched_columns
    columns_sql = ",\n            ".join(all_columns)

    sql_query = f"""
    COPY (
        SELECT 
            {columns_sql}
        FROM '{input_path}/year=*/month=*/*.parquet'
    ) TO '{output_path}' 
    (FORMAT PARQUET, PARTITION_BY (year, month));
    """

    print(f"Adding columns: {', '.join(expressions.keys())}")

    duckdb.sql(sql_query)

    # Calculate and display elapsed time
    elapsed_time = time.time() - start_time
    minutes = int(elapsed_time // 60)
    seconds = int(elapsed_time % 60)
    print(f"Enrichment completed successfully in {minutes}:{seconds:02d} minutes")


def main():
    parser = argparse.ArgumentParser(description="Enrich OSM changeset parquet tables with all available columns")
    parser.add_argument("input_path", help="Path to the input parquet dataset directory")
    parser.add_argument("output_path", help="Path to the output enriched dataset directory")
    parser.add_argument("--overwrite", action="store_true", help="Delete output directory if it exists")

    args = parser.parse_args()

    enrich_table(args.input_path, args.output_path, args.overwrite)


if __name__ == "__main__":
    main()
