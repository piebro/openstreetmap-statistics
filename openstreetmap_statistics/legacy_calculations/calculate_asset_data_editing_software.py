import argparse
import time
from pathlib import Path

import duckdb
import util


def get_software_editor_type_lists(data_path):
    """Create editor type classification from replace_rules_created_by.json"""
    name_to_info = util.load_json(Path("config") / "replace_rules_created_by.json")

    device_type = {"desktop_editors": [], "mobile_editors": [], "tools": [], "other/unspecified": []}

    for name, info in name_to_info.items():
        if "type" in info:
            if info["type"] == "desktop_editor":
                device_type["desktop_editors"].append(name)
            elif info["type"] == "mobile_editor":
                device_type["mobile_editors"].append(name)
            elif info["type"] == "tool":
                device_type["tools"].append(name)

    # Get all created_by values from the database to find unspecified ones
    created_by_sql = f"""
        SELECT DISTINCT created_by
        FROM '{data_path}/year=*/month=*/*.parquet'
        WHERE created_by IS NOT NULL
    """
    created_by_df = duckdb.sql(created_by_sql).df()
    all_created_by = set(created_by_df["created_by"].values)

    # Find software not classified in any category
    classified = set()
    for category_list in [device_type["desktop_editors"], device_type["mobile_editors"], device_type["tools"]]:
        classified.update(category_list)

    device_type["other/unspecified"] = list(all_created_by - classified)

    return device_type


def calculate_created_by_device_type_contributor_count_monthly(data_path):
    """Calculate monthly contributor counts by device type"""
    editor_type_lists = get_software_editor_type_lists(data_path)

    results = []
    for device_type_name, software_list in editor_type_lists.items():
        if not software_list:
            continue

        # Properly escape quotes in software names for SQL
        software_filter = "', '".join([name.replace("'", "''") for name in software_list])
        sql_query = f"""
            SELECT
                CONCAT(year, '-', LPAD(CAST(month as VARCHAR), 2, '0')) as months,
                COUNT(DISTINCT user_name) as contributors
            FROM '{data_path}/year=*/month=*/*.parquet'
            WHERE created_by IN ('{software_filter}')
            GROUP BY year, month
            ORDER BY year, month
        """
        df = duckdb.sql(sql_query).df()
        df = df.rename(columns={"contributors": device_type_name})
        results.append(df)

    # Merge all device type results
    final_df = results[0]
    for df in results[1:]:
        final_df = final_df.merge(df, on="months", how="outer")

    final_df = final_df.fillna(0)
    util.save_df_as_json("created_by_device_type_contributor_count_monthly", final_df)


def calculate_created_by_device_type_edit_count_monthly(data_path):
    """Calculate monthly edit counts by device type"""
    editor_type_lists = get_software_editor_type_lists(data_path)

    results = []
    for device_type_name, software_list in editor_type_lists.items():
        if not software_list:
            continue

        # Properly escape quotes in software names for SQL
        software_filter = "', '".join([name.replace("'", "''") for name in software_list])
        sql_query = f"""
            SELECT
                CONCAT(year, '-', LPAD(CAST(month as VARCHAR), 2, '0')) as months,
                CAST(SUM(edit_count) as BIGINT) as edits
            FROM '{data_path}/year=*/month=*/*.parquet'
            WHERE created_by IN ('{software_filter}')
            GROUP BY year, month
            ORDER BY year, month
        """
        df = duckdb.sql(sql_query).df()
        df = df.rename(columns={"edits": device_type_name})
        results.append(df)

    # Merge all device type results
    final_df = results[0]
    for df in results[1:]:
        final_df = final_df.merge(df, on="months", how="outer")

    final_df = final_df.fillna(0)
    util.save_df_as_json("created_by_device_type_edit_count_monthly", final_df, add_empty_months=3)
    util.save_percent("created_by_device_type_edit_count_monthly", "general_edit_count_monthly", "months", "edits")


def calculate_created_by_top_100_edit_count_monthly(data_path):
    """Calculate top 100 editing software by monthly edit count"""
    sql_query = f"""
        WITH software_monthly_edits AS (
            SELECT
                CONCAT(year, '-', LPAD(CAST(month as VARCHAR), 2, '0')) as months,
                created_by,
                CAST(SUM(edit_count) as BIGINT) as edits
            FROM '{data_path}/year=*/month=*/*.parquet'
            WHERE created_by IS NOT NULL
            GROUP BY year, month, created_by
        ),
        software_total_edits AS (
            SELECT
                created_by,
                SUM(edits) as total_edits
            FROM software_monthly_edits
            GROUP BY created_by
            ORDER BY total_edits DESC
            LIMIT 100
        )
        SELECT
            months,
            created_by,
            edits
        FROM software_monthly_edits
        WHERE created_by IN (SELECT created_by FROM software_total_edits)
        ORDER BY months, created_by
    """

    df = duckdb.sql(sql_query).df()
    df_pivoted = df.pivot(index="months", columns="created_by", values="edits").reset_index()
    df_pivoted = df_pivoted.fillna(0)

    # Sort columns by total edits (descending)
    columns_to_sort = [col for col in df_pivoted.columns if col != "months"]
    column_totals = df_pivoted[columns_to_sort].sum().sort_values(ascending=False)

    df_pivoted = df_pivoted[["months", *column_totals.index.tolist()]]

    util.save_df_as_json("created_by_top_100_edit_count_monthly", df_pivoted, add_empty_months=3)


def calculate_created_by_top_100_contributor_count_monthly(data_path):
    """Calculate top 100 editing software by monthly contributor count"""
    sql_query = f"""
        WITH software_monthly_contributors AS (
            SELECT
                CONCAT(year, '-', LPAD(CAST(month as VARCHAR), 2, '0')) as months,
                created_by,
                COUNT(DISTINCT user_name) as contributors
            FROM '{data_path}/year=*/month=*/*.parquet'
            WHERE created_by IS NOT NULL
            GROUP BY year, month, created_by
        ),
        software_total_contributors AS (
            SELECT
                created_by,
                CAST(SUM(contributors) as BIGINT) as total_contributors
            FROM software_monthly_contributors
            GROUP BY created_by
            ORDER BY total_contributors DESC
            LIMIT 100
        )
        SELECT
            months,
            created_by,
            contributors
        FROM software_monthly_contributors
        WHERE created_by IN (SELECT created_by FROM software_total_contributors)
        ORDER BY months, created_by
    """

    df = duckdb.sql(sql_query).df()
    df_pivoted = df.pivot(index="months", columns="created_by", values="contributors").reset_index()
    df_pivoted = df_pivoted.fillna(0)

    columns_to_sort = [col for col in df_pivoted.columns if col != "months"]
    df_pivoted[columns_to_sort] = df_pivoted[columns_to_sort].astype(int)

    column_totals = df_pivoted[columns_to_sort].sum().sort_values(ascending=False)
    df_pivoted = df_pivoted[["months", *column_totals.index.tolist()]]

    util.save_df_as_json("created_by_top_100_contributor_count_monthly", df_pivoted, add_empty_months=3)


def calculate_created_by_top_100_changeset_count_monthly(data_path):
    """Calculate top 100 editing software by monthly changeset count"""
    sql_query = f"""
        WITH software_monthly_changesets AS (
            SELECT
                CONCAT(year, '-', LPAD(CAST(month as VARCHAR), 2, '0')) as months,
                created_by,
                CAST(COUNT(*) as INTEGER) as changesets
            FROM '{data_path}/year=*/month=*/*.parquet'
            WHERE created_by IS NOT NULL
            GROUP BY year, month, created_by
        ),
        software_total_changesets AS (
            SELECT
                created_by,
                SUM(changesets) as total_changesets
            FROM software_monthly_changesets
            GROUP BY created_by
            ORDER BY total_changesets DESC
            LIMIT 100
        )
        SELECT
            months,
            created_by,
            changesets
        FROM software_monthly_changesets
        WHERE created_by IN (SELECT created_by FROM software_total_changesets)
        ORDER BY months, created_by
    """

    df = duckdb.sql(sql_query).df()
    df_pivoted = df.pivot(index="months", columns="created_by", values="changesets").reset_index()
    df_pivoted = df_pivoted.fillna(0)

    # Sort columns by total changesets (descending)
    columns_to_sort = [col for col in df_pivoted.columns if col != "months"]
    df_pivoted[columns_to_sort] = df_pivoted[columns_to_sort].astype(int)

    column_totals = df_pivoted[columns_to_sort].sum().sort_values(ascending=False)
    df_pivoted = df_pivoted[["months", *column_totals.index.tolist()]]

    util.save_df_as_json("created_by_top_100_changeset_count_monthly", df_pivoted, add_empty_months=3)


def calculate_created_by_top_100_contributor_count_yearly(data_path):
    """Calculate top 100 editing software by yearly contributor count"""
    sql_query = f"""
        WITH software_yearly_contributors AS (
            SELECT
                CAST(year as VARCHAR) as years,
                created_by,
                COUNT(DISTINCT user_name) as contributors
            FROM '{data_path}/year=*/month=*/*.parquet'
            WHERE created_by IS NOT NULL
            GROUP BY year, created_by
        ),
        software_total_contributors AS (
            SELECT
                created_by,
                SUM(contributors) as total_contributors
            FROM software_yearly_contributors
            GROUP BY created_by
            ORDER BY total_contributors DESC
            LIMIT 100
        )
        SELECT
            years,
            created_by,
            contributors
        FROM software_yearly_contributors
        WHERE created_by IN (SELECT created_by FROM software_total_contributors)
        ORDER BY years, created_by
    """

    df = duckdb.sql(sql_query).df()
    df_pivoted = df.pivot(index="years", columns="created_by", values="contributors").reset_index()
    df_pivoted = df_pivoted.fillna(0)

    # Sort columns by total contributors (descending)
    columns_to_sort = [col for col in df_pivoted.columns if col != "years"]
    df_pivoted[columns_to_sort] = df_pivoted[columns_to_sort].astype(int)

    column_totals = df_pivoted[columns_to_sort].sum().sort_values(ascending=False)
    df_pivoted = df_pivoted[["years", *column_totals.index.tolist()]]

    util.save_df_as_json("created_by_top_100_contributor_count_yearly", df_pivoted)


def calculate_created_by_top_100_new_contributor_count_monthly(data_path):
    """Calculate top 100 editing software by monthly new contributor count"""
    sql_query = f"""
        WITH user_first_appearance_per_software AS (
            SELECT 
                user_name,
                created_by,
                MIN(year) as first_year,
                MIN(month) as first_month
            FROM '{data_path}/year=*/month=*/*.parquet'
            WHERE created_by IS NOT NULL
            GROUP BY user_name, created_by
        ),
        first_time_users_per_software AS (
            SELECT 
                user_name, 
                created_by, 
                first_year as year, 
                first_month as month
            FROM user_first_appearance_per_software
        ),
        software_monthly_new_contributors AS (
            SELECT
                CONCAT(year, '-', LPAD(CAST(month as VARCHAR), 2, '0')) as months,
                created_by,
                COUNT(DISTINCT user_name) as new_contributors
            FROM first_time_users_per_software
            GROUP BY year, month, created_by
        ),
        software_total_new_contributors AS (
            SELECT
                created_by,
                SUM(new_contributors) as total_new_contributors
            FROM software_monthly_new_contributors
            GROUP BY created_by
            ORDER BY total_new_contributors DESC
            LIMIT 100
        )
        SELECT
            months,
            created_by,
            new_contributors
        FROM software_monthly_new_contributors
        WHERE created_by IN (SELECT created_by FROM software_total_new_contributors)
        ORDER BY months, created_by
    """

    df = duckdb.sql(sql_query).df()
    df_pivoted = df.pivot(index="months", columns="created_by", values="new_contributors").reset_index()
    df_pivoted = df_pivoted.fillna(0)

    # Sort columns by total new contributors (descending)
    columns_to_sort = [col for col in df_pivoted.columns if col != "months"]
    df_pivoted[columns_to_sort] = df_pivoted[columns_to_sort].astype(int)

    column_totals = df_pivoted[columns_to_sort].sum().sort_values(ascending=False)
    df_pivoted = df_pivoted[["months", *column_totals.index.tolist()]]

    util.save_df_as_json("created_by_top_100_new_contributor_count_monthly", df_pivoted, add_empty_months=3)


def save_editing_software(data_path):
    """Main function to calculate all editing software statistics"""
    # calculate_created_by_device_type_contributor_count_monthly(data_path)
    # calculate_created_by_device_type_edit_count_monthly(data_path)
    # calculate_created_by_top_100_edit_count_monthly(data_path)
    # calculate_created_by_top_100_contributor_count_monthly(data_path)
    # calculate_created_by_top_100_changeset_count_monthly(data_path)
    # calculate_created_by_top_100_contributor_count_yearly(data_path)

    calculate_created_by_top_100_new_contributor_count_monthly(data_path)

    # Generate top 10 versions and other derived statistics
    #######util.save_top_k(10, "created_by_top_100_contributor_count_monthly")
    #######util.save_top_k(10, "created_by_top_100_new_contributor_count_monthly")
    # util.save_top_k(10, "created_by_top_100_edit_count_monthly")
    # util.save_top_k(10, "created_by_top_100_changeset_count_monthly")

    ## Generate yearly versions
    # util.save_monthly_to_yearly("created_by_top_100_edit_count_monthly")

    ## Generate total statistics
    util.save_total("created_by_top_100_contributor_count_monthly")
    # util.save_total("created_by_top_100_edit_count_monthly")
    # util.save_total("created_by_top_100_changeset_count_monthly")
    util.save_total("created_by_top_100_new_contributor_count_monthly")

    ## Generate merged yearly + total data
    util.save_merged_yearly_total_data(
        "created_by_top_100_contributor_count_yearly", "created_by_top_100_new_contributor_count_monthly_total"
    )
    # util.save_merged_yearly_total_data("created_by_top_100_edit_count_yearly", "created_by_top_100_new_contributor_count_monthly_total")

    ## Generate percentage statistics
    # util.save_percent("created_by_top_10_edit_count_monthly", "general_edit_count_monthly", "months", "edits")
    # util.save_percent(
    #    "created_by_top_10_contributor_count_monthly", "general_contributor_count_monthly", "months", "contributors"
    # )

    ## Generate accumulated versions
    # util.save_accumulated("created_by_top_10_new_contributor_count_monthly")
    # util.save_accumulated("created_by_top_10_edit_count_monthly")
    # util.save_accumulated("created_by_top_10_changeset_count_monthly")


def main():
    parser = argparse.ArgumentParser(description="Calculate editing software statistics from OSM changeset data")
    parser.add_argument("data_path", help="Path to the directory containing parquet files")

    args = parser.parse_args()

    print(f"Processing editing software data from: {args.data_path}")

    start_time = time.time()
    save_editing_software(args.data_path)

    elapsed_time = time.time() - start_time
    minutes = int(elapsed_time // 60)
    seconds = int(elapsed_time % 60)
    print(f"Editing software processing completed in {minutes}:{seconds:02d} minutes")


if __name__ == "__main__":
    main()
