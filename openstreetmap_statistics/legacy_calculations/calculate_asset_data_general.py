import argparse
import time

import duckdb
import util


def calculate_contributor_count_monthly(data_path):
    """Calculate monthly contributor counts"""
    sql_query = f"""
        SELECT
            CONCAT(year, '-', LPAD(CAST(month as VARCHAR), 2, '0')) as months,
            COUNT(DISTINCT user_name) as contributors
        FROM '{data_path}/year=*/month=*/*.parquet'
        GROUP BY year, month
        ORDER BY year, month
    """
    util.save_query_as_json("general_contributor_count_monthly", sql_query)


def calculate_new_contributor_count_monthly(data_path):
    """Calculate monthly new contributor counts"""
    sql_query = f"""
        WITH user_first_appearance AS (
            SELECT
                user_name,
                year,
                month,
                ROW_NUMBER() OVER (PARTITION BY user_name ORDER BY year, month) as rn
            FROM (
                SELECT DISTINCT user_name, year, month
                FROM '{data_path}/year=*/month=*/*.parquet'
            )
        ),
        first_appearances AS (
            SELECT user_name, year, month
            FROM user_first_appearance
            WHERE rn = 1
        ),
        monthly_new_contributors AS (
            SELECT
                year,
                month,
                CONCAT(year, '-', LPAD(CAST(month as VARCHAR), 2, '0')) as months,
                COUNT(DISTINCT user_name) as contributors
            FROM first_appearances
            GROUP BY year, month
            ORDER BY year, month
        )
        SELECT months, contributors
        FROM monthly_new_contributors
    """
    util.save_query_as_json("general_new_contributor_count_monthly", sql_query)
    util.save_accumulated("general_new_contributor_count_monthly")


def calculate_contributor_count_more_than_k_edits_monthly(data_path):
    """Calculate monthly contributor counts with more than k edits"""
    sql_query = f"""
        WITH user_total_edits AS (
            SELECT
                user_name,
                SUM(edit_count) as total_edits
            FROM '{data_path}/year=*/month=*/*.parquet'
            GROUP BY user_name
        ),
        monthly_contributors AS (
            SELECT DISTINCT
                year,
                month,
                CONCAT(year, '-', LPAD(CAST(month as VARCHAR), 2, '0')) as months,
                user_name
            FROM '{data_path}/year=*/month=*/*.parquet'
        ),
        contributor_edit_levels AS (
            SELECT
                mc.year,
                mc.month,
                mc.months,
                mc.user_name,
                ute.total_edits,
                CASE WHEN ute.total_edits > 10 THEN 1 ELSE 0 END as more_than_10,
                CASE WHEN ute.total_edits > 100 THEN 1 ELSE 0 END as more_than_100,
                CASE WHEN ute.total_edits > 1000 THEN 1 ELSE 0 END as more_than_1000,
                CASE WHEN ute.total_edits > 10000 THEN 1 ELSE 0 END as more_than_10000,
                CASE WHEN ute.total_edits > 100000 THEN 1 ELSE 0 END as more_than_100000
            FROM monthly_contributors mc
            JOIN user_total_edits ute ON mc.user_name = ute.user_name
        )
        SELECT
            months,
            CAST(SUM(more_than_10) AS INTEGER) as "more then 10 edits",
            CAST(SUM(more_than_100) AS INTEGER) as "more then 100 edits",
            CAST(SUM(more_than_1000) AS INTEGER) as "more then 1000 edits",
            CAST(SUM(more_than_10000) AS INTEGER) as "more then 10000 edits",
            CAST(SUM(more_than_100000) AS INTEGER) as "more then 100000 edits"
        FROM contributor_edit_levels
        GROUP BY year, month, months
        ORDER BY year, month
    """
    util.save_query_as_json("general_contributor_count_more_the_k_edits_monthly", sql_query)


def calculate_edit_count_monthly(data_path):
    """Calculate monthly edit counts"""
    sql_query = f"""
        SELECT
            CONCAT(year, '-', LPAD(CAST(month as VARCHAR), 2, '0')) as months,
            CAST(SUM(edit_count) as BIGINT) as edits
        FROM '{data_path}/year=*/month=*/*.parquet'
        GROUP BY year, month
        ORDER BY year, month
    """
    util.save_query_as_json("general_edit_count_monthly", sql_query)
    util.save_accumulated("general_edit_count_monthly")
    util.save_monthly_to_yearly("general_edit_count_monthly")


def calculate_changeset_count_monthly(data_path):
    """Calculate monthly changeset counts"""
    sql_query = f"""
        SELECT
            CONCAT(year, '-', LPAD(CAST(month as VARCHAR), 2, '0')) as months,
            CAST(COUNT(*) AS INTEGER) as changesets
        FROM '{data_path}/year=*/month=*/*.parquet'
        GROUP BY year, month
        ORDER BY year, month
    """
    util.save_query_as_json("general_changeset_count_monthly", sql_query)
    util.save_accumulated("general_changeset_count_monthly")


def calculate_contributors_unique_yearly(data_path):
    """Calculate yearly unique contributor counts"""
    sql_query = f"""
        SELECT
            CAST(year as VARCHAR) as years,
            COUNT(DISTINCT user_name) as contributors
        FROM '{data_path}/year=*/month=*/*.parquet'
        GROUP BY year
        ORDER BY year
    """
    util.save_query_as_json("general_contributors_unique_yearly", sql_query)


def calculate_edit_count_per_contributor_median_monthly(data_path):
    """Calculate monthly median edit count per contributor"""
    sql_query = f"""
        WITH monthly_contributor_edits AS (
            SELECT
                year,
                month,
                CONCAT(year, '-', LPAD(CAST(month as VARCHAR), 2, '0')) as months,
                user_name,
                CAST(SUM(edit_count) as INTEGER) as user_edits
            FROM '{data_path}/year=*/month=*/*.parquet'
            GROUP BY year, month, user_name
        )
        SELECT
            months,
            CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY user_edits) as INTEGER) as "median number of edits per contributor"
        FROM monthly_contributor_edits
        GROUP BY year, month, months
        ORDER BY year, month
    """
    util.save_query_as_json("general_edit_count_per_contributor_median_monthly", sql_query)


def calculate_edit_count_per_contributor_median_monthly_since_2010(data_path):
    """Calculate monthly median edit count per contributor since 2010"""
    sql_query = f"""
        WITH monthly_contributor_edits AS (
            SELECT
                year,
                month,
                CONCAT(year, '-', LPAD(CAST(month as VARCHAR), 2, '0')) as months,
                user_name,
                CAST(SUM(edit_count) as INTEGER) as user_edits
            FROM '{data_path}/year=*/month=*/*.parquet'
            WHERE year >= 2010
            GROUP BY year, month, user_name
        )
        SELECT
            months,
            CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY user_edits) as INTEGER) as "median number of edits per contributor"
        FROM monthly_contributor_edits
        GROUP BY year, month, months
        ORDER BY year, month
    """
    util.save_query_as_json("general_edit_count_per_contributor_median_monthly_since_2010", sql_query)


def calculate_contributor_attrition_rate_yearly(data_path):
    """Calculate yearly contributor attrition rate by first edit period"""
    sql_query = f"""
        WITH user_first_edit AS (
            -- Find the first edit year for each user
            SELECT 
                user_name,
                MIN(year) as first_edit_year
            FROM 
                read_parquet('{data_path}/year=*/month=*/*.parquet')
            GROUP BY 
                user_name
        ),
        year_user_edits AS (
            -- Sum edits by year and user
            SELECT 
                year,
                user_name,
                SUM(edit_count) as total_edits
            FROM 
                read_parquet('{data_path}/year=*/month=*/*.parquet')
            GROUP BY 
                year, user_name
        ),
        merged_data AS (
            -- Join to get first edit year for each user's yearly edits
            SELECT 
                y.year,
                y.user_name,
                y.total_edits,
                u.first_edit_year,
                CASE 
                    -- Create 2-year periods based on odd/even years
                    WHEN u.first_edit_year % 2 = 1 THEN 
                        CONCAT(CAST(u.first_edit_year AS VARCHAR), '-', CAST(u.first_edit_year + 1 AS VARCHAR))
                    ELSE 
                        CONCAT(CAST(u.first_edit_year - 1 AS VARCHAR), '-', CAST(u.first_edit_year AS VARCHAR))
                END as first_edit_period
            FROM 
                year_user_edits y
            JOIN 
                user_first_edit u ON y.user_name = u.user_name
        )
        SELECT 
            CAST(year AS VARCHAR) as years,
            first_edit_period,
            SUM(total_edits) as total_edits
        FROM 
            merged_data
        GROUP BY 
            year, first_edit_period
        ORDER BY 
            year, first_edit_period
    """
    df = duckdb.sql(sql_query).df()
    df_pivoted = df.pivot(index="years", columns="first_edit_period", values="total_edits").reset_index()
    util.save_df_as_json("general_contributor_count_attrition_rate_yearly", df_pivoted)


def calculate_no_maps_me_contributor_count_monthly(data_path):
    """Calculate monthly contributor count excluding maps.me"""
    sql_query = f"""
        SELECT
            CONCAT(year, '-', LPAD(CAST(month as VARCHAR), 2, '0')) as months,
            COUNT(DISTINCT user_name) as "contributors without maps.me"
        FROM '{data_path}/year=*/month=*/*.parquet'
        WHERE created_by != 'MAPS.ME' OR created_by IS NULL
        GROUP BY year, month
        ORDER BY year, month
    """
    util.save_query_as_json("general_no_maps_me_contributor_count_monthly", sql_query)


def calculate_edit_count_map_total(data_path):
    """Calculate total edit count map data"""
    histogram_sql = f"""
        SELECT
            mid_pos_x,
            mid_pos_y,
            edit_count
        FROM '{data_path}/year=*/month=*/*.parquet'
        WHERE mid_pos_x IS NOT NULL AND mid_pos_y IS NOT NULL
    """
    df = duckdb.sql(histogram_sql).df()
    histogram = util.create_position_histogram(df)
    util.save_map("general_edit_count_map_total", histogram)


def calculate_edit_count_maps_yearly(data_path):
    """Generate yearly edit count maps using histogram approach"""
    # Get distinct years
    years_sql = f"""
        SELECT DISTINCT year
        FROM '{data_path}/year=*/month=*/*.parquet'
        ORDER BY year
    """
    years_df = duckdb.sql(years_sql).df()

    yearly_histograms = []
    map_names = []

    for year in years_df["year"].values:
        # Get data for this specific year
        year_sql = f"""
            SELECT
                mid_pos_x,
                mid_pos_y,
                edit_count
            FROM '{data_path}/year={year}/month=*/*.parquet'
            WHERE mid_pos_x IS NOT NULL AND mid_pos_y IS NOT NULL
        """
        year_df = duckdb.sql(year_sql).df()
        yearly_histograms.append(util.create_position_histogram(year_df))
        map_names.append(f"total edits {year}")

    util.save_maps("general_edit_count_maps_yearly", yearly_histograms, map_names)


def save_general(data_path):
    calculate_contributor_count_monthly(data_path)
    calculate_new_contributor_count_monthly(data_path)
    calculate_contributor_count_more_than_k_edits_monthly(data_path)
    calculate_edit_count_monthly(data_path)
    calculate_changeset_count_monthly(data_path)
    calculate_contributors_unique_yearly(data_path)
    calculate_edit_count_per_contributor_median_monthly(data_path)
    calculate_edit_count_per_contributor_median_monthly_since_2010(data_path)
    calculate_contributor_attrition_rate_yearly(data_path)
    calculate_no_maps_me_contributor_count_monthly(data_path)
    calculate_edit_count_map_total(data_path)
    calculate_edit_count_maps_yearly(data_path)


def main():
    parser = argparse.ArgumentParser(description="Calculate asset data from OSM changeset data")
    parser.add_argument("data_path", help="Path to the directory containing parquet files")

    args = parser.parse_args()

    print(f"Processing data from: {args.data_path}")

    start_time = time.time()
    save_general(args.data_path)

    elapsed_time = time.time() - start_time
    minutes = int(elapsed_time // 60)
    seconds = int(elapsed_time % 60)
    print(f"Processing completed in {minutes}:{seconds:02d} minutes")


if __name__ == "__main__":
    main()
