import argparse
import json
import time

import duckdb


def save_as_json(
    file_name, sql_query, columns, save_accumulated=False, save_monthly_to_yearly=False, save_yearly_full_only=False
):
    def save(file_name, columns, df):
        with open(f"assets/data/{file_name}.json", "w") as f:
            # remove last entry for monthly data
            data = df.values.tolist()[:-1] if columns[0] == "months" else df.values.tolist()
            json.dump({"columns": columns, "data": data}, f, indent=1)

    df = duckdb.sql(sql_query).df()
    save(file_name, columns, df)

    if save_accumulated:
        df_accumulated = df.copy()
        df_accumulated.iloc[:, 1] = df_accumulated.iloc[:, 1].cumsum()
        save(f"{file_name}_accumulated", columns, df_accumulated)

    if save_monthly_to_yearly:
        # Convert monthly data to yearly by summing up months within each year
        df_yearly = df.copy()
        df_yearly["year"] = df_yearly.iloc[:, 0].str[:4]  # Extract year from YYYY-MM format
        df_yearly_grouped = df_yearly.groupby("year").agg({df_yearly.columns[1]: "sum"}).reset_index()
        df_yearly_grouped.columns = ["years", columns[1]]

        if save_yearly_full_only:
            # Save version with only complete years (exclude current year)
            # Only remove last year if it's incomplete (last month != 12)
            last_month = df.iloc[-1, 0][-2:]  # Get MM from YYYY-MM of last row
            df_yearly_full = df_yearly_grouped[:-1] if last_month != "12" else df_yearly_grouped
            save(f"{file_name.replace('_monthly', '_yearly')}_full_years_only", ["years", columns[1]], df_yearly_full)
        else:
            save(f"{file_name.replace('_monthly', '_yearly')}", ["years", columns[1]], df_yearly_grouped)


def save_general(data_path):
    save_as_json(
        "general_contributor_count_monthly",
        f"""
		SELECT 
			CONCAT(year, '-', LPAD(CAST(month as VARCHAR), 2, '0')) as months,
			COUNT(DISTINCT user_name) as contributors
		FROM '{data_path}/year=*/month=*/*.parquet'
		GROUP BY year, month
		ORDER BY year, month
		""",
        ["months", "contributors"],
    )

    save_as_json(
        "general_new_contributor_count_monthly",
        f"""
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
		""",
        ["months", "contributors"],
        save_accumulated=True,
    )

    save_as_json(
        "general_contributor_count_more_the_k_edits_monthly",
        f"""
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
		""",
        [
            "months",
            "more then 10 edits",
            "more then 100 edits",
            "more then 1000 edits",
            "more then 10000 edits",
            "more then 100000 edits",
        ],
    )

    save_as_json(
        "general_edit_count_monthly",
        f"""
		SELECT 
			CONCAT(year, '-', LPAD(CAST(month as VARCHAR), 2, '0')) as months,
			CAST(SUM(edit_count) as INTEGER) as edits
		FROM '{data_path}/year=*/month=*/*.parquet'
		GROUP BY year, month
		ORDER BY year, month
		""",
        ["months", "edits"],
        save_accumulated=True,
        save_monthly_to_yearly=True,
        save_yearly_full_only=True,
    )

    save_as_json(
        "general_changeset_count_monthly",
        f"""
		SELECT 
			CONCAT(year, '-', LPAD(CAST(month as VARCHAR), 2, '0')) as months,
			CAST(COUNT(*) AS INTEGER) as changesets
		FROM '{data_path}/year=*/month=*/*.parquet'
		GROUP BY year, month
		ORDER BY year, month
		""",
        ["months", "changesets"],
        save_accumulated=True,
    )

    save_as_json(
        "general_contributors_unique_yearly",
        f"""
		SELECT 
			CAST(year as VARCHAR) as years,
			COUNT(DISTINCT user_name) as contributors
		FROM '{data_path}/year=*/month=*/*.parquet'
		GROUP BY year
		ORDER BY year
		""",
        ["years", "contributors"],
    )

    save_as_json(
        "general_edit_count_per_contributor_median_monthly",
        f"""
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
		""",
        ["months", "median number of edits per contributor"],
    )

    save_as_json(
        "general_edit_count_per_contributor_median_monthly_since_2010",
        f"""
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
		""",
        ["months", "median number of edits per contributor"],
    )


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
