import os
import sys

import numpy as np
import dask.dataframe as dd

import util


SAVE_DIR = "assets/data"
DATA_DIR = sys.argv[1]
PARQUET_DIR = os.path.join(DATA_DIR, "changeset_data")
MONTHS, YEARS = util.get_months_years(DATA_DIR)

created_by_tag_to_index = util.load_tag_to_index(DATA_DIR, "created_by")


def series_to_y(x, series):
    y = np.zeros(len(x), dtype=np.int64)
    y[series.index.values] = series.values
    return y.tolist()


def save_monthly_series(name, series):
    data_dict = {"month": MONTHS, name: series_to_y(MONTHS, series)}
    util.save_json(os.path.join(SAVE_DIR, f"{name}.json"), data_dict)


def save_monthly_series_list(name, series_list, y_names):
    data_dict = {
        "month": MONTHS,
        name: {str(y_name): series_to_y(MONTHS, series) for y_name, series in zip(y_names, series_list)},
    }
    util.save_json(os.path.join(SAVE_DIR, f"{name}.json"), data_dict)


def save_monthly_contibutors():
    ddf = dd.read_parquet(os.path.join(PARQUET_DIR, "general_*.parquet"))
    monthly_contributors_unique = ddf.groupby(["month_index"])["user_index"].unique().compute()
    save_monthly_series("monthly_contributor_count", monthly_contributors_unique.apply(len))
    save_monthly_series("monthly_contributor_cumsum", util.cumsum_nunique(monthly_contributors_unique))
    save_monthly_series("monthly_new_contributor_count", util.cumsum_new_nunique(monthly_contributors_unique))

    total_edits_of_contributors = ddf.groupby(["user_index"])["edits"].sum().compute()
    monthly_contributors_set = monthly_contributors_unique.apply(set)
    monthly_contributors_unique = None

    min_edit_count = [10, 100, 1_000, 10_000, 100_000]
    monthly_contributors_count_more_then_k_edits = []
    for k in min_edit_count:
        contributors_with_more_then_k_edits = set(
            total_edits_of_contributors[total_edits_of_contributors > k].index.to_numpy()
        )
        monthly_contributors_count_more_then_k_edits.append(
            monthly_contributors_set.apply(lambda x: len(x.intersection(contributors_with_more_then_k_edits)))
        )
    save_monthly_series_list(
        "monthly_contributors_count_more_then_k_edits", monthly_contributors_count_more_then_k_edits, min_edit_count
    )

    map_me_indices = (created_by_tag_to_index["MAPS.ME android"], created_by_tag_to_index["MAPS.ME ios"])
    monthly_contributor_count_no_maps_me = (
        ddf[~ddf["created_by"].isin(map_me_indices)].groupby(["month_index"])["user_index"].nunique().compute()
    )
    save_monthly_series("monthly_contributor_count_no_maps_me", monthly_contributor_count_no_maps_me)

def save_monthly_edits():
    ddf = dd.read_parquet(os.path.join(PARQUET_DIR, "general_*.parquet"))
    monthly_edits = ddf.groupby(["month_index"])["edits"].sum().compute()
    save_monthly_series("monthly_edits", monthly_edits)
    save_monthly_series("monthly_edits_cumsum", monthly_edits.cumsum())

def save_monthly_changesets():
    ddf = dd.read_parquet(os.path.join(PARQUET_DIR, "general_*.parquet"))
    monthly_changesets = ddf.groupby(["month_index"]).size().compute()
    save_monthly_series("monthly_changesets", monthly_changesets)
    save_monthly_series("monthly_changesets_cumsum", monthly_changesets.cumsum())


def main():
    # save_monthly_contibutors()
    save_monthly_edits()
    save_monthly_changesets()
    # TODO: add timings...
    


if __name__ == "__main__":
    sys.exit(main())
