import os
import sys

import numpy as np
import dask.dataframe as dd

import util
from tqdm import tqdm


SAVE_DIR = "assets/data"
DATA_DIR = sys.argv[1]
PARQUET_DIR = os.path.join(DATA_DIR, "changeset_data")
MONTHS, YEARS = util.get_months_years(DATA_DIR)
TOTAL_DATA_DICTS = 30
progress_bar = tqdm(total=TOTAL_DATA_DICTS)

created_by_tag_to_index = util.load_tag_to_index(DATA_DIR, "created_by")


def save_data_dict(name, data_dict):
    util.save_json(os.path.join(SAVE_DIR, f"{name}.json"), data_dict)
    progress_bar.update(1)

def load_data_dict(name):
    util.load_json(os.path.join(SAVE_DIR, f"{name}.json"))

def pd_series_to_y(x, series):
    if x == None:
        return series.values.tolist()[0]
    else:
        y = np.zeros(len(x), dtype=series.values.dtype)
        y[series.index.values] = series.values
        return y.tolist()

def save_y(name, x, pd_series, index_offset=3):
    y = pd_series_to_y(x, pd_series)
    if x is None:
        data_dict = {"y": y[start_index:]}
    else:
        start_index = np.max([0, np.nonzero(y)[0][0] - index_offset])
        data_dict = {"x": x[start_index:], "y": y[start_index:]}
    save_data_dict(name, data_dict)

def save_y_list(name, x, pd_series_list, y_names, index_offset=3):
    y_list = [pd_series_to_y(x, pd_series) for pd_series in pd_series_list]
    if x is None:
        data_dict = {"y_list": y_list, "y_names": y_names}
    else:
        start_index = np.min([np.max([0, np.nonzero(y)[0][0] - index_offset]) for y in y_list])
        y_list = [y[start_index:] for y in y_list]
        data_dict = {"x": x[start_index:], "y_list": y_list, "y_names": y_names}
    save_data_dict(name, data_dict)

def histogram_2d_to_xyz(pd_histogram_2d):
    histogram_2d = np.zeros((360, 180), dtype=np.uint32)
    for index, value in pd_histogram_2d.items():
        histogram_2d[index] = value
    x, y = histogram_2d.nonzero()
    x, y, z = (x.tolist(), y.tolist(), histogram_2d[x, y].tolist())
    max_z_value = int(np.max(z))
    return x, y, z, max_z_value

def save_map(name, pd_histogram_2d):
    x, y, z, max_z_value = histogram_2d_to_xyz(pd_histogram_2d)
    data_dict = {name: {"x": x, "y": y, "z": z}, "max_z_value": max_z_value}
    save_data_dict(name, data_dict)

def save_maps(name, pd_histogram_2d_list, map_names):
    data_dict = {name:{}, }
    max_z_values = []
    for pd_histogram_2d, map_name in zip(pd_histogram_2d_list, map_names):
        x, y, z, max_z_value = histogram_2d_to_xyz(pd_histogram_2d)
        max_z_values.append(max_z_value)
        data_dict[name][map_name] = {"x": x, "y": y, "z": z}
    data_dict["max_z_value"] = int(np.max(max_z_values))
    save_data_dict(name, data_dict)

def get_name_to_link(replace_rules_file_name):
    name_to_tags_and_link = util.load_json(os.path.join("src", replace_rules_file_name))
    name_to_link = {}
    for name, name_infos in name_to_tags_and_link.items():
        if "link" in name_infos:
            name_to_link[name] = name_infos["link"]

    return name_to_link


def save_monthly_contibutors():
    ddf = dd.read_parquet(os.path.join(PARQUET_DIR, "general_*.parquet"))
    monthly_contributors_unique = ddf.groupby(["month_index"])["user_index"].unique().compute()
    contributor_count_monthly = monthly_contributors_unique.apply(len)
    save_y("contributor_count_monthly", MONTHS, contributor_count_monthly)
    save_y("contributor_count_accumulated_monthly", MONTHS, util.cumsum_nunique(monthly_contributors_unique))
    save_y("new_contributor_count_monthly", MONTHS, util.cumsum_new_nunique(monthly_contributors_unique))

    total_edits_of_contributors = ddf.groupby(["user_index"])["edits"].sum().compute()
    monthly_contributors_set = monthly_contributors_unique.apply(set)
    monthly_contributors_unique = None

    min_edit_count = [10, 100, 1_000, 10_000, 100_000]
    contributor_count_more_the_k_edits_monthly = []
    for k in min_edit_count:
        contributors_with_more_then_k_edits = set(
            total_edits_of_contributors[total_edits_of_contributors > k].index.to_numpy()
        )
        contributor_count_more_the_k_edits_monthly.append(
            monthly_contributors_set.apply(lambda x: len(x.intersection(contributors_with_more_then_k_edits)))
        )
    save_y_list("contributor_count_more_the_k_edits_monthly", MONTHS, contributor_count_more_the_k_edits_monthly, min_edit_count)

    map_me_indices = (created_by_tag_to_index["MAPS.ME android"], created_by_tag_to_index["MAPS.ME ios"])
    contributor_count_no_maps_me_monthly = (
        ddf[~ddf["created_by"].isin(map_me_indices)].groupby(["month_index"])["user_index"].nunique().compute()
    )
    save_y("contributor_count_no_maps_me_monthly", MONTHS, contributor_count_no_maps_me_monthly)
    return contributor_count_monthly

def save_monthly_edits():
    ddf = dd.read_parquet(os.path.join(PARQUET_DIR, "general_*.parquet"))
    edit_count_monthly = ddf.groupby(["month_index"])["edits"].sum().compute()
    save_y("edit_count_monthly", MONTHS, edit_count_monthly)
    save_y("edit_count_accumulated_monthly", MONTHS, edit_count_monthly.cumsum())

def save_monthly_changesets():
    ddf = dd.read_parquet(os.path.join(PARQUET_DIR, "general_*.parquet"))
    changeset_count_monthly = ddf.groupby(["month_index"]).size().compute()
    save_y("changeset_count_monthly", MONTHS, changeset_count_monthly)
    save_y("changeset_count_accumulated_monthly", MONTHS, changeset_count_monthly.cumsum())

def save_general_maps():
    ddf = dd.read_parquet(os.path.join(PARQUET_DIR, "general_*.parquet"))

    monthly_map_edits = ddf[ddf["pos_x"] >= 0].groupby(["month_index", "pos_x", "pos_y"])["edits"].sum().compute()
    edit_count_map_total = monthly_map_edits.groupby(level=[1, 2]).sum()
    save_map("edit_count_map_total", edit_count_map_total)

    yearly_map_edits = ddf[ddf["pos_x"] >= 0].groupby(["year_index", "pos_x", "pos_y"])["edits"].sum().compute()
    year_maps = [yearly_map_edits[yearly_map_edits.index.get_level_values("year_index")==year_i].droplevel(0) for year_i in range(len(YEARS))]
    year_map_names = [f"total edits {year}" for year in YEARS]
    save_maps("edit_count_maps_yearly", year_maps, year_map_names)

def save_general_median():
    ddf = dd.read_parquet(os.path.join(PARQUET_DIR, "general_*.parquet"))
    monthly_contributor_edits = ddf.groupby(["month_index", "user_index"])["edits"].sum().compute()
    edit_count_per_contributor_median_monthly = monthly_contributor_edits.groupby(["month_index"]).median()

    edit_count_per_contributor_median_monthly_since_2010 = edit_count_per_contributor_median_monthly[
        edit_count_per_contributor_median_monthly.index >= 57
    ]
    edit_count_per_contributor_median_monthly_since_2010.index -= 57

    save_y("edit_count_per_contributor_median_monthly", MONTHS, edit_count_per_contributor_median_monthly)
    save_y("edit_count_per_contributor_median_monthly_since_2010", MONTHS[57:], edit_count_per_contributor_median_monthly_since_2010)




def get_tag_top_100_unit_indices_and_names(tag, unit):
    ddf = dd.read_parquet(os.path.join(PARQUET_DIR, "general_*.parquet"))
    index_to_tag = util.load_index_to_tag(DATA_DIR, tag)

    if tag == "created_by":
        ddf = ddf[ddf[tag]<4294967295]

    if unit == "contributor_count":
        total = ddf.groupby(tag)["user_index"].nunique().compute()
    elif unit == "edit_count":
        total = ddf.groupby(tag)["edits"].sum().compute()
    elif unit == "changeset_count":
        total = ddf.groupby(tag).size().compute()
    else:
        raise Exception(f"Unkown unit: {unit}")

    top_100_indices = (-total.values.astype(np.int64)).argsort()[:100]
    top_100_names = [index_to_tag[i] for i in top_100_indices]
    
    save_y_list(
        f"{tag}_top_100_{unit}_total",
        None,
        util.series_to_series_list(total, top_100_indices),
        top_100_names
    )
    return np.array([top_100_indices, top_100_names])


# def get_tag_top_10_new_contributor_count_monthly():
#     top_10_monthly_contributors = ddf[ddf["created_by"].isin(top_10_contributor_indices)].groupby(["month_index", "created_by"])["user_index"].unique().compute()
#     top_10_monthly_contributors_list = util.multi_index_series_to_series_list(top_10_monthly_contributors.apply(len), top_10_contributor_indices)

#     save_y_list("created_by_top_10_contributor_count_monthly", MONTHS, top_10_monthly_contributors_list, top_10_contributor_names)
#     save_y_list(
#         "created_by_top_10_new_contributor_count_monthly",
#         MONTHS,
#         [util.cumsum_new_nunique(series) for series in util.multi_index_series_to_series_list(top_10_monthly_contributors, top_10_contributor_indices)],
#         top_10_contributor_names
#     )

def save_tag_top_10_unit_monthly(tag, unit, top_10_indices, top_10_names, save_percent=None, save_new_contributor_count=False):
    ddf = dd.read_parquet(os.path.join(PARQUET_DIR, "general_*.parquet"))

    if unit == "contributor_count":
        top_10_monthly = ddf[ddf[tag].isin(top_10_indices)].groupby(["month_index", tag])["user_index"].unique().compute()
        top_10_monthly_list = util.multi_index_series_to_series_list(top_10_monthly.apply(len), top_10_indices)
    elif unit == "edit_count":
        top_10_monthly = ddf[ddf[tag].isin(top_10_indices)].groupby(["month_index", tag])["edits"].sum().compute()
        top_10_monthly_list = util.multi_index_series_to_series_list(top_10_monthly, top_10_indices)
    elif unit == "changeset_count":
        top_10_monthly = ddf[ddf[tag].isin(top_10_indices)].groupby(["month_index", tag]).size().compute()
        top_10_monthly_list = util.multi_index_series_to_series_list(top_10_monthly, top_10_indices)
    else:
        raise Exception(f"Unkown unit: {unit}")

    save_y_list(f"{tag}_top_10_{unit}_monthly", MONTHS, top_10_monthly_list, top_10_names)

    if save_percent is not None:
        save_y_list(
            f"{tag}_top_10_{unit}_percent_monthly",
            MONTHS,
            [s.divide(save_percent, fill_value=0)*100 for s in top_10_monthly_list],
            top_10_names
        )
    if save_new_contributor_count and unit == "contributor_count":
        save_y_list(
            f"{tag}_top_10_new_contributor_count_monthly",
            MONTHS,
            [util.cumsum_new_nunique(series) for series in util.multi_index_series_to_series_list(top_10_monthly, top_10_indices)],
            top_10_names
        )
    

def save_tag_top_100_unit_yearly(tag, unit, top_100_indices, top_100_names):
    ddf = dd.read_parquet(os.path.join(PARQUET_DIR, "general_*.parquet"))

    if unit == "contributor_count":
        top_100_yearly = ddf[ddf[tag].isin(top_100_indices)].groupby(["year_index", tag])["user_index"].nunique().compute()
    elif unit == "edit_count":
        top_100_yearly = ddf[ddf[tag].isin(top_100_indices)].groupby(["year_index", tag])["edits"].sum().compute()
    elif unit == "changeset_count":
        top_100_yearly = ddf[ddf[tag].isin(top_100_indices)].groupby(["year_index", tag]).size().compute()
    else:
        raise Exception(f"Unkown unit: {unit}")

    save_y_list(
        f"{tag}_top_100_{unit}_yearly",
        YEARS,
        util.multi_index_series_to_series_list(top_100_yearly, top_100_indices),
        top_100_names,
        index_offset=0
    )


def save_tag_top_10_contributor_count_first_changeset_monthly(tag, top_10_indices, top_10_names):
    ddf = dd.read_parquet(os.path.join(PARQUET_DIR, "general_*.parquet"))
    
    contibutor_monthly = ddf[ddf[tag].isin(top_10_indices)].groupby(["user_index"])["month_index", tag].first().compute()
    contibutor_count_monthly = contibutor_monthly.reset_index().groupby(["month_index", tag])["user_index"].count()

    save_y_list(
        f"{tag}_top_10_contributor_count_first_changeset_monthly",
        MONTHS,
        util.multi_index_series_to_series_list(contibutor_count_monthly, top_10_indices),
        top_10_names,
    )
    


def save_monthly_contributors_created_by(contributor_count_monthly):

    top_100_contributor_indices_names = get_tag_top_100_unit_indices_and_names("created_by", "contributor_count")
    top_10_contributor_indices_names = top_100_contributor_indices_names[:,:10]

    save_tag_top_10_unit_monthly(
        "created_by",
        "contributor_count",
        *top_10_contributor_indices_names,
        save_percent=contributor_count_monthly,
        save_new_contributor_count=True
    )
    save_tag_top_100_unit_yearly("created_by", "contributor_count", *top_100_contributor_indices_names)
    save_tag_top_10_contributor_count_first_changeset_monthly("created_by", *top_10_contributor_indices_names)


def get_editor_type_lists():
    created_by_tag_to_index = util.load_tag_to_index(DATA_DIR, "created_by")

    name_to_info = util.load_json(os.path.join("src", "replace_rules_created_by.json"))
    device_type = {
        "desktop_editors": [],
        "mobile_editors": [],
        "tools": [],
    }
    for name, info in name_to_info.items():
        if name not in created_by_tag_to_index:
            print(
                f"{name} in 'replace_rules_created_by.json' but not in 'tag_to_index' (this is expected when working"
                " with a part of all changesets)"
            )
            continue
        if "type" in info:
            if info["type"] == "desktop_editor":
                device_type["desktop_editors"].append(created_by_tag_to_index[name])
            elif info["type"] == "mobile_editor":
                device_type["mobile_editors"].append(created_by_tag_to_index[name])
            elif info["type"] == "tool":
                device_type["tools"].append(created_by_tag_to_index[name])
            else:
                print(f"unknown type: {name['type']} at name {name}")
    return device_type






# add_question(
#     "How many edits are added per editing software each month?",
#     "eb30",
#     # util.get_multi_line_plot(
#     "monthly new contributor count per editing software",
#     "contributors",
#     months,
#     util.multi_index_series_to_series_list(top_10_monthly_edits, top_10_indices_edits),
#     [created_by_index_to_tag[i] for i in top_10_indices_edits]
# ),
#     util.get_table(
#         "yearly edits count per editing software",
#         years,
#         util.monthly_to_yearly_with_total(monthly_edits, years, changesets.month_index_to_year_index),
#         TOPIC,
#         rank_to_name["edits"],
#     ),
# )


def main():
    util.save_json(os.path.join(SAVE_DIR, "created_by_name_to_link.json"), get_name_to_link("replace_rules_created_by.json"))

    contributor_count_monthly = save_monthly_contibutors()
    # save_monthly_edits()
    # save_monthly_changesets()
    # save_general_maps()
    # save_general_median()
    save_monthly_contributors_created_by(contributor_count_monthly)

    

    

    pass
    


if __name__ == "__main__":
    sys.exit(main())
