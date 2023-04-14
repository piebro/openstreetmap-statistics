import os
import sys
import re

import numpy as np
import dask.dataframe as dd

import util
from tqdm import tqdm


SAVE_DIR = "assets/data"
DATA_DIR = sys.argv[1]
PARQUET_DIR = os.path.join(DATA_DIR, "changeset_data")
MONTHS, YEARS = util.get_months_years(DATA_DIR)
TOTAL_DATA_DICTS = 40
progress_bar = tqdm(total=TOTAL_DATA_DICTS)

created_by_tag_to_index = util.load_tag_to_index(DATA_DIR, "created_by")


def save_data_dict(name, data_dict):
    util.save_json(os.path.join(SAVE_DIR, f"{name}.json"), data_dict)
    progress_bar.update(1)

def load_data_dict(name):
    util.load_json(os.path.join(SAVE_DIR, f"{name}.json"))

def pd_series_to_y(x, series, cumsum=False):
    if x == None:
        if isinstance(series, np.int64):
           return series
        return series.values.tolist()[0] 
    else:
        y = np.zeros(len(x), dtype=series.values.dtype)
        y[series.index.values] = series.values
        if cumsum:
            for i in range(1, len(y)):
                if y[i] == 0:
                    y[i] = y[i-1]

        return y.tolist()

def save_y(name, x, pd_series, index_offset=3, cumsum=False):
    y = pd_series_to_y(x, pd_series, cumsum)
    if x is None:
        data_dict = {"y": y[start_index:]}
    else:
        start_index = np.max([0, np.nonzero(y)[0][0] - index_offset])
        data_dict = {"x": x[start_index:], "y": y[start_index:]}
    save_data_dict(name, data_dict)

def save_y_list(name, x, pd_series_list, y_names, index_offset=3, cumsum=False):
    y_list = [pd_series_to_y(x, pd_series, cumsum) for pd_series in pd_series_list]
    if x is None:
        data_dict = {"y_list": y_list, "y_names": y_names}
    else:
        start_index = np.min([np.max([0, np.nonzero(y)[0][0] - index_offset]) for y in y_list if np.any(y)])
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


def get_software_editor_type_lists():
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
    
    device_type["desktop_editors"] = np.array(device_type["desktop_editors"], dtype=np.int64)
    device_type["mobile_editors"] = np.array(device_type["mobile_editors"], dtype=np.int64)
    device_type["tools"] = np.array(device_type["tools"], dtype=np.int64)
    device_type["other/unspecified"] = np.array(list(set(created_by_tag_to_index.values())-set(np.concatenate(list(device_type.values())))))
    return device_type


def save_created_by_editor_type_stats(ddf):
    editor_type_lists = get_software_editor_type_lists()

    save_y_list(
        "created_by_device_type_contributor_count_monthly",
        MONTHS,
        [ddf[ddf["created_by"].isin(l)].groupby(["month_index"])["user_index"].nunique().compute() for l in editor_type_lists.values()],
        list(editor_type_lists.keys())
    )
    save_y_list(
        "created_by_device_type_edit_count_monthly",
        MONTHS,
        [ddf[ddf["created_by"].isin(l)].groupby(["month_index"])["edits"].sum().compute() for l in editor_type_lists.values()],
        list(editor_type_lists.keys())
    )
    save_percent("created_by_device_type_edit_count_monthly", "general_edit_count_monthly")
    

def save_base_statistics(
    ddf,
    prefix,
    edit_count_monthly=False,
    changeset_count_monthly=False,
    contributors_unique_yearly=False,
    contributor_count_monthly=False,
    new_contributor_count_monthly=False,
    edit_count_map_total=False,
    ):

    if edit_count_monthly:
        save_y(f"{prefix}_edit_count_monthly", MONTHS, ddf.groupby(["month_index"])["edits"].sum().compute())
        

    if changeset_count_monthly:
        save_y(f"{prefix}_changeset_count_monthly", MONTHS, ddf.groupby(["month_index"]).size().compute())

    if contributors_unique_yearly:
        save_y(f"{prefix}_contributors_unique_yearly", YEARS, ddf.groupby(["year_index"])["user_index"].nunique().compute())
    
    if contributor_count_monthly or new_contributor_count_monthly:
        contributors_unique_monthly = ddf.groupby(["month_index"])["user_index"].unique().compute()
        if contributor_count_monthly:
            save_y(f"{prefix}_contributor_count_monthly", MONTHS, contributors_unique_monthly.apply(len))
        if new_contributor_count_monthly:
            save_y(f"{prefix}_new_contributor_count_monthly", MONTHS, util.cumsum_new_nunique(contributors_unique_monthly))
        contributors_unique_monthly = None

    if edit_count_map_total:
        save_map(f"{prefix}_edit_count_map_total", ddf[ddf["pos_x"] >= 0].groupby(["pos_x", "pos_y"])["edits"].sum().compute())


def get_tag_top_k_and_save_top_k_total(ddf, tag, prefix, k, contributor_count=False, edit_count=False, changeset_count=False):
    index_to_tag = util.load_index_to_tag(DATA_DIR, tag)

    if tag in ["created_by", "corporation", "streetcomplete"]:
        highest_number = np.iinfo(ddf[tag].dtype).max
        ddf = ddf[ddf[tag]<highest_number]
    
    units = []
    if contributor_count:
        units.append("contributor_count")
    if edit_count:
        units.append("edit_count")
    if changeset_count:
        units.append("changeset_count")

    unit_to_top_k_indices_and_names = {"tag": tag}
    for unit in units:
        if unit == "contributor_count":
            total = ddf.groupby(tag)["user_index"].nunique().compute()
        if unit == "edit_count":
            total = ddf.groupby(tag)["edits"].sum().compute()
        if unit == "changeset_count":
            total = ddf.groupby(tag).size().compute()

        indices = total.index[(-total.values.astype(np.int64)).argsort()[:k]]
        names = [index_to_tag[i] for i in indices]
        unit_to_top_k_indices_and_names[unit] = (indices, names)

        save_y_list(
            f"{prefix}{tag}_top_{k}_{unit}_total",
            None,
            util.series_to_series_list(total, indices),
            names
        )

    return unit_to_top_k_indices_and_names

def save_base_statistics_tag(
    ddf,
    tag,
    k=100,
    prefix=None,
    edit_count_monthly=False,
    changeset_count_monthly=False,
    contributors_unique_yearly=False,
    contributor_count_monthly=False,
    new_contributor_count_monthly=False,
    edit_count_map_total=False
    ):
    
    prefix = "" if prefix is None else f"{prefix}_"

    top_k = get_tag_top_k_and_save_top_k_total(
        ddf,
        tag,
        prefix,
        k=k,
        contributor_count=(contributors_unique_yearly or contributor_count_monthly or new_contributor_count_monthly),
        edit_count=edit_count_monthly,
        changeset_count=changeset_count_monthly
    )

    if edit_count_monthly:
        indices, names = top_k["edit_count"]
        monthly_list = util.multi_index_series_to_series_list(
            ddf[ddf[tag].isin(indices)].groupby(["month_index", tag])["edits"].sum().compute(),
            indices
        )
        save_y_list(f"{prefix}{tag}_top_{k}_edit_count_monthly", MONTHS, monthly_list, names)
    
    if changeset_count_monthly:
        indices, names = top_k["changeset_count"]
        monthly_list = util.multi_index_series_to_series_list(
            ddf[ddf[tag].isin(indices)].groupby(["month_index", tag]).size().compute(),
            indices
        )
        save_y_list(f"{prefix}{tag}_top_{k}_changeset_count_monthly", MONTHS, monthly_list, names)

    if contributors_unique_yearly:
        indices, names = top_k["contributor_count"]
        yearly_list = util.multi_index_series_to_series_list(
            ddf[ddf[tag].isin(indices)].groupby(["year_index", tag])["user_index"].nunique().compute(),
            indices
        )
        save_y_list(f"{prefix}{tag}_top_{k}_contributor_count_yearly", YEARS, yearly_list, names)
    
    if contributor_count_monthly or new_contributor_count_monthly:
        indices, names = top_k["contributor_count"]
        unique_monthly_list = util.multi_index_series_to_series_list(
            ddf[ddf[tag].isin(indices)].groupby(["month_index", tag])["user_index"].unique().compute(),
            indices
        )
        if contributor_count_monthly:
            save_y_list(
                f"{prefix}{tag}_top_{k}_contributor_count_monthly",
                MONTHS,
                [unique_monthly.apply(len) for unique_monthly in unique_monthly_list],
                names
            )
        if new_contributor_count_monthly:
            save_y_list(
                f"{prefix}{tag}_top_{k}_new_contributor_count_monthly",
                MONTHS,
                [util.cumsum_new_nunique(unique_monthly) for unique_monthly in unique_monthly_list],
                names
            )

    if edit_count_map_total:
        indices, names = top_k["edit_count"]
        indices = indices[:10]
        names = names[:10]
        ddf_is_in = ddf[ddf[tag].isin(indices)]
        tag_map_edits = ddf_is_in[ddf_is_in["pos_x"] >= 0].groupby(["pos_x", "pos_y", tag])["edits"].sum().compute()
        tag_maps = [tag_map_edits[tag_map_edits.index.get_level_values(tag)==i].droplevel(2) for i in indices]
        save_maps(f"{prefix}{tag}_top_10_edit_count_maps_total", tag_maps, names)


def save_edit_count_map_yearly(ddf, prefix):
    yearly_map_edits = ddf[ddf["pos_x"] >= 0].groupby(["year_index", "pos_x", "pos_y"])["edits"].sum().compute()
    year_maps = [yearly_map_edits[yearly_map_edits.index.get_level_values("year_index")==year_i].droplevel(0) for year_i in range(len(YEARS))]
    year_map_names = [f"total edits {year}" for year in YEARS]
    save_maps(f"{prefix}_edit_count_maps_yearly", year_maps, year_map_names)

def save_general_edit_count_per_contributor_median_monthly(ddf):
    contributor_edits_monthly = ddf.groupby(["month_index", "user_index"])["edits"].sum().compute()
    edit_count_per_contributor_median_monthly = contributor_edits_monthly.groupby(["month_index"]).median()

    edit_count_per_contributor_median_monthly_since_2010 = edit_count_per_contributor_median_monthly[
        edit_count_per_contributor_median_monthly.index >= 57
    ]
    edit_count_per_contributor_median_monthly_since_2010.index -= 57

    save_y("general_edit_count_per_contributor_median_monthly", MONTHS, edit_count_per_contributor_median_monthly)
    save_y("general_edit_count_per_contributor_median_monthly_since_2010", MONTHS[57:], edit_count_per_contributor_median_monthly_since_2010)

def save_general_contributor_count_more_the_k_edits_monthly(ddf):
    ddf = dd.read_parquet(os.path.join(PARQUET_DIR, "general_*.parquet"))

    total_edits_of_contributors = ddf.groupby(["user_index"])["edits"].sum().compute()
    contributors_unique_monthly_set = ddf.groupby(["month_index"])["user_index"].unique().compute().apply(set)

    min_edit_count = [10, 100, 1_000, 10_000, 100_000]
    contributor_count_more_the_k_edits_monthly = []
    for k in min_edit_count:
        contributors_with_more_than_k_edits = set(
            total_edits_of_contributors[total_edits_of_contributors > k].index.to_numpy()
        )
        contributor_count_more_the_k_edits_monthly.append(
            contributors_unique_monthly_set.apply(lambda x: len(x.intersection(contributors_with_more_than_k_edits)))
        )
    save_y_list("general_contributor_count_more_the_k_edits_monthly", MONTHS, contributor_count_more_the_k_edits_monthly, min_edit_count)


def save_tag_top_10_contributor_count_first_changeset_monthly(ddf, tag, data_name_for_top_names):
    names = util.load_json(os.path.join("assets", "data", f"{data_name_for_top_names}.json"))["y_names"][:10]
    tag_to_index = util.load_tag_to_index(DATA_DIR, tag)
    indices = [tag_to_index[tag_name] for tag_name in names]

    contibutor_monthly = ddf[ddf[tag].isin(indices)].groupby(["user_index"])["month_index", tag].first().compute()
    contibutor_count_monthly = contibutor_monthly.reset_index().groupby(["month_index", tag])["user_index"].count()

    save_y_list(
        f"{tag}_top_10_contributor_count_first_changeset_monthly",
        MONTHS,
        util.multi_index_series_to_series_list(contibutor_count_monthly, indices),
        names,
    )


def save_accumulated(data_name):
    data = util.load_json(os.path.join("assets", "data", f"{data_name}.json"))
    if "y" in data:
        data["y"] = np.cumsum(data["y"])
    if "y_list" in data:
        data["y_list"] = [np.cumsum(y) for y in data["y_list"]]
    util.save_json(os.path.join("assets", "data", f"{data_name}_accumulated.json"), data)

def save_top_k(k, data_name):
    data = util.load_json(os.path.join("assets", "data", f"{data_name}.json"))
    data["y_list"] = data["y_list"][:k]
    data["y_names"] = data["y_names"][:k]
    new_data_name = re.sub(r"_top_\d+_", f"_top_{k}_", data_name)
    util.save_json(os.path.join("assets", "data", f"{new_data_name}.json"), data)

def save_percent(data_name, data_name_divide, divide_y=None):
    if divide_y is None:
        data_divide = util.load_json(os.path.join("assets", "data", f"{data_name_divide}.json"))
        divide_y = data_divide["y"]

    data = util.load_json(os.path.join("assets", "data", f"{data_name}.json"))
    if "y" in data:
        data["y"] = (util.save_div(data["y"], divide_y[-len(data["y"]):])*100).tolist()
    if "y_list" in data:
        data["y_list"] = [(util.save_div(y, divide_y[-len(y):])*100).tolist() for y in data["y_list"]]
    util.save_json(os.path.join("assets", "data", f"{data_name}_percent.json"), data)

def save_monthly_to_yearly(data_name):
    data = util.load_json(os.path.join("assets", "data", f"{data_name}.json"))
    years = sorted(list(set([month[:4] for month in data["x"]])))
    
    if "y" in data:
        year_to_value = {year:0 for year in years}
        for value, month in zip(data["y"], data["x"]):
            year_to_value[month[:4]] += value
        data["y"] = [year_to_value[year] for year in years]
    if "y_list" in data:
        new_y_list = []
        for y in data["y_list"]:
            year_to_value = {year:0 for year in years}
            for value, month in zip(y, data["x"]):
                year_to_value[month[:4]] += value
            new_y_list.append([year_to_value[year] for year in years])
        data["y_list"] = new_y_list

    data["x"] = years
    util.save_json(os.path.join("assets", "data", f"{data_name.replace('_monthly', '_yearly')}.json"), data)

def save_sum_of_top_k(data_name):
    data = util.load_json(os.path.join("assets", "data", f"{data_name}.json"))
    save_data = {
        "x": data["x"],
        "y": np.sum(data["y_list"], axis=0)
    }
    util.save_json(os.path.join("assets", "data", f"{data_name}_sum_top_k.json"), save_data)

def save_div(save_data_name, data_name, data_name_divide):
    data = util.load_json(os.path.join("assets", "data", f"{data_name}.json"))
    data_divide = util.load_json(os.path.join("assets", "data", f"{data_name_divide}.json"))
    if "y" in data:
        data["y"] = (util.save_div(data["y"], data_divide["y"][-len(data["y"]):])).tolist()
    if "y_list" in data:
        data["y_list"] = [(util.save_div(y, data_divide["y"][-len(y):])).tolist() for y in data["y_list"]]
    util.save_json(os.path.join("assets", "data", f"{save_data_name}.json"), data)


def main():
    ddf = dd.read_parquet(os.path.join(PARQUET_DIR, "general_*.parquet"))
    # util.save_json(os.path.join(SAVE_DIR, "created_by_name_to_link.json"), get_name_to_link("replace_rules_created_by.json"))
    # util.save_json(os.path.join(SAVE_DIR, "imagery_and_source_name_to_link.json"), get_name_to_link("replace_rules_imagery_and_source.json"))
    # util.save_json(os.path.join(SAVE_DIR, "corporation_name_to_link.json"), {name: link for name, (link, _) in util.load_json(os.path.join("assets", "corporation_contributors.json")).items()})

    # topic: general
    # save_base_statistics(ddf, "general", edit_count_monthly=True, changeset_count_monthly=True, contributors_unique_yearly=True, contributor_count_monthly=True, new_contributor_count_monthly=True, edit_count_map_total=True)
    # save_general_contributor_count_more_the_k_edits_monthly(ddf)
    # save_edit_count_map_yearly(ddf, "general")
    # save_general_edit_count_per_contributor_median_monthly(ddf)
    
    # map_me_indices = np.array([created_by_tag_to_index["MAPS.ME android"], created_by_tag_to_index["MAPS.ME ios"]])
    # save_base_statistics(ddf[~ddf["created_by"].isin(map_me_indices)], "general_no_maps_me", contributor_count_monthly=True)

    # save_accumulated("general_new_contributor_count_monthly")
    # save_accumulated("general_edit_count_monthly")
    # save_accumulated("general_changeset_count_monthly")
    
    
    # # topic: Editing Software
    #save_base_statistics_tag(ddf, "created_by", edit_count_monthly=True, changeset_count_monthly=True, contributors_unique_yearly=True, contributor_count_monthly=True, new_contributor_count_monthly=True)

    # save_top_10("created_by_top_100_contributor_count_monthly")
    # save_top_10("created_by_top_100_new_contributor_count_monthly")
    # save_top_10("created_by_top_100_edit_count_monthly")
    # save_top_10("created_by_top_100_changeset_count_monthly")

    # save_percent("created_by_top_10_contributor_count_monthly", "general_contributor_count_monthly")
    # save_tag_top_10_contributor_count_first_changeset_monthly(ddf, "created_by", "created_by_top_10_contributor_count_monthly")
    # save_monthly_to_yearly("created_by_top_100_edit_count_monthly")
    # save_percent("created_by_top_10_edit_count_monthly", "general_edit_count_monthly")

    # save_accumulated("created_by_top_10_new_contributor_count_monthly")
    # save_accumulated("created_by_top_10_edit_count_monthly")
    # save_accumulated("created_by_top_100_changeset_count_monthly")
    # save_created_by_editor_type_stats(ddf)

    # topic: Corporations
    # save_base_statistics_tag(ddf, "corporation", edit_count_monthly=True, changeset_count_monthly=True, new_contributor_count_monthly=True, edit_count_map_total=True)
    # save_sum_of_top_k("corporation_top_100_edit_count_monthly")
    # save_percent("corporation_top_100_edit_count_monthly_sum_top_k", "general_edit_count_monthly")
    # save_top_10("corporation_top_100_new_contributor_count_monthly")
    # save_top_10("corporation_top_100_edit_count_monthly")
    # save_top_10("corporation_top_100_changeset_count_monthly")
    # save_monthly_to_yearly("corporation_top_100_edit_count_monthly")    
    # save_accumulated("corporation_top_10_new_contributor_count_monthly")
    # save_accumulated("corporation_top_10_edit_count_monthly")
    # save_accumulated("corporation_top_10_changeset_count_monthly")
    


    # topic: Source, Imagery Service, Hashtags
    ddf_all_tags = dd.read_parquet(os.path.join(PARQUET_DIR, "all_tags_*.parquet"))
    all_tags_tag_to_index = util.load_tag_to_index(DATA_DIR, "all_tags")

    # for tag, tag_name_in_all_tags in [("source", "source"), ("imagery", "imagery_used"), ("hashtag", "hashtags")]:
    #     save_base_statistics(ddf_all_tags[ddf_all_tags["all_tags"] == all_tags_tag_to_index[tag_name_in_all_tags]], tag, edit_count_monthly=True)
    #     save_percent(f"{tag}_edit_count_monthly", "general_edit_count_monthly")
        
    #     ddf_tag = dd.read_parquet(os.path.join(PARQUET_DIR, f"{tag}_*.parquet"))
    #     edit_count_map_total = True if tag == "hashtag" else False
    #     save_base_statistics_tag(ddf_tag, tag, edit_count_monthly=True, changeset_count_monthly=True, contributors_unique_yearly=True, contributor_count_monthly=True, new_contributor_count_monthly=True, edit_count_map_total=edit_count_map_total)
    #     save_top_10(f"{tag}_top_100_contributor_count_monthly")
    #     save_top_10(f"{tag}_top_100_new_contributor_count_monthly")
    #     save_top_10(f"{tag}_top_100_edit_count_monthly")
    #     save_top_10(f"{tag}_top_100_changeset_count_monthly")
    #     save_monthly_to_yearly(f"{tag}_top_100_contributor_count_monthly")
    #     save_monthly_to_yearly(f"{tag}_top_100_edit_count_monthly")
    #     save_accumulated(f"{tag}_top_10_new_contributor_count_monthly")
    #     save_accumulated(f"{tag}_top_10_edit_count_monthly")
    #     save_accumulated(f"{tag}_top_10_changeset_count_monthly")


    # topic: streetcomplete
    # save_base_statistics(ddf_all_tags[ddf_all_tags["all_tags"] == all_tags_tag_to_index["StreetComplete"]], tag, edit_count_monthly=True, contributor_count_monthly=True, edit_count_map_total=True)
    # save_percent("streetcomplete_contributor_count_monthly", "general_contributor_count_monthly")
    # save_percent("streetcomplete_edit_count_monthly", "general_edit_count_monthly")

    # save_base_statistics_tag(ddf, "streetcomplete", k=300, edit_count_monthly=True, changeset_count_monthly=True, new_contributor_count_monthly=True)
    # save_monthly_to_yearly("streetcomplete_top_300_edit_count_monthly")
    # save_top_k(10, "streetcomplete_top_300_new_contributor_count_monthly")
    # save_top_k(10, "streetcomplete_top_300_edit_count_monthly")
    # save_top_k(10, "streetcomplete_top_300_changeset_count_monthly")
    # save_accumulated("streetcomplete_top_10_new_contributor_count_monthly")
    # save_accumulated("streetcomplete_top_10_edit_count_monthly")
    # save_accumulated("streetcomplete_top_10_changeset_count_monthly")



    # topic: bot
    # save_base_statistics(ddf[ddf["bot"]==True], "bot", edit_count_monthly=True, changeset_count_monthly=True, contributor_count_monthly=True, new_contributor_count_monthly=True, edit_count_map_total=True)
    # save_percent("bot_edit_count_monthly", "general_edit_count_monthly")
    # save_accumulated("bot_new_contributor_count_monthly")
    # save_accumulated("bot_edit_count_monthly")
    # save_accumulated("bot_changeset_count_monthly")
    # save_div("bot_avg_edit_count_per_changeset_monthly", "bot_edit_count_monthly", "bot_changeset_count_monthly")

    # save_base_statistics_tag(ddf[ddf["bot"]==True], "created_by", k=100, prefix="bot", edit_count_monthly=True)
    # save_monthly_to_yearly("bot_created_by_top_100_edit_count_monthly")


    # topic: Tags
    # save_base_statistics_tag(ddf_all_tags, "all_tags", k=100, changeset_count_monthly=True)
    # save_monthly_to_yearly("all_tags_top_100_changeset_count_monthly")
    # save_top_k(10, "all_tags_top_100_changeset_count_monthly")
    # save_percent("all_tags_top_10_changeset_count_monthly", "general_changeset_count_monthly")

    selected_editors = ["JOSM", "iD", "Potlatch", "StreetComplete", "RapiD", "Vespucci"]
    monthly_data = util.load_json(os.path.join("assets", "data", f"created_by_top_100_changeset_count_monthly.json"))
    editor_name_to_changeset_count_monthly = {name: y for name, y in zip(monthly_data["y_names"], monthly_data["y_list"])}
    for selected_editor_name in selected_editors:
        editor_index = created_by_tag_to_index[selected_editor_name]
        save_base_statistics_tag(ddf_all_tags[ddf_all_tags["created_by"]==editor_index], f"all_tags", prefix=f"created_by_{selected_editor_name}", k=10, changeset_count_monthly=True)
        save_percent(f"created_by_{selected_editor_name}_all_tags_top_10_changeset_count_monthly", None, editor_name_to_changeset_count_monthly[selected_editor_name])



    


if __name__ == "__main__":
    sys.exit(main())
