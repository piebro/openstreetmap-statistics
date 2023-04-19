import os
import json
import re

import numpy as np
import pandas as pd
import dask.dataframe as dd


DEFAULT_PLOT_LAYOUT = {
    "font": {"family": "Times", "size": "15"},
    "paper_bgcolor": "#dfdfdf",
    "plot_bgcolor": "#dfdfdf",
    "margin": {"l": 55, "r": 55, "b": 55, "t": 55},
}

DEFAULT_COLOR_PALETTE = [
    (31, 119, 180),
    (255, 127, 14),
    (44, 160, 44),
    (214, 39, 40),
    (148, 103, 189),
    (140, 86, 75),
    (227, 119, 194),
    (127, 127, 127),
    (188, 189, 34),
    (23, 190, 207),
]


def save_json(file_path, obj):
    with open(file_path, "w", encoding="UTF-8") as json_file:
        json_file.write(pd.io.json.dumps(obj, double_precision=2))


def load_json(file_path):
    with open(file_path, "r", encoding="UTF-8") as json_file:
        return json.load(json_file)


def get_months_years(data_dir):
    with open(os.path.join(data_dir, "months.txt"), "r", encoding="UTF-8") as f:
        months = [line[:-1] for line in f.readlines()]
    years = sorted(list(set([m[:4] for m in months])))
    return months, years


def list_to_dict(l):
    return {e: i for i, e in enumerate(l)}


def load_index_to_tag(data_dir, data_name):
    with open(os.path.join(data_dir, f"index_to_tag_{data_name}.txt"), "r", encoding="UTF-8") as f:
        return [l[:-1] for l in f.readlines()]


def load_tag_to_index(data_dir, data_name):
    return list_to_dict(load_index_to_tag(data_dir, data_name))


def load_ddf(data_dir, tag, columns=None, filters=None):
    ddf = dd.read_parquet(os.path.join(data_dir, "changeset_data", tag), columns=columns, filters=filters)
    return ddf


def multi_index_series_to_series_list(multi_index_series, level_1_indices):
    return [multi_index_series[multi_index_series.index.get_level_values(1) == i].droplevel(1) for i in level_1_indices]


def series_to_series_list(series, level_0_indices):
    return [series[series.index.get_level_values(0) == i] for i in level_0_indices]


def cumsum_new_nunique(series):
    previous_set = set()
    indices = []
    values = []
    for index, value in series.items():
        value = set(value)
        indices.append(index)
        values.append(len(value - previous_set))
        previous_set.update(value)

    if isinstance(indices[0], tuple):
        return pd.Series(data=values, index=pd.MultiIndex.from_tuples(indices, names=series.index.names))
    return pd.Series(data=values, index=indices)


def cumsum_new_nunique_set_list(set_list):
    previous_set = set()
    values = []
    for s in set_list:
        values.append(len(s - previous_set))
        previous_set.update(s)
    return values


def safe_div(a, b):
    a, b = np.array(a, dtype=float), np.array(b, dtype=float)
    return np.divide(a, b, out=np.zeros_like(a), where=(b != 0), casting="unsafe")


def save_base_statistics(
    data_dir,
    progress_bar,
    prefix,
    ddf,
    edit_count_monthly=False,
    changeset_count_monthly=False,
    contributors_unique_yearly=False,
    contributor_count_monthly=False,
    new_contributor_count_monthly=False,
    edit_count_map_total=False,
):

    months, years = get_months_years(data_dir)

    if edit_count_monthly:
        # ddf = load_ddf(data_dir, "general", columns=["month_index", "edits"])
        save_y(
            progress_bar, f"{prefix}_edit_count_monthly", months, ddf.groupby(["month_index"])["edits"].sum().compute()
        )

    if changeset_count_monthly:
        # ddf = load_ddf(data_dir, "general", columns=["month_index"])
        save_y(progress_bar, f"{prefix}_changeset_count_monthly", months, ddf.groupby(["month_index"]).size().compute())

    if contributors_unique_yearly:
        # ddf = load_ddf(data_dir, "general", columns=["year_index", "user_index"])
        save_y(
            progress_bar,
            f"{prefix}_contributors_unique_yearly",
            years,
            ddf.groupby(["year_index"])["user_index"].nunique().compute(),
        )

    if contributor_count_monthly or new_contributor_count_monthly:
        # ddf = load_ddf(data_dir, "general", columns=["month_index", "user_index"])
        contributors_unique_monthly = ddf.groupby(["month_index"])["user_index"].unique().compute()
        if contributor_count_monthly:
            save_y(progress_bar, f"{prefix}_contributor_count_monthly", months, contributors_unique_monthly.apply(len))
        if new_contributor_count_monthly:
            save_y(
                progress_bar,
                f"{prefix}_new_contributor_count_monthly",
                months,
                cumsum_new_nunique(contributors_unique_monthly),
            )
        contributors_unique_monthly = None

    if edit_count_map_total:
        # ddf = load_ddf(data_dir, "general", columns=["edits", "pos_x", "pos_y"])
        save_map(
            progress_bar,
            f"{prefix}_edit_count_map_total",
            ddf[ddf["pos_x"] >= 0].groupby(["pos_x", "pos_y"])["edits"].sum().compute(),
        )


def save_base_statistics_tag(
    data_dir,
    progress_bar,
    tag,
    ddf,
    contributor_count_ddf_name=None,
    k=100,
    prefix=None,
    edit_count_monthly=False,
    changeset_count_monthly=False,
    contributors_unique_yearly=False,
    contributor_count_monthly=False,
    new_contributor_count_monthly=False,
    edit_count_map_total=False,
):

    months, years = get_months_years(data_dir)
    prefix = "" if prefix is None else f"{prefix}_"
    top_k = get_tag_top_k_and_save_top_k_total(
        data_dir,
        progress_bar,
        tag,
        ddf,
        prefix,
        k=k,
        contributor_count=(contributors_unique_yearly or contributor_count_monthly or new_contributor_count_monthly),
        edit_count=edit_count_monthly,
        changeset_count=changeset_count_monthly,
    )

    if edit_count_monthly:
        indices, names = top_k["edit_count"]
        monthly_list = multi_index_series_to_series_list(
            ddf[ddf[tag].isin(indices)].groupby(["month_index", tag])["edits"].sum().compute(), indices
        )
        save_y_list(progress_bar, f"{prefix}{tag}_top_{k}_edit_count_monthly", months, monthly_list, names)

    if changeset_count_monthly:
        indices, names = top_k["changeset_count"]
        monthly_list = multi_index_series_to_series_list(
            ddf[ddf[tag].isin(indices)].groupby(["month_index", tag]).size().compute(), indices
        )
        save_y_list(progress_bar, f"{prefix}{tag}_top_{k}_changeset_count_monthly", months, monthly_list, names)

    if contributors_unique_yearly:
        indices, names = top_k["contributor_count"]
        yearly_list = multi_index_series_to_series_list(
            ddf[ddf[tag].isin(indices)].groupby(["year_index", tag])["user_index"].nunique().compute(), indices
        )
        save_y_list(progress_bar, f"{prefix}{tag}_top_{k}_contributor_count_yearly", years, yearly_list, names)

    if contributor_count_monthly or new_contributor_count_monthly:
        indices, names = top_k["contributor_count"]
        unique_monthly_list = [[set() for _ in range(len(months))] for _ in range(len(indices))]
        for month_i in range(len(months)):
            filters = [("month_index", "==", month_i)]
            temp_ddf = load_ddf(data_dir, contributor_count_ddf_name, ("user_index", tag), filters)
            contributor_unique_month = (
                temp_ddf[temp_ddf[tag].isin(indices)].groupby([tag])["user_index"].unique().compute()
            )
            for i, tag_index in enumerate(indices):
                value = contributor_unique_month[contributor_unique_month.index == tag_index].values
                if len(value) > 0:
                    unique_monthly_list[i][month_i] = set(value[0].tolist())

        if contributor_count_monthly:
            save_y_list(
                progress_bar,
                f"{prefix}{tag}_top_{k}_contributor_count_monthly",
                months,
                None,
                names,
                y_list=[[len(unique_set) for unique_set in month_list] for month_list in unique_monthly_list],
            )

        if new_contributor_count_monthly:
            save_y_list(
                progress_bar,
                f"{prefix}{tag}_top_{k}_new_contributor_count_monthly",
                months,
                None,
                names,
                y_list=[cumsum_new_nunique_set_list(month_list) for month_list in unique_monthly_list],
            )

    if edit_count_map_total:
        indices, names = top_k["edit_count"]
        indices = indices[:10]
        names = names[:10]
        ddf_is_in = ddf[ddf[tag].isin(indices)]
        tag_map_edits = ddf_is_in[ddf_is_in["pos_x"] >= 0].groupby(["pos_x", "pos_y", tag])["edits"].sum().compute()
        tag_maps = [tag_map_edits[tag_map_edits.index.get_level_values(tag) == i].droplevel(2) for i in indices]
        save_maps(progress_bar, f"{prefix}{tag}_top_10_edit_count_maps_total", tag_maps, names)


def get_tag_top_k_and_save_top_k_total(
    data_dir, progress_bar, tag, ddf, prefix, k, contributor_count=False, edit_count=False, changeset_count=False
):
    index_to_tag = load_index_to_tag(data_dir, tag)

    if tag in ["created_by", "corporation", "streetcomplete"]:
        highest_number = np.iinfo(ddf[tag].dtype).max
        ddf = ddf[ddf[tag] < highest_number]

    units = []
    if edit_count:
        units.append("edit_count")
    if changeset_count:
        units.append("changeset_count")
    if contributor_count:
        units.append("contributor_count")

    unit_to_top_k_indices_and_names = {"tag": tag}
    for unit in units:
        if unit == "contributor_count":
            # TODO: this is by far the slowest part in gathering all statistics. Are the ways for speeding it up?
            # Maybe use https://docs.dask.org/en/stable/generated/dask.dataframe.Series.nunique_approx.html
            # as a fist approximation and then get the right statistics on a subset of all tags
            # or reducing the number of different tags (e.g. less distinct sources)
            total = ddf.groupby(tag)["user_index"].nunique().compute()
        if unit == "edit_count":
            total = ddf.groupby(tag)["edits"].sum().compute()
        if unit == "changeset_count":
            total = ddf.groupby(tag).size().compute()

        indices = total.index[(-total.values.astype(np.int64)).argsort()[:k]]
        names = [index_to_tag[i] for i in indices]
        unit_to_top_k_indices_and_names[unit] = (indices, names)

        save_y_list(
            progress_bar, f"{prefix}{tag}_top_{k}_{unit}_total", None, series_to_series_list(total, indices), names
        )

    return unit_to_top_k_indices_and_names


def save_edit_count_map_yearly(years, progress_bar, prefix, ddf):
    yearly_map_edits = ddf[ddf["pos_x"] >= 0].groupby(["year_index", "pos_x", "pos_y"])["edits"].sum().compute()
    year_maps = [
        yearly_map_edits[yearly_map_edits.index.get_level_values("year_index") == year_i].droplevel(0)
        for year_i in range(len(years))
    ]
    year_map_names = [f"total edits {year}" for year in years]
    save_maps(progress_bar, f"{prefix}_edit_count_maps_yearly", year_maps, year_map_names)


def save_tag_top_10_contributor_count_first_changeset_monthly(
    months, progress_bar, tag, ddf, tag_to_index, data_name_for_top_names
):
    names = load_json(os.path.join("assets", "data", f"{data_name_for_top_names}.json"))["y_names"][:10]
    indices = [tag_to_index[tag_name] for tag_name in names]

    contibutor_monthly = ddf[ddf[tag].isin(indices)].groupby(["user_index"])["month_index", tag].first().compute()
    contibutor_count_monthly = contibutor_monthly.reset_index().groupby(["month_index", tag])["user_index"].count()

    save_y_list(
        progress_bar,
        f"{tag}_top_10_contributor_count_first_changeset_monthly",
        months,
        multi_index_series_to_series_list(contibutor_count_monthly, indices),
        names,
    )


def save_data_dict(progress_bar, name, data_dict):
    save_json(os.path.join("assets", "data", f"{name}.json"), data_dict)
    print(name)
    progress_bar.update(1)


def load_data_dict(name):
    load_json(os.path.join("assets", "data", f"{name}.json"))


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
                    y[i] = y[i - 1]

        return y.tolist()


def save_y(progress_bar, name, x, pd_series, index_offset=3, cumsum=False, y=None):
    if y is None:
        y = pd_series_to_y(x, pd_series, cumsum)
    if x is None:
        data_dict = {"y": y[start_index:]}
    else:
        start_index = np.max([0, np.nonzero(y)[0][0] - index_offset])
        data_dict = {"x": x[start_index:], "y": y[start_index:]}
    save_data_dict(progress_bar, name, data_dict)


def save_y_list(progress_bar, name, x, pd_series_list, y_names, index_offset=3, cumsum=False, y_list=None):
    if y_list is None:
        y_list = [pd_series_to_y(x, pd_series, cumsum) for pd_series in pd_series_list]
    if x is None:
        data_dict = {"y_list": y_list, "y_names": y_names}
    else:
        start_index = np.min([np.max([0, np.nonzero(y)[0][0] - index_offset]) for y in y_list if np.any(y)])
        y_list = [y[start_index:] for y in y_list]
        data_dict = {"x": x[start_index:], "y_list": y_list, "y_names": y_names}
    save_data_dict(progress_bar, name, data_dict)


def histogram_2d_to_xyz(pd_histogram_2d):
    histogram_2d = np.zeros((360, 180), dtype=np.uint32)
    for index, value in pd_histogram_2d.items():
        histogram_2d[index] = value
    x, y = histogram_2d.nonzero()
    x, y, z = (x.tolist(), y.tolist(), histogram_2d[x, y].tolist())
    max_z_value = int(np.max(z))
    return x, y, z, max_z_value


def save_map(progress_bar, name, pd_histogram_2d):
    x, y, z, max_z_value = histogram_2d_to_xyz(pd_histogram_2d)
    data_dict = {name: {"x": x, "y": y, "z": z}, "max_z_value": max_z_value}
    save_data_dict(progress_bar, name, data_dict)


def save_maps(progress_bar, name, pd_histogram_2d_list, map_names):
    data_dict = {
        name: {},
    }
    max_z_values = []
    for pd_histogram_2d, map_name in zip(pd_histogram_2d_list, map_names):
        x, y, z, max_z_value = histogram_2d_to_xyz(pd_histogram_2d)
        max_z_values.append(max_z_value)
        data_dict[name][map_name] = {"x": x, "y": y, "z": z}
    data_dict["max_z_value"] = int(np.max(max_z_values))
    save_data_dict(progress_bar, name, data_dict)


def get_name_to_link(replace_rules_file_name):
    name_to_tags_and_link = load_json(os.path.join("src", replace_rules_file_name))
    name_to_link = {}
    for name, name_infos in name_to_tags_and_link.items():
        if "link" in name_infos:
            name_to_link[name] = name_infos["link"]

    return name_to_link


def save_accumulated(data_name):
    data = load_json(os.path.join("assets", "data", f"{data_name}.json"))
    if "y" in data:
        data["y"] = np.cumsum(data["y"])
    if "y_list" in data:
        data["y_list"] = [np.cumsum(y) for y in data["y_list"]]
    save_json(os.path.join("assets", "data", f"{data_name}_accumulated.json"), data)


def save_top_k(k, data_name):
    data = load_json(os.path.join("assets", "data", f"{data_name}.json"))
    data["y_list"] = data["y_list"][:k]
    data["y_names"] = data["y_names"][:k]
    new_data_name = re.sub(r"_top_\d+_", f"_top_{k}_", data_name)
    save_json(os.path.join("assets", "data", f"{new_data_name}.json"), data)


def save_percent(data_name, data_name_divide, divide_y=None):
    if divide_y is None:
        data_divide = load_json(os.path.join("assets", "data", f"{data_name_divide}.json"))
        divide_y = data_divide["y"]

    data = load_json(os.path.join("assets", "data", f"{data_name}.json"))
    if "y" in data:
        data["y"] = (safe_div(data["y"], divide_y[-len(data["y"]) :]) * 100).tolist()
    if "y_list" in data:
        data["y_list"] = [(safe_div(y, divide_y[-len(y) :]) * 100).tolist() for y in data["y_list"]]
    save_json(os.path.join("assets", "data", f"{data_name}_percent.json"), data)


def save_monthly_to_yearly(data_name):
    data = load_json(os.path.join("assets", "data", f"{data_name}.json"))
    years = sorted(list(set([month[:4] for month in data["x"]])))
    
    
    if "y" in data:
        year_to_value = {year: 0 for year in years}
        for value, month in zip(data["y"], data["x"]):
            year_to_value[month[:4]] += value
        data["y"] = [year_to_value[year] for year in years]
    if "y_list" in data:
        new_y_list = []
        for y in data["y_list"]:
            year_to_value = {year: 0 for year in years}
            for value, month in zip(y, data["x"]):
                year_to_value[month[:4]] += value
            new_y_list.append([year_to_value[year] for year in years])
        
        if np.sum([new_y[0] for new_y in new_y_list]) == 0:
            new_y_list = [new_y[1:] for new_y in new_y_list]
            years = years[1:]

        data["y_list"] = new_y_list
    data["x"] = years
    save_json(os.path.join("assets", "data", f"{data_name.replace('_monthly', '_yearly')}.json"), data)


def save_sum_of_top_k(data_name):
    data = load_json(os.path.join("assets", "data", f"{data_name}.json"))
    save_data = {"x": data["x"], "y": np.sum(data["y_list"], axis=0)}
    save_json(os.path.join("assets", "data", f"{data_name}_sum_top_k.json"), save_data)


def save_div(save_data_name, data_name, data_name_divide):
    data = load_json(os.path.join("assets", "data", f"{data_name}.json"))
    data_divide = load_json(os.path.join("assets", "data", f"{data_name_divide}.json"))
    if "y" in data:
        data["y"] = (safe_div(data["y"], data_divide["y"][-len(data["y"]) :])).tolist()
    if "y_list" in data:
        data["y_list"] = [(safe_div(y, data_divide["y"][-len(y) :])).tolist() for y in data["y_list"]]
    save_json(os.path.join("assets", "data", f"{save_data_name}.json"), data)
