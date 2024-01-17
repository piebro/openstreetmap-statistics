import json
import re
from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd


def save_json(file_path, obj):
    with Path(file_path).open("w", encoding="UTF-8") as json_file:
        json_file.write(pd.io.json.dumps(obj, double_precision=2))


def load_json(file_path):
    with Path(file_path).open("r", encoding="UTF-8") as json_file:
        return json.load(json_file)


def get_months_years(data_dir):
    with (Path(data_dir) / "months.txt").open("r", encoding="UTF-8") as f:
        months = [line[:-1] for line in f.readlines()]
    years = sorted({m[:4] for m in months})
    return np.array(months), np.array(years)


def get_month_year_dicts(data_dir):
    months, years = get_months_years(data_dir)
    return {"month": list_to_index_dict(months), "year": list_to_index_dict(years)}


def get_year_index_to_month_indices(data_dir):
    months, years = get_months_years(data_dir)
    years = years.tolist()

    year_index_to_month_indices = [[] for _ in years]
    for month_i, month in enumerate(months):
        year_index_to_month_indices[years.index(month[:4])].append(month_i)

    return year_index_to_month_indices


def list_to_dict(input_list):
    return {e: i for i, e in enumerate(input_list)}


def list_to_index_dict(input_list):
    return dict(enumerate(input_list))


def load_index_to_tag(data_dir, data_name):
    with (Path(data_dir) / f"index_to_tag_{data_name}.txt").open("r", encoding="UTF-8") as f:
        return [line[:-1] for line in f.readlines()]


def load_tag_to_index(data_dir, data_name):
    return list_to_dict(load_index_to_tag(data_dir, data_name))


def load_ddf(data_dir, tag, columns=None, filters=None):
    return dd.read_parquet(Path(data_dir) / "changeset_data" / tag, columns=columns, filters=filters)


def cumsum_new_nunique(series):
    previous_set = set()
    indices = []
    values = []
    for index, value in series.items():
        value_set = set(value)
        indices.append(index)
        values.append(len(value_set - previous_set))
        previous_set.update(value_set)

    if isinstance(indices[0], tuple):
        index = pd.MultiIndex.from_tuples(indices, names=series.index.names)
    else:
        index = pd.Index(indices, name=series.index.name)

    return pd.Series(data=values, index=index, name=series.name)


def cumsum_new_nunique_set_list(set_list):
    previous_set = set()
    values = []
    for s in set_list:
        values.append(len(s - previous_set))
        previous_set.update(s)
    return values


def save_data(time_dict, progress_bar, name, pd_df_or_series, columns):
    pd_df_or_series = pd_df_or_series.reset_index()

    start_index_offset = None
    if "month_index" in pd_df_or_series:
        pd_df_or_series["months"] = pd_df_or_series["month_index"].map(time_dict["month"])
        start_index_offset = 3
    elif "year_index" in pd_df_or_series:
        pd_df_or_series["years"] = pd_df_or_series["year_index"].map(time_dict["year"])
        start_index_offset = 0
    columns = list(columns)

    # delete rows with only zeros in them
    if start_index_offset is not None:
        value_columns = [c for c in columns if c not in ["months", "years"]]
        if_zero_row = (pd_df_or_series[value_columns] != 0).any(axis=1).to_numpy()

        if len(np.nonzero(if_zero_row)[0]) > 0:
            start_index = np.max([0, np.nonzero(if_zero_row)[0][0] - start_index_offset])
            pd_df_or_series = pd_df_or_series.loc[start_index:]

    pd_df_or_series[columns].to_json(Path("assets") / "data" / f"{name}.json", orient="split", index=False, indent=1)
    if progress_bar is not None:
        print(name)
        progress_bar.update(1)


def histogram_2d_to_xyz(pd_histogram_2d):
    histogram_2d = np.zeros((360, 180), dtype=np.uint32)
    for index, value in pd_histogram_2d.items():
        histogram_2d[index] = value
    x, y = histogram_2d.nonzero()
    z = histogram_2d[x, y]
    return x, y, z


def save_map(progress_bar, name, pd_histogram_2d):
    x, y, z = histogram_2d_to_xyz(pd_histogram_2d)
    xyz_df = pd.DataFrame({"x": [x], "y": [y], "z": [z], "max_z_value": int(np.max(z))})
    save_data(None, progress_bar, name, xyz_df, ("max_z_value", "x", "y", "z"))


def save_maps(progress_bar, name, pd_histogram_2d_list, map_names):
    x_list, y_list, z_list, max_z_value_list = [], [], [], []
    for pd_histogram_2d in pd_histogram_2d_list:
        x, y, z = histogram_2d_to_xyz(pd_histogram_2d)
        x_list.append(x)
        y_list.append(y)
        z_list.append(z)
        max_z_value = int(np.max(z)) if len(z) > 0 else 0
        max_z_value_list.append(max_z_value)

    xyz_df = pd.DataFrame(
        {
            "map_name": map_names,
            "x": x_list,
            "y": y_list,
            "z": z_list,
            "max_z_value": [np.max(max_z_value_list)] * len(map_names),
        },
    )
    save_data(None, progress_bar, name, xyz_df, ("map_name", "max_z_value", "x", "y", "z"))


def save_base_statistics(
    data_dir,
    progress_bar,
    prefix,
    ddf,
    edit_count_monthly=False,
    changeset_count_monthly=False,
    contributor_count_yearly=False,
    contributor_count_monthly=False,
    new_contributor_count_monthly=False,
    edit_count_map_total=False,
):
    time_dict = get_month_year_dicts(data_dir)

    if edit_count_monthly:
        save_data(
            time_dict,
            progress_bar,
            f"{prefix}_edit_count_monthly",
            ddf.groupby(["month_index"], observed=False)["edits"].sum().compute(),
            ("months", "edits"),
        )

    if changeset_count_monthly:
        save_data(
            time_dict,
            progress_bar,
            f"{prefix}_changeset_count_monthly",
            ddf.groupby(["month_index"], observed=False).size().compute().rename("changesets"),
            ("months", "changesets"),
        )

    if contributor_count_yearly:
        save_data(
            time_dict,
            progress_bar,
            f"{prefix}_contributors_unique_yearly",
            ddf.groupby(["year_index"], observed=False)["user_index"].nunique().compute().rename("contributors"),
            ("years", "contributors"),
        )

    if contributor_count_monthly or new_contributor_count_monthly:
        contributors_unique_monthly = (
            ddf.groupby(["month_index"], observed=False)["user_index"].unique().compute().rename("contributors")
        )
        if contributor_count_monthly:
            save_data(
                time_dict,
                progress_bar,
                f"{prefix}_contributor_count_monthly",
                contributors_unique_monthly.apply(len),
                ("months", "contributors"),
            )
        if new_contributor_count_monthly:
            save_data(
                time_dict,
                progress_bar,
                f"{prefix}_new_contributor_count_monthly",
                cumsum_new_nunique(contributors_unique_monthly),
                ("months", "contributors"),
            )
        contributors_unique_monthly = None

    if edit_count_map_total:
        save_map(
            progress_bar,
            f"{prefix}_edit_count_map_total",
            ddf[ddf["pos_x"] >= 0].groupby(["pos_x", "pos_y"], observed=False)["edits"].sum().compute(),
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
    contributor_count_yearly=False,
    contributor_count_monthly=False,
    new_contributor_count_monthly=False,
    top_10_edit_count_map_total=False,
    top_50_edit_count_map_total=False,
):
    months, years = get_months_years(data_dir)
    time_dict = get_month_year_dicts(data_dir)

    prefix = "" if prefix is None else f"{prefix}_"
    top_k = get_tag_top_k_and_save_top_k_total(
        data_dir,
        progress_bar,
        time_dict,
        tag,
        ddf,
        prefix,
        k=k,
        contributor_count=(contributor_count_yearly or contributor_count_monthly or new_contributor_count_monthly),
        edit_count=edit_count_monthly,
        changeset_count=changeset_count_monthly,
    )

    if edit_count_monthly:
        indices, names = top_k["edit_count"]
        df = (
            ddf[ddf[tag].isin(indices)]
            .groupby(["month_index", tag], observed=False)["edits"]
            .sum()
            .compute()
            .reset_index()
        )
        df = pd.pivot_table(df, values="edits", index="month_index", columns=tag)[indices]
        df.columns = names

        save_data(
            time_dict,
            progress_bar,
            f"{prefix}{tag}_top_{k}_edit_count_monthly",
            df,
            ("months", *df.columns.to_numpy()),
        )

    if changeset_count_monthly:
        indices, names = top_k["changeset_count"]
        df = (
            ddf[ddf[tag].isin(indices)]
            .groupby(["month_index", tag], observed=False)
            .size()
            .compute()
            .rename("changesets")
            .reset_index()
        )
        df = pd.pivot_table(df, values="changesets", index="month_index", columns=tag)[indices]
        df.columns = names

        save_data(
            time_dict,
            progress_bar,
            f"{prefix}{tag}_top_{k}_changeset_count_monthly",
            df,
            ("months", *df.columns.to_numpy()),
        )

    if contributor_count_yearly or contributor_count_monthly or new_contributor_count_monthly:
        indices, names = top_k["contributor_count"]
        unique_monthly_list = [[set() for _ in range(len(months))] for _ in range(len(indices))]

        for month_i in range(len(months)):
            filters = [("month_index", "==", month_i)]
            temp_ddf = load_ddf(data_dir, contributor_count_ddf_name, ("user_index", tag), filters)
            contributor_unique_month = (
                temp_ddf[temp_ddf[tag].isin(indices)].groupby([tag], observed=False)["user_index"].unique().compute()
            )
            for i, tag_index in enumerate(indices):
                value = contributor_unique_month[contributor_unique_month.index == tag_index].to_numpy()
                if len(value) > 0:
                    unique_monthly_list[i][month_i] = set(value[0].tolist())

        if contributor_count_yearly:
            year_index_to_month_indices = get_year_index_to_month_indices(data_dir)
            contributor_count_yearly_list = []
            for month_list in unique_monthly_list:
                contributor_count_yearly = []
                for year_i, _ in enumerate(years):
                    contributor_count_yearly.append(
                        len(set().union(*[month_list[month_i] for month_i in year_index_to_month_indices[year_i]])),
                    )
                contributor_count_yearly_list.append(contributor_count_yearly)

            df = pd.DataFrame(np.array(contributor_count_yearly_list).transpose(), columns=names).rename_axis(
                "year_index",
            )
            save_data(
                time_dict,
                progress_bar,
                f"{prefix}{tag}_top_{k}_contributor_count_yearly",
                df,
                ("years", *df.columns.to_numpy()),
            )

        if contributor_count_monthly:
            df = pd.DataFrame(
                np.array(
                    [[len(unique_set) for unique_set in month_list] for month_list in unique_monthly_list],
                ).transpose(),
                columns=names,
            ).rename_axis("month_index")
            save_data(
                time_dict,
                progress_bar,
                f"{prefix}{tag}_top_{k}_contributor_count_monthly",
                df,
                ("months", *df.columns.to_numpy()),
            )

        if new_contributor_count_monthly:
            df = pd.DataFrame(
                np.array([cumsum_new_nunique_set_list(month_list) for month_list in unique_monthly_list]).transpose(),
                columns=names,
            ).rename_axis("month_index")
            save_data(
                time_dict,
                progress_bar,
                f"{prefix}{tag}_top_{k}_new_contributor_count_monthly",
                df,
                ("months", *df.columns.to_numpy()),
            )

    max_map_index = 0
    if top_10_edit_count_map_total:
        max_map_index = 10
    if top_50_edit_count_map_total:
        max_map_index = 50

    for i in range(0, max_map_index, 10):
        indices, names = top_k["edit_count"]
        top_indices = indices[i : i + 10]
        top_names = names[i : i + 10]
        if len(top_names) == 0:
            break

        ddf_is_in = ddf[ddf[tag].isin(top_indices)]
        tag_map_edits = (
            ddf_is_in[ddf_is_in["pos_x"] >= 0].groupby(["pos_x", "pos_y", tag], observed=False)["edits"].sum().compute()
        )
        tag_maps = [tag_map_edits[tag_map_edits.index.get_level_values(tag) == i].droplevel(2) for i in top_indices]

        if i == 0:
            save_name = f"{prefix}{tag}_top_10_edit_count_maps_total"
        else:
            save_name = f"{prefix}{tag}_top_{i}_to_{i+10}_edit_count_maps_total"

        save_maps(progress_bar, save_name, tag_maps, top_names)


def get_tag_top_k_and_save_top_k_total(
    data_dir,
    progress_bar,
    time_dict,
    tag,
    ddf,
    prefix,
    k,
    contributor_count=False,
    edit_count=False,
    changeset_count=False,
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
            # TODO: this is by far the slowest part in gathering all statistics (for tags with many tagnames).
            # Are there ways for speeding it up?
            # Maybe use https://docs.dask.org/en/stable/generated/dask.dataframe.Series.nunique_approx.html
            # as a fist approximation and then get the right statistics on a subset of all tags
            # or reducing the number of different tags (e.g. less distinct sources)
            total = ddf.groupby(tag, observed=False)["user_index"].nunique().compute()
        if unit == "edit_count":
            total = ddf.groupby(tag, observed=False)["edits"].sum().compute()
        if unit == "changeset_count":
            total = ddf.groupby(tag, observed=False).size().compute()

        indices = total.index[(-total.to_numpy().astype(np.int64)).argsort()[:k]]
        names = [index_to_tag[i] for i in indices]
        unit_to_top_k_indices_and_names[unit] = (indices, names)

        df = pd.DataFrame([total.loc[indices].to_numpy()], columns=names)
        save_data(
            time_dict,
            progress_bar,
            f"{prefix}{tag}_top_{k}_{unit}_total",
            df,
            df.columns,
        )

    return unit_to_top_k_indices_and_names


def save_edit_count_map_yearly(years, progress_bar, prefix, ddf):
    yearly_map_edits = (
        ddf[ddf["pos_x"] >= 0].groupby(["year_index", "pos_x", "pos_y"], observed=False)["edits"].sum().compute()
    )
    year_maps = [
        yearly_map_edits[yearly_map_edits.index.get_level_values("year_index") == year_i].droplevel(0)
        for year_i in range(len(years))
    ]
    year_map_names = [f"total edits {year}" for year in years]
    save_maps(progress_bar, f"{prefix}_edit_count_maps_yearly", year_maps, year_map_names)


def save_tag_top_10_contributor_count_first_changeset_monthly(
    time_dict,
    progress_bar,
    tag,
    ddf,
    tag_to_index,
    data_name_for_top_names,
):
    df_names = pd.read_json(Path("assets") / "data" / f"{data_name_for_top_names}.json", orient="split")
    names = get_columns_without_months_and_years(df_names)
    indices = [tag_to_index[tag_name] for tag_name in names]

    contibutor_monthly = (
        ddf[ddf[tag].isin(indices)].groupby(["user_index"], observed=False)["month_index", tag].first().compute()
    )
    contibutor_count_monthly = (
        contibutor_monthly.reset_index().groupby(["month_index", tag], observed=False)["user_index"].count()
    )
    contibutor_count_monthly = contibutor_count_monthly.rename("contributors").reset_index()

    df = pd.pivot_table(contibutor_count_monthly, values="contributors", index="month_index", columns=tag)[indices]
    df.columns = names

    save_data(
        time_dict,
        progress_bar,
        f"{tag}_top_10_contributor_count_first_changeset_monthly",
        df,
        ("months", *df.columns.to_numpy()),
    )


def get_name_to_link(replace_rules_file_name):
    name_to_tags_and_link = load_json(Path("src") / replace_rules_file_name)
    name_to_link = {}
    for name, name_infos in name_to_tags_and_link.items():
        if "link" in name_infos:
            name_to_link[name] = name_infos["link"]

    return name_to_link


def get_columns_without_months_and_years(df):
    return [v for v in df.columns.to_numpy() if v not in ["months", "years"]]


def save_accumulated(data_name):
    df = pd.read_json(Path("assets") / "data" / f"{data_name}.json", orient="split")
    columns = get_columns_without_months_and_years(df)
    df[columns] = df[columns].cumsum()
    df.to_json(Path("assets") / "data" / f"{data_name}_accumulated.json", orient="split", index=False, indent=1)


def save_top_k(k, data_name):
    df = pd.read_json(Path("assets") / "data" / f"{data_name}.json", orient="split")
    if "months" in df:
        df = df[["months", *get_columns_without_months_and_years(df)[:k]]]
    else:
        df = df[get_columns_without_months_and_years(df)[:k]]
    new_data_name = re.sub(r"_top_\d+_", f"_top_{k}_", data_name)
    df.to_json(Path("assets") / "data" / f"{new_data_name}.json", orient="split", index=False, indent=1)


def save_percent(data_name, data_name_divide, merge_on_column_name, divide_column_name):
    df_divide = pd.read_json(Path("assets") / "data" / f"{data_name_divide}.json", orient="split")
    df_divide = df_divide.rename(columns={divide_column_name: f"{divide_column_name}_divide"})

    df = pd.read_json(Path("assets") / "data" / f"{data_name}.json", orient="split")
    columns = get_columns_without_months_and_years(df)
    merged_df = df.merge(df_divide, on=merge_on_column_name, suffixes=("", "_divide"))

    merged_df[columns] = (
        merged_df[columns].div(merged_df[f"{divide_column_name}_divide"], axis="index").mul(100, axis="index")
    )
    merged_df[[merge_on_column_name, *columns]].to_json(
        Path("assets") / "data" / f"{data_name}_percent.json",
        orient="split",
        index=False,
        indent=1,
        double_precision=1,
    )


def save_monthly_to_yearly(data_name, only_full_years=False):
    if "contributor" in data_name:
        raise ValueError("save_monthly_to_yearly does not work for contributors")
    df = pd.read_json(Path("assets") / "data" / f"{data_name}.json", orient="split")

    if "months" not in df:
        raise ValueError("'month' in in dataframe")

    df["months"] = df["months"].apply(lambda v: v[:4])
    df = df.rename(columns={"months": "years"})
    years = df["years"].to_numpy()

    df = df.groupby(["years"], observed=False).sum().reset_index()
    if only_full_years and years[-12] != years[-1]:
        df = df[:-1]

    suffix = "_only_full_years" if only_full_years else ""
    df.to_json(
        Path("assets") / "data" / f"{data_name.replace('_monthly', '_yearly')}{suffix}.json",
        orient="split",
        index=False,
        indent=1,
    )


def save_sum_of_top_k(data_name):
    df = pd.read_json(Path("assets") / "data" / f"{data_name}.json", orient="split")
    new_df = df[get_columns_without_months_and_years(df)].sum(axis=1).to_frame().rename(columns={0: "top_k_sum"})
    if "months" in df:
        new_df.insert(0, "months", df["months"])
    elif "years" in df:
        new_df.insert(0, "years", df["years"])
    new_df.to_json(Path("assets") / "data" / f"{data_name}_sum_top_k.json", orient="split", index=False, indent=1)


def save_div(save_data_name, data_name, data_name_divide, merge_on_column_name, divide_column_name):
    df_divide = pd.read_json(Path("assets") / "data" / f"{data_name_divide}.json", orient="split")
    df_divide = df_divide.rename(columns={divide_column_name: f"{divide_column_name}_divide"})

    df = pd.read_json(Path("assets") / "data" / f"{data_name}.json", orient="split")
    columns = get_columns_without_months_and_years(df)
    merged_df = df.merge(df_divide, on=merge_on_column_name, suffixes=("", "_divide"))

    merged_df[columns] = merged_df[columns].div(merged_df[f"{divide_column_name}_divide"], axis="index")
    merged_df[[merge_on_column_name, *columns]].to_json(
        Path("assets") / "data" / f"{save_data_name}.json",
        orient="split",
        index=False,
        indent=1,
        double_precision=2,
    )


def save_merged_yearly_total_data(yearly_data_name, total_data_name):
    df_yearly = pd.read_json(Path("assets") / "data" / f"{yearly_data_name}.json", orient="split")
    df_total = pd.read_json(Path("assets") / "data" / f"{total_data_name}.json", orient="split")
    df_total["years"] = "total"
    df_concat = pd.concat([df_yearly, df_total], ignore_index=True)
    df_concat.to_json(
        Path("assets") / "data" / f"{yearly_data_name}_total.json",
        orient="split",
        index=False,
        indent=1,
    )
