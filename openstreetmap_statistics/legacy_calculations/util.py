import json
import re
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


def save_json(file_path, obj):
    with Path(file_path).open("w", encoding="UTF-8") as json_file:
        json_file.write(pd.io.json.dumps(obj, double_precision=2))


def load_json(file_path):
    with Path(file_path).open("r", encoding="UTF-8") as json_file:
        return json.load(json_file)


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


def save_df_as_json(file_name, pd_df_or_series):
    # Remove last row if 'months' column exists, because it is not a full month
    if "months" in pd_df_or_series.columns:
        pd_df_or_series = pd_df_or_series.iloc[:-1]

    pd_df_or_series.to_json(Path("assets") / "data" / f"{file_name}.json", orient="split", index=False, indent=1)


def save_query_as_json(file_name, sql_query):
    df = duckdb.sql(sql_query).df()
    save_df_as_json(file_name, df)


def histogram_2d_to_xyz(pd_histogram_2d):
    histogram_2d = np.zeros((360, 180), dtype=np.uint32)
    for (x_index, y_index), value in pd_histogram_2d.items():
        histogram_2d[x_index % 360, y_index % 180] = value
    x, y = histogram_2d.nonzero()
    z = histogram_2d[x, y]
    return x, y, z


def save_map(name, pd_histogram_2d):
    x, y, z = histogram_2d_to_xyz(pd_histogram_2d)
    xyz_df = pd.DataFrame({"max_z_value": int(np.max(z)), "x": [x], "y": [y], "z": [z]})
    save_df_as_json(name, xyz_df)


def save_maps(name, pd_histogram_2d_list, map_names):
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
            "max_z_value": [np.max(max_z_value_list)] * len(map_names),
            "x": x_list,
            "y": y_list,
            "z": z_list,
        },
    )
    save_df_as_json(name, xyz_df)


def create_position_histogram(df):
    """Create a position histogram from a DataFrame with mid_pos_x, mid_pos_y, and edit_count columns."""
    if df.empty:
        return pd.Series(dtype=np.uint32)

    position_groups = df.groupby(["mid_pos_x", "mid_pos_y"])["edit_count"].sum()

    # Create histogram as a Series with tuple indexing
    valid_positions = {}
    for (x, y), edit_sum in position_groups.items():
        valid_positions[(x, y)] = edit_sum

    return pd.Series(valid_positions, dtype=np.uint32)
