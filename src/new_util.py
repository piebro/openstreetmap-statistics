import json
import warnings
from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from bs4 import BeautifulSoup
from IPython.display import HTML

warnings.filterwarnings("ignore", category=FutureWarning, module="dask.dataframe.io.parquet.core")


def reset_data_and_plots():
    for file in Path("data").iterdir():
        file.unlink()
    with Path("plots.json").open("w") as f:
        f.write("{}")


def get_months_years(data_dir):
    with (Path(data_dir) / "months.txt").open("r", encoding="UTF-8") as f:
        months = [line[:-1] for line in f.readlines()]
    years = sorted({m[:4] for m in months})
    return np.array(months), np.array(years)


def get_month_year_dicts(data_dir):
    months, years = get_months_years(data_dir)
    return {"month": list_to_index_dict(months), "year": list_to_index_dict(years)}


def load_index_to_tag(data_dir, data_name):
    with (Path(data_dir) / f"index_to_tag_{data_name}.txt").open("r", encoding="UTF-8") as f:
        return [line[:-1] for line in f.readlines()]

def list_to_index_dict(input_list):
    return dict(enumerate(input_list))


def list_to_dict(input_list):
    return {e: i for i, e in enumerate(input_list)}
    

def load_tag_to_index(data_dir, data_name):
    return list_to_dict(load_index_to_tag(data_dir, data_name))


def get_year_index_to_month_indices(data_dir):
    months, years = get_months_years(data_dir)
    years = years.tolist()

    year_index_to_month_indices = [[] for _ in years]
    for month_i, month in enumerate(months):
        year_index_to_month_indices[years.index(month[:4])].append(month_i)

    return year_index_to_month_indices


def load_ddf(data_dir, tag, columns=None, filters=None):
    return dd.read_parquet(Path(data_dir) / "changeset_data" / tag, columns=columns, filters=filters)

def save_data(data_dir, name, pd_df_or_series):
    pd_df_or_series = pd_df_or_series.copy(deep=True)
    pd_df = pd_df_or_series.reset_index() if isinstance(pd_df_or_series, pd.Series) else pd_df_or_series
    time_dict = get_month_year_dicts(data_dir)

    start_index_offset = None
    if "month_index" in pd_df:
        pd_df.insert(0, "months", pd_df["month_index"].map(time_dict["month"]))
        pd_df = pd_df.drop(columns=["month_index"])
        start_index_offset = 3
    elif "year_index" in pd_df:
        pd_df.insert(0, "years", pd_df["year_index"].map(time_dict["year"]))
        pd_df = pd_df.drop(columns=["year_index"])
        start_index_offset = 0
    columns = list(pd_df.columns)

    # delete rows with only zeros in them
    if start_index_offset is not None:
        value_columns = [c for c in columns if c not in ["months", "years"]]
        if_zero_row = (pd_df[value_columns] != 0).any(axis=1).to_numpy()

        if len(np.nonzero(if_zero_row)[0]) > 0:
            start_index = np.max([0, np.nonzero(if_zero_row)[0][0] - start_index_offset])
            pd_df = pd_df.loc[start_index:]

    pd_df.to_json(Path("data") / f"{name}.json", orient="split", index=False, indent=1)

def get_plot_config(filename, title, x=None, y_list=None, x_unit=None, y_unit=None):
    df = pd.read_json(Path("data") / f"{filename}.json", orient="split")
    if x is None:
        x = df.columns[0]
    if y_list is None:
        y_list = df.columns[1:]
    
    if x_unit is None:
        x_unit = x
    if y_unit is None:
        y_unit = y_list[0]

    config = {
        "filename": filename,
        "type": "plotly_plot",
        "layout": {
            "font": {"family": "Times", "size": 15},
            "paper_bgcolor": "#dfdfdf",
            "plot_bgcolor": "#dfdfdf",
            "margin": {"l": 55, "r": 55, "b": 55, "t": 55},
            "title": {"text": title, "x": 0.5, "xanchor": "center"},
            "xaxis": {"title": { "text": x_unit }, "gridcolor": "#d1d1d1", "linecolor": "#d1d1d1"},
            "yaxis": {"title": { "text": y_unit }, "gridcolor": "#d1d1d1", "rangemode": "tozero"},

            
        },
        "traces": []
    }

    for y in y_list:
        name = "" if len(y_list) == 1 else y
        config["traces"].append({
            "type": "scatter",
            "x": df[x].tolist(),
            "y": df[y].tolist(),
            "mode": "lines",
            "name": name,
            "hovertemplate": "%{x}<br>%{y:,} " + y_unit
        })
    
    return config

def save_plot_config(config):
    configs_path = Path("plots.json")
    if configs_path.exists():
        with configs_path.open("r", encoding="UTF-8") as json_file:
            configs = json.load(json_file)
    else:
        configs = {}
    
    configs[config["filename"]] = config
    with configs_path.open("w", encoding="UTF-8") as json_file:
        json.dump(configs, json_file)
    

def show_plot(plot_config):
    if plot_config["type"] == "plotly_plot":
        fig = go.Figure()
        for trace in plot_config["traces"]:
            if trace["type"] == "scatter":
                fig.add_trace(go.Scatter(trace))

        fig.update_layout({
            "autosize": False,
            "height": 500,
            "width": 1000,
        })
        fig.update_layout(plot_config["layout"])
        fig.show()
    elif plot_config["type"] == "table":
        table_css_path = "../../../assets/table.css"
        link_css = f'<link rel="stylesheet" type="text/css" href="{table_css_path}">'
        div_html = f'<div style="max-height: 300px; overflow-y: auto; color: #000; background-color: #fff;">{plot_config["innerHTML"]}</div>'
        display(HTML(link_css + div_html))
    else:
        ValueError(f"unknown plot type: {plot_config['type']}")

def get_html_table_str(filename, transpose_new_index_column=None, add_rank=False, last_column_name_values=(None, None)):
    df = pd.read_json(Path("data") / f"{filename}.json", orient="split")
    if transpose_new_index_column is not None:
        df = df.set_index(transpose_new_index_column).transpose()

    df.columns.name = None
    df = df.reset_index().rename(columns={"index": "Editor"})

    if add_rank:
        df.insert(0, "Rank", range(1, 1 + len(df)))

    if last_column_name_values[0] is not None:
        df[last_column_name_values[0]] = last_column_name_values[1]

    df = df.map(lambda x: f"{x:,}" if isinstance(x, (int, float)) else x)

    html = df.to_html(index=False, border=0)
    html = html.replace('<tr style="text-align: right;">', "<tr>").replace('<table class="dataframe">', "<table>")
    soup = BeautifulSoup(html, "html.parser")

    num_of_sticky_columns = 2 if add_rank else 1

    for tr in soup.find_all("tr")[1:]:
        cells = tr.find_all("td")
        for cell in cells[:num_of_sticky_columns]:
            cell.name = "th"

    return {
        "filename": filename,
        "type": "table",
        "innerHTML": str(soup)
    }




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



def save_percent(data_name, data_name_divide, merge_on_column_name, divide_column_name):
    df_divide = pd.read_json(Path("data") / f"{data_name_divide}.json", orient="split")
    df_divide = df_divide.rename(columns={divide_column_name: f"{divide_column_name}_divide"})

    df = pd.read_json(Path("data") / f"{data_name}.json", orient="split")
    columns = get_columns_without_months_and_years(df)
    merged_df = df.merge(df_divide, on=merge_on_column_name, suffixes=("", "_divide"))

    merged_df[columns] = (
        merged_df[columns].div(merged_df[f"{divide_column_name}_divide"], axis="index").mul(100, axis="index")
    )
    merged_df[[merge_on_column_name, *columns]].to_json(
        Path("data") / f"{data_name}_percent.json",
        orient="split",
        index=False,
        indent=1,
        double_precision=1,
    )

def get_columns_without_months_and_years(df):
    return [v for v in df.columns.to_numpy() if v not in ["months", "years"]]


def save_monthly_to_yearly(data_name, only_full_years=False):
    if "contributor" in data_name:
        raise ValueError("save_monthly_to_yearly does not work for contributors")
    df = pd.read_json(Path("data") / f"{data_name}.json", orient="split")

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
        Path("data") / f"{data_name.replace('_monthly', '_yearly')}{suffix}.json",
        orient="split",
        index=False,
        indent=1,
    )


def save_accumulated(data_name):
    df = pd.read_json(Path("data") / f"{data_name}.json", orient="split")
    columns = get_columns_without_months_and_years(df)
    df[columns] = df[columns].cumsum()
    df.to_json(Path("data") / f"{data_name}_accumulated.json", orient="split", index=False, indent=1)


def get_top_k_contributor_count(data_dir, ddf, tag, k):
    if tag in ["created_by", "corporation", "streetcomplete"]:
        highest_number = np.iinfo(ddf[tag].dtype).max
        ddf = ddf[ddf[tag] < highest_number]

    # counting the contibutor count only for tags that have a minimum amount of changesets for efficiency
    # min_amount_of_changeset should be lower then the contributor count of the top 500 from previous runs
    total_changeset_count = ddf.groupby(tag).size().compute().sort_values(ascending=False)
    tag_to_min_amount_of_changeset = {
        "created_by": 30,
        "imagery": 500,
        "hashtag": 2500,
        "source": 250,
        "corporation": 0,
        "streetcomplete": 0,
    }
    min_amount_of_changeset = tag_to_min_amount_of_changeset.get(tag, 0)
    filtered_ddf = ddf[ddf[tag].isin(total_changeset_count[total_changeset_count > min_amount_of_changeset].index)]

    total_contributor_count = filtered_ddf.groupby(tag)["user_index"].nunique().compute().sort_values(ascending=False)

    indices = total_contributor_count.index.to_numpy()[:k]
    index_to_tag = load_index_to_tag(data_dir, tag)
    names = [index_to_tag[i] for i in indices]
    
    values = total_contributor_count[indices[:k]].to_numpy()
    return indices[:k], names[:k], values

def get_top_k_edit_count(data_dir, ddf, tag, k):
    if tag in ["created_by", "corporation", "streetcomplete"]:
        highest_number = np.iinfo(ddf[tag].dtype).max
        ddf = ddf[ddf[tag] < highest_number]
    
    total_edit_count = ddf.groupby(tag)["edits"].sum().compute().sort_values(ascending=False)

    indices = total_edit_count.index.to_numpy()[:k]
    index_to_tag = load_index_to_tag(data_dir, tag)
    names = [index_to_tag[i] for i in indices]
    
    values = total_edit_count[indices[:k]].to_numpy()
    return indices[:k], names[:k], values

def get_top_k_changeset_count(data_dir, ddf, tag, k):
    if tag in ["created_by", "corporation", "streetcomplete"]:
        highest_number = np.iinfo(ddf[tag].dtype).max
        ddf = ddf[ddf[tag] < highest_number]
    
    total_changeset_count = ddf.groupby(tag).size().compute().sort_values(ascending=False)

    indices = total_changeset_count.index.to_numpy()[:k]
    index_to_tag = load_index_to_tag(data_dir, tag)
    names = [index_to_tag[i] for i in indices]

    values = total_changeset_count[indices[:k]].to_numpy()
    return indices[:k], names[:k], values


def top_k_unique_contributor_monthly_list(data_dir, contributor_count_ddf_filename, tag, indices):
    months, _ = get_months_years(data_dir)
    unique_monthly_list = [[set() for _ in range(len(months))] for _ in range(len(indices))]

    for month_i in range(len(months)):
        filters = [("month_index", "==", month_i)]
        temp_ddf = load_ddf(data_dir, contributor_count_ddf_filename, ("user_index", tag), filters)
        contributor_unique_month = (
            temp_ddf[temp_ddf[tag].isin(indices)].groupby([tag], observed=False)["user_index"].unique().compute()
        )
        for i, tag_index in enumerate(indices):
            value = contributor_unique_month[contributor_unique_month.index == tag_index].to_numpy()
            if len(value) > 0:
                unique_monthly_list[i][month_i] = set(value[0].tolist())
    return unique_monthly_list
