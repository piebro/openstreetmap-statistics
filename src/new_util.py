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

def get_html_table_str(filename, transpose_new_index_column=None, add_rank=False):
    df = pd.read_json(Path("data") / f"{filename}.json", orient="split")
    if transpose_new_index_column is not None:
        df = df.set_index(transpose_new_index_column)
        df = df.transpose()
    
    # TODO: add "," for big numbers.
    html = df.to_html(index=True, border=0)
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

