import os
import json
import numpy as np

default_plot_layout = {
    "font": {"family": 'Times', "size":"15"},
    "paper_bgcolor":"#dfdfdf",
    "plot_bgcolor":"#dfdfdf",
    "margin": {"l": 55,"r": 55,"b": 55,"t": 55},
}

def save_json(file_path, obj):
    with open(os.path.join("assets", file_path), "w") as f:
        f.write(json.dumps(obj, separators=(",", ":")))

def get_months_years(data_dir):
    with open(os.path.join(data_dir, f"months.txt")) as f:
        months = [line[:-1] for line in f.readlines()]
    years = sorted(list(set([m[:4] for m in months])))
    return months, years

def list_to_dict(l):
    return {e:i for i, e in enumerate(l)}

def load_index_to_tag(data_dir, data_name):
    with open(os.path.join(data_dir, f"index_to_tag_{data_name}.txt")) as f:
        return [l[:-1] for l in f.readlines()]

def load_top_k_list(data_dir, tag_name):
    with open(os.path.join(data_dir, "top_k.json"), 'r') as json_file:
        return json.load(json_file)[tag_name]


def save_div(a, b):
    a, b = np.array(a, dtype=float), np.array(b, dtype=float)
    return np.divide(a, b, out=np.zeros_like(a), where=(b!=0), casting="unsafe")

def get_percent(a, b):
    return np.round(save_div(a, b), 4) * 100

def set_to_length(set_list):
    if isinstance(set_list[0], set):
        return np.array([len(s) for s in set_list])
    else:
        return np.array([set_to_length(inner_set_list) for inner_set_list in set_list])

def monthly_to_yearly_with_total(y_monthly, years, month_index_to_year_index, dtype=np.int64):
    if isinstance(y_monthly[0], (int, np.integer)):
        y_yearly = np.zeros(len(years)+1, dtype)
        for month_i, y in enumerate(y_monthly):
            y_yearly[month_index_to_year_index[month_i]] += y
        y_yearly[-1] = np.sum(y_yearly)
        return y_yearly
    else:
        return np.array([monthly_to_yearly_with_total(inner_y_monthly, years, month_index_to_year_index) for inner_y_monthly in y_monthly])

def monthly_set_to_yearly_with_total(y_monthly_set_list, years, month_index_to_year_index):
    if isinstance(y_monthly_set_list[0], set):
        y_yearly = [set() for _ in years]
        for month_i, y in enumerate(y_monthly_set_list):
            y_yearly[month_index_to_year_index[month_i]].update(y)
        y_yearly_len = [len(y_year) for y_year in y_yearly]
        y_yearly_len.append(len(set().union(*y_yearly)))
        return np.array(y_yearly_len)
    else:
        return np.array([monthly_set_to_yearly_with_total(inner_set_list, years, month_index_to_year_index) for inner_set_list in y_monthly_set_list])

def cumsum(y):
    if isinstance(y[0], (int, np.integer)):
        return np.cumsum(y)
    else:
        return [np.cumsum(e) for e in y]

def set_cumsum(set_list, dtype=np.int64):
    if isinstance(set_list[0], set):
        current_set = set()
        monthly_accumulated = np.empty(len(set_list), dtype)
        for i, s in enumerate(set_list):
            current_set.update(s)
            monthly_accumulated[i] = len(current_set)
        return monthly_accumulated
    else:
        return np.array([set_cumsum(inner_set_list, dtype) for inner_set_list in set_list])

# def total_map_set_to_total_map(total_map, dtype=np.int64):
#     return np.array([[len(contributors_set) for contributors_set in map_column] for map_column in total_map], dtype)


def trim_x_axis_to_non_zero_data(plot, offset=3):
    start_x_index = max(0, np.min([np.nonzero(trace["y"])[0][0] for trace in plot["traces"] if np.any(trace["y"])])-offset)
    
    for trace in plot["traces"]:
        trace["y"] = trace["y"][start_x_index:]
        trace["x"] = trace["x"][start_x_index:]

def get_single_line_plot(plot_title, unit, x, y, percent=False):
    if percent:
        y = [round(float(yy), 2) for yy in y]
    else:
        y = [int(yy) for yy in y]
    plot = {
        "traces": [{"x": x, "y": y, "mode": "lines", "name": "", "hovertemplate": "%{x}<br>%{y:,} " + unit}],
        "config": {"displayModeBar": False},
        "layout": {**default_plot_layout, "title": {"text":plot_title}, "xaxis":{"title":{"text":"time"}}, "yaxis":{"title":{"text":unit}},}
    }
    if percent:
        plot["layout"]["yaxis"]["range"] = [0,100]
        plot["traces"][0]["hovertemplate"] = "%{x}<br>%{y}%"
    trim_x_axis_to_non_zero_data(plot, offset=3)
    return ("plot", plot)

def get_multi_line_plot(plot_title, unit, x, y_list, y_names, percent=False):
    if percent:
        y_list = [[round(float(yy), 2) for yy in y] for y in y_list]
    else:
        y_list = [[int(yy) for yy in y] for y in y_list]
    plot = {
        "traces": [{"x": x, "y": y, "mode": "lines", "name": name, "hovertemplate": "%{x}<br>%{y:,} " + unit} for y, name in zip(y_list, y_names)],
        "config": {"displayModeBar": False},
        "layout": {**default_plot_layout, "title": {"text":plot_title}, "xaxis":{"title":{"text":"time"}}, "yaxis":{"title":{"text":unit}},}
    }
    if percent:
        plot["layout"]["yaxis"]["range"] = [0,100]
        for trace in plot["traces"]:
            trace["hovertemplate"] = "%{x}<br>%{y}%"
            trace["stackgroup"] = "one"
    trim_x_axis_to_non_zero_data(plot, offset=3)
    return ("plot", plot)

def get_table(table_title, x, y_list, y_names_head, y_names):
    start_xy_index = np.min([np.nonzero(y)[0][0] for y in y_list if np.any(y)])

    head = ["Rank", y_names_head] if len(y_list) > 1 else []
    head.extend(x[start_xy_index:])
    head.append("Total")

    body = []
    for i, (name, y) in enumerate(zip(y_names, y_list)):
        row = [str(i+1), name] if len(y_list) > 1 else []
        row.extend([f"{yy:,}" for yy in list(y[start_xy_index:])])
        body.append(row)

    table_json = {
        "title": table_title,
        "head": head,
        "body": body,
    }
    return ("table", table_json)

def get_map_plot(title, histogram_2d, max_z_value=None):
    if max_z_value is None:
        max_z_value = int(np.max(histogram_2d))
    else:
        max_z_value = int(max_z_value)
    
    colorscale = [(0, 'rgba(255,255,255,0)'), (0.00000001, 'rgb(12,51,131)'), (1/1000, 'rgb(10,136,186)'), (1/100, 'rgb(242,211,56)'), (1/10, 'rgb(242,143,56)'), (1, 'rgb(217,30,30)')]
    x, y = histogram_2d.nonzero()
    x, y, z = (x.tolist(), y.tolist(), histogram_2d[x, y].tolist())

    plot = {
        "config": {"displayModeBar": False},
        "layout": {
            **default_plot_layout,
            "images": [dict(source="assets/background_map.png", xref="x", yref="y", x=0, y=180, sizex=360, sizey=180, sizing="stretch", opacity=1, layer="below")],
            "xaxis": dict(showgrid=False, visible=False),
            "yaxis": dict(showgrid=False, visible=False, scaleanchor="x", scaleratio=1),
            "margin": {"l": 20,"r": 20,"b": 35,"t": 35},
            "coloraxis": {"colorscale":colorscale, "cmin":0, "cmax":max_z_value},
            "title": {"text":title},
        },
        "traces":[dict(type='histogram2d', x=x, y=y, z=z, zmax=max_z_value, histfunc="sum", autobinx=False, xbins=dict(start=0, end=360, size=1), autobiny=False, ybins=dict(start=0, end=180, size=1), coloraxis="coloraxis")]
    }
    return ("map", plot)

def get_js_str(topic, question, url_hash, div_elements):
    js_str_arr = [f'url_hash:"{url_hash}"']
    for i, (t, e) in enumerate(div_elements):
        if t == 'plot':
            js_str_arr.append(f'{i}: {json.dumps(e, separators=(",", ":"))}')
        elif t == 'text':
            js_str_arr.append(f'{i}: "{e}"')
        elif t == 'map':
            title = e["layout"]["title"]["text"]
            save_path = os.path.join("map_data", f'{topic.lower().replace(" ", "_")}_{title.lower().replace(" ", "_")}.json').replace("#","")
            save_json(save_path, e)
            js_str_arr.append(f'{i}: "{save_path}"')
        elif t == 'table':
            save_path = os.path.join("table_data", f'{topic.lower().replace(" ", "_")}_{e["title"].lower().replace(" ", "_")}.json')
            save_json(save_path, e)
            js_str_arr.append(f'{i}: "{save_path}"')
    
    update_str_arr = ['update: async function(){']
    update_str_arr.extend([f'await add_{t}("{topic}","{question}","{i}");' for i, (t, _) in enumerate(div_elements)])
    update_str_arr.append('}')
    js_str_arr.append(''.join(update_str_arr))

    save_plot_str_arr = ['save_plot: function(){']
    save_plot_str_arr.extend([f'save_{t}("{topic}","{question}","{i}");' for i, (t, _) in enumerate(div_elements) if t != "table"])
    save_plot_str_arr.append('}')
    js_str_arr.append(''.join(save_plot_str_arr))

    save_data_str_arr = ['save_data: function(){']
    save_data_str_arr.extend([f'save_{t}_data("{topic}","{question}","{i}");' for i, (t, _) in enumerate(div_elements)])
    save_data_str_arr.append('}')
    js_str_arr.append(''.join(save_data_str_arr))

    return f'data["{topic}"]["{question}"]={{{",".join(js_str_arr)}}};'



