import os
import sys
import json
import numpy as np
import util

# init
data_dir = sys.argv[1]
months, years = util.get_months_years(data_dir)
year_to_year_index = util.list_to_dict(years)
month_index_to_year_index = {month_i: year_to_year_index[month[:4]] for month_i, month in enumerate(months)}

with open(os.path.join("assets", "corporation_contributors.json"), 'r') as json_file:
    corporation_contributors = json.load(json_file)
corporations = np.array(list(corporation_contributors.keys()))
corporations_with_link = np.array([f'<a href="{corporation_contributors[corporation][0]}">{corporation}</a>' for corporation in corporations])
corporation_count = len(corporations)
user_name_to_corporation_id = {}
for i, corporation in enumerate(corporations):
    for user_name in corporation_contributors[corporation][1]:
        user_name_to_corporation_id[user_name] = i

mo_ch = np.zeros((corporation_count, len(months)), dtype=np.int64)
mo_ed = np.zeros((corporation_count, len(months)), dtype=np.int64)
mo_ed_all = np.zeros((len(months)), dtype=np.int64)
mo_ed_that_are_corporate = np.zeros((len(months)), dtype=np.int64)
total_map_ed = np.zeros((corporation_count, 360, 180), dtype=np.int64)
mo_co_set = [[set() for _ in range(len(months))] for _ in range(corporation_count)]

# accumulate data
for line in sys.stdin:
    data = line[:-1].split(",")
    edits = int(data[0])
    month_index = int(data[1])
    user_id = int(data[2])
    user_name = data[3]
    x, y = data[4], data[5]

    mo_ed_all[month_index] += edits

    if user_name not in user_name_to_corporation_id:
        continue
    corporation_id = user_name_to_corporation_id[user_name]

    mo_ed_that_are_corporate[month_index] += edits

    mo_ch[corporation_id, month_index] += 1
    mo_ed[corporation_id, month_index] += edits
    mo_co_set[corporation_id][month_index].add(user_id)

    if len(x)>0:
        total_map_ed[corporation_id, int(x), int(y)] += edits

# preprocess data
total_changesets = np.array([np.sum(v) for v in mo_ch])
sort_indices_changesets = np.argsort(-total_changesets)
mo_ch = mo_ch[sort_indices_changesets]
corporations_ch = corporations[sort_indices_changesets]

total_edits = np.array([np.sum(v) for v in mo_ed])
sort_indices_edits = np.argsort(-total_edits)
mo_ed = mo_ed[sort_indices_edits]
corporations_ed = corporations[sort_indices_edits]
corporations_with_link_ed = corporations_with_link[sort_indices_edits]
total_map_ed = total_map_ed[sort_indices_edits]
total_map_ed_max_z_value = np.max(total_map_ed[:10])

total_contributors = np.array([len(set.union(*v)) for v in mo_co_set])
sort_indices_contributors = np.argsort(-total_contributors)
monthly_co = util.set_to_length(mo_co_set)[sort_indices_contributors]
monthly_co_acc = util.set_cumsum(mo_co_set)[sort_indices_contributors]
corporations_co = corporations[sort_indices_contributors]

# save plots
topic = "Corporations"
with open("assets/data.js", "a") as f:
    f.write(f"data['{topic}']={{}}\n")

    question = "How many edits are added from corporations each month?"
    f.write(util.get_js_str(topic, question, "7034", [
        util.get_single_line_plot("percent of edits from corporation per month", "%", months, util.get_percent(mo_ed_that_are_corporate, mo_ed_all), percent=True)
    ]))

    question = "Which corporations are contributing how much?"
    f.write(util.get_js_str(topic, question, "b34d", [
        util.get_multi_line_plot("monthly edits per corporation", "edits", months, mo_ed[:10], corporations_ed[:10]),
        util.get_table("yearly edits per corporation", years, util.monthly_to_yearly_with_total(mo_ed, years, month_index_to_year_index), topic, corporations_with_link_ed)
    ]))

    question = "What's the total amount of contributors, edits and changesets from corporations over time?"
    f.write(util.get_js_str(topic, question, "4ef4", [
        util.get_multi_line_plot("total contributor count of corporations", "contributors", months, monthly_co_acc, corporations_co[:10]),
        util.get_multi_line_plot("total edit count of corporations", "edits", months, util.cumsum(mo_ed), corporations_ed[:10]),
        util.get_multi_line_plot("total changeset count of corporations", "changesets", months, util.cumsum(mo_ch), corporations_ch[:10])
    ]))

    question = "Where are the top 10 corporations contributing?"
    f.write(util.get_js_str(topic, question, "e19b", [
        util.get_map_plot(f"total edits of the corporation: {name}", m, total_map_ed_max_z_value) for m, name in zip(total_map_ed[:10], corporations_ed[:10])
    ]))


    
