import sys
import numpy as np
import util

# init
data_dir = sys.argv[1]
months, years = util.get_months_years(data_dir)
year_to_year_index = util.list_to_dict(years)
month_index_to_year_index = {month_i: year_to_year_index[month[:4]] for month_i, month in enumerate(months)}
tag_to_id = util.list_to_dict(util.load_index_to_tag(data_dir, "created_by"))
maps_me_android_id = tag_to_id["MAPS.ME android"]
maps_me_ios_id = tag_to_id["MAPS.ME ios"]


mo_ch = np.zeros((len(months)), dtype=np.int64)
mo_ed = np.zeros((len(months)), dtype=np.int64)
ye_map_ed = np.zeros((len(years), 360, 180), dtype=np.int64)
total_map_ed = np.zeros((360, 180), dtype=np.int64)
mo_co_set = [set() for _ in range(len(months))]
mo_co_set_without_maps_me = [set() for _ in range(len(months))]
mo_co_edit_count = [dict() for _ in range(len(months))]

# accumulate data
for line in sys.stdin:
    data = line[:-1].split(",")
    edits = int(data[0])
    month_index = int(data[1])
    user_id = int(data[2])
    x, y = data[4], data[5]

    mo_ch[month_index] += 1
    mo_ed[month_index] += edits
    mo_co_set[month_index].add(user_id)
    if user_id not in mo_co_edit_count[month_index]:
        mo_co_edit_count[month_index][user_id] = 0    
    mo_co_edit_count[month_index][user_id] += edits

    if len(x)>0:
        ye_map_ed[month_index_to_year_index[month_index], int(x), int(y)] += edits
        total_map_ed[int(x), int(y)] += edits

    if len(data[7])==0 or (int(data[7]) != maps_me_android_id and int(data[7]) != maps_me_ios_id):
        mo_co_set_without_maps_me[month_index].add(user_id)
        

mo_co = util.set_to_length(mo_co_set)
mo_co_without_maps_me = util.set_to_length(mo_co_set_without_maps_me)
ye_map_ed_max_z_value = np.max(ye_map_ed)
mo_co_edit_count_higher_then_100 = [np.sum(np.array(list(mo_co_edit_count[month_index].values()))>100) for month_index in range(len(months))]

# save plots
topic = "General"
with open("assets/data.js", "a") as f:
    f.write(f"data['{topic}']={{}}\n")

    question = "How many people are contributing each month?"
    f.write(util.get_js_str(topic, question, "63f6", [
        util.get_single_line_plot("contributors per month", "contributors", months, mo_co),
        util.get_single_line_plot("contributors with more then 100 edits per month", "contributors", months, mo_co_edit_count_higher_then_100),
    ]))

    question = "Why is there rapid growth in monthly contributors in 2016?"
    f.write(util.get_js_str(topic, question, "21d9", [
        ("text", "That's because a lot of new people were contributing using the maps.me app. Looking at the plot of monthly contributors not using maps.me shows that there is linear growth."),
        util.get_single_line_plot("contributors per month without maps.me contributors", "contributors", months, mo_co_without_maps_me),
    ]))

    question = "How many edits are added each month?"
    f.write(util.get_js_str(topic, question, "fe79", [
        util.get_single_line_plot("edits per month", "edits", months, mo_ed)
    ]))

    question = "What's the total amount of contributors, edits and changesets over time?"
    f.write(util.get_js_str(topic, question, "7026", [
        util.get_single_line_plot("total contributor count", "contributors", months, util.set_cumsum(mo_co_set)),
        util.get_single_line_plot("total edit count", "edits", months, util.cumsum(mo_ed)),
        util.get_single_line_plot("total changeset count", "changesets", months, util.cumsum(mo_ch))
    ]))

    question = "Where are edits made?"
    f.write(util.get_js_str(topic, question, "727b", [
        util.get_map_plot("total edits", total_map_ed)
    ]))

    question = "Where are edits made each year?"
    f.write(util.get_js_str(topic, question, "bd16", [
        util.get_map_plot(f"total edits {year}", m, ye_map_ed_max_z_value) for m, year in zip(ye_map_ed, years)
    ]))

    question = "How many edits does a contributor make on average each month?"
    f.write(util.get_js_str(topic, question, "a3ed", [
        util.get_single_line_plot("average number of edits per contributor per month", "average number of edits per contributor", months, np.round(util.save_div(mo_ed, mo_co), 2))
    ]))

    question = "What's the average edit count per changeset each month?"
    f.write(util.get_js_str(topic, question, "fded", [
        util.get_single_line_plot("average number of edits per changeset per month", "average number of edits per changeset", months, np.round(util.save_div(mo_ed, mo_ch), 2))
    ]))

    
