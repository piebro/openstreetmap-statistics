import sys
import numpy as np
import util

# init
data_dir = sys.argv[1]
months, years = util.get_months_years(data_dir)
year_to_year_index = util.list_to_dict(years)
month_index_to_year_index = {month_i: year_to_year_index[month[:4]] for month_i, month in enumerate(months)}
created_by_index_to_tag = util.load_index_to_tag(data_dir, "created_by")

mo_ch = np.zeros((len(months)), dtype=np.int64)
mo_ed = np.zeros((len(months)), dtype=np.int64)
mo_ed_all = np.zeros((len(months)), dtype=np.int64)
total_map_edits = np.zeros((360, 180), dtype=np.int64)
mo_co_set = [set() for _ in range(len(months))]
created_by_dict = {}

# accumulate data
for line in sys.stdin:
    data = line[:-1].split(",")
    edits = int(data[0])
    month_index = int(data[1])
    user_id = int(data[2])
    x, y = data[4], data[5]

    mo_ed_all[month_index] += edits
    bot_used = (len(data[6]) > 0 and data[6] == "1")
    if not bot_used:
        continue

    mo_ch[month_index] += 1
    mo_ed[month_index] += edits
    mo_co_set[month_index].add(user_id)
    if len(x)>0:
        total_map_edits[int(x), int(y)] += edits
    
    if len(data[7])>0:
        created_by = int(data[7])
        if created_by not in created_by_dict:
            created_by_dict[created_by] = np.zeros((len(months)), dtype=np.int64)
        created_by_dict[created_by][month_index] += edits

created_by_items = list(created_by_dict.items())
created_by_ed = np.array([v for _, v in created_by_items])
sort_indices = np.argsort(-np.sum(created_by_ed, axis=1))
created_by_ed_names = np.array([created_by_index_to_tag[key] for key, _ in created_by_items])[sort_indices]
created_by_ed = created_by_ed[sort_indices]

# save plots
topic = "Bot"
with open("assets/data.js", "a") as f:
    f.write(f"data['{topic}']={{}}\n")

    question = "How often are edits created with the help of bots?"
    f.write(util.get_js_str(topic, question, "785b", [
        util.get_single_line_plot("edits created with a bot per month", "edits", months, mo_ed),
        util.get_single_line_plot("percent of edits created with a bot per month", "percent", months, util.get_percent(mo_ed, mo_ed_all), percent=True)
    ]))

    question = "What's the total amount of contributors, edits and changesets that use bots over time?"
    f.write(util.get_js_str(topic, question, "0725", [
        util.get_single_line_plot("total contributor count that used a bot", "contributors", months, util.set_cumsum(mo_co_set)),
        util.get_single_line_plot("total edit count that used a bot", "edits", months, util.cumsum(mo_ed)),
        util.get_single_line_plot("total changeset count that used a bot", "changesets", months, util.cumsum(mo_ch))
    ]))

    question = "How many distinct users use bots per month?"
    f.write(util.get_js_str(topic, question, "da7d", [
        util.get_single_line_plot("contributors using bots per month", "contributors", months, util.set_to_length(mo_co_set))
    ]))
    
    question = "Where are bots used?"
    f.write(util.get_js_str(topic, question, "ed95", [
        util.get_map_plot("total edits using bots", total_map_edits)
    ]))

    question = "What's the average edit count per changeset over time?"
    f.write(util.get_js_str(topic, question, "ae72", [
        util.get_single_line_plot("average number of edits using bots per changeset per month", "average number of edits per changeset", months, np.round(util.save_div(mo_ed, mo_ch), 2))
    ]))
    
    question = "What are the most used bot tools?"
    f.write(util.get_js_str(topic, question, "e985", [
        util.get_table("yearly edit count per bot", years, util.monthly_to_yearly_with_total(created_by_ed[:100], years, month_index_to_year_index), topic, created_by_ed_names[:100])
    ]))
