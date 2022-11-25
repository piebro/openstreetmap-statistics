import sys
import numpy as np
import util

# init
data_dir = sys.argv[1]
top_k = 100
months, years = util.get_months_years(data_dir)
year_to_year_index = util.list_to_dict(years)
month_index_to_year_index = {month_i: year_to_year_index[month[:4]] for month_i, month in enumerate(months)}

top_ids = util.load_top_k_list(data_dir, "streetcomplete_quest_type")
ed_id_to_rank = util.list_to_dict(top_ids["edits"])
co_id_to_rank = util.list_to_dict(top_ids["contributors"])

index_to_tag = util.load_index_to_tag(data_dir, "streetcomplete_quest_type")
ed_rank_to_name = [index_to_tag[edit_id] for edit_id in top_ids["edits"]]
co_rank_to_name = [index_to_tag[contributor_id] for contributor_id in top_ids["contributors"]]

mo_ed = np.zeros((top_k, len(months)), dtype=np.int64)
mo_ed_sc = np.zeros((len(months)), dtype=np.int64)
mo_ed_all = np.zeros((len(months)), dtype=np.int64)
mo_ed_that_use_tag = np.zeros((len(months)), dtype=np.int64)
total_map_ed = np.zeros((360, 180), dtype=np.int64)
mo_co_set = [[set() for _ in range(len(months))] for _ in range(top_k)]
mo_co_set_sc = [set() for _ in range(len(months))]
mo_co_set_all = [set() for _ in range(len(months))]

# accumulate data
for line in sys.stdin:
    data = line[:-1].split(",")
    edits = int(data[0])
    month_index = int(data[1])
    user_id = int(data[2])
    x, y = data[4], data[5]

    mo_ed_all[month_index] += edits
    mo_co_set_all[month_index].add(user_id)

    if len(data[8])==0:
        continue
    quest_type_id = int(data[8])

    mo_ed_sc[month_index] += edits
    mo_co_set_sc[month_index].add(user_id)

    if quest_type_id in ed_id_to_rank:
        rank = ed_id_to_rank[quest_type_id]
        mo_ed[rank, month_index] += edits
        if len(x)>0:
            total_map_ed[int(x), int(y)] += edits

    if quest_type_id in co_id_to_rank:
        rank = co_id_to_rank[quest_type_id]
        mo_co_set[rank][month_index].add(user_id)

mo_co_sc = util.set_to_length(mo_co_set_sc)

# save plots
topic = "StreetComplete"
with open("assets/data.js", "a") as f:
    f.write(f"data['{topic}']={{}}\n")

    question = "How popular is StreetComplete in the OSM community?"
    f.write(util.get_js_str(topic, question, "465b", [
        util.get_single_line_plot("percent of contributors that use streetcomplete per month", "%", months, util.get_percent(mo_co_sc, util.set_to_length(mo_co_set_all)), percent=True),
        util.get_single_line_plot("contributors that use streetcomplete per month", "contributors", months, mo_co_sc),
        util.get_single_line_plot("percent of edits made with streetcomplete per month", "%", months, util.get_percent(mo_ed_sc, mo_ed_all), percent=True),
        util.get_single_line_plot("edits made with streetcomplete per month", "edits", months, mo_ed_sc)
    ]))

    question = "How many edits does each quest have?"
    f.write(util.get_js_str(topic, question, "6773", [
        util.get_table("yearly edit count per quest", years, util.monthly_to_yearly_with_total(mo_ed, years, month_index_to_year_index), topic, ed_rank_to_name)
    ]))

    question = "What's the total amount of contributors and edits of quests over time?"
    f.write(util.get_js_str(topic, question, "d06d", [
        util.get_multi_line_plot("total contributor count of quests", "contributors", months, util.set_cumsum(mo_co_set), co_rank_to_name[:10]),
        util.get_multi_line_plot("total edit count of quests", "edits", months, util.cumsum(mo_ed), ed_rank_to_name[:10]),
    ]))
    
    question = "Where is StreetComplete used the most?"
    f.write(util.get_js_str(topic, question, "52ed", [
        util.get_map_plot(f"total edits with streetcomplete", total_map_ed)
    ]))



    
