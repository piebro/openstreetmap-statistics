import sys
import numpy as np
import util

# init
data_dir = sys.argv[1]
top_k = 100
months, years = util.get_months_years(data_dir)
year_to_year_index = util.list_to_dict(years)
month_index_to_year_index = {month_i: year_to_year_index[month[:4]] for month_i, month in enumerate(months)}

top_ids = util.load_top_k_list(data_dir, "imagery")
ch_id_to_rank = util.list_to_dict(top_ids["changesets"])
ed_id_to_rank = util.list_to_dict(top_ids["edits"])
co_id_to_rank = util.list_to_dict(top_ids["contributors"])

index_to_tag = util.load_index_to_tag(data_dir, "imagery")
ch_rank_to_name = [index_to_tag[contributor_id] for contributor_id in top_ids["changesets"]]
ed_rank_to_name = [index_to_tag[edit_id] for edit_id in top_ids["edits"]]
co_rank_to_name = [index_to_tag[contributor_id] for contributor_id in top_ids["contributors"]]

mo_ch = np.zeros((top_k, len(months)), dtype=np.int64)
mo_ed = np.zeros((top_k, len(months)), dtype=np.int64)
mo_ed_all = np.zeros((len(months)), dtype=np.int64)
mo_ed_that_use_tag = np.zeros((len(months)), dtype=np.int64)
mo_co_set = [[set() for _ in range(len(months))] for _ in range(top_k)]

# accumulate data
for line in sys.stdin:
    data = line[:-1].split(",")
    edits = int(data[0])
    month_index = int(data[1])
    user_id = int(data[2])
    x, y = data[4], data[5]

    mo_ed_all[month_index] += edits

    if len(data[9])==0:
        continue

    mo_ed_that_use_tag[month_index] += edits

    for imagery_id in data[9].split(";"):
        imagery_id = int(imagery_id)

        if imagery_id in ch_id_to_rank:
            rank = ch_id_to_rank[imagery_id]
            mo_ch[rank, month_index] += 1

        if imagery_id in ed_id_to_rank:
            rank = ed_id_to_rank[imagery_id]
            mo_ed[rank, month_index] += edits

        if imagery_id in co_id_to_rank:
            rank = co_id_to_rank[imagery_id]
            mo_co_set[rank][month_index].add(user_id)

# save plots
topic = "Imagery Service"
with open("assets/data.js", "a") as f:
    f.write(f"data['{topic}']={{}}\n")

    question = "How popular are imagery services?"
    f.write(util.get_js_str(topic, question, "4f2c", [
        util.get_single_line_plot("monthly edits that use at least one imagery service", "%", months, util.get_percent(mo_ed_that_use_tag, mo_ed_all), percent=True),
        ("text", "This graph is based on imagery tag set automatically by iD, Vespucci and Go Map!!. As other editors are not using it and iD is vastly more popular than other relevant editors this graph is very close to 'market share of iD by edit volume'. JOSM users are typically using source field to note actually used sources")
    ]))

    question = "How many contributors does each imagery service have per month?"
    f.write(util.get_js_str(topic, question, "5bc5", [
        util.get_multi_line_plot("monthly contributor count per imagery software", "contributors", months, util.set_to_length(mo_co_set)[:10], co_rank_to_name[:10]),
        util.get_table("yearly contributor count per imagery software", years, util.monthly_set_to_yearly_with_total(mo_co_set, years, month_index_to_year_index), topic, co_rank_to_name)
    ]))

    question = "How many edits does each imagery service have per month?"
    f.write(util.get_js_str(topic, question, "af79", [
        util.get_multi_line_plot("monthly edits count per imagery service", "edits", months, mo_ed[:10], ed_rank_to_name[:10]),
        util.get_table("yearly edits count per imagery service", years, util.monthly_to_yearly_with_total(mo_ed, years, month_index_to_year_index), topic, ed_rank_to_name)
    ]))

    question = "What's the total amount of contributors, edits and changesets of imagery service over time?"
    f.write(util.get_js_str(topic, question, "327d", [
        util.get_multi_line_plot("total contributor count of imagery service", "contributors", months, util.set_cumsum(mo_co_set), co_rank_to_name[:10]),
        util.get_multi_line_plot("total edit count of imagery service", "edits", months, util.cumsum(mo_ed), ed_rank_to_name[:10]),
        util.get_multi_line_plot("total changeset count of imagery service", "changesets", months, util.cumsum(mo_ch), ch_rank_to_name[:10])
    ]))


    
