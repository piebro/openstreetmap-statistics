import sys
import numpy as np
import util

# init
DATA_DIR = sys.argv[1]
TOP_K = 100
months, years = util.get_months_years(DATA_DIR)
year_to_year_index = util.list_to_dict(years)
month_index_to_year_index = {month_i: year_to_year_index[month[:4]] for month_i, month in enumerate(months)}

top_ids = util.load_top_k_list(DATA_DIR, "hashtag")
ch_id_to_rank = util.list_to_dict(top_ids["changesets"])
ed_id_to_rank = util.list_to_dict(top_ids["edits"])
co_id_to_rank = util.list_to_dict(top_ids["contributors"])

index_to_tag = util.load_index_to_tag(DATA_DIR, "hashtag")
ch_rank_to_name = [index_to_tag[contributor_id] for contributor_id in top_ids["changesets"]]
ed_rank_to_name = [index_to_tag[edit_id] for edit_id in top_ids["edits"]]
co_rank_to_name = [index_to_tag[contributor_id] for contributor_id in top_ids["contributors"]]

mo_ch = np.zeros((TOP_K, len(months)), dtype=np.int64)
mo_ed = np.zeros((TOP_K, len(months)), dtype=np.int64)
mo_ed_all = np.zeros((len(months)), dtype=np.int64)
mo_ed_that_use_tag = np.zeros((len(months)), dtype=np.int64)
total_map_ed = np.zeros((10, 360, 180), dtype=np.int64)
mo_co_set = [[set() for _ in range(len(months))] for _ in range(TOP_K)]
mo_co_set_all = [set() for _ in range(len(months))]
mo_co_set_that_use_tag = [set() for _ in range(len(months))]

# accumulate data
for line in sys.stdin:
    data = line[:-1].split(",")
    edits = int(data[0])
    month_index = int(data[1])
    user_id = int(data[2])
    x, y = data[4], data[5]

    mo_ed_all[month_index] += edits
    mo_co_set_all[month_index].add(user_id)

    if len(data[10]) == 0:
        continue

    mo_ed_that_use_tag[month_index] += edits
    mo_co_set_that_use_tag[month_index].add(user_id)

    for hashtag_id in data[10].split(";"):
        hashtag_id = int(hashtag_id)

        if hashtag_id in ch_id_to_rank:
            rank = ch_id_to_rank[hashtag_id]
            mo_ch[rank, month_index] += 1

        if hashtag_id in ed_id_to_rank:
            rank = ed_id_to_rank[hashtag_id]
            mo_ed[rank, month_index] += edits
            if len(x) > 0 and rank < 10:
                total_map_ed[rank, int(x), int(y)] += edits

        if hashtag_id in co_id_to_rank:
            rank = co_id_to_rank[hashtag_id]
            mo_co_set[rank][month_index].add(user_id)

# save plots
TOPIC = "Hashtags"
with util.add_questions(TOPIC) as add_question:

    add_question(
        "How popular are hashtags?",
        "2e20",
        util.get_single_line_plot(
            "percent of monthly edits that use at least one hashtag",
            "%",
            months,
            util.get_percent(mo_ed_that_use_tag, mo_ed_all),
            percent=True,
        ),
        util.get_single_line_plot(
            "percent of monthly contributors that use at least one hashtag",
            "%",
            months,
            util.get_percent(util.set_to_length(mo_co_set_that_use_tag), util.set_to_length(mo_co_set_all)),
            percent=True,
        ),
    )

    add_question(
        "How many contributors does each hashtag have per month?",
        "bd85",
        util.get_multi_line_plot(
            "monthly contributor count per hashtag",
            "contributors",
            months,
            util.set_to_length(mo_co_set[:10]),
            co_rank_to_name[:10],
        ),
        util.get_table(
            "yearly contributor count per hashtag",
            years,
            util.monthly_set_to_yearly_with_total(mo_co_set, years, month_index_to_year_index),
            TOPIC,
            co_rank_to_name,
        ),
    )

    add_question(
        "How many edits does each hashtag have per month?",
        "f0e6",
        util.get_multi_line_plot("monthly edits count per hashtag", "edits", months, mo_ed[:10], ed_rank_to_name[:10]),
        util.get_table(
            "yearly edits count per hashtag",
            years,
            util.monthly_to_yearly_with_total(mo_ed, years, month_index_to_year_index),
            TOPIC,
            ed_rank_to_name,
        ),
    )

    add_question(
        "What's the total amount of contributors, edits and changesets of hashtags over time?",
        "4b4a",
        ("text", f"There are {len(index_to_tag):,} different hashtags"),
        util.get_multi_line_plot(
            "total contributor count of hashtags",
            "contributors",
            months,
            util.set_cumsum(mo_co_set),
            co_rank_to_name[:10],
        ),
        util.get_multi_line_plot(
            "total edit count of hashtags", "edits", months, util.cumsum(mo_ed), ed_rank_to_name[:10]
        ),
        util.get_multi_line_plot(
            "total changeset count of hashtags", "changesets", months, util.cumsum(mo_ch), ch_rank_to_name[:10]
        ),
    )

    add_question(
        "Where are the top 10 hashtags used?",
        "bea0",
        *[
            util.get_map_plot(f"total edits for the hashtag: {name}", m)
            for m, name in zip(total_map_ed, ed_rank_to_name[:10])
        ],
    )
