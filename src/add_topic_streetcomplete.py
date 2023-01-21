import sys
import numpy as np
import util

# init
DATA_DIR = sys.argv[1]
TOP_K = 300
months, years = util.get_months_years(DATA_DIR)
year_to_year_index = util.list_to_dict(years)
month_index_to_year_index = {month_i: year_to_year_index[month[:4]] for month_i, month in enumerate(months)}

top_ids = util.load_top_k_list(DATA_DIR, "streetcomplete_quest_type")
ed_id_to_rank = util.list_to_dict(top_ids["edits"])
co_id_to_rank = util.list_to_dict(top_ids["contributors"])

index_to_tag = util.load_index_to_tag(DATA_DIR, "streetcomplete_quest_type")
ed_rank_to_name = [index_to_tag[edit_id] for edit_id in top_ids["edits"]]
co_rank_to_name = [index_to_tag[contributor_id] for contributor_id in top_ids["contributors"]]

name_to_color = util.get_unique_name_to_color_mapping(ed_rank_to_name, co_rank_to_name)

mo_ed = np.zeros((TOP_K, len(months)), dtype=np.int64)
mo_ed_sc = np.zeros((len(months)), dtype=np.int64)
mo_ed_all = np.zeros((len(months)), dtype=np.int64)
mo_ed_that_use_tag = np.zeros((len(months)), dtype=np.int64)
total_map_ed = np.zeros((360, 180), dtype=np.int64)
mo_co_set = [[set() for _ in range(len(months))] for _ in range(TOP_K)]
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

    if len(data[8]) == 0:
        continue
    quest_type_id = int(data[8])

    mo_ed_sc[month_index] += edits
    mo_co_set_sc[month_index].add(user_id)

    if quest_type_id in ed_id_to_rank:
        rank = ed_id_to_rank[quest_type_id]
        mo_ed[rank, month_index] += edits
        if len(x) > 0:
            total_map_ed[int(x), int(y)] += edits

    if quest_type_id in co_id_to_rank:
        rank = co_id_to_rank[quest_type_id]
        mo_co_set[rank][month_index].add(user_id)

mo_co_sc = util.set_to_length(mo_co_set_sc)

# save plots
TOPIC = "StreetComplete"
with util.add_questions(TOPIC) as add_question:

    add_question(
        "How popular is StreetComplete in the OSM community?",
        "465b",
        util.get_text_element(
            "<a href='https://streetcomplete.app/'>StreetComplete</a> is an Android app where editing is done by"
            " answering predefined questions called 'quests'. This editor is much simpler to use than alternatives"
            ", but does not allow freeform editing. For example, adding missing opening hours is really easy, but"
            " you cannot map a missing road."
        ),
        util.get_single_line_plot(
            "percent of contributors that use streetcomplete per month",
            "%",
            months,
            util.get_percent(mo_co_sc, util.set_to_length(mo_co_set_all)),
            percent=True,
        ),
        util.get_single_line_plot("contributors that use streetcomplete per month", "contributors", months, mo_co_sc),
        util.get_single_line_plot(
            "percent of edits made with streetcomplete per month",
            "%",
            months,
            util.get_percent(mo_ed_sc, mo_ed_all),
            percent=True,
        ),
        util.get_single_line_plot("edits made with streetcomplete per month", "edits", months, mo_ed_sc),
    )

    add_question(
        "How many edits does each quest have?",
        "6773",
        util.get_table(
            "yearly edit count per quest",
            years,
            util.monthly_to_yearly_with_total(mo_ed, years, month_index_to_year_index),
            TOPIC,
            ed_rank_to_name,
        ),
    )

    add_question(
        "What's the total amount of contributors and edits of the top quests over time?",
        "d06d",
        util.get_multi_line_plot(
            "total contributor count of quests",
            "contributors",
            months,
            util.set_cumsum(mo_co_set),
            co_rank_to_name[:10],
            colors=[name_to_color[name] for name in co_rank_to_name[:10]],
        ),
        util.get_multi_line_plot(
            "total edit count of quests",
            "edits",
            months,
            util.cumsum(mo_ed),
            ed_rank_to_name[:10],
            colors=[name_to_color[name] for name in ed_rank_to_name[:10]],
        ),
    )

    add_question(
        "What's the total amount of contributors and edits of all quests over time?",
        "3ff2",
        util.get_multi_line_plot(
            "total contributor count of quests",
            "contributors",
            months,
            util.set_cumsum(mo_co_set),
            co_rank_to_name,
            colors=[name_to_color[name] for name in co_rank_to_name],
            async_load=True,
        ),
        util.get_multi_line_plot(
            "total edit count of quests",
            "edits",
            months,
            util.cumsum(mo_ed),
            ed_rank_to_name,
            colors=[name_to_color[name] for name in ed_rank_to_name],
            async_load=True,
        ),
    )

    add_question(
        "Where is StreetComplete used the most?",
        "52ed",
        util.get_map_plot("total edits with streetcomplete", total_map_ed),
    )
