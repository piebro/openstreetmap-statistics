import sys
import numpy as np
import util

# init
DATA_DIR = sys.argv[1]
changesets = util.Changesets(DATA_DIR)
top_k, index_to_rank, rank_to_name = util.get_rank_infos(DATA_DIR, "streetcomplete_quest_type")
name_to_color = util.get_unique_name_to_color_mapping(rank_to_name["edits"], rank_to_name["contributors"])

months, years = changesets.months, changesets.years
monthly_edits = np.zeros((top_k, len(months)), dtype=np.int64)
monthly_edits_sc = np.zeros((len(months)), dtype=np.int64)
total_map_edits = np.zeros((360, 180), dtype=np.int64)
monthly_contributor_sets = [[set() for _ in range(len(months))] for _ in range(top_k)]
monthly_contributor_sets_sc = [set() for _ in range(len(months))]

# accumulate data
for csv_line in sys.stdin:
    changesets.update_data_with_csv_str(csv_line)
    month_index = changesets.month_index
    user_index = changesets.user_index
    edits = changesets.edits
    sc_index = changesets.streetcomplete_quest_type_index

    if sc_index is None:
        continue

    monthly_edits_sc[month_index] += edits
    monthly_contributor_sets_sc[month_index].add(user_index)

    if sc_index in index_to_rank["edits"]:
        rank = index_to_rank["edits"][sc_index]
        monthly_edits[rank, month_index] += edits
        if changesets.pos_x is not None:
            total_map_edits[changesets.pos_x, changesets.pos_y] += edits

    if sc_index in index_to_rank["contributors"]:
        rank = index_to_rank["contributors"][sc_index]
        monthly_contributor_sets[rank][month_index].add(user_index)

monthly_contributor_sc = util.set_to_length(monthly_contributor_sets_sc)

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
            util.get_percent(monthly_contributor_sc, changesets.monthly_contributors),
            percent=True,
        ),
        util.get_single_line_plot(
            "contributors that use streetcomplete per month", "contributors", months, monthly_contributor_sc
        ),
        util.get_single_line_plot(
            "percent of edits made with streetcomplete per month",
            "%",
            months,
            util.get_percent(monthly_edits_sc, changesets.monthly_edits),
            percent=True,
        ),
        util.get_single_line_plot("edits made with streetcomplete per month", "edits", months, monthly_edits_sc),
    )

    add_question(
        "How many edits does each quest have?",
        "6773",
        util.get_table(
            "yearly edit count per quest",
            years,
            util.monthly_to_yearly_with_total(monthly_edits, years, changesets.month_index_to_year_index),
            TOPIC,
            rank_to_name["edits"],
        ),
    )

    add_question(
        "What's the total amount of contributors and edits of the top quests over time?",
        "d06d",
        util.get_multi_line_plot(
            "total contributor count of quests",
            "contributors",
            months,
            util.set_cumsum(monthly_contributor_sets),
            rank_to_name["contributors"][:10],
            colors=[name_to_color[name] for name in rank_to_name["contributors"][:10]],
        ),
        util.get_multi_line_plot(
            "total edit count of quests",
            "edits",
            months,
            util.cumsum(monthly_edits),
            rank_to_name["edits"][:10],
            colors=[name_to_color[name] for name in rank_to_name["edits"][:10]],
        ),
    )

    add_question(
        "What's the total amount of contributors and edits of all quests over time?",
        "3ff2",
        util.get_multi_line_plot(
            "total contributor count of quests",
            "contributors",
            months,
            util.set_cumsum(monthly_contributor_sets),
            rank_to_name["contributors"],
            colors=[name_to_color[name] for name in rank_to_name["contributors"]],
            async_load=True,
        ),
        util.get_multi_line_plot(
            "total edit count of quests",
            "edits",
            months,
            util.cumsum(monthly_edits),
            rank_to_name["edits"],
            colors=[name_to_color[name] for name in rank_to_name["edits"]],
            async_load=True,
        ),
    )

    add_question(
        "Where is StreetComplete used the most?",
        "52ed",
        util.get_map_plot("total edits with streetcomplete", total_map_edits),
    )
