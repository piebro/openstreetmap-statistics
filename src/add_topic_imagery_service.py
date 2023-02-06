import sys
import numpy as np
import util

# init
DATA_DIR = sys.argv[1]
changesets = util.Changesets(DATA_DIR)
top_k, index_to_rank, rank_to_name = util.get_rank_infos(DATA_DIR, "imagery")

months, years = changesets.months, changesets.years
monthly_changesets = np.zeros((top_k, len(months)), dtype=np.int64)
montly_edits = np.zeros((top_k, len(months)), dtype=np.int64)
montly_edits_that_use_tag = np.zeros((len(months)), dtype=np.int64)
montly_contributor_sets = [[set() for _ in range(len(months))] for _ in range(top_k)]

# accumulate data
for csv_line in sys.stdin:
    changesets.update_data_with_csv_str(csv_line)
    month_index = changesets.month_index

    if len(changesets.imagery_indices) == 0:
        continue

    montly_edits_that_use_tag[month_index] += changesets.edits

    for imagery_index in changesets.imagery_indices:
        if imagery_index in index_to_rank["changesets"]:
            rank = index_to_rank["changesets"][imagery_index]
            monthly_changesets[rank, month_index] += 1

        if imagery_index in index_to_rank["edits"]:
            rank = index_to_rank["edits"][imagery_index]
            montly_edits[rank, month_index] += changesets.edits

        if imagery_index in index_to_rank["contributors"]:
            rank = index_to_rank["contributors"][imagery_index]
            montly_contributor_sets[rank][month_index].add(changesets.user_index)


# save plots
TOPIC = "Imagery Service"
with util.add_questions(TOPIC) as add_question:

    add_question(
        "How popular are imagery services?",
        "4f2c",
        util.get_single_line_plot(
            "monthly edits that use at least one imagery service",
            "%",
            months,
            util.get_percent(montly_edits_that_use_tag, changesets.monthly_edits),
            percent=True,
        ),
        util.get_text_element(
            "This graph is based on imagery tag set automatically by iD, Vespucci and Go Map!!. As other editors are"
            " not using it and iD is vastly more popular than other relevant editors this graph is very close to"
            " 'market share of iD by edit volume'. JOSM users are typically using source field to note actually"
            " used sources"
        ),
    )

    add_question(
        "How many contributors does each imagery service have per month?",
        "5bc5",
        util.get_multi_line_plot(
            "monthly contributor count per imagery software",
            "contributors",
            months,
            util.set_to_length(montly_contributor_sets)[:10],
            rank_to_name["contributors"][:10],
        ),
        util.get_table(
            "yearly contributor count per imagery software",
            years,
            util.monthly_set_to_yearly_with_total(montly_contributor_sets, years, changesets.month_index_to_year_index),
            TOPIC,
            rank_to_name["contributors"],
        ),
    )

    add_question(
        "How many edits does each imagery service have per month?",
        "af79",
        util.get_multi_line_plot(
            "monthly edits count per imagery service", "edits", months, montly_edits[:10], rank_to_name["edits"][:10]
        ),
        util.get_table(
            "yearly edits count per imagery service",
            years,
            util.monthly_to_yearly_with_total(montly_edits, years, changesets.month_index_to_year_index),
            TOPIC,
            rank_to_name["edits"],
        ),
    )

    add_question(
        "What's the total amount of contributors, edits and changesets of imagery service over time?",
        "327d",
        util.get_multi_line_plot(
            "total contributor count of imagery service",
            "contributors",
            months,
            util.set_cumsum(montly_contributor_sets),
            rank_to_name["contributors"][:10],
        ),
        util.get_multi_line_plot(
            "total edit count of imagery service",
            "edits",
            months,
            util.cumsum(montly_edits),
            rank_to_name["edits"][:10],
        ),
        util.get_multi_line_plot(
            "total changeset count of imagery service",
            "changesets",
            months,
            util.cumsum(monthly_changesets),
            rank_to_name["changesets"][:10],
        ),
    )
