import sys
import numpy as np
import util

# init
DATA_DIR = sys.argv[1]
changesets = util.Changesets(DATA_DIR)
top_k, index_to_rank, rank_to_name = util.get_rank_infos(DATA_DIR, "source")

months, years = changesets.months, changesets.years
monthly_changesets = np.zeros((top_k, len(months)), dtype=np.int64)
montly_edits = np.zeros((top_k, len(months)), dtype=np.int64)
montly_edits_that_use_tag = np.zeros((len(months)), dtype=np.int64)
montly_contributor_sets = [[set() for _ in range(len(months))] for _ in range(top_k)]

# accumulate data
for csv_line in sys.stdin:
    changesets.update_data_with_csv_str(csv_line)
    month_index = changesets.month_index

    if len(changesets.source_indices) == 0:
        continue

    montly_edits_that_use_tag[month_index] += changesets.edits

    for source_indices in changesets.source_indices:
        if source_indices in index_to_rank["changesets"]:
            rank = index_to_rank["changesets"][source_indices]
            monthly_changesets[rank, month_index] += 1

        if source_indices in index_to_rank["edits"]:
            rank = index_to_rank["edits"][source_indices]
            montly_edits[rank, month_index] += changesets.edits

        if source_indices in index_to_rank["contributors"]:
            rank = index_to_rank["contributors"][source_indices]
            montly_contributor_sets[rank][month_index].add(changesets.user_index)


# save plots
TOPIC = "Source"
with util.add_questions(TOPIC) as add_question:

    add_question(
        "How often is the 'source' tag used?",
        "9267",
        util.get_single_line_plot(
            "monthly edits that use the source tag",
            "%",
            months,
            util.get_percent(montly_edits_that_use_tag, changesets.monthly_edits),
            percent=True,
        ),
    )

    add_question(
        "How many contributors use which source each month?",
        "6c08",
        util.get_multi_line_plot(
            "monthly contributor count per source",
            "contributors",
            months,
            util.set_to_length(montly_contributor_sets)[:10],
            rank_to_name["contributors"][:10],
        ),
        util.get_table(
            "yearly contributor count per source",
            years,
            util.monthly_set_to_yearly_with_total(montly_contributor_sets, years, changesets.month_index_to_year_index),
            TOPIC,
            rank_to_name["contributors"],
        ),
    )

    add_question(
        "How many edits does each source have per month?",
        "daf4",
        util.get_multi_line_plot(
            "monthly edits count per source", "edits", months, montly_edits[:10], rank_to_name["edits"][:10]
        ),
        util.get_table(
            "yearly edits count per source",
            years,
            util.monthly_to_yearly_with_total(montly_edits, years, changesets.month_index_to_year_index),
            TOPIC,
            rank_to_name["edits"],
        ),
    )

    add_question(
        "What's the total amount of contributors, edits and changesets of sources over time?",
        "7e84",
        util.get_multi_line_plot(
            "total contributor count of sources",
            "contributors",
            months,
            util.set_cumsum(montly_contributor_sets),
            rank_to_name["contributors"][:10],
        ),
        util.get_multi_line_plot(
            "total edit count of sources",
            "edits",
            months,
            util.cumsum(montly_edits),
            rank_to_name["edits"][:10],
        ),
        util.get_multi_line_plot(
            "total changeset count of sources",
            "changesets",
            months,
            util.cumsum(monthly_changesets),
            rank_to_name["changesets"][:10],
        ),
    )
