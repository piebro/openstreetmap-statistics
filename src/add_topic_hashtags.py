import sys
import numpy as np
import util

# init
DATA_DIR = sys.argv[1]
changesets = util.Changesets(DATA_DIR)
top_k, index_to_rank, rank_to_name = util.get_rank_infos(DATA_DIR, "hashtag")

months, years = changesets.months, changesets.years
monthly_changesets = np.zeros((top_k, len(months)), dtype=np.int64)
monthly_edits = np.zeros((top_k, len(months)), dtype=np.int64)
monthly_edits_that_use_tag = np.zeros((len(months)), dtype=np.int64)
total_map_ed = np.zeros((10, 360, 180), dtype=np.int64)
monthly_contributor_sets = [[set() for _ in range(len(months))] for _ in range(top_k)]
monthly_contributor_sets_that_use_tag = [set() for _ in range(len(months))]

# accumulate data
for csv_line in sys.stdin:
    changesets.update_data_with_csv_str(csv_line)
    month_index = changesets.month_index

    if len(changesets.hashtag_indices) == 0:
        continue

    monthly_edits_that_use_tag[month_index] += changesets.edits
    monthly_contributor_sets_that_use_tag[month_index].add(changesets.user_index)

    for hashtag_index in changesets.hashtag_indices:
        if hashtag_index in index_to_rank["changesets"]:
            rank = index_to_rank["changesets"][hashtag_index]
            monthly_changesets[rank, month_index] += 1

        if hashtag_index in index_to_rank["edits"]:
            rank = index_to_rank["edits"][hashtag_index]
            monthly_edits[rank, month_index] += changesets.edits
            if changesets.pos_x is not None and rank < 10:
                total_map_ed[rank, changesets.pos_x, changesets.pos_y] += changesets.edits

        if hashtag_index in index_to_rank["contributors"]:
            rank = index_to_rank["contributors"][hashtag_index]
            monthly_contributor_sets[rank][month_index].add(changesets.user_index)

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
            util.get_percent(monthly_edits_that_use_tag, changesets.monthly_edits),
            percent=True,
        ),
        util.get_single_line_plot(
            "percent of monthly contributors that use at least one hashtag",
            "%",
            months,
            util.get_percent(
                util.set_to_length(monthly_contributor_sets_that_use_tag), changesets.monthly_contributors
            ),
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
            util.set_to_length(monthly_contributor_sets[:10]),
            rank_to_name["contributors"][:10],
        ),
        util.get_table(
            "yearly contributor count per hashtag",
            years,
            util.monthly_set_to_yearly_with_total(
                monthly_contributor_sets, years, changesets.month_index_to_year_index
            ),
            TOPIC,
            rank_to_name["contributors"],
        ),
    )

    add_question(
        "How many edits does each hashtag have per month?",
        "f0e6",
        util.get_multi_line_plot(
            "monthly edits count per hashtag", "edits", months, monthly_edits[:10], rank_to_name["edits"][:10]
        ),
        util.get_table(
            "yearly edits count per hashtag",
            years,
            util.monthly_to_yearly_with_total(monthly_edits, years, changesets.month_index_to_year_index),
            TOPIC,
            rank_to_name["edits"],
        ),
    )

    add_question(
        "What's the total amount of contributors, edits and changesets of hashtags over time?",
        "4b4a",
        ("text", f"There are {len(util.load_index_to_tag(DATA_DIR, 'hashtag')):,} different hashtags"),
        util.get_multi_line_plot(
            "total contributor count of hashtags",
            "contributors",
            months,
            util.set_cumsum(monthly_contributor_sets),
            rank_to_name["contributors"][:10],
        ),
        util.get_multi_line_plot(
            "total edit count of hashtags", "edits", months, util.cumsum(monthly_edits), rank_to_name["edits"][:10]
        ),
        util.get_multi_line_plot(
            "total changeset count of hashtags",
            "changesets",
            months,
            util.cumsum(monthly_changesets),
            rank_to_name["changesets"][:10],
        ),
    )

    add_question(
        "Where are the top 10 hashtags used?",
        "bea0",
        *[
            util.get_map_plot(f"total edits for the hashtag: {name}", m)
            for m, name in zip(total_map_ed, rank_to_name["edits"][:10])
        ],
    )
