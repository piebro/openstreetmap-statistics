import sys
import numpy as np
import util

# init
DATA_DIR = sys.argv[1]
changesets = util.Changesets(DATA_DIR)
created_by_index_to_tag = util.load_index_to_tag(DATA_DIR, "created_by")

months, years = changesets.months, changesets.years
monthly_changesets = np.zeros((len(months)), dtype=np.int64)
monthly_edits = np.zeros((len(months)), dtype=np.int64)
total_map_edits = np.zeros((360, 180), dtype=np.int64)
monthly_contributor_sets = [set() for _ in range(len(months))]
created_by_dict = {}

# accumulate data
for csv_line in sys.stdin:
    changesets.update_data_with_csv_str(csv_line)
    month_index = changesets.month_index

    if not changesets.bot_used:
        continue

    monthly_changesets[month_index] += 1
    monthly_edits[month_index] += changesets.edits
    monthly_contributor_sets[month_index].add(changesets.user_index)

    if changesets.pos_x is not None:
        total_map_edits[changesets.pos_x, changesets.pos_y] += changesets.edits

    if changesets.created_by_index is not None:
        if changesets.created_by_index not in created_by_dict:
            created_by_dict[changesets.created_by_index] = np.zeros((len(months)), dtype=np.int64)
        created_by_dict[changesets.created_by_index][month_index] += changesets.edits

created_by_items = list(created_by_dict.items())
created_by_edits = np.array([v for _, v in created_by_items])
sort_indices = np.argsort(-np.sum(created_by_edits, axis=1))
created_by_edit_names = np.array([created_by_index_to_tag[key] for key, _ in created_by_items])[sort_indices]
created_by_edits = created_by_edits[sort_indices]

# save plots
TOPIC = "Bot"
with util.add_questions(TOPIC) as add_question:

    add_question(
        "How often are edits created with the help of bots?",
        "785b",
        util.get_single_line_plot("edits created with a bot per month", "edits", months, monthly_edits),
        util.get_single_line_plot(
            "percent of edits created with a bot per month",
            "percent",
            months,
            util.get_percent(monthly_edits, changesets.monthly_edits),
            percent=True,
        ),
    )

    add_question(
        "What's the total amount of contributors, edits and changesets that use bots over time?",
        "0725",
        util.get_single_line_plot(
            "total contributor count that used a bot", "contributors", months, util.set_cumsum(monthly_contributor_sets)
        ),
        util.get_single_line_plot("total edit count that used a bot", "edits", months, util.cumsum(monthly_edits)),
        util.get_single_line_plot(
            "total changeset count that used a bot", "changesets", months, util.cumsum(monthly_changesets)
        ),
    )

    add_question(
        "How many distinct users use bots per month?",
        "da7d",
        util.get_single_line_plot(
            "contributors using bots per month", "contributors", months, util.set_to_length(monthly_contributor_sets)
        ),
    )

    add_question("Where are bots used?", "ed95", util.get_map_plot("total edits using bots", total_map_edits))

    add_question(
        "What's the average edit count per changeset over time?",
        "ae72",
        util.get_single_line_plot(
            "average number of edits per changeset per month using bots",
            "average number of edits per changeset",
            months,
            np.round(util.save_div(monthly_edits, monthly_changesets), 2),
        ),
    )

    add_question(
        "What are the most used bot tools?",
        "e985",
        util.get_table(
            "yearly edit count per bot",
            years,
            util.monthly_to_yearly_with_total(created_by_edits[:100], years, changesets.month_index_to_year_index),
            TOPIC,
            created_by_edit_names[:100],
        ),
    )
