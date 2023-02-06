import os
import sys
import numpy as np
import util

# init
DATA_DIR = sys.argv[1]
changesets = util.Changesets(DATA_DIR)
user_name_index_to_tag = util.load_index_to_tag(DATA_DIR, "user_name")

corporation_contributors = util.load_json(os.path.join("assets", "corporation_contributors.json"))
corporations = np.array(list(corporation_contributors.keys()))
corporations_with_link = np.array(
    [f'<a href="{corporation_contributors[corporation][0]}">{corporation}</a>' for corporation in corporations]
)
user_name_to_corporation_id = {}
for i, corporation in enumerate(corporations):
    for user_name in corporation_contributors[corporation][1]:
        user_name_to_corporation_id[user_name] = i

months, years = changesets.months, changesets.years
monthly_changesets = np.zeros((len(corporations), len(months)), dtype=np.int64)
monthly_edits = np.zeros((len(corporations), len(months)), dtype=np.int64)
monthly_edits_that_are_corporate = np.zeros((len(months)), dtype=np.int64)
total_map_edits = np.zeros((len(corporations), 360, 180), dtype=np.int64)
monthly_contributor_sets = [[set() for _ in range(len(months))] for _ in range(len(corporations))]

# accumulate data
for csv_line in sys.stdin:
    changesets.update_data_with_csv_str(csv_line)
    month_index = changesets.month_index
    user_index = changesets.user_index
    edits = changesets.edits

    user_name = user_name_index_to_tag[user_index]
    if user_name not in user_name_to_corporation_id:
        continue
    corporation_id = user_name_to_corporation_id[user_name]

    monthly_edits_that_are_corporate[month_index] += edits

    monthly_changesets[corporation_id, month_index] += 1
    monthly_edits[corporation_id, month_index] += edits
    monthly_contributor_sets[corporation_id][month_index].add(user_index)

    if changesets.pos_x is not None:
        total_map_edits[corporation_id, changesets.pos_x, changesets.pos_y] += edits

# preprocess data
total_changesets = np.array([np.sum(v) for v in monthly_changesets])
sort_indices_changesets = np.argsort(-total_changesets)
monthly_changesets = monthly_changesets[sort_indices_changesets]
corporations_changesets = corporations[sort_indices_changesets]

total_edits = np.array([np.sum(v) for v in monthly_edits])
sort_indices_edits = np.argsort(-total_edits)
monthly_edits = monthly_edits[sort_indices_edits]
corporations_edits = corporations[sort_indices_edits]
corporations_with_link_edits = corporations_with_link[sort_indices_edits]
total_map_edits = total_map_edits[sort_indices_edits]
total_map_edits_max_z_value = np.max(total_map_edits[:10])

total_contributors = np.array([len(set.union(*v)) for v in monthly_contributor_sets])
sort_indices_contributors = np.argsort(-total_contributors)
monthly_contributor_accurancy = util.set_cumsum(monthly_contributor_sets)[sort_indices_contributors]
corporations_contibutors = corporations[sort_indices_contributors]

# save plots
TOPIC = "Corporations"
with util.add_questions(TOPIC) as add_question:

    add_question(
        "How many edits are added from corporations each month?",
        "7034",
        util.get_single_line_plot(
            "percent of edits from corporation per month",
            "%",
            months,
            util.get_percent(monthly_edits_that_are_corporate, changesets.monthly_edits),
            percent=True,
        ),
    )

    add_question(
        "Which corporations are contributing how much?",
        "b34d",
        util.get_multi_line_plot(
            "monthly edits per corporation", "edits", months, monthly_edits[:10], corporations_edits[:10]
        ),
        util.get_table(
            "yearly edits per corporation",
            years,
            util.monthly_to_yearly_with_total(monthly_edits, years, changesets.month_index_to_year_index),
            TOPIC,
            corporations_with_link_edits,
        ),
    )

    add_question(
        "What's the total amount of contributors, edits and changesets from corporations over time?",
        "4ef4",
        util.get_multi_line_plot(
            "total contributor count of corporations",
            "contributors",
            months,
            monthly_contributor_accurancy,
            corporations_contibutors[:10],
        ),
        util.get_multi_line_plot(
            "total edit count of corporations", "edits", months, util.cumsum(monthly_edits), corporations_edits[:10]
        ),
        util.get_multi_line_plot(
            "total changeset count of corporations",
            "changesets",
            months,
            util.cumsum(monthly_changesets),
            corporations_changesets[:10],
        ),
    )

    add_question(
        "Where are the top 10 corporations contributing?",
        "e19b",
        *[
            util.get_map_plot(f"total edits of the corporation: {name}", m, total_map_edits_max_z_value)
            for m, name in zip(total_map_edits[:10], corporations_edits[:10])
        ],
    )
