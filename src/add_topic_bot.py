import sys
import numpy as np
import util

# init
DATA_DIR = sys.argv[1]
months, years = util.get_months_years(DATA_DIR)
year_to_year_index = util.list_to_dict(years)
month_index_to_year_index = {month_i: year_to_year_index[month[:4]] for month_i, month in enumerate(months)}
created_by_index_to_tag = util.load_index_to_tag(DATA_DIR, "created_by")

mo_ch = np.zeros((len(months)), dtype=np.int64)
mo_ed = np.zeros((len(months)), dtype=np.int64)
mo_ed_all = np.zeros((len(months)), dtype=np.int64)
total_map_edits = np.zeros((360, 180), dtype=np.int64)
mo_co_set = [set() for _ in range(len(months))]
created_by_dict = {}

# accumulate data
for csv_line in sys.stdin:
    data = util.CSVData(csv_line)
    mo_ed_all[data.month_index] += data.edits
    if not data.bot_used:
        continue

    mo_ch[data.month_index] += 1
    mo_ed[data.month_index] += data.edits
    mo_co_set[data.month_index].add(data.user_index)

    if data.pos_x is not None:
        total_map_edits[data.pos_x, data.pos_y] += data.edits

    if data.created_by_index is not None:
        if data.created_by_index not in created_by_dict:
            created_by_dict[data.created_by_index] = np.zeros((len(months)), dtype=np.int64)
        created_by_dict[data.created_by_index][data.month_index] += data.edits

created_by_items = list(created_by_dict.items())
created_by_ed = np.array([v for _, v in created_by_items])
sort_indices = np.argsort(-np.sum(created_by_ed, axis=1))
created_by_ed_names = np.array([created_by_index_to_tag[key] for key, _ in created_by_items])[sort_indices]
created_by_ed = created_by_ed[sort_indices]

# save plots
TOPIC = "Bot"
with util.add_questions(TOPIC) as add_question:

    add_question(
        "How often are edits created with the help of bots?",
        "785b",
        util.get_single_line_plot("edits created with a bot per month", "edits", months, mo_ed),
        util.get_single_line_plot(
            "percent of edits created with a bot per month",
            "percent",
            months,
            util.get_percent(mo_ed, mo_ed_all),
            percent=True,
        ),
    )

    add_question(
        "What's the total amount of contributors, edits and changesets that use bots over time?",
        "0725",
        util.get_single_line_plot(
            "total contributor count that used a bot", "contributors", months, util.set_cumsum(mo_co_set)
        ),
        util.get_single_line_plot("total edit count that used a bot", "edits", months, util.cumsum(mo_ed)),
        util.get_single_line_plot("total changeset count that used a bot", "changesets", months, util.cumsum(mo_ch)),
    )

    add_question(
        "How many distinct users use bots per month?",
        "da7d",
        util.get_single_line_plot(
            "contributors using bots per month", "contributors", months, util.set_to_length(mo_co_set)
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
            np.round(util.save_div(mo_ed, mo_ch), 2),
        ),
    )

    add_question(
        "What are the most used bot tools?",
        "e985",
        util.get_table(
            "yearly edit count per bot",
            years,
            util.monthly_to_yearly_with_total(created_by_ed[:100], years, month_index_to_year_index),
            TOPIC,
            created_by_ed_names[:100],
        ),
    )
