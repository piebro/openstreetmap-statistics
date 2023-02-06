import sys
import numpy as np
import util

# init
DATA_DIR = sys.argv[1]
TOP_K_PLOT = 10
changesets = util.Changesets(DATA_DIR)
top_k_table, index_to_rank, rank_to_name = util.get_rank_infos(DATA_DIR, "all_tags")
all_tags_index_to_tag = np.array(util.load_index_to_tag(DATA_DIR, "all_tags"))

selected_editing_software_names = [
    "JOSM",
    "iD",
    "Potlatch",
    "StreetComplete",
    "RapiD",
    "Go Map!!",
    "Vespucci",
]
created_by_tag_to_index = util.list_to_dict(util.load_index_to_tag(DATA_DIR, "created_by"))
selected_editing_software = [created_by_tag_to_index[name] for name in selected_editing_software_names]
selected_editing_software_index_to_rank = {index: rank for rank, index in enumerate(selected_editing_software)}

months, years = changesets.months, changesets.years
monthly_changesets = np.zeros((top_k_table, len(months)), dtype=np.int64)
monthly_changesets_es = np.zeros((len(selected_editing_software), len(months)), dtype=np.int64)
monthly_changesets_es_tags = np.zeros(
    (len(selected_editing_software), len(all_tags_index_to_tag), len(months)), dtype=np.int64
)

# accumulate data
for csv_line in sys.stdin:
    changesets.update_data_with_csv_str(csv_line)
    month_index = changesets.month_index
    all_tags_indices = changesets.all_tags_indices
    user_index = changesets.user_index
    edits = changesets.edits

    if len(all_tags_indices) == 0:
        continue

    for all_tags_index in set(all_tags_indices):  # set(), because some tags might be there more then once.
        if all_tags_index in index_to_rank["changesets"]:
            rank = index_to_rank["changesets"][all_tags_index]
            monthly_changesets[rank, month_index] += 1

    if changesets.created_by_index in selected_editing_software_index_to_rank:
        rank = selected_editing_software_index_to_rank[changesets.created_by_index]
        monthly_changesets_es[rank, month_index] += 1
        for all_tags_index in all_tags_indices:
            monthly_changesets_es_tags[rank, all_tags_index, month_index] += 1

monthly_changesets_es_tags_top_10 = []
for all_tags_count in monthly_changesets_es_tags:
    total_tag_count = np.sum(all_tags_count, axis=1)
    top_all_tag_indices = np.argsort(-total_tag_count)[:10]
    top_all_tag_indices = [i for i in top_all_tag_indices if total_tag_count[i] > 100]
    monthly_changesets_es_tags_top_10.append(top_all_tag_indices)

# save plots
TOPIC = "Changeset Tags"
with util.add_questions(TOPIC) as add_question:

    add_question(
        "What are the most popular tags for changesets?",
        "b370",
        util.get_multi_line_plot(
            "percent of occurance of tag in changesets per month",
            "percent",
            months,
            util.get_percent(monthly_changesets[:TOP_K_PLOT], changesets.monthly_changsets),
            rank_to_name["changesets"][:TOP_K_PLOT],
            percent=True,
        ),
        util.get_multi_line_plot(
            "occurance of tag in changesets per month",
            "num of occurance",
            months,
            monthly_changesets[:TOP_K_PLOT],
            rank_to_name["changesets"][:TOP_K_PLOT],
        ),
        util.get_table(
            "yearly occurance of tag in changesets",
            years,
            util.monthly_to_yearly_with_total(monthly_changesets, years, changesets.month_index_to_year_index),
            TOPIC,
            rank_to_name["changesets"],
        ),
    )
    add_question(
        "What are the most popular tags for changesets per selected Editing Softwares?",
        "2fb7",
        util.get_text_element(
            "In general the changeset tags are set by the Editing Software. Thats why its interesting to look at"
            " the tag usage per editing software.",
        ),
        *[
            util.get_multi_line_plot(
                f"{name}: percent of occurance of tag in changesets per month",
                "percent",
                months,
                util.get_percent(
                    monthly_changesets_es_tags[rank, monthly_changesets_es_tags_top_10[rank]],
                    monthly_changesets_es[rank],
                ),
                all_tags_index_to_tag[monthly_changesets_es_tags_top_10[rank]],
                percent=True,
            )
            for rank, name in enumerate(selected_editing_software_names)
        ],
    )
