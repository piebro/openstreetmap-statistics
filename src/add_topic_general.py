import sys
import numpy as np
import util

# init
DATA_DIR = sys.argv[1]
changesets = util.Changesets(DATA_DIR)
top_k, index_to_rank, rank_to_name = util.get_rank_infos(DATA_DIR, "hashtag")
tag_to_id = util.list_to_dict(util.load_index_to_tag(DATA_DIR, "created_by"))
maps_me_android_ios_indices = (tag_to_id["MAPS.ME android"], tag_to_id["MAPS.ME ios"])

months, years = changesets.months, changesets.years
montly_edits_list = [[] for _ in range(len(months))]
yearly_map_edits = np.zeros((len(years), 360, 180), dtype=np.int64)
total_map_ed = np.zeros((360, 180), dtype=np.int64)
monthly_contributor_sets = [set() for _ in range(len(months))]
monthly_contributor_sets_without_maps_me = [set() for _ in range(len(months))]
monthly_contributor_edit_count = [{} for _ in range(len(months))]
contributor_first_edit = {}

# accumulate data
for csv_line in sys.stdin:
    changesets.update_data_with_csv_str(csv_line)
    month_index = changesets.month_index
    user_index = changesets.user_index
    edits = changesets.edits

    montly_edits_list[month_index].append(edits)
    monthly_contributor_sets[month_index].add(user_index)
    if user_index not in monthly_contributor_edit_count[month_index]:
        monthly_contributor_edit_count[month_index][user_index] = 0
    monthly_contributor_edit_count[month_index][user_index] += edits

    if user_index not in contributor_first_edit:
        contributor_first_edit[user_index] = month_index
    else:
        if month_index < contributor_first_edit[user_index]:
            contributor_first_edit[user_index] = month_index

    if changesets.pos_x is not None:
        yearly_map_edits[changesets.month_index_to_year_index[month_index], changesets.pos_x, changesets.pos_y] += edits
        total_map_ed[changesets.pos_x, changesets.pos_y] += edits

    if changesets.created_by_index is None or changesets.created_by_index not in maps_me_android_ios_indices:
        monthly_contributor_sets_without_maps_me[month_index].add(changesets.user_index)


monthly_contributor_without_maps_me = util.set_to_length(monthly_contributor_sets_without_maps_me)
yearly_map_edits_max_z_value = np.max(yearly_map_edits)
monthly_contributor_edit_count_higher_than_100 = [
    np.sum(np.array(list(monthly_contributor_edit_count[month_index].values())) > 100)
    for month_index in range(len(months))
]

month_index, first_edit_count = np.unique(list(contributor_first_edit.values()), return_counts=True)
monthly_new_contributors = np.zeros((len(months)))
monthly_new_contributors[month_index] = first_edit_count

median_edits_per_month_per_changeset = util.get_median(montly_edits_list)
median_edits_per_month_per_contributor = util.get_median(
    [list(monthly_contributor_edit_count[month_index].values()) for month_index in range(len(months))]
)

# save plots
TOPIC = "General"
with util.add_questions(TOPIC) as add_question:

    add_question(
        "How many people are contributing each month?",
        "63f6",
        util.get_single_line_plot("contributors per month", "contributors", months, changesets.monthly_contributors),
        util.get_single_line_plot("new contributors per month", "contributors", months, monthly_new_contributors),
        util.get_single_line_plot(
            "contributors with more than 100 edits per month",
            "contributors",
            months,
            monthly_contributor_edit_count_higher_than_100,
        ),
    )

    add_question(
        "Why is there rapid growth in monthly contributors in 2016?",
        "21d9",
        util.get_text_element(
            "That's because a lot of new people were contributing using the maps.me app. Looking at the plot of"
            " monthly contributors not using maps.me shows that there is linear growth. It is also worth noting"
            " that vast majority of maps.me mappers made only few edits. And due to definciencies in provided"
            " editor quality of their edits was really low."
        ),
        util.get_single_line_plot(
            "contributors per month without maps.me contributors",
            "contributors",
            months,
            monthly_contributor_without_maps_me,
        ),
    )

    add_question(
        "How many edits are added each month?",
        "fe79",
        util.get_single_line_plot("edits per month", "edits", months, changesets.monthly_edits),
    )

    add_question(
        "What's the total amount of contributors, edits and changesets over time?",
        "7026",
        util.get_single_line_plot(
            "total contributor count", "contributors", months, util.set_cumsum(monthly_contributor_sets)
        ),
        util.get_single_line_plot("total edit count", "edits", months, util.cumsum(changesets.monthly_edits)),
        util.get_single_line_plot(
            "total changeset count", "changesets", months, util.cumsum(changesets.monthly_changsets)
        ),
    )

    add_question("Where are edits made?", "727b", util.get_map_plot("total edits", total_map_ed))

    add_question(
        "Where are edits made each year?",
        "bd16",
        *[
            util.get_map_plot(f"total edits {year}", m, yearly_map_edits_max_z_value)
            for m, year in zip(yearly_map_edits, years)
        ],
    )

    add_question(
        "What's the median edit count per contributor each month?",
        "a3ed",
        util.get_single_line_plot(
            "median number of edits per contributor per month",
            "median number of edits per contributor",
            months,
            median_edits_per_month_per_contributor,
        ),
        util.get_single_line_plot(
            "median number of edits per contributor per month since 2010",
            "median number of edits per contributor",
            months[57:],
            median_edits_per_month_per_contributor[57:],
        ),
    )

    add_question(
        "What's the median edit count per changeset each month?",
        "fded",
        util.get_single_line_plot(
            "median number of edits per changeset per month",
            "median number of edits per changeset",
            months,
            median_edits_per_month_per_changeset,
        ),
        util.get_single_line_plot(
            "median number of edits per changeset per month since 2010",
            "median number of edits per changeset",
            months[57:],
            median_edits_per_month_per_changeset[57:],
        ),
    )
