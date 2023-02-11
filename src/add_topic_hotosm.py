import sys
import numpy as np
import util

# init
DATA_DIR = sys.argv[1]
TABLE_TOP_K = 200
changesets = util.Changesets(DATA_DIR)

months, years = changesets.months, changesets.years
monthly_edits = np.zeros((len(months)), dtype=np.int64)
monthly_contributor_sets = [set() for _ in range(len(months))]

hashtag_index_to_tag = util.load_index_to_tag(DATA_DIR, "hashtag")
hotosm_project_index_to_yearly_edits = {
    index: np.zeros(len(years), dtype=np.int64)
    for index, tag in enumerate(hashtag_index_to_tag)
    if tag[:16] == "#hotosm-project-"
}
hotosm_project_index_to_yearly_contributor = {
    index: [set() for _ in years] for index in hotosm_project_index_to_yearly_edits.keys()
}

# accumulate data
for csv_line in sys.stdin:
    changesets.update_data_with_csv_str(csv_line)
    month_index = changesets.month_index
    year_index = changesets.month_index_to_year_index[month_index]
    edits = changesets.edits
    user_index = changesets.user_index

    for hashtag_index in changesets.hashtag_indices:
        if hashtag_index in hotosm_project_index_to_yearly_edits:
            hotosm_project_index_to_yearly_edits[hashtag_index][year_index] += edits
            hotosm_project_index_to_yearly_contributor[hashtag_index][year_index].add(user_index)

    for hashtag_index in changesets.hashtag_indices:
        if hashtag_index in hotosm_project_index_to_yearly_edits:
            monthly_edits[month_index] += edits
            monthly_contributor_sets[month_index].add(user_index)
            break

indices = hotosm_project_index_to_yearly_edits.keys()
yearly_names = np.array([hashtag_index_to_tag[index] for index in indices])

yearly_edits = np.array([hotosm_project_index_to_yearly_edits[index] for index in indices])
yearly_edits_with_total = util.yearly_to_yearly_with_total(yearly_edits)
edits_sort_indices = np.argsort(-yearly_edits_with_total[:, -1])

yearly_contributor_set = np.array([hotosm_project_index_to_yearly_contributor[index] for index in indices])
yearly_contribtuor_with_total = util.yearly_set_to_yearly_with_total(yearly_contributor_set)
contributor_sort_indices = np.argsort(-yearly_contribtuor_with_total[:, -1])


# save plots
TOPIC = "Humanitarian OpenStreetMap Team"
with util.add_questions(TOPIC) as add_question:

    add_question(
        "How popular is hotosm?",
        "17e4",
        util.get_single_line_plot(
            "monthly contributor count to hotosm",
            "contributors",
            months,
            util.set_to_length(monthly_contributor_sets),
        ),
        util.get_single_line_plot(
            "monthly edits count to hotosm",
            "edits",
            months,
            monthly_edits,
        ),
    )

    add_question(
        "Which Projects have the most contributors?",
        "3c19",
        util.get_text_element(
            "The contributor count per project is determined by how many individual users (individual OSM user names)"
            " have used the project hashtag. The numbers seem to be <b>higher than the official numbers</b> of the"
            " project. I don't know why that's the case. If you have an idea feel free to open an"
            " <a href='https://github.com/piebro/openstreetmap-statistics/issues'>issue</a>."
        ),
        util.get_table(
            "yearly contributor count per project",
            years,
            yearly_contribtuor_with_total[contributor_sort_indices][:TABLE_TOP_K],
            "Project Number",
            yearly_names[contributor_sort_indices][:TABLE_TOP_K],
        ),
    )

    add_question(
        "Which Projects have the most edits?",
        "1e56",
        util.get_text_element(
            "The edit count per project is the sum of all edits that use the project hashtag. The number of edits is"
            " determined by how many nodes, ways or relations were created or changed. The number seems to be "
            "<b>higher than the official numbers</b> of the project. This could be, because the Humanitarian OSM Team"
            " uses another metric for counting edits. For example, they might count adding a build as only one edit."
        ),
        util.get_table(
            "yearly edits count per project",
            years,
            yearly_edits_with_total[edits_sort_indices][:TABLE_TOP_K],
            "Project Number",
            yearly_names[edits_sort_indices][:TABLE_TOP_K],
        ),
    )
