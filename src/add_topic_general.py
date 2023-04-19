import os
import sys
import numpy as np
import util
import dask.dataframe as dd

# init
DATA_DIR = sys.argv[1]
<<<<<<< HEAD
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
=======
months, years = util.get_months_years(DATA_DIR)
ddf = dd.read_parquet(os.path.join(DATA_DIR, "changeset_data", "general_*.parquet"))


def get_monthly_contributor_count_more_then_k_edits(ddf, monthly_contributor, min_edit_count):
    total_edits_of_contributors = ddf.groupby(["user_index"])["edits"].sum().compute()
    monthly_contributor_set = monthly_contributor.apply(set)

    monthly_contributor_count_more_then_k_edits = []
    for k in min_edit_count:
        contributors_with_more_then_k_edits = set(
            total_edits_of_contributors[total_edits_of_contributors > k].index.to_numpy()
        )
        monthly_contributor_count_more_then_k_edits.append(
            monthly_contributor_set.apply(lambda x: len(x.intersection(contributors_with_more_then_k_edits)))
        )
    return monthly_contributor_count_more_then_k_edits

>>>>>>> added saving changset data as parquet files

# save plots
TOPIC = "General"
with util.add_questions(TOPIC) as add_question:
    monthly_contributor = ddf.groupby(["month_index"])["user_index"].unique().compute()
    monthly_contributor_count = monthly_contributor.apply(len)
    monthly_new_contributors_count = util.cumsum_new_nunique(monthly_contributor)
    min_edit_count = [1e1, 1e2, 1e3, 1e4, 1e5]
    add_question(
        "How many people are contributing each month?",
        "63f6",
<<<<<<< HEAD
        util.get_single_line_plot("contributors per month", "contributors", months, changesets.monthly_contributors),
        util.get_single_line_plot("new contributors per month", "contributors", months, monthly_new_contributors),
        util.get_single_line_plot(
            "contributors with more than 100 edits per month",
            "contributors",
            months,
            monthly_contributor_edit_count_higher_than_100,
=======
        util.get_single_line_plot("contributors per month", "contributors", months, monthly_contributor_count),
        util.get_single_line_plot("new contributors per month", "contributors", months, monthly_new_contributors_count),
        util.get_multi_line_plot(
            "contributors with more then k edits total",
            "contributors",
            months,
            get_monthly_contributor_count_more_then_k_edits(ddf, monthly_contributor, min_edit_count),
            [f"{int(k):,}" for k in min_edit_count],
>>>>>>> added saving changset data as parquet files
        ),
    )
    monthly_contributor_cumsum = util.cumsum_nunique(monthly_contributor)
    monthly_contributor = None  # to save memory

    created_by_tag_to_index = util.load_tag_to_index(DATA_DIR, "created_by")
    monthly_contributor_count_no_maps_me = (
        ddf[
            ~ddf["created_by"].isin(
                (created_by_tag_to_index["MAPS.ME android"], created_by_tag_to_index["MAPS.ME ios"])
            )
        ]
        .groupby(["month_index"])["user_index"]
        .nunique()
        .compute()
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
            monthly_contributor_count_no_maps_me,
        ),
    )
    monthly_contributor_count_no_maps_me = None  # to save memory

    monthly_edits = ddf.groupby(["month_index"])["edits"].sum().compute()
    add_question(
        "How many edits are added each month?",
        "fe79",
        util.get_single_line_plot("edits per month", "edits", months, monthly_edits),
    )

    monthly_changesets = ddf.groupby(["month_index"]).size().compute()
    add_question(
        "What's the total amount of contributors, edits and changesets over time?",
        "7026",
        util.get_single_line_plot("total contributor count", "contributors", months, monthly_contributor_cumsum),
        util.get_single_line_plot("total edit count", "edits", months, monthly_edits.cumsum()),
        util.get_single_line_plot("total changeset count", "changesets", months, monthly_changesets.cumsum()),
    )

    monthly_map_edits = ddf[ddf["pos_x"] >= 0].groupby(["month_index", "pos_x", "pos_y"])["edits"].sum().compute()
    total_map_edits = monthly_map_edits.groupby(level=[1, 2]).sum()
    add_question("Where are edits made?", "727b", util.get_map_plot("total edits", total_map_edits))

    year_index_to_month_indices = util.get_year_index_to_month_indices(DATA_DIR)
    yearly_map_edits = [
        monthly_map_edits[monthly_map_edits.index.isin(month_indices, level="month_index")].groupby(level=[1, 2]).sum()
        for month_indices in year_index_to_month_indices
    ]
    yearly_map_edits_max_value = np.max([map_edits.max() for map_edits in yearly_map_edits])
    add_question(
        "Where are edits made each year?",
        "bd16",
        *[
            util.get_map_plot(f"total edits {year}", m, yearly_map_edits_max_value)
            for m, year in zip(yearly_map_edits, years)
        ],
    )

    monthly_contributor_edits = ddf.groupby(["month_index", "user_index"])["edits"].sum().compute()
    median_edits_per_month_per_contributor = monthly_contributor_edits.groupby(["month_index"]).median()

    median_edits_per_month_per_contributor_since_2010 = median_edits_per_month_per_contributor[
        median_edits_per_month_per_contributor.index >= 57
    ]
    median_edits_per_month_per_contributor_since_2010.index -= 57

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
            median_edits_per_month_per_contributor_since_2010,
        ),
    )

    # TODO: possible with changeset_index in database
    # monthly_contributor_changesets = ddf.groupby(["month_index", "user_index"]).size().compute()
    # median_changesets_per_month_per_contributor = monthly_contributor_changesets.groupby(["month_index"]).median()

    # median_changesets_per_month_per_contributor_since_2010 = median_changesets_per_month_per_contributor[median_changesets_per_month_per_contributor.index >= 57]
    # median_changesets_per_month_per_contributor_since_2010.index -= 57

    # add_question(
    #     "What's the median edit count per changeset each month?",
    #     "fded",
    #     util.get_single_line_plot(
    #         "median number of edits per changeset per month",
    #         "median number of edits per changeset",
    #         months,
    #         median_changesets_per_month_per_contributor,
    #     ),
    #     util.get_single_line_plot(
    #         "median number of edits per changeset per month since 2010",
    #         "median number of edits per changeset",
    #         months[57:],
    #         median_changesets_per_month_per_contributor_since_2010,
    #     ),
    # )
