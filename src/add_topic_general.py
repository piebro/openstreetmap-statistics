import sys
import numpy as np
import util

# init
DATA_DIR = sys.argv[1]
months, years = util.get_months_years(DATA_DIR)
year_to_year_index = util.list_to_dict(years)
month_index_to_year_index = {month_i: year_to_year_index[month[:4]] for month_i, month in enumerate(months)}
tag_to_id = util.list_to_dict(util.load_index_to_tag(DATA_DIR, "created_by"))
maps_me_android_id = tag_to_id["MAPS.ME android"]
maps_me_ios_id = tag_to_id["MAPS.ME ios"]

mo_ch = np.zeros((len(months)), dtype=np.int64)
mo_ed = np.zeros((len(months)), dtype=np.int64)
mo_ed_list = [[] for _ in range(len(months))]
ye_map_ed = np.zeros((len(years), 360, 180), dtype=np.int64)
total_map_ed = np.zeros((360, 180), dtype=np.int64)
mo_co_set = [set() for _ in range(len(months))]
mo_co_set_without_maps_me = [set() for _ in range(len(months))]
mo_co_edit_count = [{} for _ in range(len(months))]
co_first_edit = {}

# accumulate data
for line in sys.stdin:
    data = line[:-1].split(",")
    edits = int(data[0])
    month_index = int(data[1])
    user_id = int(data[2])
    x, y = data[4], data[5]

    mo_ch[month_index] += 1
    mo_ed[month_index] += edits
    mo_ed_list[month_index].append(edits)
    mo_co_set[month_index].add(user_id)
    if user_id not in mo_co_edit_count[month_index]:
        mo_co_edit_count[month_index][user_id] = 0
    mo_co_edit_count[month_index][user_id] += edits

    if user_id not in co_first_edit:
        co_first_edit[user_id] = month_index
    else:
        if month_index < co_first_edit[user_id]:
            co_first_edit[user_id] = month_index

    if len(x) > 0:
        ye_map_ed[month_index_to_year_index[month_index], int(x), int(y)] += edits
        total_map_ed[int(x), int(y)] += edits

    if len(data[7]) == 0 or (int(data[7]) != maps_me_android_id and int(data[7]) != maps_me_ios_id):
        mo_co_set_without_maps_me[month_index].add(user_id)


mo_co = util.set_to_length(mo_co_set)
mo_co_without_maps_me = util.set_to_length(mo_co_set_without_maps_me)
ye_map_ed_max_z_value = np.max(ye_map_ed)
mo_co_edit_count_higher_then_100 = [
    np.sum(np.array(list(mo_co_edit_count[month_index].values())) > 100) for month_index in range(len(months))
]

month_index, first_edit_count = np.unique(list(co_first_edit.values()), return_counts=True)
mo_new_co = np.zeros((len(months)))
mo_new_co[month_index] = first_edit_count

median_ed_per_mo_per_ch = util.get_median(mo_ed_list)
median_ed_per_mo_per_co = util.get_median(
    [list(mo_co_edit_count[month_index].values()) for month_index in range(len(months))]
)

# save plots
TOPIC = "General"
with util.add_questions(TOPIC) as add_question:

    add_question(
        "How many people are contributing each month?",
        "63f6",
        util.get_single_line_plot("contributors per month", "contributors", months, mo_co),
        util.get_single_line_plot("new contributors per month", "contributors", months, mo_new_co),
        util.get_single_line_plot(
            "contributors with more then 100 edits per month",
            "contributors",
            months,
            mo_co_edit_count_higher_then_100,
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
            "contributors per month without maps.me contributors", "contributors", months, mo_co_without_maps_me
        ),
    )

    add_question(
        "How many edits are added each month?",
        "fe79",
        util.get_single_line_plot("edits per month", "edits", months, mo_ed),
    )

    add_question(
        "What's the total amount of contributors, edits and changesets over time?",
        "7026",
        util.get_single_line_plot("total contributor count", "contributors", months, util.set_cumsum(mo_co_set)),
        util.get_single_line_plot("total edit count", "edits", months, util.cumsum(mo_ed)),
        util.get_single_line_plot("total changeset count", "changesets", months, util.cumsum(mo_ch)),
    )

    add_question("Where are edits made?", "727b", util.get_map_plot("total edits", total_map_ed))

    add_question(
        "Where are edits made each year?",
        "bd16",
        *[util.get_map_plot(f"total edits {year}", m, ye_map_ed_max_z_value) for m, year in zip(ye_map_ed, years)],
    )

    add_question(
        "What's the median edit count per contributor each month?",
        "a3ed",
        util.get_single_line_plot(
            "median number of edits per contributor per month",
            "median number of edits per contributor",
            months,
            median_ed_per_mo_per_co,
        ),
        util.get_single_line_plot(
            "median number of edits per contributor per month since 2010",
            "median number of edits per contributor",
            months[57:],
            median_ed_per_mo_per_co[57:],
        ),
    )

    add_question(
        "What's the median edit count per changeset each month?",
        "fded",
        util.get_single_line_plot(
            "median number of edits per changeset per month",
            "median number of edits per changeset",
            months,
            median_ed_per_mo_per_ch,
        ),
        util.get_single_line_plot(
            "median number of edits per changeset per month since 2010",
            "median number of edits per changeset",
            months[57:],
            median_ed_per_mo_per_ch[57:],
        ),
    )
