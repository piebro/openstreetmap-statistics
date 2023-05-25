import os
import sys

import numpy as np
import dask.dataframe as dd

import util
from tqdm import tqdm


DATA_DIR = sys.argv[1]
MONTHS, YEARS = util.get_months_years(DATA_DIR)
progress_bar = tqdm(total=88)


def save_topic_general():
    def save_general_contributor_count_more_the_k_edits_monthly():
        ddf = util.load_ddf(DATA_DIR, "general", ("month_index", "edits", "user_index"))
        total_edits_of_contributors = ddf.groupby(["user_index"])["edits"].sum().compute()
        contributors_unique_monthly_set = ddf.groupby(["month_index"])["user_index"].unique().compute().apply(set)

        min_edit_count = [10, 100, 1_000, 10_000, 100_000]
        contributor_count_more_the_k_edits_monthly = []
        for k in min_edit_count:
            contributors_with_more_than_k_edits = set(
                total_edits_of_contributors[total_edits_of_contributors > k].index.to_numpy()
            )
            contributor_count_more_the_k_edits_monthly.append(
                contributors_unique_monthly_set.apply(
                    lambda x: len(x.intersection(contributors_with_more_than_k_edits))
                )
            )
        util.save_y_list(
            progress_bar,
            "general_contributor_count_more_the_k_edits_monthly",
            MONTHS,
            contributor_count_more_the_k_edits_monthly,
            min_edit_count,
        )

    def save_general_edit_count_per_contributor_median_monthly():
        edit_count_per_contributor_median_monthly = []
        for i in range(len(MONTHS)):
            filters = [("month_index", "==", i)]
            ddf = util.load_ddf(DATA_DIR, "general", ("edits", "user_index"), filters)
            contributor_edits_month = ddf.groupby(["user_index"])["edits"].sum().compute()
            edit_count_per_contributor_median_monthly.append(contributor_edits_month.median())

        util.save_y(
            progress_bar,
            "general_edit_count_per_contributor_median_monthly",
            MONTHS,
            None,
            y=edit_count_per_contributor_median_monthly,
        )
        util.save_y(
            progress_bar,
            "general_edit_count_per_contributor_median_monthly_since_2010",
            MONTHS[57:],
            None,
            y=edit_count_per_contributor_median_monthly[57:],
        )

    def save_general_contributor_attrition_rate():
        ddf = util.load_ddf(DATA_DIR, "general", ("year_index", "edits", "user_index"))
        ddf_user_first_edit_year = ddf.groupby(["user_index"])["year_index"].min().compute()
        ddf_user_first_edit_year.name = "first_edit_year_index"
        ddf_year_user_edits = (
            ddf.groupby(["year_index", "user_index"])["edits"].sum().compute().reset_index("year_index")
        )
        merge = dd.merge(ddf_year_user_edits, ddf_user_first_edit_year, on="user_index")
        merge_edit_sum = merge.groupby(["year_index", "first_edit_year_index"])["edits"].sum()

        year_indices = list(range(len(YEARS)))
        merge_edit_sum_series_list = util.multi_index_series_to_series_list(
            merge_edit_sum, list(range(len(YEARS))), drop_level=0
        )
        y_list = np.array(
            [util.pd_series_to_y(year_indices, pd_series, False) for pd_series in merge_edit_sum_series_list]
        ).T
        two_year_y_list = []
        two_year = []
        for i, (y, year) in enumerate(zip(y_list, YEARS)):
            if i % 2 == 0:
                two_year_y_list.append(y)
                two_year.append(year)
            else:
                two_year_y_list[-1] += y
                two_year[-1] += "-" + year

        util.save_y_list(
            progress_bar,
            "general_contributor_count_attrition_rate_yearly",
            YEARS,
            None,
            y_names=list(reversed(two_year)),
            y_list=list(reversed(two_year_y_list)),
        )

    ddf = util.load_ddf(DATA_DIR, "general", ("month_index", "year_index", "edits", "user_index", "pos_x", "pos_y"))
    util.save_base_statistics(
        DATA_DIR,
        progress_bar,
        "general",
        ddf,
        edit_count_monthly=True,
        changeset_count_monthly=True,
        contributor_count_yearly=True,
        contributor_count_monthly=True,
        new_contributor_count_monthly=True,
        edit_count_map_total=True,
    )
    save_general_contributor_count_more_the_k_edits_monthly()
    util.save_edit_count_map_yearly(
        YEARS, progress_bar, "general", util.load_ddf(DATA_DIR, "general", ("year_index", "edits", "pos_x", "pos_y"))
    )
    save_general_edit_count_per_contributor_median_monthly()

    ddf = util.load_ddf(DATA_DIR, "general", ("month_index", "user_index", "created_by"))
    created_by_tag_to_index = util.load_tag_to_index(DATA_DIR, "created_by")
    map_me_indices = np.array([created_by_tag_to_index["MAPS.ME android"], created_by_tag_to_index["MAPS.ME ios"]])
    util.save_y(
        progress_bar,
        "general_no_maps_me_contributor_count_monthly",
        MONTHS,
        ddf[~ddf["created_by"].isin(map_me_indices)].groupby(["month_index"])["user_index"].nunique().compute(),
    )

    util.save_accumulated("general_new_contributor_count_monthly")
    util.save_accumulated("general_edit_count_monthly")
    util.save_accumulated("general_changeset_count_monthly")
    util.save_monthly_to_yearly("general_edit_count_monthly", only_full_years=True)
    util.save_monthly_to_yearly("general_edit_count_monthly")

    save_general_contributor_attrition_rate()
    util.save_percent("general_contributor_count_attrition_rate_yearly", "general_edit_count_yearly")


def save_topic_editing_software():
    def get_software_editor_type_lists():
        created_by_tag_to_index = util.load_tag_to_index(DATA_DIR, "created_by")

        name_to_info = util.load_json(os.path.join("src", "replace_rules_created_by.json"))
        device_type = {
            "desktop_editors": [],
            "mobile_editors": [],
            "tools": [],
        }
        for name, info in name_to_info.items():
            if name not in created_by_tag_to_index:
                print(
                    f"{name} in 'replace_rules_created_by.json' but not in 'tag_to_index' (this is expected when working"
                    " with a part of all changesets)"
                )
                continue
            if "type" in info:
                if info["type"] == "desktop_editor":
                    device_type["desktop_editors"].append(created_by_tag_to_index[name])
                elif info["type"] == "mobile_editor":
                    device_type["mobile_editors"].append(created_by_tag_to_index[name])
                elif info["type"] == "tool":
                    device_type["tools"].append(created_by_tag_to_index[name])
                else:
                    print(f"unknown type: {name['type']} at name {name}")

        device_type["desktop_editors"] = np.array(device_type["desktop_editors"], dtype=np.int64)
        device_type["mobile_editors"] = np.array(device_type["mobile_editors"], dtype=np.int64)
        device_type["tools"] = np.array(device_type["tools"], dtype=np.int64)
        device_type["other/unspecified"] = np.array(
            list(set(created_by_tag_to_index.values()) - set(np.concatenate(list(device_type.values()))))
        )
        return device_type

    def save_created_by_editor_type_stats():
        editor_type_lists = get_software_editor_type_lists()

        ddf = util.load_ddf(DATA_DIR, "general", ("month_index", "user_index", "created_by"))
        util.save_y_list(
            progress_bar,
            "created_by_device_type_contributor_count_monthly",
            MONTHS,
            [
                ddf[ddf["created_by"].isin(l)].groupby(["month_index"])["user_index"].nunique().compute()
                for l in editor_type_lists.values()
            ],
            list(editor_type_lists.keys()),
        )

        ddf = util.load_ddf(DATA_DIR, "general", ("month_index", "edits", "created_by"))
        util.save_y_list(
            progress_bar,
            "created_by_device_type_edit_count_monthly",
            MONTHS,
            [
                ddf[ddf["created_by"].isin(l)].groupby(["month_index"])["edits"].sum().compute()
                for l in editor_type_lists.values()
            ],
            list(editor_type_lists.keys()),
        )
        util.save_percent("created_by_device_type_edit_count_monthly", "general_edit_count_monthly")

    util.save_base_statistics_tag(
        DATA_DIR,
        progress_bar,
        "created_by",
        util.load_ddf(DATA_DIR, "general", ("month_index", "year_index", "edits", "user_index", "created_by")),
        contributor_count_ddf_name="general",
        edit_count_monthly=True,
        changeset_count_monthly=True,
        contributor_count_yearly=True,
        contributor_count_monthly=True,
        new_contributor_count_monthly=True,
    )

    util.save_top_k(10, "created_by_top_100_contributor_count_monthly")

    # TODO: this takes quiet long. Maybe speed it up somehow
    util.save_tag_top_10_contributor_count_first_changeset_monthly(
        MONTHS,
        progress_bar,
        "created_by",
        util.load_ddf(DATA_DIR, "general", ("month_index", "user_index", "created_by")),
        util.load_tag_to_index(DATA_DIR, "created_by"),
        "created_by_top_10_contributor_count_monthly",
    )

    save_created_by_editor_type_stats()

    util.save_top_k(10, "created_by_top_100_new_contributor_count_monthly")
    util.save_top_k(10, "created_by_top_100_edit_count_monthly")
    util.save_top_k(10, "created_by_top_100_changeset_count_monthly")

    util.save_monthly_to_yearly("created_by_top_100_edit_count_monthly")
    util.save_percent("created_by_top_10_edit_count_monthly", "general_edit_count_monthly")
    util.save_percent("created_by_top_10_contributor_count_monthly", "general_contributor_count_monthly")

    util.save_accumulated("created_by_top_10_new_contributor_count_monthly")
    util.save_accumulated("created_by_top_10_edit_count_monthly")
    util.save_accumulated("created_by_top_10_changeset_count_monthly")


def save_topic_corporation():
    util.save_base_statistics_tag(
        DATA_DIR,
        progress_bar,
        "corporation",
        util.load_ddf(DATA_DIR, "general", ("month_index", "edits", "user_index", "pos_x", "pos_y", "corporation")),
        contributor_count_ddf_name="general",
        edit_count_monthly=True,
        changeset_count_monthly=True,
        new_contributor_count_monthly=True,
        edit_count_map_total=True,
    )
    util.save_sum_of_top_k("corporation_top_100_edit_count_monthly")
    util.save_percent("corporation_top_100_edit_count_monthly_sum_top_k", "general_edit_count_monthly")
    util.save_top_k(10, "corporation_top_100_new_contributor_count_monthly")
    util.save_top_k(10, "corporation_top_100_edit_count_monthly")
    util.save_top_k(10, "corporation_top_100_changeset_count_monthly")
    util.save_monthly_to_yearly("corporation_top_100_edit_count_monthly")
    util.save_accumulated("corporation_top_10_new_contributor_count_monthly")
    util.save_accumulated("corporation_top_10_edit_count_monthly")
    util.save_accumulated("corporation_top_10_changeset_count_monthly")


def save_topic_source_imagery_hashtag():

    all_tags_tag_to_index = util.load_tag_to_index(DATA_DIR, "all_tags")

    for tag, tag_name_in_all_tags in [("source", "source"), ("imagery", "imagery_used"), ("hashtag", "hashtags")]:
        ddf_all_tags = util.load_ddf(DATA_DIR, "all_tags", ["month_index", "edits", "all_tags"])
        util.save_base_statistics(
            DATA_DIR,
            progress_bar,
            tag,
            ddf_all_tags[ddf_all_tags["all_tags"] == all_tags_tag_to_index[tag_name_in_all_tags]],
            edit_count_monthly=True,
        )
        util.save_percent(f"{tag}_edit_count_monthly", "general_edit_count_monthly")

        util.save_base_statistics_tag(
            DATA_DIR,
            progress_bar,
            tag,
            util.load_ddf(DATA_DIR, tag, None),
            contributor_count_ddf_name=tag,
            edit_count_monthly=True,
            changeset_count_monthly=True,
            contributor_count_yearly=True,
            contributor_count_monthly=True,
            new_contributor_count_monthly=True,
            edit_count_map_total=(tag == "hashtag"),
        )
        util.save_top_k(10, f"{tag}_top_100_contributor_count_monthly")
        util.save_top_k(10, f"{tag}_top_100_new_contributor_count_monthly")
        util.save_top_k(10, f"{tag}_top_100_edit_count_monthly")
        util.save_top_k(10, f"{tag}_top_100_changeset_count_monthly")
        util.save_monthly_to_yearly(f"{tag}_top_100_edit_count_monthly")
        util.save_accumulated(f"{tag}_top_10_new_contributor_count_monthly")
        util.save_accumulated(f"{tag}_top_10_edit_count_monthly")
        util.save_accumulated(f"{tag}_top_10_changeset_count_monthly")


def save_topic_streetcomplete():
    ddf_all_tags = util.load_ddf(
        DATA_DIR, "all_tags", ("month_index", "edits", "user_index", "pos_x", "pos_y", "all_tags")
    )
    all_tags_tag_to_index = util.load_tag_to_index(DATA_DIR, "all_tags")
    util.save_base_statistics(
        DATA_DIR,
        progress_bar,
        "streetcomplete",
        ddf_all_tags[ddf_all_tags["all_tags"] == all_tags_tag_to_index["StreetComplete"]],
        edit_count_monthly=True,
        contributor_count_monthly=True,
        edit_count_map_total=True,
    )
    util.save_percent("streetcomplete_contributor_count_monthly", "general_contributor_count_monthly")
    util.save_percent("streetcomplete_edit_count_monthly", "general_edit_count_monthly")

    util.save_base_statistics_tag(
        DATA_DIR,
        progress_bar,
        "streetcomplete",
        util.load_ddf(DATA_DIR, "general", ("month_index", "edits", "user_index", "streetcomplete")),
        contributor_count_ddf_name="general",
        k=300,
        edit_count_monthly=True,
        changeset_count_monthly=True,
        new_contributor_count_monthly=True,
    )
    util.save_monthly_to_yearly("streetcomplete_top_300_edit_count_monthly")
    util.save_top_k(10, "streetcomplete_top_300_new_contributor_count_monthly")
    util.save_top_k(10, "streetcomplete_top_300_edit_count_monthly")
    util.save_top_k(10, "streetcomplete_top_300_changeset_count_monthly")
    util.save_accumulated("streetcomplete_top_10_new_contributor_count_monthly")
    util.save_accumulated("streetcomplete_top_10_edit_count_monthly")
    util.save_accumulated("streetcomplete_top_10_changeset_count_monthly")


def save_topic_bot():
    ddf = util.load_ddf(
        DATA_DIR, "general", ("month_index", "edits", "user_index", "pos_x", "pos_y", "bot", "created_by")
    )
    util.save_base_statistics(
        DATA_DIR,
        progress_bar,
        "bot",
        ddf[ddf["bot"] == True],
        edit_count_monthly=True,
        changeset_count_monthly=True,
        contributor_count_monthly=True,
        new_contributor_count_monthly=True,
        edit_count_map_total=True,
    )
    util.save_percent("bot_edit_count_monthly", "general_edit_count_monthly")
    util.save_accumulated("bot_new_contributor_count_monthly")
    util.save_accumulated("bot_edit_count_monthly")
    util.save_accumulated("bot_changeset_count_monthly")
    util.save_div("bot_avg_edit_count_per_changeset_monthly", "bot_edit_count_monthly", "bot_changeset_count_monthly")

    util.save_base_statistics_tag(
        DATA_DIR, progress_bar, "created_by", ddf[ddf["bot"] == True], k=100, prefix="bot", edit_count_monthly=True
    )
    util.save_monthly_to_yearly("bot_created_by_top_100_edit_count_monthly")


def save_topic_tags():
    ddf_all_tags = util.load_ddf(DATA_DIR, "all_tags", ("month_index", "all_tags", "created_by"))
    util.save_base_statistics_tag(DATA_DIR, progress_bar, "all_tags", ddf_all_tags, k=100, changeset_count_monthly=True)
    util.save_monthly_to_yearly("all_tags_top_100_changeset_count_monthly")
    util.save_top_k(10, "all_tags_top_100_changeset_count_monthly")
    util.save_percent("all_tags_top_10_changeset_count_monthly", "general_changeset_count_monthly")

    selected_editors = ["JOSM", "iD", "Potlatch", "StreetComplete", "Rapid", "Vespucci"]
    monthly_data = util.load_json(os.path.join("assets", "data", f"created_by_top_100_changeset_count_monthly.json"))
    editor_name_to_changeset_count_monthly = {
        name: y for name, y in zip(monthly_data["y_names"], monthly_data["y_list"])
    }
    created_by_tag_to_index = util.load_tag_to_index(DATA_DIR, "created_by")
    for selected_editor_name in selected_editors:
        editor_index = created_by_tag_to_index[selected_editor_name]
        util.save_base_statistics_tag(
            DATA_DIR,
            progress_bar,
            "all_tags",
            ddf_all_tags[ddf_all_tags["created_by"] == editor_index],
            prefix=f"created_by_{selected_editor_name}",
            k=10,
            changeset_count_monthly=True,
        )
        util.save_percent(
            f"created_by_{selected_editor_name}_all_tags_top_10_changeset_count_monthly",
            None,
            editor_name_to_changeset_count_monthly[selected_editor_name],
        )


def save_name_to_link():
    util.save_json(
        os.path.join("assets", "data", "created_by_name_to_link.json"),
        util.get_name_to_link("replace_rules_created_by.json"),
    )
    util.save_json(
        os.path.join("assets", "data", "imagery_and_source_name_to_link.json"),
        util.get_name_to_link("replace_rules_imagery_and_source.json"),
    )
    corporation_name_to_link = {
        name: link
        for name, (link, _) in util.load_json(os.path.join("assets", "corporation_contributors.json")).items()
    }
    util.save_json(os.path.join("assets", "data", "corporation_name_to_link.json"), corporation_name_to_link)


def main():
    save_name_to_link()

    save_topic_general()
    save_topic_editing_software()
    save_topic_corporation()
    save_topic_source_imagery_hashtag()
    save_topic_streetcomplete()
    save_topic_bot()
    save_topic_tags()


if __name__ == "__main__":
    sys.exit(main())
