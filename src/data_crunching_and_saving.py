import os
import sys
import re

import numpy as np
import dask.dataframe as dd

import util
from tqdm import tqdm


DATA_DIR = sys.argv[1]
PARQUET_DIR = os.path.join(DATA_DIR, "changeset_data")
MONTHS, YEARS = util.get_months_years(DATA_DIR)

TOTAL_DATA_DICTS = 87
progress_bar = tqdm(total=TOTAL_DATA_DICTS)


def save_topic_general():
    def save_general_contributor_count_more_the_k_edits_monthly(ddf):
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

    def save_general_edit_count_per_contributor_median_monthly(ddf):
        contributor_edits_monthly = ddf.groupby(["month_index", "user_index"])["edits"].sum().compute()
        edit_count_per_contributor_median_monthly = contributor_edits_monthly.groupby(["month_index"]).median()

        edit_count_per_contributor_median_monthly_since_2010 = edit_count_per_contributor_median_monthly[
            edit_count_per_contributor_median_monthly.index >= 57
        ]
        edit_count_per_contributor_median_monthly_since_2010.index -= 57

        util.save_y(
            progress_bar,
            "general_edit_count_per_contributor_median_monthly",
            MONTHS,
            edit_count_per_contributor_median_monthly,
        )
        util.save_y(
            progress_bar,
            "general_edit_count_per_contributor_median_monthly_since_2010",
            MONTHS[57:],
            edit_count_per_contributor_median_monthly_since_2010,
        )

    ddf = dd.read_parquet(os.path.join(PARQUET_DIR, "general_*.parquet"))
    created_by_tag_to_index = util.load_tag_to_index(DATA_DIR, "created_by")

    util.save_base_statistics(
        DATA_DIR,
        progress_bar,
        ddf,
        "general",
        edit_count_monthly=True,
        changeset_count_monthly=True,
        contributors_unique_yearly=True,
        contributor_count_monthly=True,
        new_contributor_count_monthly=True,
        edit_count_map_total=True,
    )
    save_general_contributor_count_more_the_k_edits_monthly(ddf)
    util.save_edit_count_map_yearly(YEARS, progress_bar, ddf, "general")
    save_general_edit_count_per_contributor_median_monthly(ddf)

    map_me_indices = np.array([created_by_tag_to_index["MAPS.ME android"], created_by_tag_to_index["MAPS.ME ios"]])
    util.save_base_statistics(
        DATA_DIR,
        progress_bar,
        ddf[~ddf["created_by"].isin(map_me_indices)],
        "general_no_maps_me",
        contributor_count_monthly=True,
    )

    util.save_accumulated("general_new_contributor_count_monthly")
    util.save_accumulated("general_edit_count_monthly")
    util.save_accumulated("general_changeset_count_monthly")


def save_topic_editing_software():
    def save_created_by_editor_type_stats(ddf):
        editor_type_lists = get_software_editor_type_lists()

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

    ddf = dd.read_parquet(os.path.join(PARQUET_DIR, "general_*.parquet"))
    created_by_tag_to_index = util.load_tag_to_index(DATA_DIR, "created_by")

    util.save_base_statistics_tag(
        DATA_DIR,
        progress_bar,
        ddf,
        "created_by",
        edit_count_monthly=True,
        changeset_count_monthly=True,
        contributors_unique_yearly=True,
        contributor_count_monthly=True,
        new_contributor_count_monthly=True,
    )

    util.save_top_k(10, "created_by_top_100_contributor_count_monthly")
    util.save_top_k(10, "created_by_top_100_new_contributor_count_monthly")
    util.save_top_k(10, "created_by_top_100_edit_count_monthly")
    util.save_top_k(10, "created_by_top_100_changeset_count_monthly")

    util.save_percent("created_by_top_10_contributor_count_monthly", "general_contributor_count_monthly")
    util.save_tag_top_10_contributor_count_first_changeset_monthly(
        MONTHS, progress_bar, ddf, "created_by", created_by_tag_to_index, "created_by_top_10_contributor_count_monthly"
    )
    util.save_monthly_to_yearly("created_by_top_100_edit_count_monthly")
    util.save_percent("created_by_top_10_edit_count_monthly", "general_edit_count_monthly")

    util.save_accumulated("created_by_top_10_new_contributor_count_monthly")
    util.save_accumulated("created_by_top_10_edit_count_monthly")
    util.save_accumulated("created_by_top_100_changeset_count_monthly")
    save_created_by_editor_type_stats(ddf)


def save_topic_corporation():
    ddf = dd.read_parquet(os.path.join(PARQUET_DIR, "general_*.parquet"))

    util.save_base_statistics_tag(
        DATA_DIR,
        progress_bar,
        ddf,
        "corporation",
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
    ddf_all_tags = dd.read_parquet(os.path.join(PARQUET_DIR, "all_tags_*.parquet"))
    all_tags_tag_to_index = util.load_tag_to_index(DATA_DIR, "all_tags")

    # for tag, tag_name_in_all_tags in [("source", "source"), ("imagery", "imagery_used"), ("hashtag", "hashtags")]:
    for tag, tag_name_in_all_tags in [("hashtag", "hashtag")]:
        util.save_base_statistics(
            DATA_DIR,
            progress_bar,
            ddf_all_tags[ddf_all_tags["all_tags"] == all_tags_tag_to_index[tag_name_in_all_tags]],
            tag,
            edit_count_monthly=True,
        )
        util.save_percent(f"{tag}_edit_count_monthly", "general_edit_count_monthly")

        ddf_tag = dd.read_parquet(os.path.join(PARQUET_DIR, f"{tag}_*.parquet"))
        edit_count_map_total = True if tag == "hashtag" else False
        util.save_base_statistics_tag(
            DATA_DIR,
            progress_bar,
            ddf_tag,
            tag,
            edit_count_monthly=True,
            changeset_count_monthly=True,
            contributors_unique_yearly=True,
            contributor_count_monthly=True,
            new_contributor_count_monthly=True,
            edit_count_map_total=edit_count_map_total,
        )
        util.save_top_k(10, f"{tag}_top_100_contributor_count_monthly")
        util.save_top_k(10, f"{tag}_top_100_new_contributor_count_monthly")
        util.save_top_k(10, f"{tag}_top_100_edit_count_monthly")
        util.save_top_k(10, f"{tag}_top_100_changeset_count_monthly")
        util.save_monthly_to_yearly(f"{tag}_top_100_contributor_count_monthly")
        util.save_monthly_to_yearly(f"{tag}_top_100_edit_count_monthly")
        util.save_accumulated(f"{tag}_top_10_new_contributor_count_monthly")
        util.save_accumulated(f"{tag}_top_10_edit_count_monthly")
        util.save_accumulated(f"{tag}_top_10_changeset_count_monthly")


def save_topic_streetcomplete():
    ddf = dd.read_parquet(os.path.join(PARQUET_DIR, "general_*.parquet"))
    ddf_all_tags = dd.read_parquet(os.path.join(PARQUET_DIR, "all_tags_*.parquet"))
    all_tags_tag_to_index = util.load_tag_to_index(DATA_DIR, "all_tags")

    util.save_base_statistics(
        DATA_DIR,
        progress_bar,
        ddf_all_tags[ddf_all_tags["all_tags"] == all_tags_tag_to_index["StreetComplete"]],
        "streetcomplete",
        edit_count_monthly=True,
        contributor_count_monthly=True,
        edit_count_map_total=True,
    )
    util.save_percent("streetcomplete_contributor_count_monthly", "general_contributor_count_monthly")
    util.save_percent("streetcomplete_edit_count_monthly", "general_edit_count_monthly")

    util.save_base_statistics_tag(
        DATA_DIR,
        progress_bar,
        ddf,
        "streetcomplete",
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
    ddf = dd.read_parquet(os.path.join(PARQUET_DIR, "general_*.parquet"))
    util.save_base_statistics(
        DATA_DIR,
        progress_bar,
        ddf[ddf["bot"] == True],
        "bot",
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
        DATA_DIR, progress_bar, ddf[ddf["bot"] == True], "created_by", k=100, prefix="bot", edit_count_monthly=True
    )
    util.save_monthly_to_yearly("bot_created_by_top_100_edit_count_monthly")


def save_topic_tags():
    ddf_all_tags = dd.read_parquet(os.path.join(PARQUET_DIR, "all_tags_*.parquet"))
    created_by_tag_to_index = util.load_tag_to_index(DATA_DIR, "created_by")

    util.save_base_statistics_tag(DATA_DIR, progress_bar, ddf_all_tags, "all_tags", k=100, changeset_count_monthly=True)
    util.save_monthly_to_yearly("all_tags_top_100_changeset_count_monthly")
    util.save_top_k(10, "all_tags_top_100_changeset_count_monthly")
    util.save_percent("all_tags_top_10_changeset_count_monthly", "general_changeset_count_monthly")

    selected_editors = ["JOSM", "iD", "Potlatch", "StreetComplete", "RapiD", "Vespucci"]
    monthly_data = util.load_json(os.path.join("assets", "data", f"created_by_top_100_changeset_count_monthly.json"))
    editor_name_to_changeset_count_monthly = {
        name: y for name, y in zip(monthly_data["y_names"], monthly_data["y_list"])
    }
    for selected_editor_name in selected_editors:
        editor_index = created_by_tag_to_index[selected_editor_name]
        util.save_base_statistics_tag(
            DATA_DIR,
            progress_bar,
            ddf_all_tags[ddf_all_tags["created_by"] == editor_index],
            f"all_tags",
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

    # save_topic_general()
    # save_topic_editing_software()
    # save_topic_corporation()
    save_topic_source_imagery_hashtag()
    # save_topic_streetcomplete()
    # save_topic_bot()
    # save_topic_tags()


if __name__ == "__main__":
    sys.exit(main())
