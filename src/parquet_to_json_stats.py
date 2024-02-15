import pickle
import sys
import warnings
from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd
import util
from tqdm import tqdm

warnings.filterwarnings("ignore", category=FutureWarning, module="dask.dataframe.io.parquet.core")
DATA_DIR = sys.argv[1]
MONTHS, YEARS = util.get_months_years(DATA_DIR)
# TIME_DICT = util.get_month_year_dicts(DATA_DIR)
# progress_bar = tqdm(total=91)


def save_topic_general():
    def save_general_contributor_count_more_the_k_edits_monthly():
        ddf = util.load_ddf(DATA_DIR, "general", ("month_index", "edits", "user_index"))
        total_edits_of_contributors = ddf.groupby(["user_index"], observed=False)["edits"].sum().compute()
        contributors_unique_monthly_set = (
            ddf.groupby(["month_index"], observed=False)["user_index"].unique().compute().apply(set)
        )

        min_edit_count = [10, 100, 1_000, 10_000, 100_000]
        contributor_count_more_the_k_edits_monthly = []
        for k in min_edit_count:
            contributors_with_more_than_k_edits = set(
                total_edits_of_contributors[total_edits_of_contributors > k].index.to_numpy(),
            )
            contributor_count_more_the_k_edits_monthly.append(
                contributors_unique_monthly_set.apply(
                    lambda x: len(x.intersection(contributors_with_more_than_k_edits)),
                ).rename(f"more then {k} edits"),
            )
        df = pd.concat(contributor_count_more_the_k_edits_monthly, axis=1)
        util.save_data(
            TIME_DICT,
            progress_bar,
            "general_contributor_count_more_the_k_edits_monthly",
            df,
            ("months", *df.columns.to_numpy()),
        )

    def save_general_edit_count_per_contributor_median_monthly():
        edit_count_per_contributor_median_monthly = []
        for i in range(len(MONTHS)):
            filters = [("month_index", "==", i)]
            ddf = util.load_ddf(DATA_DIR, "general", ("edits", "user_index"), filters)
            contributor_edits_month = ddf.groupby(["user_index"], observed=False)["edits"].sum().compute()
            edit_count_per_contributor_median_monthly.append(contributor_edits_month.median())
        df = pd.DataFrame({"median number of edits per contributor": edit_count_per_contributor_median_monthly})
        df.index.name = "month_index"

        util.save_data(
            TIME_DICT,
            progress_bar,
            "general_edit_count_per_contributor_median_monthly",
            df,
            ("months", "median number of edits per contributor"),
        )
        util.save_data(
            TIME_DICT,
            progress_bar,
            "general_edit_count_per_contributor_median_monthly_since_2010",
            df.iloc[57:],
            ("months", "median number of edits per contributor"),
        )

    def save_general_contributor_attrition_rate():
        ddf = util.load_ddf(DATA_DIR, "general", ("year_index", "edits", "user_index"))

        ddf_user_first_edit_year = ddf.groupby(["user_index"], observed=False)["year_index"].min().compute()
        ddf_user_first_edit_year.name = "first_edit_year_index"
        ddf_year_user_edits = (
            ddf.groupby(["year_index", "user_index"], observed=False)["edits"].sum().compute().reset_index("year_index")
        )
        merge = dd.merge(ddf_year_user_edits, ddf_user_first_edit_year, on="user_index")
        merge["first edit"] = merge["first_edit_year_index"].apply(
            lambda i: f"{YEARS[i-1]}-{YEARS[i]}" if i % 2 else f"{YEARS[i]}-{int(YEARS[i])+1}",
        )
        merge_edit_sum = merge.groupby(["year_index", "first edit"], observed=False)["edits"].sum().reset_index()
        df = pd.pivot_table(merge_edit_sum, values="edits", index="year_index", columns="first edit")

        util.save_data(
            TIME_DICT,
            progress_bar,
            "general_contributor_count_attrition_rate_yearly",
            df,
            ("years", *df.columns.to_numpy()),
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
        YEARS,
        progress_bar,
        "general",
        util.load_ddf(DATA_DIR, "general", ("year_index", "edits", "pos_x", "pos_y")),
    )

    save_general_edit_count_per_contributor_median_monthly()

    ddf = util.load_ddf(DATA_DIR, "general", ("month_index", "user_index", "created_by"))
    created_by_tag_to_index = util.load_tag_to_index(DATA_DIR, "created_by")
    map_me_indices = np.array([created_by_tag_to_index["MAPS.ME android"], created_by_tag_to_index["MAPS.ME ios"]])
    util.save_data(
        TIME_DICT,
        progress_bar,
        "general_no_maps_me_contributor_count_monthly",
        ddf[~ddf["created_by"].isin(map_me_indices)]
        .groupby(["month_index"], observed=False)["user_index"]
        .nunique()
        .compute()
        .rename("contributors without maps.me"),
        ("months", "contributors without maps.me"),
    )

    util.save_accumulated("general_new_contributor_count_monthly")
    util.save_accumulated("general_edit_count_monthly")
    util.save_accumulated("general_changeset_count_monthly")
    util.save_monthly_to_yearly("general_edit_count_monthly", only_full_years=True)
    util.save_monthly_to_yearly("general_edit_count_monthly")

    save_general_contributor_attrition_rate()
    util.save_percent("general_contributor_count_attrition_rate_yearly", "general_edit_count_yearly", "years", "edits")


def save_topic_editing_software():
    def get_software_editor_type_lists():
        created_by_tag_to_index = util.load_tag_to_index(DATA_DIR, "created_by")

        name_to_info = util.load_json(Path("src") / "replace_rules_created_by.json")
        device_type = {
            "desktop_editors": [],
            "mobile_editors": [],
            "tools": [],
        }
        name_not_in_tag_to_index_list = []
        for name, info in name_to_info.items():
            if name not in created_by_tag_to_index:
                name_not_in_tag_to_index_list.append(name)
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
        if len(name_not_in_tag_to_index_list) > 0:
            print(
                f"{name_not_in_tag_to_index_list} in 'replace_rules_created_by.json' but not in 'tag_to_index'"
                " (this is expected when working with a part of all changesets)",
            )

        device_type["desktop_editors"] = np.array(device_type["desktop_editors"], dtype=np.int64)
        device_type["mobile_editors"] = np.array(device_type["mobile_editors"], dtype=np.int64)
        device_type["tools"] = np.array(device_type["tools"], dtype=np.int64)
        device_type["other/unspecified"] = np.array(
            list(set(created_by_tag_to_index.values()) - set(np.concatenate(list(device_type.values())))),
        )
        return device_type

    def save_created_by_editor_type_stats():
        editor_type_lists = get_software_editor_type_lists()

        for tag, tag_name in [("user_index", "contributor"), ("edits", "edit")]:
            ddf = util.load_ddf(DATA_DIR, "general", ("month_index", tag, "created_by"))
            df = pd.concat(
                [
                    ddf[ddf["created_by"].isin(v)]
                    .groupby(["month_index"], observed=False)[tag]
                    .nunique()
                    .compute()
                    .rename(k)
                    for k, v in editor_type_lists.items()
                ],
                axis=1,
            )
            util.save_data(
                TIME_DICT,
                progress_bar,
                f"created_by_device_type_{tag_name}_count_monthly",
                df,
                ("months", *df.columns.to_numpy()),
            )

        ddf = util.load_ddf(DATA_DIR, "general", ("month_index", "edits", "created_by"))
        df = pd.concat(
            [
                ddf[ddf["created_by"].isin(v)]
                .groupby(["month_index"], observed=False)["edits"]
                .sum()
                .compute()
                .rename(k)
                for k, v in editor_type_lists.items()
            ],
            axis=1,
        )
        util.save_data(
            TIME_DICT,
            progress_bar,
            "created_by_device_type_edit_count_monthly",
            df,
            ("months", *df.columns.to_numpy()),
        )
        util.save_percent("created_by_device_type_edit_count_monthly", "general_edit_count_monthly", "months", "edits")

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
        TIME_DICT,
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
    util.save_merged_yearly_total_data(
        "created_by_top_100_contributor_count_yearly",
        "created_by_top_100_contributor_count_total",
    )
    util.save_merged_yearly_total_data("created_by_top_100_edit_count_yearly", "created_by_top_100_edit_count_total")
    util.save_percent("created_by_top_10_edit_count_monthly", "general_edit_count_monthly", "months", "edits")
    util.save_percent(
        "created_by_top_10_contributor_count_monthly",
        "general_contributor_count_monthly",
        "months",
        "contributors",
    )
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
        top_10_edit_count_map_total=True,
        top_50_edit_count_map_total=True,
    )
    util.save_sum_of_top_k("corporation_top_100_edit_count_monthly")
    util.save_percent(
        "corporation_top_100_edit_count_monthly_sum_top_k",
        "general_edit_count_monthly",
        "months",
        "edits",
    )
    util.save_top_k(10, "corporation_top_100_new_contributor_count_monthly")
    util.save_top_k(10, "corporation_top_100_edit_count_monthly")
    util.save_top_k(10, "corporation_top_100_changeset_count_monthly")
    util.save_monthly_to_yearly("corporation_top_100_edit_count_monthly")
    util.save_accumulated("corporation_top_10_new_contributor_count_monthly")
    util.save_accumulated("corporation_top_10_edit_count_monthly")
    util.save_accumulated("corporation_top_10_changeset_count_monthly")
    util.save_merged_yearly_total_data("corporation_top_100_edit_count_yearly", "corporation_top_100_edit_count_total")


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
        util.save_percent(f"{tag}_edit_count_monthly", "general_edit_count_monthly", "months", "edits")

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
            top_10_edit_count_map_total=(tag == "hashtag"),
        )
        util.save_top_k(10, f"{tag}_top_100_contributor_count_monthly")
        util.save_top_k(10, f"{tag}_top_100_new_contributor_count_monthly")
        util.save_top_k(10, f"{tag}_top_100_edit_count_monthly")
        util.save_top_k(10, f"{tag}_top_100_changeset_count_monthly")
        util.save_monthly_to_yearly(f"{tag}_top_100_edit_count_monthly")
        util.save_accumulated(f"{tag}_top_10_new_contributor_count_monthly")
        util.save_accumulated(f"{tag}_top_10_edit_count_monthly")
        util.save_accumulated(f"{tag}_top_10_changeset_count_monthly")
        util.save_merged_yearly_total_data(
            f"{tag}_top_100_contributor_count_yearly",
            f"{tag}_top_100_contributor_count_total",
        )
        util.save_merged_yearly_total_data(f"{tag}_top_100_edit_count_yearly", f"{tag}_top_100_edit_count_total")


def save_topic_streetcomplete():
    ddf_all_tags = util.load_ddf(
        DATA_DIR,
        "all_tags",
        ("month_index", "edits", "user_index", "pos_x", "pos_y", "all_tags"),
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
    util.save_percent(
        "streetcomplete_contributor_count_monthly",
        "general_contributor_count_monthly",
        "months",
        "contributors",
    )
    util.save_percent("streetcomplete_edit_count_monthly", "general_edit_count_monthly", "months", "edits")

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
    util.save_merged_yearly_total_data(
        "streetcomplete_top_300_edit_count_yearly",
        "streetcomplete_top_300_edit_count_total",
    )


def save_topic_bot():
    ddf = util.load_ddf(
        DATA_DIR,
        "general",
        ("month_index", "edits", "user_index", "pos_x", "pos_y", "bot", "created_by"),
    )
    util.save_base_statistics(
        DATA_DIR,
        progress_bar,
        "bot",
        ddf[ddf["bot"]],
        edit_count_monthly=True,
        changeset_count_monthly=True,
        contributor_count_monthly=True,
        new_contributor_count_monthly=True,
        edit_count_map_total=True,
    )
    util.save_percent("bot_edit_count_monthly", "general_edit_count_monthly", "months", "edits")
    util.save_accumulated("bot_new_contributor_count_monthly")
    util.save_accumulated("bot_edit_count_monthly")
    util.save_accumulated("bot_changeset_count_monthly")
    util.save_div(
        "bot_avg_edit_count_per_changeset_monthly",
        "bot_edit_count_monthly",
        "bot_changeset_count_monthly",
        "months",
        "changesets",
    )

    util.save_base_statistics_tag(
        DATA_DIR,
        progress_bar,
        "created_by",
        ddf[ddf["bot"]],
        k=100,
        prefix="bot",
        edit_count_monthly=True,
    )
    util.save_monthly_to_yearly("bot_created_by_top_100_edit_count_monthly")
    util.save_merged_yearly_total_data(
        "bot_created_by_top_100_edit_count_yearly",
        "bot_created_by_top_100_edit_count_total",
    )


def save_topic_tags():
    ddf_all_tags = util.load_ddf(DATA_DIR, "all_tags", ("month_index", "all_tags", "created_by"))
    util.save_base_statistics_tag(DATA_DIR, progress_bar, "all_tags", ddf_all_tags, k=100, changeset_count_monthly=True)
    util.save_monthly_to_yearly("all_tags_top_100_changeset_count_monthly")
    util.save_top_k(10, "all_tags_top_100_changeset_count_monthly")
    util.save_percent(
        "all_tags_top_10_changeset_count_monthly",
        "general_changeset_count_monthly",
        "months",
        "changesets",
    )
    util.save_merged_yearly_total_data(
        "all_tags_top_100_changeset_count_yearly",
        "all_tags_top_100_changeset_count_total",
    )

    selected_editors = ["JOSM", "iD", "Potlatch", "StreetComplete", "Rapid", "Vespucci"]
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
            "created_by_top_100_changeset_count_monthly",
            "months",
            selected_editor_name,
        )


def save_name_to_link():
    util.save_json(
        Path("assets") / "data" / "created_by_name_to_link.json",
        util.get_name_to_link("replace_rules_created_by.json"),
    )
    util.save_json(
        Path("assets") / "data" / "imagery_and_source_name_to_link.json",
        util.get_name_to_link("replace_rules_imagery_and_source.json"),
    )
    corporation_name_to_link = {
        name: link for name, (link, _) in util.load_json(Path("assets") / "corporation_contributors.json").items()
    }
    util.save_json(Path("assets") / "data" / "corporation_name_to_link.json", corporation_name_to_link)



        
def init_cache(cache_dir, empty_cache_at_start=True):
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    if empty_cache_at_start:
        for file_path in cache_dir.iterdir():
            file_path.unlink()
    return cache_dir


def clear_cache():
    for file_path in CACHE_DIR.iterdir():
        file_path.unlink()
    CACHE_DIR.rmdir()


def load_parquet_func(filename, columns):
    return lambda: util.load_ddf(DATA_DIR, filename, columns)


def execute_queries(queries):
    data_dir = sys.argv[1]
    months, years = util.get_months_years(data_dir)
    time_dict = util.get_month_year_dicts(data_dir)
    progress_bar = tqdm(total=91)
    for query in queries:
        print(query["question"], query["filename"])
        if "ddf" in query:
            ddf = query["ddf"]()
            data = query["query"](ddf)
        else:
            data = query["query"]()
        
        if ("data_type" not in query) or (query["data_type"] == "table"):
           util.save_data(time_dict, progress_bar, query["filename"], data)
        elif query["data_type"] == "map":
            util.save_map(progress_bar, query["filename"], data)
        elif query["data_type"] == "maps":
            util.save_maps(progress_bar, query["filename"], data[0], data[1])




def get_contributor_count_without_maps_me_query(ddf):
    created_by_tag_to_index = util.load_tag_to_index(DATA_DIR, "created_by")
    editor_indices = np.array([created_by_tag_to_index["MAPS.ME android"], created_by_tag_to_index["MAPS.ME ios"]])
    ddf_without_editors = ddf[~ddf["created_by"].isin(editor_indices)]
    return ddf_without_editors.groupby(["month_index"])["user_index"].nunique().rename("contributors without maps.me")


def get_general_contributor_count_more_the_k_edits_monthly(ddf):
    total_edits_of_contributors = ddf.groupby(["user_index"], observed=False)["edits"].sum().compute()
    contributors_unique_monthly_set = (
        ddf.groupby(["month_index"], observed=False)["user_index"].unique().compute().apply(set)
    )

    min_edit_count = [10, 100, 1_000, 10_000, 100_000]
    contributor_count_more_the_k_edits_monthly = []
    for k in min_edit_count:
        contributors_with_more_than_k_edits = set(
            total_edits_of_contributors[total_edits_of_contributors > k].index.to_numpy(),
        )
        contributor_count_more_the_k_edits_monthly.append(
            contributors_unique_monthly_set.apply(
                lambda x: len(x.intersection(contributors_with_more_than_k_edits)),
            ).rename(f"more then {k} edits"),
        )
    return pd.concat(contributor_count_more_the_k_edits_monthly, axis=1).reset_index()


def get_general_edit_count_per_contributor_median_monthly():
    edit_count_per_contributor_median_monthly = []
    for i in range(len(MONTHS)):
        filters = [("month_index", "==", i)]
        ddf = util.load_ddf(DATA_DIR, "general", ("edits", "user_index"), filters)
        contributor_edits_month = ddf.groupby(["user_index"], observed=False)["edits"].sum().compute()
        edit_count_per_contributor_median_monthly.append(contributor_edits_month.median())
    df = pd.DataFrame({"median number of edits per contributor": edit_count_per_contributor_median_monthly})
    df.index.name = "month_index"
    return df.reset_index()


def get_general_contributor_attrition_rate(ddf):
    ddf_user_first_edit_year = ddf.groupby(["user_index"], observed=False)["year_index"].min().compute()
    ddf_user_first_edit_year.name = "first_edit_year_index"
    ddf_year_user_edits = (
        ddf.groupby(["year_index", "user_index"], observed=False)["edits"].sum().compute().reset_index("year_index")
    )
    merge = dd.merge(ddf_year_user_edits, ddf_user_first_edit_year, on="user_index")
    merge["first edit"] = merge["first_edit_year_index"].apply(
        lambda i: f"{YEARS[i-1]}-{YEARS[i]}" if i % 2 else f"{YEARS[i]}-{int(YEARS[i])+1}",
    )
    merge_edit_sum = merge.groupby(["year_index", "first edit"], observed=False)["edits"].sum().reset_index()
    return pd.pivot_table(merge_edit_sum, values="edits", index="year_index", columns="first edit").reset_index()


def get_edit_count_map_yearly(ddf):
    yearly_map_edits = (
        ddf[ddf["pos_x"] >= 0].groupby(["year_index", "pos_x", "pos_y"], observed=False)["edits"].sum().compute()
    )
    year_maps = [
        yearly_map_edits[yearly_map_edits.index.get_level_values("year_index") == year_i].droplevel(0)
        for year_i in range(len(YEARS))
    ]
    year_map_names = [f"total edits {year}" for year in YEARS]
    return [year_maps, year_map_names]


def get_top_k_monthly_edit_count(ddf, tag, k, prefix=""):
    indices, names = get_cached_top_k(ddf, tag, "edit_count", k, prefix=prefix)
    df = (
        ddf[ddf[tag].isin(indices)]
        .groupby(["month_index", tag], observed=False)["edits"]
        .sum()
        .compute()
        .reset_index()
    )
    df = pd.pivot_table(df, values="edits", index="month_index", columns=tag)[indices]
    df.columns = names
    return df.reset_index()


def get_top_k_monthly_changeset_count(ddf, tag, k, prefix=""):
    indices, names = get_cached_top_k(ddf, tag, "changeset_count", k, prefix=prefix)
    df = (
        ddf[ddf[tag].isin(indices)]
        .groupby(["month_index", tag], observed=False)
        .size()
        .compute()
        .rename("changesets")
        .reset_index()
    )
    df = pd.pivot_table(df, values="changesets", index="month_index", columns=tag)[indices]
    df.columns = names
    return df.reset_index()


def get_cached_top_k_unique_monthly_list(ddf, contributor_count_ddf_filename, tag, k):
    pkl_path = CACHE_DIR / f"top_k_{tag}_unique_monthly_list.pkl"
    if pkl_path in list(CACHE_DIR.iterdir()):
        return pickle.load(pkl_path.open("rb"))

    indices, names = get_cached_top_k(ddf, tag, "contributor_count")
    unique_monthly_list = [[set() for _ in range(len(MONTHS))] for _ in range(k)]

    for month_i in range(len(MONTHS)):
        filters = [("month_index", "==", month_i)]
        temp_ddf = util.load_ddf(DATA_DIR, contributor_count_ddf_filename, ("user_index", tag), filters)
        contributor_unique_month = (
            temp_ddf[temp_ddf[tag].isin(indices)].groupby([tag], observed=False)["user_index"].unique().compute()
        )
        for i, tag_index in enumerate(indices):
            value = contributor_unique_month[contributor_unique_month.index == tag_index].to_numpy()
            if len(value) > 0:
                unique_monthly_list[i][month_i] = set(value[0].tolist())
    if cache_fn:
        pickle.dump((unique_monthly_list, names), pkl_path.open("wb"))
    return unique_monthly_list, names


def get_top_k_yearly_contributor_count(ddf, contributor_count_ddf_filename, tag, k):
    unique_monthly_list, names = get_cached_top_k_unique_monthly_list(ddf, contributor_count_ddf_filename, tag, k)

    year_index_to_month_indices = util.get_year_index_to_month_indices(DATA_DIR)
    contributor_count_yearly_list = []
    for month_list in unique_monthly_list:
        contributor_count_yearly = []
        for year_i, _ in enumerate(YEARS):
            contributor_count_yearly.append(
                len(set().union(*[month_list[month_i] for month_i in year_index_to_month_indices[year_i]])),
            )
        contributor_count_yearly_list.append(contributor_count_yearly)

    return pd.DataFrame(np.array(contributor_count_yearly_list).transpose(), columns=names).rename_axis(
        "year_index",
    ).reset_index()


def get_top_k_monthly_contributor_count(ddf, contributor_count_ddf_filename, tag, k):
    unique_monthly_list, names = get_cached_top_k_unique_monthly_list(ddf, contributor_count_ddf_filename, tag, k)
    data = np.array([[len(unique_set) for unique_set in month_list] for month_list in unique_monthly_list]).transpose()
    return pd.DataFrame(data, columns=names).rename_axis("month_index").reset_index()


def get_top_k_monthly_new_contributor_count(ddf, contributor_count_ddf_filename, tag, k):
    unique_monthly_list, names = get_cached_top_k_unique_monthly_list(ddf, contributor_count_ddf_filename, tag, k)
    data = np.array([util.cumsum_new_nunique_set_list(month_list) for month_list in unique_monthly_list]).transpose()
    data = data[:,:len(names)] # there might be empty entries if k is bigger then there are instances, remove them
    return pd.DataFrame(data, columns=names).rename_axis("month_index").reset_index()


def get_cached_contributors_unique_monthly(ddf, prefix):
    pkl_path = CACHE_DIR / f"{prefix}_contributors_unique_monthly.pkl"
    if pkl_path in list(CACHE_DIR.iterdir()):
        return pickle.load(pkl_path.open("rb"))

    data = ddf.groupby(["month_index"], observed=False)["user_index"].unique().rename("contributors").compute()
    pickle.dump(data, pkl_path.open("wb"))
    return data


def get_cached_total_changeset_df(ddf, tag, unit, prefix=""):
    pkl_path = CACHE_DIR / f"top_{prefix}{tag}_{unit}.pkl"
    if pkl_path in list(CACHE_DIR.iterdir()):
        return pickle.load(pkl_path.open("rb"))
    
    total_df = ddf.groupby(tag).size().compute().sort_values(ascending=False)
    pickle.dump(total_df, pkl_path.open("wb"))
    return total_df


def get_cached_top_k(ddf, tag, unit, k, max_k_cached=500, prefix=""):
    if k > max_k_cached:
        ValueError("K is bigger then the maximal cached value.")
    
    pkl_path = CACHE_DIR / f"top_{prefix}{tag}_{unit}.pkl"
    if pkl_path in list(CACHE_DIR.iterdir()):
        indices, names = pickle.load(pkl_path.open("rb"))
        return indices[:k], names[:k]
    
    index_to_tag = util.load_index_to_tag(DATA_DIR, tag)
    
    if tag in ["created_by", "corporation", "streetcomplete"]:
        highest_number = np.iinfo(ddf[tag].dtype).max
        ddf = ddf[ddf[tag] < highest_number]

    if unit == "edit_count":
        total = ddf.groupby(tag)["edits"].sum().compute().sort_values(ascending=False)
    elif unit == "changeset_count":
        total = get_cached_total_changeset_df(ddf, tag, unit, prefix=prefix)
    elif unit == "contributor_count":
        # this is for efficiency and should be lower then the contributor count of the top 500 from previous runs
        tag_to_min_amount_of_changeset = {
            "created_by": 30,
            "imagery": 500,
            "hashtag": 2500,
            "source": 250,
            "corporation": 0,
            "streetcomplete": 0,
        }
        total_changesets = get_cached_total_changeset_df(ddf, tag, unit)
        total = util.get_total_contributor_count_tag_optimized(
            ddf, tag, total_changesets, tag_to_min_amount_of_changeset[tag]
        )
    
    indices = total.index.to_numpy()[:max_k_cached]
    names = [index_to_tag[i] for i in indices]

    pickle.dump((indices, names), pkl_path.open("wb"))
    return indices[:k], names[:k]






def get_tag_top_10_contributor_count_first_changeset_monthly(ddf, tag):
    indices, names = get_cached_top_k(ddf, tag, "contributor_count", 10)

    contibutor_monthly = (
        ddf[ddf[tag].isin(indices)].groupby(["user_index"], observed=False)["month_index", tag].first().compute()
    )
    contibutor_count_monthly = (
        contibutor_monthly.reset_index().groupby(["month_index", tag], observed=False)["user_index"].count()
    )
    contibutor_count_monthly = contibutor_count_monthly.rename("contributors").reset_index()

    df = pd.pivot_table(contibutor_count_monthly, values="contributors", index="month_index", columns=tag)[indices]
    df.columns = names
    return df.reset_index()


def get_cached_software_editor_type_lists():
    pkl_path = CACHE_DIR / "software_editor_type_lists.pkl"
    if pkl_path in list(CACHE_DIR.iterdir()):
        return pickle.load(pkl_path.open("rb"))

    created_by_tag_to_index = util.load_tag_to_index(DATA_DIR, "created_by")

    name_to_info = util.load_json(Path("src") / "replace_rules_created_by.json")
    editor_type_lists = {
        "desktop_editors": [],
        "mobile_editors": [],
        "tools": [],
    }
    for name, info in name_to_info.items():
        if name not in created_by_tag_to_index:
            continue
        if "type" in info:
            if info["type"] == "desktop_editor":
                editor_type_lists["desktop_editors"].append(created_by_tag_to_index[name])
            elif info["type"] == "mobile_editor":
                editor_type_lists["mobile_editors"].append(created_by_tag_to_index[name])
            elif info["type"] == "tool":
                editor_type_lists["tools"].append(created_by_tag_to_index[name])
            else:
                print(f"unknown type: {name['type']} at name {name}")

    editor_type_lists["desktop_editors"] = np.array(editor_type_lists["desktop_editors"], dtype=np.int64)
    editor_type_lists["mobile_editors"] = np.array(editor_type_lists["mobile_editors"], dtype=np.int64)
    editor_type_lists["tools"] = np.array(editor_type_lists["tools"], dtype=np.int64)
    editor_type_lists["other/unspecified"] = np.array(
        list(set(created_by_tag_to_index.values()) - set(np.concatenate(list(editor_type_lists.values())))),
    )
    pickle.dump(editor_type_lists, pkl_path.open("wb"))
    return editor_type_lists


def get_created_by_device_type_edit_count_monthly(ddf):
    editor_type_lists = get_cached_software_editor_type_lists()
    return pd.concat(
        [
            ddf[ddf["created_by"].isin(v)]
            .groupby(["month_index"], observed=False)["edits"]
            .sum()
            .compute()
            .rename(k)
            for k, v in editor_type_lists.items()
        ],
        axis=1,
    ).reset_index()


def get_created_by_device_type_contributor_count_monthly(ddf):
    editor_type_lists = get_cached_software_editor_type_lists()
    return pd.concat(
        [
            ddf[ddf["created_by"].isin(v)]
            .groupby(["month_index"], observed=False)["user_index"]
            .nunique()
            .compute()
            .rename(k)
            for k, v in editor_type_lists.items()
        ],
        axis=1,
    ).reset_index()

    
def get_total_edit_count_map_top_k(ddf, tag, start_index, end_index):
    indices, names = get_cached_top_k(ddf, tag, "edit_count", end_index)
    indices = indices[start_index:]
    ddf_is_in = ddf[ddf[tag].isin(indices)]
    tag_map_edits = (
        ddf_is_in[ddf_is_in["pos_x"] >= 0].groupby(["pos_x", "pos_y", tag], observed=False)["edits"].sum().compute()
    )
    maps = [tag_map_edits[tag_map_edits.index.get_level_values(tag) == i].droplevel(2) for i in indices]
    return [maps, names[start_index:]]


def load_parqut_filtered_all_tags_func(tag_name, columns):
    def load_parqut_filtered_all_tags():
        all_tags_tag_to_index = util.load_tag_to_index(DATA_DIR, "all_tags")
        ddf_all_tags = util.load_ddf(DATA_DIR, "all_tags", [*columns, "all_tags"])
        return ddf_all_tags[ddf_all_tags["all_tags"] == all_tags_tag_to_index[tag_name]]
    return load_parqut_filtered_all_tags


def get_top_k_total():
    pass


CACHE_DIR = init_cache("temp_cache", empty_cache_at_start=True)

def main():

    queries = []

    # general
    queries.extend([
        # {
        #     "question": "What is the monthly edit count?",
        #     "filename": "general_edit_count_monthly",
        #     "ddf": load_parquet_func("general", ["month_index", "edits"]),
        #     "query": lambda ddf: ddf.groupby(["month_index"], observed=False)["edits"].sum().compute(),
        #     "data_type": "table",
        # },
        # {
        #     "question": "What is the monthly changeset count?",
        #     "filename": "general_changeset_count_monthly",
        #     "ddf": load_parquet_func("general", ["month_index"]),
        #     "query": lambda ddf: ddf.groupby(["month_index"], observed=False).size().rename("changesets").compute(),
        #     "data_type": "table",
        # },
        # {
        #     "question": "What is the monthly contributor count?",
        #     "filename": "general_contributor_count_monthly",
        #     "ddf": load_parquet_func("general", ["month_index", "user_index"]),
        #     "query": lambda ddf: get_cached_contributors_unique_monthly(ddf, "general").apply(len),
        #     "data_type": "table",
        # },
        # {
        #     "question": "What is the monthly new contributor count?",
        #     "filename": "general_new_contributor_count_monthly",
        #     "ddf": load_parquet_func("general", ["month_index", "user_index"]),
        #     "query": lambda ddf: util.cumsum_new_nunique(get_cached_contributors_unique_monthly(ddf, "general")),
        #     "data_type": "table",
        # },
        # {
        #     "question": "What is the monthly contributor count without without maps.me?",
        #     "filename": "general_no_maps_me_contributor_count_monthly",
        #     "ddf": load_parquet_func("general", ["month_index", "user_index", "created_by"]),
        #     "query": lambda ddf: get_contributor_count_without_maps_me_query(ddf).compute(),
        #     "data_type": "table",
        # },
        # {
        #     "question": "Where are edits made?",
        #     "filename": "general_edit_count_map_total",
        #     "ddf": load_parquet_func("general", ["edits", "pos_x", "pos_y"]),
        #     "query": lambda ddf: ddf[ddf["pos_x"] >= 0].groupby(["pos_x", "pos_y"], observed=False)["edits"].sum().compute(),
        #     "data_type": "map",
        # },
        # {
        #     "question": "Where are edits made each year?",
        #     "filename": "general_edit_count_maps_yearly",
        #     "ddf": load_parquet_func("general", ["year_index", "edits", "pos_x", "pos_y"]),
        #     "query": lambda ddf: get_edit_count_map_yearly(ddf),
        #     "data_type": "maps",
        # },
        {
            "question": "What is the monthly contributor count for contributors with more the k edits?",
            "filename": "general_contributor_count_more_the_k_edits_monthly",
            "ddf": load_parquet_func("general", ["month_index", "edits", "user_index"]),
            "query": lambda ddf: get_general_contributor_count_more_the_k_edits_monthly(ddf),
            "data_type": "table",
        },
        {
            "question": "What is the monthly median contributor count per contributors?",
            "filename": "general_edit_count_per_contributor_median_monthly",
            "query": get_general_edit_count_per_contributor_median_monthly,
            "data_type": "table",
        },
        # {
        #     "question": "What is the yearly contributor count attrition rate?",
        #     "filename": "general_contributor_count_attrition_rate_yearly",
        #     "ddf": load_parquet_func("general", ["year_index", "edits", "user_index"]),
        #     "query": lambda ddf: get_general_contributor_attrition_rate(ddf),
        #     "data_type": "table",
        # },
    ])

    # Editing Software
    queries.extend([
        # {
        #     "question": "What is the monthly edit count per editing software?",
        #     "filename": "created_by_top_100_edit_count_monthly",
        #     "ddf": load_parquet_func("general", ["month_index", "edits", "created_by"]),
        #     "query": lambda ddf: get_top_k_monthly_edit_count(ddf, "created_by", 100),
        # },
        # {
        #     "question": "What is the monthly changeset count per editing software?",
        #     "filename": "created_by_top_100_changeset_count_monthly",
        #     "ddf": load_parquet_func("general", ["month_index", "created_by"]),
        #     "query": lambda ddf: get_top_k_monthly_changeset_count(ddf, "created_by", 100),
        # },
        # {
        #     "question": "What is the yearly contributor count per editing software?",
        #     "filename": "created_by_top_100_contributor_count_yearly",
        #     "ddf": load_parquet_func("general", ["month_index", "user_index", "created_by"]),
        #     "query": lambda ddf: get_top_k_yearly_contributor_count(ddf, "general", "created_by", 100),
        # },
        # {
        #     "question": "What is the monthly contributor count per editing software?",
        #     "filename": "created_by_top_100_contributor_count_monthly",
        #     "ddf": load_parquet_func("general", ["month_index", "user_index", "created_by"]),
        #     "query": lambda ddf: get_top_k_monthly_contributor_count(ddf, "general", "created_by", 100),
        # },
        # {
        #     "question": "What is the monthly new contributor count per editing software?",
        #     "filename": "created_by_top_100_new_contributor_count_monthly",
        #     "ddf": load_parquet_func("general", ["month_index", "user_index", "created_by"]),
        #     "query": lambda ddf: get_top_k_monthly_new_contributor_count(ddf, "general", "created_by", 100),
        # },
        # {
        #     "question": "Which editing software is used for the first edit?",
        #     "filename": "created_by_top_10_contributor_count_first_changeset_monthly",
        #     "ddf": load_parquet_func("general", ["month_index", "user_index", "created_by"]),
        #     "query": lambda ddf: get_tag_top_10_contributor_count_first_changeset_monthly(ddf, "created_by"),
        # },
        # {
        #     "question": "What kind of devices are used for mapping (edit count)?",
        #     "filename": "created_by_device_type_edit_count_monthly",
        #     "ddf": load_parquet_func("general", ["month_index", "edits", "created_by"]),
        #     "query": lambda ddf: get_created_by_device_type_edit_count_monthly(ddf),
        # },
        # {
        #     "question": "What kind of devices are used for mapping (contributor count)?",
        #     "filename": "created_by_device_type_contributor_count_monthly",
        #     "ddf": load_parquet_func("general", ["month_index", "user_index", "created_by"]),
        #     "query": lambda ddf: get_created_by_device_type_contributor_count_monthly(ddf),
        # },
    ])
  
    # Corporation
    queries.extend([
        # {
        #     "question": "What is the monthly edit count per Corporation?",
        #     "filename": "corporation_top_100_edit_count_monthly",
        #     "ddf": load_parquet_func("general", ["month_index", "edits", "corporation"]),
        #     "query": lambda ddf: get_top_k_monthly_edit_count(ddf, "corporation", 100),
        # },
        # {
        #     "question": "What is the monthly changeset count per Corporation?",
        #     "filename": "corporation_top_100_changeset_count_monthly",
        #     "ddf": load_parquet_func("general", ["month_index", "corporation"]),
        #     "query": lambda ddf: get_top_k_monthly_changeset_count(ddf, "corporation", 100),
        # },
        # {
        #     "question": "What is the monthly new contributor count per Corporation?",
        #     "filename": "corporation_top_100_new_contributor_count_monthly",
        #     "ddf": load_parquet_func("general", ["month_index", "user_index", "corporation"]),
        #     "query": lambda ddf: get_top_k_monthly_new_contributor_count(ddf, "general", "corporation", 100),
        # },
        # {
        #     "question": "Where are the top 10 corporations contributing?",
        #     "filename": "corporation_top_10_edit_count_maps_total",
        #     "ddf": load_parquet_func("general", ["edits", "pos_x", "pos_y", "corporation"]),
        #     "query": lambda ddf: get_total_edit_count_map_top_k(ddf, "corporation", 0, 10),
        #     "data_type": "maps",
        # },
        # {
        #     "question": "Where are the top 10 to 20 corporations contributing?",
        #     "filename": "corporation_top_10_to_20_edit_count_maps_total",
        #     "ddf": load_parquet_func("general", ["edits", "pos_x", "pos_y", "corporation"]),
        #     "query": lambda ddf: get_total_edit_count_map_top_k(ddf, "corporation", 10, 20),
        #     "data_type": "maps",
        # },
        # {
        #     "question": "Where are the top 20 to 30 corporations contributing?",
        #     "filename": "corporation_top_20_to_30_edit_count_maps_total",
        #     "ddf": load_parquet_func("general", ["edits", "pos_x", "pos_y", "corporation"]),
        #     "query": lambda ddf: get_total_edit_count_map_top_k(ddf, "corporation", 20, 30),
        #     "data_type": "maps",
        # },
        # {
        #     "question": "Where are the top 30 to 40 corporations contributing?",
        #     "filename": "corporation_top_30_to_40_edit_count_maps_total",
        #     "ddf": load_parquet_func("general", ["edits", "pos_x", "pos_y", "corporation"]),
        #     "query": lambda ddf: get_total_edit_count_map_top_k(ddf, "corporation", 30, 40),
        #     "data_type": "maps",
        # },
    ])

    # # Source
    queries.extend([
        # {
        #     "question": "How often is the 'source' tag used?",
        #     "filename": "source_edit_count_monthly",
        #     "ddf": load_parqut_filtered_all_tags_func("source", ["month_index", "edits"]),
        #     "query": lambda ddf: ddf.groupby(["month_index"], observed=False)["edits"].sum().compute()
        # },
        # {
        #     "question": "What is the monthly edit count per source?",
        #     "filename": "source_top_100_edit_count_monthly",
        #     "ddf": load_parquet_func("source", ["month_index", "edits", "source"]),
        #     "query": lambda ddf: get_top_k_monthly_edit_count(ddf, "source", 100),
        # },
        # {
        #     "question": "What is the monthly changeset count per source?",
        #     "filename": "source_top_100_changeset_count_monthly",
        #     "ddf": load_parquet_func("source", ["month_index", "source"]),
        #     "query": lambda ddf: get_top_k_monthly_changeset_count(ddf, "source", 100),
        # },
        # {
        #     "question": "What is the yearly contributor count per source?",
        #     "filename": "source_top_100_contributor_count_yearly",
        #     "ddf": load_parquet_func("source", ["month_index", "user_index", "source"]),
        #     "query": lambda ddf: get_top_k_yearly_contributor_count(ddf, "source", "source", 100),
        # },
        # {
        #     "question": "What is the monthly contributor count per source?",
        #     "filename": "source_top_100_contributor_count_monthly",
        #     "ddf": load_parquet_func("source", ["month_index", "user_index", "source"]),
        #     "query": lambda ddf: get_top_k_monthly_contributor_count(ddf, "source", "source", 100),
        # },
        # {
        #     "question": "What is the monthly new contributor count per source?",
        #     "filename": "source_top_100_new_contributor_count_monthly",
        #     "ddf": load_parquet_func("source", ["month_index", "user_index", "source"]),
        #     "query": lambda ddf: get_top_k_monthly_new_contributor_count(ddf, "source", "source", 100),
        # },
    ])


    # # Imagery Service
    queries.extend([
        # {
        #     "question": "How popular are imagery services?",
        #     "filename": "imagery_edit_count_monthly",
        #     "ddf": load_parqut_filtered_all_tags_func("imagery_used", ["month_index", "edits"]),
        #     "query": lambda ddf: ddf.groupby(["month_index"], observed=False)["edits"].sum().compute()
        # },
        # {
        #     "question": "What is the monthly edit count per imagery service?",
        #     "filename": "imagery_top_100_edit_count_monthly",
        #     "ddf": load_parquet_func("imagery", ["month_index", "edits", "imagery"]),
        #     "query": lambda ddf: get_top_k_monthly_edit_count(ddf, "imagery", 100),
        # },
        # {
        #     "question": "What is the monthly changeset count per imagery service?",
        #     "filename": "imagery_top_100_changeset_count_monthly",
        #     "ddf": load_parquet_func("imagery", ["month_index", "imagery"]),
        #     "query": lambda ddf: get_top_k_monthly_changeset_count(ddf, "imagery", 100),
        # },
        # {
        #     "question": "What is the yearly contributor count per imagery service?",
        #     "filename": "imagery_top_100_contributor_count_yearly",
        #     "ddf": load_parquet_func("imagery", ["month_index", "user_index", "imagery"]),
        #     "query": lambda ddf: get_top_k_yearly_contributor_count(ddf, "imagery", "imagery", 100),
        # },
        # {
        #     "question": "What is the monthly contributor count per imagery service?",
        #     "filename": "imagery_top_100_contributor_count_monthly",
        #     "ddf": load_parquet_func("imagery", ["month_index", "user_index", "imagery"]),
        #     "query": lambda ddf: get_top_k_monthly_contributor_count(ddf, "imagery", "imagery", 100),
        # },
        # {
        #     "question": "What is the monthly new contributor count per imagery service?",
        #     "filename": "imagery_top_100_new_contributor_count_monthly",
        #     "ddf": load_parquet_func("imagery", ["month_index", "user_index", "imagery"]),
        #     "query": lambda ddf: get_top_k_monthly_new_contributor_count(ddf, "imagery", "imagery", 100),
        # },
    ])


    # # Hachtags
    queries.extend([
        # {
        #     "question": "How popular are hashtags?",
        #     "filename": "hashtag_edit_count_monthly",
        #     "ddf": load_parqut_filtered_all_tags_func("hashtags", ["month_index", "edits"]),
        #     "query": lambda ddf: ddf.groupby(["month_index"], observed=False)["edits"].sum().compute()
        # },
        # {
        #     "question": "What is the monthly edit count per hashtag?",
        #     "filename": "hashtag_top_100_edit_count_monthly",
        #     "ddf": load_parquet_func("hashtag", ["month_index", "edits", "hashtag"]),
        #     "query": lambda ddf: get_top_k_monthly_edit_count(ddf, "hashtag", 100),
        # },
        # {
        #     "question": "What is the monthly changeset count per hashtag?",
        #     "filename": "hashtag_top_100_changeset_count_monthly",
        #     "ddf": load_parquet_func("hashtag", ["month_index", "hashtag"]),
        #     "query": lambda ddf: get_top_k_monthly_changeset_count(ddf, "hashtag", 100),
        # },
        # {
        #     "question": "What is the yearly contributor count per hashtag?",
        #     "filename": "hashtag_top_100_contributor_count_yearly",
        #     "ddf": load_parquet_func("hashtag", ["month_index", "user_index", "hashtag"]),
        #     "query": lambda ddf: get_top_k_yearly_contributor_count(ddf, "hashtag", "hashtag", 100),
        # },
        # {
        #     "question": "What is the monthly contributor count per hashtag?",
        #     "filename": "hashtag_top_100_contributor_count_monthly",
        #     "ddf": load_parquet_func("hashtag", ["month_index", "user_index", "hashtag"]),
        #     "query": lambda ddf: get_top_k_monthly_contributor_count(ddf, "hashtag", "hashtag", 100),
        # },
        # {
        #     "question": "What is the monthly new contributor count per hashtag?",
        #     "filename": "hashtag_top_100_new_contributor_count_monthly",
        #     "ddf": load_parquet_func("hashtag", ["month_index", "user_index", "hashtag"]),
        #     "query": lambda ddf: get_top_k_monthly_new_contributor_count(ddf, "hashtag", "hashtag", 100),
        # },
        # {
        #     "question": "Where are the most edits of the top 10 hashtags?",
        #     "filename": "hashtag_top_10_edit_count_maps_total",
        #     "ddf": load_parquet_func("hashtag", ["edits", "pos_x", "pos_y", "hashtag"]),
        #     "query": lambda ddf: get_total_edit_count_map_top_k(ddf, "hashtag", 0, 10),
        #     "data_type": "maps",
        # },
    ])
    

    # # StreetComplete
    queries.extend([
        # {
        #     "question": "How many edits are made with StreetComplete per month?",
        #     "filename": "streetcomplete_edit_count_monthly",
        #     "ddf": load_parqut_filtered_all_tags_func("StreetComplete", ["month_index", "edits"]),
        #     "query": lambda ddf: ddf.groupby(["month_index"], observed=False)["edits"].sum().compute()
        # },
        # {
        #     "question": "How many contributors use StreetComplete per month?",
        #     "filename": "streetcomplete_contributor_count_monthly",
        #     "ddf": load_parqut_filtered_all_tags_func("StreetComplete", ["month_index", "user_index"]),
        #     "query": lambda ddf: get_cached_contributors_unique_monthly(ddf, "StreetComplete").apply(len),
        # },
        # {
        #     "question": "Where are edits made with StreetComplete?",
        #     "filename": "streetcomplete_edit_count_map_total",
        #     "ddf": load_parqut_filtered_all_tags_func("StreetComplete", ["edits", "pos_x", "pos_y"]),
        #     "query": lambda ddf: ddf[ddf["pos_x"] >= 0].groupby(["pos_x", "pos_y"], observed=False)["edits"].sum().compute(),
        #     "data_type": "map",
        # },
        # {
        #     "question": "What is the monthly edit count per StreetComplete quest?",
        #     "filename": "streetcomplete_top_300_edit_count_monthly",
        #     "ddf": load_parquet_func("general", ["month_index", "edits", "streetcomplete"]),
        #     "query": lambda ddf: get_top_k_monthly_edit_count(ddf, "streetcomplete", 300),
        # },
        # {
        #     "question": "What is the monthly changeset count per StreetComplete quest?",
        #     "filename": "streetcomplete_top_300_changeset_count_monthly",
        #     "ddf": load_parquet_func("general", ["month_index", "streetcomplete"]),
        #     "query": lambda ddf: get_top_k_monthly_changeset_count(ddf, "streetcomplete", 300),
        # },
        # {
        #     "question": "What is the monthly new contributor count per StreetComplete quest?",
        #     "filename": "streetcomplete_top_300_new_contributor_count_monthly",
        #     "ddf": load_parquet_func("general", ["month_index", "user_index", "streetcomplete"]),
        #     "query": lambda ddf: get_top_k_monthly_new_contributor_count(ddf, "general", "streetcomplete", 238),
        # },
    ])
    

    # # Bot
    queries.extend([
        # {
        #     "question": "How many edits are made with bots per month?",
        #     "filename": "bot_edit_count_monthly",
        #     "ddf": load_parqut_filtered_all_tags_func("bot", ["month_index", "edits"]),
        #     "query": lambda ddf: ddf.groupby(["month_index"], observed=False)["edits"].sum().compute()
        # },
        # {
        #     "question": "How many changeset are made with bots per month?",
        #     "filename": "bot_changeset_count_monthly",
        #     "ddf": load_parqut_filtered_all_tags_func("bot", ["month_index"]),
        #     "query": lambda ddf: ddf.groupby(["month_index"], observed=False).size().rename("changesets").compute(),
        # },
        # {
        #     "question": "How many contributors use bots per month?",
        #     "filename": "bot_contributor_count_monthly",
        #     "ddf": load_parqut_filtered_all_tags_func("bot", ["month_index", "user_index"]),
        #     "query": lambda ddf: get_cached_contributors_unique_monthly(ddf, "bot").apply(len),
        # },
        # {
        #     "question": "How many new contributors use bots per month?",
        #     "filename": "bot_new_contributor_count_monthly",
        #     "ddf": load_parqut_filtered_all_tags_func("bot", ["month_index", "user_index"]),
        #     "query": lambda ddf: util.cumsum_new_nunique(get_cached_contributors_unique_monthly(ddf, "bot")),
        # },
        # {
        #     "question": "Where are edits made with bots?",
        #     "filename": "bot_edit_count_map_total",
        #     "ddf": load_parqut_filtered_all_tags_func("bot", ["edits", "pos_x", "pos_y"]),
        #     "query": lambda ddf: ddf[ddf["pos_x"] >= 0].groupby(["pos_x", "pos_y"], observed=False)["edits"].sum().compute(),
        #     "data_type": "map",
        # },
        # {
        #     "question": "What is the monthly edit count of the top bots?",
        #     "filename": "bot_created_by_top_100_edit_count_monthly",
        #     "ddf": load_parquet_func("general", ["month_index", "edits", "created_by", "bot"]),
        #     "query": lambda ddf: get_top_k_monthly_edit_count(ddf[ddf["bot"]], "created_by", 100, prefix="bot_"),
        # },
    ])


    # # Tags
    created_by_tag_to_index = util.load_tag_to_index(DATA_DIR, "created_by")
    queries.extend([
        # {
        #     "question": "What is the monthly changeset count per tag?",
        #     "filename": "all_tags_top_100_changeset_count_monthly",
        #     "ddf": load_parquet_func("all_tags", ["month_index", "all_tags"]),
        #     "query": lambda ddf: get_top_k_monthly_changeset_count(ddf, "all_tags", 100),
        # },
        # {
        #     "question": "What is the monthly changeset count per tag using JOSM?",
        #     "filename": "created_by_JOSM_all_tags_top_10_changeset_count_monthly",
        #     "ddf": load_parquet_func("all_tags", ["month_index", "created_by", "all_tags"]),
        #     "query": lambda ddf: get_top_k_monthly_changeset_count(ddf[ddf["created_by"] == created_by_tag_to_index["JOSM"]], "all_tags", 10, prefix="created_by_JOSM"),
        # },
        # {
        #     "question": "What is the monthly changeset count per tag using iD?",
        #     "filename": "created_by_iD_all_tags_top_10_changeset_count_monthly",
        #     "ddf": load_parquet_func("all_tags", ["month_index", "created_by", "all_tags"]),
        #     "query": lambda ddf: get_top_k_monthly_changeset_count(ddf[ddf["created_by"] == created_by_tag_to_index["iD"]], "all_tags", 10, prefix="created_by_iD"),
        # },
        # {
        #     "question": "What is the monthly changeset count per tag using Potlatch?",
        #     "filename": "created_by_Potlatch_all_tags_top_10_changeset_count_monthly",
        #     "ddf": load_parquet_func("all_tags", ["month_index", "created_by", "all_tags"]),
        #     "query": lambda ddf: get_top_k_monthly_changeset_count(ddf[ddf["created_by"] == created_by_tag_to_index["Potlatch"]], "all_tags", 10, prefix="created_by_Potlatch"),
        # },
        # {
        #     "question": "What is the monthly changeset count per tag using StreetComplete?",
        #     "filename": "created_by_StreetComplete_all_tags_top_10_changeset_count_monthly",
        #     "ddf": load_parquet_func("all_tags", ["month_index", "created_by", "all_tags"]),
        #     "query": lambda ddf: get_top_k_monthly_changeset_count(ddf[ddf["created_by"] == created_by_tag_to_index["StreetComplete"]], "all_tags", 10, prefix="created_by_StreetComplete"),
        # },
        # {
        #     "question": "What is the monthly changeset count per tag using Rapid?",
        #     "filename": "created_by_Rapid_all_tags_top_10_changeset_count_monthly",
        #     "ddf": load_parquet_func("all_tags", ["month_index", "created_by", "all_tags"]),
        #     "query": lambda ddf: get_top_k_monthly_changeset_count(ddf[ddf["created_by"] == created_by_tag_to_index["Rapid"]], "all_tags", 10, prefix="created_by_Rapid"),
        # },
        # {
        #     "question": "What is the monthly changeset count per tag using Vespucci?",
        #     "filename": "created_by_Vespucci_all_tags_top_10_changeset_count_monthly",
        #     "ddf": load_parquet_func("all_tags", ["month_index", "created_by", "all_tags"]),
        #     "query": lambda ddf: get_top_k_monthly_changeset_count(ddf[ddf["created_by"] == created_by_tag_to_index["Vespucci"]], "all_tags", 10, prefix="created_by_Vespucci"),
        # },
    ])

    

    save_name_to_link()
    execute_queries(queries)
    clear_cache()

    # TODO: make cache explicit for all cached functions. Only use it if there is a cache_fn. Evtl nicht mehr relevant

    

    # # General
    # util.save_accumulated("general_new_contributor_count_monthly")
    # util.save_accumulated("general_edit_count_monthly")
    # util.save_accumulated("general_changeset_count_monthly")
    # util.save_monthly_to_yearly("general_edit_count_monthly", only_full_years=True)
    # util.save_monthly_to_yearly("general_edit_count_monthly")
    # util.save_percent("general_contributor_count_attrition_rate_yearly", "general_edit_count_yearly", "years", "edits")
    # util.save_different_timespan("general_edit_count_per_contributor_median_monthly", MONTHS, "2010-01")


    # # Editing Software
    # util.save_top_k(10, "created_by_top_100_contributor_count_monthly")
    # util.save_top_k(10, "created_by_top_100_new_contributor_count_monthly")
    # util.save_top_k(10, "created_by_top_100_edit_count_monthly")
    # util.save_top_k(10, "created_by_top_100_changeset_count_monthly")
    # util.save_monthly_to_yearly("created_by_top_100_edit_count_monthly")


    # TODO:
    # df = pd.DataFrame([total.loc[indices].to_numpy()], columns=names)
    #     save_data(
    #         time_dict,
    #         progress_bar,
    #         f"{prefix}{tag}_top_{k}_{unit}_total",
    #         df,
    #         df.columns,
    #     )
    


    # util.save_merged_yearly_total_data(
    #     "created_by_top_100_contributor_count_yearly",
    #     "created_by_top_100_contributor_count_total",
    # )
    # util.save_merged_yearly_total_data("created_by_top_100_edit_count_yearly", "created_by_top_100_edit_count_total")
    # util.save_percent("created_by_top_10_edit_count_monthly", "general_edit_count_monthly", "months", "edits")
    # util.save_percent(
    #     "created_by_top_10_contributor_count_monthly",
    #     "general_contributor_count_monthly",
    #     "months",
    #     "contributors",
    # )
    # util.save_accumulated("created_by_top_10_new_contributor_count_monthly")
    # util.save_accumulated("created_by_top_10_edit_count_monthly")
    # util.save_accumulated("created_by_top_10_changeset_count_monthly")
    # util.save_percent("created_by_device_type_edit_count_monthly", "general_edit_count_monthly", "months", "edits")


    # # Corporation
    # util.save_sum_of_top_k("corporation_top_100_edit_count_monthly")
    # util.save_percent(
    #     "corporation_top_100_edit_count_monthly_sum_top_k",
    #     "general_edit_count_monthly",
    #     "months",
    #     "edits",
    # )
    # util.save_top_k(10, "corporation_top_100_new_contributor_count_monthly")
    # util.save_top_k(10, "corporation_top_100_edit_count_monthly")
    # util.save_top_k(10, "corporation_top_100_changeset_count_monthly")
    # util.save_monthly_to_yearly("corporation_top_100_edit_count_monthly")
    # util.save_accumulated("corporation_top_10_new_contributor_count_monthly")
    # util.save_accumulated("corporation_top_10_edit_count_monthly")
    # util.save_accumulated("corporation_top_10_changeset_count_monthly")
    # util.save_merged_yearly_total_data("corporation_top_100_edit_count_yearly", "corporation_top_100_edit_count_total")


    # # Source, Imagery, Hashtag
    # for tag in ("source", "imagery", "hashtag"):
    #     util.save_percent(f"{tag}_edit_count_monthly", "general_edit_count_monthly", "months", "edits")
    #     util.save_top_k(10, f"{tag}_top_100_contributor_count_monthly")
    #     util.save_top_k(10, f"{tag}_top_100_new_contributor_count_monthly")
    #     util.save_top_k(10, f"{tag}_top_100_edit_count_monthly")
    #     util.save_top_k(10, f"{tag}_top_100_changeset_count_monthly")
    #     util.save_monthly_to_yearly(f"{tag}_top_100_edit_count_monthly")
    #     util.save_accumulated(f"{tag}_top_10_new_contributor_count_monthly")
    #     util.save_accumulated(f"{tag}_top_10_edit_count_monthly")
    #     util.save_accumulated(f"{tag}_top_10_changeset_count_monthly")
    #     util.save_merged_yearly_total_data(
    #         f"{tag}_top_100_contributor_count_yearly",
    #         f"{tag}_top_100_contributor_count_total",
    #     )
    #     util.save_merged_yearly_total_data(f"{tag}_top_100_edit_count_yearly", f"{tag}_top_100_edit_count_total")


    # # StreetComplete
    # util.save_percent(
    #     "streetcomplete_contributor_count_monthly",
    #     "general_contributor_count_monthly",
    #     "months",
    #     "contributors",
    # )
    # util.save_percent("streetcomplete_edit_count_monthly", "general_edit_count_monthly", "months", "edits")
    # util.save_monthly_to_yearly("streetcomplete_top_300_edit_count_monthly")
    # util.save_top_k(10, "streetcomplete_top_300_new_contributor_count_monthly")
    # util.save_top_k(10, "streetcomplete_top_300_edit_count_monthly")
    # util.save_top_k(10, "streetcomplete_top_300_changeset_count_monthly")
    # util.save_accumulated("streetcomplete_top_10_new_contributor_count_monthly")
    # util.save_accumulated("streetcomplete_top_10_edit_count_monthly")
    # util.save_accumulated("streetcomplete_top_10_changeset_count_monthly")
    # util.save_merged_yearly_total_data(
    #     "streetcomplete_top_300_edit_count_yearly",
    #     "streetcomplete_top_300_edit_count_total",
    # )

    # # Bot
    # util.save_percent("bot_edit_count_monthly", "general_edit_count_monthly", "months", "edits")
    # util.save_accumulated("bot_new_contributor_count_monthly")
    # util.save_accumulated("bot_edit_count_monthly")
    # util.save_accumulated("bot_changeset_count_monthly")
    # util.save_div(
    #     "bot_avg_edit_count_per_changeset_monthly",
    #     "bot_edit_count_monthly",
    #     "bot_changeset_count_monthly",
    #     "months",
    #     "changesets",
    # )
    # util.save_monthly_to_yearly("bot_created_by_top_100_edit_count_monthly")
    # util.save_merged_yearly_total_data(
    #     "bot_created_by_top_100_edit_count_yearly",
    #     "bot_created_by_top_100_edit_count_total",
    # )


    # # Tags
    # util.save_monthly_to_yearly("all_tags_top_100_changeset_count_monthly")
    # util.save_top_k(10, "all_tags_top_100_changeset_count_monthly")
    # util.save_percent(
    #     "all_tags_top_10_changeset_count_monthly",
    #     "general_changeset_count_monthly",
    #     "months",
    #     "changesets",
    # )
    # util.save_merged_yearly_total_data(
    #     "all_tags_top_100_changeset_count_yearly",
    #     "all_tags_top_100_changeset_count_total",
    # )
    # for selected_editor in selected_editors:
    #     util.save_percent(
    #         f"created_by_{selected_editor}_all_tags_top_10_changeset_count_monthly",
    #         "created_by_top_100_changeset_count_monthly",
    #         "months",
    #         selected_editor,
    #     )


if __name__ == "__main__":
    sys.exit(main())
