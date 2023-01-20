import sys
import numpy as np
import util

# init
DATA_DIR = sys.argv[1]
TOP_K_TABLE = 100
TOP_K_PLOT = 10
months, years = util.get_months_years(DATA_DIR)
year_to_year_index = util.list_to_dict(years)
month_index_to_year_index = {month_i: year_to_year_index[month[:4]] for month_i, month in enumerate(months)}

top_ids = util.load_top_k_list(DATA_DIR, "created_by")
ch_id_to_rank = util.list_to_dict(top_ids["changesets"])
ed_id_to_rank = util.list_to_dict(top_ids["edits"])
co_id_to_rank = util.list_to_dict(top_ids["contributors"])

index_to_tag = util.load_index_to_tag(DATA_DIR, "created_by")
ch_rank_to_name = [index_to_tag[contributor_id] for contributor_id in top_ids["changesets"]]
ed_rank_to_name = [index_to_tag[edit_id] for edit_id in top_ids["edits"]]
co_rank_to_name = [index_to_tag[contributor_id] for contributor_id in top_ids["contributors"]]

names_in_top_k_plots = []
names_in_top_k_plots.extend([n for n in ch_rank_to_name[:TOP_K_PLOT] if n not in names_in_top_k_plots])
names_in_top_k_plots.extend([n for n in ed_rank_to_name[:TOP_K_PLOT] if n not in names_in_top_k_plots])
names_in_top_k_plots.extend([n for n in co_rank_to_name[:TOP_K_PLOT] if n not in names_in_top_k_plots])
ch_rank_to_color_top_k_plot = [
    util.DEFAULT_COLOR_PALETTE[names_in_top_k_plots.index(n)] for n in ch_rank_to_name[:TOP_K_PLOT]
]
ed_rank_to_color_top_k_plot = [
    util.DEFAULT_COLOR_PALETTE[names_in_top_k_plots.index(n)] for n in ed_rank_to_name[:TOP_K_PLOT]
]
co_rank_to_color_top_k_plot = [
    util.DEFAULT_COLOR_PALETTE[names_in_top_k_plots.index(n)] for n in co_rank_to_name[:TOP_K_PLOT]
]

tag_to_index = util.list_to_dict(index_to_tag)
desktop_editor_names = [
    "ArcGIS Editor for OpenStreetMap",
    "Deriviste",
    "gnome-maps",
    "iD",
    "iD-indoor",
    "JOSM",
    "Level0",
    "Map builder",
    "MapComplete",
    "MapContrib",
    "MapRoulette",
    "Merkaartor",
    "https://openaedmap.org",
    "OpenRecycleMap",
    "OsmHydrant",
    "OsmInEdit",
    "Osmose Editor",
    "Osmose Raw Editor",
    "Pic4Review",
    "Potlatch",
    "QGIS OSM",
    "RapiD",
    "RawEdit",
    "rosemary",  # most of it is wheelmap.org
    "wheelmap.org",
]
desktop_editors = [tag_to_index[tag] for tag in desktop_editor_names]

mobile_editor_names = [
    "Every Door Android",
    "Every Door iOS",
    "Go Map!!",
    "iLOE",
    "MAPS.ME android",
    "MAPS.ME ios",
    "Mapzen Beta",
    "Mapzen POI Collector",
    "Mapzen POI Collector for Android",
    "OMaps ios",
    "Organic Maps android",
    "Organic Maps ios",
    "OpenMaps iPhone",
    "OpenMaps for iOS",
    "OsmAnd",
    "OsmAnd+",
    "OsmAnd~",
    "OsmAnd Maps",
    "OSMapTuner",
    "OSMPOIEditor",
    "OSM Contributor",
    "Osm Go!",
    "POI+",
    "Pushpin iOS",
    "StreetComplete",
    "StreetComplete_ee",
    "Vespucci",
]
mobile_editors = [tag_to_index[tag] for tag in mobile_editor_names]

tool_names = [
    "AND node cleaner",
    "addr2osm",
    "autoAWS",
    "bash script",
    "bot-source-cadastre.py",
    "bulk_upload.py",
    "bulk_upload.py/Apr Python",
    "bulk_upload.py/error Python",
    "bulk_upload.py/khalilst Python/",
    "bulk_upload.py/posiki",
    "bulk_upload_sax.py",
    "bulkyosm.py",
    "custom upload script written in ruby",
    "FindvejBot",
    "FixDoubleNodes",
    "FixKarlsruheSchema",
    "https_all_the_things",
    "https://git.nzoss.org.nz/ewblen/osmlinzaddr/blob/master/osmlinzaddr.py",
    "Jeff's Uploader",
    "LangToolsOSM",
    "LINZ Address Import",
    "LINZ Data Import",
    "mat's little ruby script",
    "MyUploader",
    "naptan2osm",
    "osmapi",
    "osmapis",
    "Osmaxil",
    "OsmSharp",
    "osmtools",
    "osm-bulk-upload/upload.py",
    "OsmPipeline",
    "osmupload.py",
    "posiki_python_script",
    "PythonOsmApi",
    "Redaction bot",
    "RevertUI",
    "reverter_plugin",  # might be a plugin for JOSM, it's still a tool though
    "reverter;JOSM",
    "Roy",
    "simple_revert.py",
    "SviMik",
    "upload.py",
    "./upload.py",
]
tools = [tag_to_index[tag] for tag in tool_names]

device_type_labels = ["desktop editor", "mobile editor", "tools", "other/unspecified"]

mo_ch = np.zeros((TOP_K_TABLE, len(months)), dtype=np.int64)
mo_ed = np.zeros((TOP_K_TABLE, len(months)), dtype=np.int64)
mo_ed_all = np.zeros((len(months)), dtype=np.int64)
mo_ed_device_type = np.zeros((4, len(months)), dtype=np.int64)
mo_co_set = [[set() for _ in range(len(months))] for _ in range(TOP_K_TABLE)]
mo_co_set_all = [set() for _ in range(len(months))]
mo_co_device_type_set = [[set() for _ in range(len(months))] for _ in range(4)]
co_first_edit_per_editing_software = [dict() for _ in range(TOP_K_PLOT)]
co_first_editing_software = dict()

# accumulate data
for line in sys.stdin:
    data = line[:-1].split(",")
    edits = int(data[0])
    month_index = int(data[1])
    user_id = int(data[2])
    x, y = data[4], data[5]

    mo_ed_all[month_index] += edits
    mo_co_set_all[month_index].add(user_id)

    if len(data[7]) == 0:
        continue
    created_by_id = int(data[7])

    if user_id not in co_first_editing_software:
        co_first_editing_software[user_id] = (month_index, created_by_id)
    else:
        if month_index < co_first_editing_software[user_id][0]:
            co_first_editing_software[user_id] = (month_index, created_by_id)

    if created_by_id in ch_id_to_rank:
        rank = ch_id_to_rank[created_by_id]
        mo_ch[rank, month_index] += 1

    if created_by_id in ed_id_to_rank:
        rank = ed_id_to_rank[created_by_id]
        mo_ed[rank, month_index] += edits

    if created_by_id in co_id_to_rank:
        rank = co_id_to_rank[created_by_id]
        mo_co_set[rank][month_index].add(user_id)

        if rank < TOP_K_PLOT:
            if user_id not in co_first_edit_per_editing_software[rank]:
                co_first_edit_per_editing_software[rank][user_id] = month_index
            else:
                if month_index < co_first_edit_per_editing_software[rank][user_id]:
                    co_first_edit_per_editing_software[rank][user_id] = month_index

    if created_by_id in desktop_editors:
        mo_ed_device_type[0, month_index] += edits
        mo_co_device_type_set[0][month_index].add(user_id)
    elif created_by_id in mobile_editors:
        mo_ed_device_type[1, month_index] += edits
        mo_co_device_type_set[1][month_index].add(user_id)
    elif created_by_id in tools:
        mo_ed_device_type[2, month_index] += edits
        mo_co_device_type_set[2][month_index].add(user_id)
    else:
        mo_ed_device_type[3, month_index] += edits
        mo_co_device_type_set[3][month_index].add(user_id)


mo_co_first_editing_software = np.zeros((TOP_K_PLOT, len(months)), dtype=np.int32)
for month_index, created_by_id in co_first_editing_software.values():
    if created_by_id in co_id_to_rank:
        rank = co_id_to_rank[created_by_id]
        if rank < TOP_K_PLOT:
            mo_co_first_editing_software[rank][month_index] += 1

mo_new_co_per_editing_software = np.zeros((TOP_K_PLOT, len(months)), dtype=np.int32)
for rank in range(TOP_K_PLOT):
    month_index, first_edit_count = np.unique(
        list(co_first_edit_per_editing_software[rank].values()), return_counts=True
    )
    mo_new_co_per_editing_software[rank][month_index] = first_edit_count

mo_co = util.set_to_length(mo_co_set)


# save plots
TOPIC = "Editing Software"
with util.add_questions(TOPIC) as add_question:

    add_question(
        "How many people are contributing per editing software each month?",
        "c229",
        util.get_multi_line_plot(
            "monthly contributor count per editing software",
            "contributors",
            months,
            mo_co[:TOP_K_PLOT],
            co_rank_to_name[:TOP_K_PLOT],
        ),
        util.get_multi_line_plot(
            "monthly new contributor count per editing software",
            "contributors",
            months,
            mo_new_co_per_editing_software,
            co_rank_to_name[:TOP_K_PLOT],
        ),
        util.get_table(
            "yearly contributor count per editing software",
            years,
            util.monthly_set_to_yearly_with_total(mo_co_set, years, month_index_to_year_index),
            TOPIC,
            co_rank_to_name,
        ),
    )

    add_question(
        "How popular is each editing software per month?",
        "158c",
        util.get_multi_line_plot(
            "percent of contributors that use each editing software per month",
            "%",
            months,
            util.get_percent(mo_co[:TOP_K_PLOT], util.set_to_length(mo_co_set_all)),
            co_rank_to_name[:TOP_K_PLOT],
            percent=True,
        ),
    )

    add_question(
        "Which editing software is used for the first edit?",
        "7662",
        util.get_multi_line_plot(
            "monthly first editing software contributor count",
            "contributors",
            months,
            mo_co_first_editing_software,
            co_rank_to_name[:TOP_K_PLOT],
        ),
    )

    add_question(
        "How many edits are added per editing software each month?",
        "eb30",
        util.get_multi_line_plot(
            "monthly edits count per editing software",
            "edits",
            months,
            mo_ed[:TOP_K_PLOT],
            ed_rank_to_name[:TOP_K_PLOT],
        ),
        util.get_table(
            "yearly edits count per editing software",
            years,
            util.monthly_to_yearly_with_total(mo_ed, years, month_index_to_year_index),
            TOPIC,
            ed_rank_to_name,
        ),
    )

    add_question(
        "What's the market share of edits per month?",
        "a008",
        util.get_multi_line_plot(
            "market share of edits per month",
            "%",
            months,
            util.get_percent(mo_ed, mo_ed_all)[:TOP_K_PLOT],
            ed_rank_to_name[:TOP_K_PLOT],
            percent=True,
            on_top_of_each_other=True,
        ),
    )

    add_question(
        "What's the total amount of contributors, edits and changesets of editing software over time?",
        "6320",
        util.get_text_element(
            f"There are {len(index_to_tag):,} different editing software names for the 'created_by' tag."
            " That's quite a big number. However, most names are user or organization names, from people"
            " misunderstanding the tag.",
        ),
        util.get_multi_line_plot(
            "total contributor count of editing software",
            "contributors",
            months,
            util.set_cumsum(mo_co_set),
            co_rank_to_name[:TOP_K_PLOT],
            colors=co_rank_to_color_top_k_plot,
        ),
        util.get_multi_line_plot(
            "total edit count of editing software",
            "edits",
            months,
            util.cumsum(mo_ed),
            ed_rank_to_name[:TOP_K_PLOT],
            colors=ed_rank_to_color_top_k_plot,
        ),
        util.get_multi_line_plot(
            "total changeset count of editing software",
            "changesets",
            months,
            util.cumsum(mo_ch),
            ch_rank_to_name[:TOP_K_PLOT],
            colors=ch_rank_to_color_top_k_plot,
        ),
    )

    add_question(
        "What kind of devices are used for mapping?",
        "8ba9",
        util.get_multi_line_plot(
            "monthly contributor count per device",
            "contributors",
            months,
            util.set_to_length(mo_co_device_type_set),
            device_type_labels,
        ),
        util.get_multi_line_plot(
            "monthly edit count per device", "edits", months, mo_ed_device_type, device_type_labels
        ),
        util.get_multi_line_plot(
            "market share of edit per device",
            "%",
            months,
            util.get_percent(mo_ed_device_type, mo_ed_all),
            device_type_labels,
            percent=True,
        ),
    )
