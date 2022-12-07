import sys
import numpy as np
import util

# init
data_dir = sys.argv[1]
top_k_table = 100
top_k_plot = 10
months, years = util.get_months_years(data_dir)
year_to_year_index = util.list_to_dict(years)
month_index_to_year_index = {month_i: year_to_year_index[month[:4]] for month_i, month in enumerate(months)}

top_ids = util.load_top_k_list(data_dir, "created_by")
ch_id_to_rank = util.list_to_dict(top_ids["changesets"])
ed_id_to_rank = util.list_to_dict(top_ids["edits"])
co_id_to_rank = util.list_to_dict(top_ids["contributors"])

index_to_tag = util.load_index_to_tag(data_dir, "created_by")
ch_rank_to_name = [index_to_tag[contributor_id] for contributor_id in top_ids["changesets"]]
ed_rank_to_name = [index_to_tag[edit_id] for edit_id in top_ids["edits"]]
co_rank_to_name = [index_to_tag[contributor_id] for contributor_id in top_ids["contributors"]]

names_in_top_k_plots = []
names_in_top_k_plots.extend([n for n in ch_rank_to_name[:top_k_plot] if n not in names_in_top_k_plots])
names_in_top_k_plots.extend([n for n in ed_rank_to_name[:top_k_plot] if n not in names_in_top_k_plots])
names_in_top_k_plots.extend([n for n in co_rank_to_name[:top_k_plot] if n not in names_in_top_k_plots])
#names_in_top_k_plots = list(set([*ch_rank_to_name[:top_k_plot], *ed_rank_to_name[:top_k_plot], *co_rank_to_name[:top_k_plot]]))
ch_rank_to_color_top_k_plot = [util.colors[names_in_top_k_plots.index(n)] for n in ch_rank_to_name[:top_k_plot]]
ed_rank_to_color_top_k_plot = [util.colors[names_in_top_k_plots.index(n)] for n in ed_rank_to_name[:top_k_plot]]
co_rank_to_color_top_k_plot = [util.colors[names_in_top_k_plots.index(n)] for n in co_rank_to_name[:top_k_plot]]

tag_to_index = util.list_to_dict(index_to_tag)
desktop_editors = [tag_to_index[tag] for tag in ["JOSM", "iD", "Potlatch", "Merkaartor", "RapiD", "OsmHydrant", "gnome-maps", "reverter_plugin", "reverter;JOSM", "ArcGIS Editor for OpenStreetMap"]]
mobile_editors = [tag_to_index[tag] for tag in ["MAPS.ME android", "MAPS.ME ios", "StreetComplete", "Vespucci", "Go Map!!", "OsmAnd", "rosemary", "OsmAnd+", "OsmAnd~", "Organic Maps android"]]
tools = [tag_to_index[tag] for tag in ["osmtools", "bulk_upload.py", "upload.py", "bulk_upload.py/posiki", "Redaction bot", "posiki_python_script", "osmupload.py", "PythonOsmApi", "bulk_upload_sax.py", "autoAWS", "Jeff's Uploader", "osm-bulk-upload/upload.py", "FindvejBot", "AND node cleaner"]]
device_type_labels = ["desktop editor", "mobile editor", "tools", "other/unspecified"]

mo_ch = np.zeros((top_k_table, len(months)), dtype=np.int64)
mo_ed = np.zeros((top_k_table, len(months)), dtype=np.int64)
mo_ed_all = np.zeros((len(months)), dtype=np.int64)
mo_ed_device_type = np.zeros((4, len(months)), dtype=np.int64)
mo_co_set = [[set() for _ in range(len(months))] for _ in range(top_k_table)]
mo_co_set_all = [set() for _ in range(len(months))]
mo_co_device_type_set = [[set() for _ in range(len(months))] for _ in range(4)]
co_first_edit_per_editing_software = [dict() for _ in range(top_k_plot)]
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

    if len(data[7])==0:
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

        if rank < top_k_plot:
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


mo_co_first_editing_software = np.zeros((top_k_plot, len(months)), dtype=np.int32)
for month_index, created_by_id in co_first_editing_software.values():
    if created_by_id in co_id_to_rank:
        rank = co_id_to_rank[created_by_id]
        if rank < top_k_plot:
            mo_co_first_editing_software[rank][month_index] += 1

mo_new_co_per_editing_software = np.zeros((top_k_plot, len(months)), dtype=np.int32)
for rank in range(top_k_plot):
    month_index, first_edit_count = np.unique(list(co_first_edit_per_editing_software[rank].values()), return_counts=True)
    mo_new_co_per_editing_software[rank][month_index] = first_edit_count
    
mo_co = util.set_to_length(mo_co_set)


# save plots
topic = "Editing Software"
with open("assets/data.js", "a") as f:
    f.write(f"data['{topic}']={{}}\n")

    question = "How many people are contributing per editing software each month?"
    f.write(util.get_js_str(topic, question, "c229", [
        util.get_multi_line_plot("monthly contributor count per editing software", "contributors", months, mo_co[:top_k_plot], co_rank_to_name[:top_k_plot]),
        util.get_multi_line_plot("monthly new contributor count per editing software", "contributors", months, mo_new_co_per_editing_software, co_rank_to_name[:top_k_plot]),
        util.get_table("yearly contributor count per editing software", years, util.monthly_set_to_yearly_with_total(mo_co_set, years, month_index_to_year_index), topic, co_rank_to_name)
    ]))

    question = "How popular is each editing software per month?"
    f.write(util.get_js_str(topic, question, "158c", [
        util.get_multi_line_plot("percent of contributors that use each editing software per month", "%", months, util.get_percent(mo_co[:top_k_plot], util.set_to_length(mo_co_set_all)), co_rank_to_name[:top_k_plot], percent=True),
    ]))

    question = "Which editing software is used for the first edit?"
    f.write(util.get_js_str(topic, question, "7662", [
        util.get_multi_line_plot("monthly first editing software contributor count", "contributors", months, mo_co_first_editing_software, co_rank_to_name[:top_k_plot]),
    ]))

    question = "How many edits are added per editing software each month?"
    f.write(util.get_js_str(topic, question, "eb30", [
        util.get_multi_line_plot("monthly edits count per editing software", "edits", months, mo_ed[:top_k_plot], ed_rank_to_name[:top_k_plot]),
        util.get_table("yearly edits count per editing software", years, util.monthly_to_yearly_with_total(mo_ed, years, month_index_to_year_index), topic, ed_rank_to_name)
    ]))

    question = "What's the market share of edits per month?"
    f.write(util.get_js_str(topic, question, "a008", [
        util.get_multi_line_plot("market share of edits per month", "%", months, util.get_percent(mo_ed, mo_ed_all)[:top_k_plot], ed_rank_to_name[:top_k_plot], percent=True, on_top_of_each_other=True)
    ]))

    question = "What's the total amount of contributors, edits and changesets of editing software over time?"
    f.write(util.get_js_str(topic, question, "6320", [
        ("text", f"There are {len(index_to_tag):,} different editing software names for the 'created_by' tag. That's quite a big number. However, most names are user or organization names, from people misunderstanding the tag."), 
        util.get_multi_line_plot("total contributor count of editing software", "contributors", months, util.set_cumsum(mo_co_set), co_rank_to_name[:top_k_plot], colors=co_rank_to_color_top_k_plot),
        util.get_multi_line_plot("total edit count of editing software", "edits", months, util.cumsum(mo_ed), ed_rank_to_name[:top_k_plot], colors=ed_rank_to_color_top_k_plot),
        util.get_multi_line_plot("total changeset count of editing software", "changesets", months, util.cumsum(mo_ch), ch_rank_to_name[:top_k_plot], colors=ch_rank_to_color_top_k_plot)
    ]))

    question = "What kind of devices are used for mapping?"
    f.write(util.get_js_str(topic, question, "8ba9", [
        util.get_multi_line_plot("monthly contributor count per device", "contributors", months, util.set_to_length(mo_co_device_type_set), device_type_labels),
        util.get_multi_line_plot("monthly edit count per device", "edits", months, mo_ed_device_type, device_type_labels),
        util.get_multi_line_plot("market share of edit per device", "%", months, util.get_percent(mo_ed_device_type, mo_ed_all), device_type_labels, percent=True)
    ]))

    
