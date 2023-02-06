import re
import sys
import os
import gzip
import json
from datetime import datetime

import numpy as np

created_by_regex = re.compile(r"(?: *\(.*|(?: v\. |/| |-|_| \(| v)\d+\.?\d*.*)")

# change some streetcomplete quest type tags that changed their name over the time to the newest name
# https://github.com/streetcomplete/StreetComplete/issues/1749#issuecomment-593450124
sc_quest_type_tag_changes = {
    "AddAccessibleForPedestrians": "AddProhibitedForPedestrians",
    "AddWheelChairAccessPublicTransport": "AddWheelchairAccessPublicTransport",
    "AddWheelChairAccessToilets": "AddWheelchairAccessPublicTransport",
    "AddSidewalks": "AddSidewalk",
}


def get_tags(tags_str):
    tags = {}
    if len(tags_str) > 0:
        for key_value in tags_str.split(","):
            key, value = key_value.split("=")
            tags[key] = value
    return tags


def debug_regex(regex, text):
    sub = re.sub(regex, "", text)
    sys.stderr.write(f'{text != sub}: "{text}"  =>  "{sub}"\n')


class IndexDict:
    def __init__(self, name, top_k):
        self.counter = -1
        self.dict = {}
        self.top_k_counter_changesets = []
        self.top_k_counter_edits = []
        self.top_k_counter_contributors = []
        self.name = name
        self.top_k = top_k

    def add_no_top_k_int(self, key):
        if key not in self.dict:
            self.counter += 1
            self.dict[key] = self.counter
        return self.dict[key]

    def add(self, key, edits, user_index):
        if key not in self.dict:
            self.counter += 1
            self.dict[key] = self.counter
            self.top_k_counter_changesets.append(0)
            self.top_k_counter_edits.append(0)
            self.top_k_counter_contributors.append(set())
        index = self.dict[key]
        self.top_k_counter_changesets[index] += 1
        self.top_k_counter_edits[index] += edits
        self.top_k_counter_contributors[index].add(user_index)
        return str(index)

    def add_keys(self, keys, edits, user_index):
        return ";".join([self.add(key, edits, user_index) for key in keys])

    def save(self, save_dir):
        revesed_dict = {value: key for key, value in self.dict.items()}
        filepath = os.path.join(save_dir, f"index_to_tag_{self.name}.txt")
        with open(filepath, "w", encoding="UTF-8") as f:
            for line in [revesed_dict[key] for key in sorted(revesed_dict.keys())]:
                f.write(f"{line}\n")

        top_k_counter_contributors = np.array(
            [len(contributor_set) for contributor_set in self.top_k_counter_contributors], dtype=np.int64
        )
        top_k_dict = {
            "changesets": np.argsort(-np.array(self.top_k_counter_changesets, dtype=np.int64))[: self.top_k].tolist(),
            "edits": np.argsort(-np.array(self.top_k_counter_edits, dtype=np.int64))[: self.top_k].tolist(),
            "contributors": np.argsort(-top_k_counter_contributors)[: self.top_k].tolist(),
        }

        with open(os.path.join(save_dir, f"top_k_{self.name}.json"), "w", encoding="UTF-8") as f:
            f.write(json.dumps(top_k_dict, separators=(",", ":")))


def get_month_and_month_to_index():
    first_year = 2005
    first_month = 4
    now = datetime.now()
    last_year = now.year
    last_month = now.month - 1
    years = [str(year) for year in range(first_year, last_year + 1)]
    months = []
    for year in years:
        months.extend(f"{year}-{month:02d}" for month in range(1, 13))
    months = months[first_month - 1 : -12 + last_month]
    month_to_index = {month: str(i) for i, month in enumerate(months)}
    return months, month_to_index


def add_pos(data):
    if len(data[7][1:]) > 0:
        min_x = float(data[7][1:])
        max_x = float(data[9][1:])
        pos_x = round(((min_x + max_x) / 2) - 180) % 360

        min_y = float(data[8][1:])
        max_y = float(data[10][1:])
        pos_y = round(((min_y + max_y) / 2) + 90) % 180
        return str(pos_x), str(pos_y)
    else:
        return "", ""


def add_created_by_and_sc_quest_type(tags, index_dicts, edits, user_index):
    if "created_by" in tags and len(tags["created_by"]) > 0:
        created_by = tags["created_by"].replace("%20%", " ").replace("%2c%", ",")
        created_by = re.sub(created_by_regex, "", created_by)
        created_by_index = index_dicts["created_by"].add(created_by, edits, user_index)

        if created_by == "StreetComplete" and "StreetComplete:quest_type" in tags:
            sc_quest_type_tag = tags["StreetComplete:quest_type"]
            sc_quest_type_tag = sc_quest_type_tag_changes.get(sc_quest_type_tag, sc_quest_type_tag)
            sc_quest_type_index = index_dicts["streetcomplete_quest_type"].add(sc_quest_type_tag, edits, user_index)
            return created_by_index, sc_quest_type_index
        else:
            return created_by_index, ""
    else:
        return "", ""


def add_imagery(tags, index_dicts, edits, user_index):
    if "imagery_used" in tags and len(tags["imagery_used"]) > 0:
        imagery_list = [
            key for key in tags["imagery_used"].replace("%20%", " ").replace("%2c%", ",").split(";") if len(key) > 0
        ]
        for i in range(len(imagery_list)):
            if imagery_list[i][0] == " ":
                imagery_list[i] = imagery_list[i][1:]

            if imagery_list[i][:4] == "Bing":
                imagery_list[i] = "Bing Maps Aerial"
            elif imagery_list[i][:8] == "Custom (":
                imagery_list[i] = "Custom"
            elif imagery_list[i][-4:] == ".gpx":
                imagery_list[i] = ".gpx data file"
            elif imagery_list[i][:25] == "https://tasks.hotosm.org/":
                imagery_list[i] = "tasks.hotosm.org/"
            elif imagery_list[i][:35] == "http://www.openstreetmap.org/trace/":
                imagery_list[i] = "www.openstreetmap.org/trace/"
            elif imagery_list[i][:36] == "https://www.openstreetmap.org/trace/":
                imagery_list[i] = "www.openstreetmap.org/trace/"
            elif imagery_list[i][-13:] == "/{x}/{y}.png)":
                imagery_list[i] = "unknown"

        return index_dicts["imagery"].add_keys(imagery_list, edits, user_index)
    else:
        return ""


def add_hashtags(tags, index_dicts, edits, user_index):
    if "hashtags" in tags:
        return index_dicts["hashtag"].add_keys(tags["hashtags"].lower().split(";"), edits, user_index)
    else:
        return ""


def add_source(tags, index_dicts, edits, user_index):
    if "source" in tags and len(tags["source"]) > 0:
        source_list = [
            key for key in tags["source"].replace("%20%", " ").replace("%2c%", ",").split(";") if len(key) > 0
        ]
        for i in range(len(source_list)):
            if source_list[i][0] == " ":
                source_list[i] = source_list[i][1:]

            if source_list[i][:7] == "http://":
                source_list[i] = source_list[i][7:].split("/")[0]
            elif source_list[i][:8] == "https://":
                source_list[i] = source_list[i][8:].split("/")[0]
        return index_dicts["source"].add_keys(source_list, edits, user_index)
    else:
        return ""


def add_all_tags(tags, index_dicts, edits, user_index):
    return index_dicts["all_tags"].add_keys([tag_name.split(":")[0] for tag_name in tags.keys()], edits, user_index)


def add_bot_usage(tags):
    if "bot" in tags and tags["bot"] == "yes":
        return "1"
    else:
        return ""


def osmium_line_to_csv_str(osmium_line, month_to_index, index_dicts, counter):
    data = osmium_line.split(" ")
    if data[2][1:8] not in month_to_index:  # if its the current month
        return None

    month_index = int(month_to_index[data[2][1:8]])
    edits = int(data[1][1:])
    user_index = int(index_dicts["user_name"].add_no_top_k_int(data[6][1:]))

    counter["total_changesets"] += 1
    counter["monthly_changsets"][month_index] += 1
    counter["total_edits"] += edits
    counter["monthly_edits"][month_index] += edits
    counter["monthly_contributors"][month_index].add(user_index)

    csv_str_array = []
    csv_str_array.append(str(month_index))
    csv_str_array.append(str(edits))
    csv_str_array.append(str(user_index))
    csv_str_array.extend(add_pos(data))

    tags = get_tags(data[11][1:-1])
    csv_str_array.extend(add_created_by_and_sc_quest_type(tags, index_dicts, edits, user_index))
    csv_str_array.append(add_imagery(tags, index_dicts, edits, user_index))
    csv_str_array.append(add_hashtags(tags, index_dicts, edits, user_index))
    csv_str_array.append(add_source(tags, index_dicts, edits, user_index))
    csv_str_array.append(add_all_tags(tags, index_dicts, edits, user_index))
    csv_str_array.append(add_bot_usage(tags))

    return ",".join(csv_str_array)


def main():
    save_dir = sys.argv[1]
    os.makedirs(save_dir, exist_ok=True)
    months, month_to_index = get_month_and_month_to_index()

    with open(os.path.join(save_dir, "months.txt"), "w", encoding="UTF-8") as f:
        f.writelines("\n".join(months))
        f.writelines("\n")

    index_dicts = {
        "user_name": IndexDict("user_name", 100),
        "created_by": IndexDict("created_by", 100),
        "streetcomplete_quest_type": IndexDict("streetcomplete_quest_type", 300),
        "imagery": IndexDict("imagery", 100),
        "hashtag": IndexDict("hashtag", 100),
        "source": IndexDict("source", 100),
        "all_tags": IndexDict("all_tags", 100),
    }

    top_k_counter = {}
    for tag in ("all_tags", "created_by", "hashtag", "imagery", "source", "streetcomplete_quest_type"):
        top_k_counter[tag] = {"changeset": [], "edits": [], "contributors": []}

    counter = {
        "total_changesets": 0,
        "total_edits": 0,
        "monthly_changsets": np.zeros((len(months)), dtype=np.int64),
        "monthly_edits": np.zeros((len(months)), dtype=np.int64),
        "monthly_contributors": [set() for _ in range(len(months))],
    }

    with gzip.open(os.path.join(save_dir, "changesets.csv.gz"), "w") as f:
        for osmium_line in sys.stdin:
            csv_str = osmium_line_to_csv_str(osmium_line, month_to_index, index_dicts, counter)
            if csv_str is None:
                continue

            f.write(f"{csv_str}\n".encode())

    counter["total_contributor"] = len(index_dicts["user_name"].dict)
    counter["monthly_changsets"] = counter["monthly_changsets"].tolist()
    counter["monthly_edits"] = counter["monthly_edits"].tolist()
    counter["monthly_contributors"] = [len(s) for s in counter["monthly_contributors"]]

    with open(os.path.join(save_dir, "infos.json"), "w", encoding="UTF-8") as f:
        f.write(json.dumps(counter, separators=(",", ":")))

    for index_dict in index_dicts.values():
        index_dict.save(save_dir)


if __name__ == "__main__":
    main()
