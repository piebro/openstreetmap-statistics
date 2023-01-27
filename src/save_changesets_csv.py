import re
import sys
import os
from datetime import datetime
from dataclasses import dataclass

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
    def __init__(self, name):
        self.counter = -1
        self.dict = {}
        self.name = name

    def add(self, key):
        if key not in self.dict:
            self.counter += 1
            self.dict[key] = self.counter
        return str(self.dict[key])

    def add_keys(self, keys):
        return ";".join([self.add(key) for key in keys])

    def save(self, save_dir):
        revesed_dict = {value: key for key, value in self.dict.items()}
        filepath = os.path.join(save_dir, f"index_to_tag_{self.name}.txt")
        with open(filepath, "w", encoding="UTF-8") as f:
            for line in [revesed_dict[key] for key in sorted(revesed_dict.keys())]:
                f.write(f"{line}\n")


@dataclass
class CSVLine:
    month_index: str = ""
    edits: str = ""
    pos_x: str = ""
    pos_y: str = ""
    user_index: str = ""
    created_by_index: str = ""
    streetcomplete_quest_type_index: str = ""
    imagery_index_list: str = ""
    hashtag_index_list: str = ""
    source_index: str = ""
    bot_used: str = ""
    all_tags: str = ""

    def add_pos(self, data):
        if len(data[7][1:]) > 0:
            min_x = float(data[7][1:])
            max_x = float(data[9][1:])
            self.pos_x = str(round(((min_x + max_x) / 2) - 180) % 360)

            min_y = float(data[8][1:])
            max_y = float(data[10][1:])
            self.pos_y = str(round(((min_y + max_y) / 2) + 90) % 180)

    def add_created_by_and_sc_quest_type(self, tags, index_dict_created_by, index_dict_sc_quest_type):
        if "created_by" in tags and len(tags["created_by"]) > 0:
            created_by = tags["created_by"].replace("%20%", " ").replace("%2c%", ",")
            created_by = re.sub(created_by_regex, "", created_by)
            self.created_by_index = index_dict_created_by.add(created_by)

            if created_by == "StreetComplete" and "StreetComplete:quest_type" in tags:
                sc_quest_type_tag = tags["StreetComplete:quest_type"]
                sc_quest_type_tag = sc_quest_type_tag_changes.get(sc_quest_type_tag, sc_quest_type_tag)
                self.streetcomplete_quest_type_index = index_dict_sc_quest_type.add(sc_quest_type_tag)

    def add_imagery(self, tags, index_dict_imagery):
        if "imagery_used" in tags and len(tags["imagery_used"]) > 0:
            imagery_list = [
                im for im in tags["imagery_used"].replace("%20%", " ").replace("%2c%", ",").split(";") if len(im) > 0
            ]
            for i in range(len(imagery_list)):
                imagery_list[i] = imagery_list[i]
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

            self.imagery_index_list = index_dict_imagery.add_keys(imagery_list)

    def add_hashtags(self, tags, index_dict_hashtag):
        if "hashtags" in tags:
            self.hashtag_index_list = index_dict_hashtag.add_keys(tags["hashtags"].lower().split(";"))

    def add_source(self, tags, index_dict_source):
        if "source" in tags:
            source = tags["source"].replace("%20%", " ").replace("%2c%", ",")
            if "http://" in source:
                source = source[7:].split("/")[0]
            elif "https://" in source:
                source = source[8:].split("/")[0]
            self.source_index = index_dict_source.add(source)

    def get_str(self):
        return ",".join(
            [
                self.month_index,
                self.edits,
                self.pos_x,
                self.pos_y,
                self.user_index,
                self.created_by_index,
                self.streetcomplete_quest_type_index,
                self.imagery_index_list,
                self.hashtag_index_list,
                self.source_index,
                self.bot_used,
                self.all_tags,
            ]
        )


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


def main():
    save_dir = sys.argv[1]
    os.makedirs(save_dir, exist_ok=True)
    months, month_to_index = get_month_and_month_to_index()

    with open(os.path.join(save_dir, "months.txt"), "w", encoding="UTF-8") as f:
        f.writelines("\n".join(months))
        f.writelines("\n")

    index_dict_user_name = IndexDict("user_name")
    index_dict_created_by = IndexDict("created_by")
    index_dict_sc_quest_type = IndexDict("streetcomplete_quest_type")
    index_dict_imagery = IndexDict("imagery")
    index_dict_hashtag = IndexDict("hashtag")
    index_dict_source = IndexDict("source")
    index_dict_all_tags = IndexDict("all_tags")

    for line in sys.stdin:
        data = line.split(" ")
        if data[2][1:8] not in month_to_index:  # if its the current month: continue
            continue

        csv_line = CSVLine()
        csv_line.month_index = month_to_index[data[2][1:8]]
        csv_line.edits = data[1][1:]
        csv_line.user_index = index_dict_user_name.add(data[6][1:])
        csv_line.add_pos(data)

        tags = get_tags(data[11][1:-1])
        csv_line.add_created_by_and_sc_quest_type(tags, index_dict_created_by, index_dict_sc_quest_type)
        csv_line.add_imagery(tags, index_dict_imagery)
        csv_line.add_hashtags(tags, index_dict_hashtag)
        csv_line.add_source(tags, index_dict_source)
        csv_line.bot_used = str(int("bot" in tags and tags["bot"] == "yes"))
        csv_line.all_tags = index_dict_all_tags.add_keys(tags.keys())

        sys.stdout.write(f"{csv_line.get_str()}\n")

    index_dict_user_name.save(save_dir)
    index_dict_created_by.save(save_dir)
    index_dict_sc_quest_type.save(save_dir)
    index_dict_imagery.save(save_dir)
    index_dict_hashtag.save(save_dir)
    index_dict_source.save(save_dir)
    index_dict_all_tags.save(save_dir)


if __name__ == "__main__":
    main()
