import re
import sys
import os
from datetime import datetime

created_by_regex = re.compile(r"(?: *\(.*|(?: v\. |/| |-|_| \(| v)\d+\.?\d*.*)")
# imagery_regex = re.compile(r"(?: *\(.*| *\d{4})")

# change some streetcomplete quest type tags that changed their name over the time to the newest name
# https://github.com/streetcomplete/StreetComplete/issues/1749#issuecomment-593450124
streetcomplete_quest_type_tag_changes = {
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


def add_to_index_dict(index_dict, tag):
    if tag not in index_dict[1]:
        index_dict[0] += 1
        index_dict[1][tag] = index_dict[0]
    return str(index_dict[1][tag])


def debug_regex(regex, text):
    sub = re.sub(regex, "", text)
    sys.stderr.write(f'{text != sub}: "{text}"  =>  "{sub}"\n')


def main():
    save_dir = sys.argv[1]
    os.makedirs(save_dir, exist_ok=True)

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

    index_dict_created_by = [-1, {}]
    index_dict_imagery = [-1, {}]
    index_dict_hashtag = [-1, {}]
    index_dict_streetcomplete_quest_type = [-1, {}]

    # csv head: edits, month_index, user_id, user_name, pos_x, pos_y, bot_used, created_by,
    # streetcomplete_quest_type, imagery_list, hashtag_list, other_tags
    with open(os.path.join(save_dir, "months.txt"), "w", encoding="UTF-8") as f:
        f.writelines("\n".join(months))
        f.writelines("\n")

    for line in sys.stdin:
        data = line.split(" ")
        if data[2][1:8] not in month_to_index:  # if its the current month: continue
            continue

        num_of_edits = data[1][1:]
        month_index = month_to_index[data[2][1:8]]
        user_id = data[5][1:]
        user_name = data[6][1:]

        if len(data[7][1:]) > 0:
            pos_x = str(
                round(((float(data[7][1:]) + float(data[9][1:])) / 2) - 180) % 360
            )  # pos_x = ((min_x + max_x) / 2) - 180
            pos_y = str(
                round(((float(data[8][1:]) + float(data[10][1:])) / 2) + 90) % 180
            )  # pos_y = ((min_y + max_y) / 2) + 90
        else:
            pos_x, pos_y = "", ""

        tags = get_tags(data[11][1:-1])

        bot_used = str(int("bot" in tags and tags["bot"] == "yes"))

        created_by = None
        if "created_by" in tags and len(tags["created_by"]) > 0:
            created_by = tags["created_by"].replace("%20%", " ").replace("%2c%", ",")
            # debug_regex(created_by_regex, created_by)
            created_by = re.sub(created_by_regex, "", created_by)
            created_by_id = add_to_index_dict(index_dict_created_by, created_by)
        else:
            created_by_id = ""

        if created_by == "StreetComplete" and "StreetComplete:quest_type" in tags:
            streetcomplete_quest_type_tag = tags["StreetComplete:quest_type"]
            if streetcomplete_quest_type_tag in streetcomplete_quest_type_tag_changes:
                streetcomplete_quest_type_tag = streetcomplete_quest_type_tag_changes[streetcomplete_quest_type_tag]
            streetcomplete_quest_type_id = add_to_index_dict(
                index_dict_streetcomplete_quest_type, tags["StreetComplete:quest_type"]
            )
        else:
            streetcomplete_quest_type_id = ""

        if "imagery_used" in tags and len(tags["imagery_used"]) > 0:
            imagery_list = [
                imagery
                for imagery in tags["imagery_used"].replace("%20%", " ").replace("%2c%", ",").split(";")
                if len(imagery) > 0
            ]
            for i in range(len(imagery_list)):
                # debug_regex(imagery_regex, imagery_list[i])
                # imagery_list[i] = re.sub(imagery_regex, "", imagery_list[i])
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
                imagery_list[i] = add_to_index_dict(index_dict_imagery, imagery_list[i])

            imagery_ids = ";".join(imagery_list)
        else:
            imagery_ids = ""

        if "hashtags" in tags:
            hashtag_ids = ";".join(
                [add_to_index_dict(index_dict_hashtag, hashtag) for hashtag in tags["hashtags"].lower().split(";")]
            )
        else:
            hashtag_ids = ""

        sys.stdout.write(
            f"{num_of_edits},{month_index},{user_id},{user_name},{pos_x},{pos_y},{bot_used},{created_by_id},"
            f"{streetcomplete_quest_type_id},{imagery_ids},{hashtag_ids}\n"
        )
        sys.stdout.flush()

    # save index dicts
    for filename, index_dict in [
        ("created_by", index_dict_created_by),
        ("imagery", index_dict_imagery),
        ("hashtag", index_dict_hashtag),
        ("streetcomplete_quest_type", index_dict_streetcomplete_quest_type),
    ]:
        revesed_dict = {value: key for key, value in index_dict[1].items()}
        filepath = os.path.join(save_dir, f"index_to_tag_{filename}.txt")
        with open(filepath, "w", encoding="UTF-8") as f:
            for line in [revesed_dict[key] for key in sorted(revesed_dict.keys())]:
                f.write(f"{line}\n")

    with open(os.path.join(save_dir, "months.txt"), "w", encoding="UTF-8") as f:
        f.writelines("\n".join(months))
        f.writelines("\n")


if __name__ == "__main__":
    main()
