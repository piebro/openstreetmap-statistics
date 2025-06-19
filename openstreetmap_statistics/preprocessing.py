import argparse
import json
import shutil
from pathlib import Path

import osmium
import pyarrow as pa
import pyarrow.parquet as pq


def create_replace_rules(file_path_str):
    with Path(file_path_str).open(encoding="utf-8") as f:
        name_to_tags_and_link = json.load(f)

    tag_to_name = {}
    starts_with_list = []
    ends_with_list = []
    contains_list = []

    for name, name_infos in name_to_tags_and_link.items():
        if "aliases" in name_infos:
            for alias in name_infos["aliases"]:
                tag_to_name[alias] = name
        if "starts_with" in name_infos:
            starts_with_list.extend(
                [(len(starts_with), starts_with, name) for starts_with in name_infos["starts_with"]],
            )
        if "ends_with" in name_infos:
            ends_with_list.extend(
                [(len(ends_with), ends_with, name) for ends_with in name_infos["ends_with"]],
            )
        if "contains" in name_infos:
            contains_list.extend([(compare_str, name) for compare_str in name_infos["contains"]])

    return {
        "tag_to_name": tag_to_name,
        "starts_with_list": starts_with_list,
        "ends_with_list": ends_with_list,
        "contains_list": contains_list,
    }


def load_user_name_to_corporation_dict(file_path_str):
    corporation_contributors = json.load(Path(file_path_str).open())
    user_name_to_corporation = {}
    for corporation_name, (_, user_name_list) in corporation_contributors.items():
        for user_name in user_name_list:
            user_name_to_corporation[user_name] = corporation_name
    return user_name_to_corporation


def split_source_excluding_brackets(s):
    """Split a string into parts based on separator characters, while excluding characters that are within brackets."""
    bracket_pairs = {"(": ")", "{": "}"}
    stack = []
    last_index = 0
    parts = []

    for i, char in enumerate(s):
        if char in bracket_pairs:
            stack.append(bracket_pairs[char])
        elif stack and char == stack[-1]:
            stack.pop()
        elif not stack and char in (";", "|", "+", "/", "&", ","):
            # Split the string at this character if it's a separator and not within brackets.
            # Special case for URLs: don't split if the separator is '/' and 'http' is part of the string.
            if char == "/" and "http" in s:
                continue
            parts.append(s[last_index:i])
            last_index = i + 1

    parts.append(s[last_index:])
    return parts


def extract_all_tags(tags):
    """Extract all tag prefixes (before colon)"""
    return sorted({tag_name.split(":")[0] for tag_name in tags})


def get_normalized_coordinates(c):
    """Normalize geographic coordinates like in original script."""
    # Extract coordinates from bounding box
    if c.bounds.valid():
        pos_x = (c.bounds.bottom_left.lon + c.bounds.top_right.lon) / 2
        pos_y = (c.bounds.bottom_left.lat + c.bounds.top_right.lat) / 2
        normalized_x = round(((pos_x) - 180) % 360)  # -180 to +180 -> 0 to 360
        normalized_y = round(((pos_y) + 90) % 180)  # -90 to +90 -> 0 to 180
        return normalized_x, normalized_y
    else:
        return -1, -1


def replace_with_rules(tag, replace_rules):
    """Apply replace rules to normalize tag values."""
    if tag in replace_rules["tag_to_name"]:
        return replace_rules["tag_to_name"][tag]

    for compare_str_length, compare_str, replace_str in replace_rules["starts_with_list"]:
        if tag[:compare_str_length] == compare_str:
            return replace_str

    for compare_str_length, compare_str, replace_str in replace_rules["ends_with_list"]:
        if tag[-compare_str_length:] == compare_str:
            return replace_str

    for compare_str, replace_str in replace_rules["contains_list"]:
        if compare_str in tag:
            return replace_str

    return tag


def get_created_by(tags, replace_rules):
    if "created_by" in tags and len(tags["created_by"]) > 0:
        created_by_raw = tags["created_by"].replace("%20%", " ").replace("%2c%", ",")
        return replace_with_rules(created_by_raw, replace_rules)
    return None


def get_imagery_used(tags, replace_rules):
    imagery_used = tags.get("imagery_used", "")

    if not imagery_used:
        return None

    imagery_used = imagery_used.replace("%20%", " ").replace("%2c%", ",")

    imagery_list = [x.strip() for x in imagery_used.split(";") if x.strip()]
    normalized_imagery = []

    for imagery in imagery_list:
        normalized = replace_with_rules(imagery.strip(), replace_rules)
        normalized_imagery.append(normalized)

    return normalized_imagery


def get_hashtags(tags):
    hashtags_raw = tags.get("hashtags", "")
    if hashtags_raw:
        return [tag.strip().lower() for tag in hashtags_raw.split(";") if tag.strip()]
    return None


def get_source(tags, replace_rules):
    source = tags.get("source", "")
    if not source:
        return None

    # Clean up source prefixes
    if source.startswith("source%3d%"):
        source = source[10:]
    elif source.startswith("source:"):
        source = source[7:]
    elif source.startswith("source"):
        source = source[6:]

    # Split using bracket-aware function
    source_list = split_source_excluding_brackets(source)
    normalized_sources = []

    for source_raw in source_list:
        source_clean = source_raw.strip()
        if not source_clean:
            continue

        # Normalize with rules
        normalized = replace_with_rules(source_clean, replace_rules)

        # Extract domain from URLs
        if normalized.startswith("https://"):
            normalized = "https://" + normalized.split("/")[2]
        elif normalized.startswith("http://"):
            normalized = "http://" + normalized.split("/")[2]

        normalized_sources.append(normalized)

    return normalized_sources


def get_mobile_os(tags):
    """Extract mobile OS from the original created_by tag."""
    created_by_raw = tags.get("created_by", "")
    if not created_by_raw:
        return None

    created_by_lower = created_by_raw.lower()
    if "android" in created_by_lower:
        return "Android"
    elif "ios" in created_by_lower:
        return "iOS"
    else:
        return None


def get_streetcomplete_quest(created_by, tags):
    if created_by == "StreetComplete" and "StreetComplete:quest_type" in tags:
        quest_type = tags["StreetComplete:quest_type"]
        match quest_type:
            case "AddAccessibleForPedestrians":
                return "AddProhibitedForPedestrians"
            case "AddWheelChairAccessPublicTransport":
                return "AddWheelchairAccessPublicTransport"
            case "AddWheelChairAccessToilets":
                return "AddWheelchairAccessPublicTransport"
            case "AddSidewalks":
                return "AddSidewalk"
            case _:
                return quest_type
    return None


class ChangesetHandler(osmium.SimpleHandler):
    def __init__(self, batch_size, output_path, schema):
        osmium.SimpleHandler.__init__(self)
        self.batch_size = batch_size
        self.output_path = output_path
        self.schema = schema
        self.count = 0
        self.batch_count = 0

        # Initialize columnar data storage (dict of lists)
        self.data = {
            "year": [],
            "month": [],
            "edit_count": [],
            "user_index": [],
            "pos_x": [],
            "pos_y": [],
            "is_bot": [],
            "corporation": [],
            "created_by": [],
            "imagery_used": [],
            "hashtags": [],
            "source": [],
            "streetcomplete_quest": [],
            "mobile_os": [],
            "all_tags": [],
        }

        self.replace_rules_created_by = create_replace_rules("config/replace_rules_created_by.json")
        self.replace_rules_imagery = create_replace_rules("config/replace_rules_imagery_and_source.json")
        self.replace_rules_source = create_replace_rules("config/replace_rules_imagery_and_source.json")
        self.user_name_to_corporation = load_user_name_to_corporation_dict("config/corporation_contributors.json")
        self.user_name_to_index = {}

    def _save_batch(self):
        """Save current batch to disk and clear memory"""
        if not self.data["year"]:  # No data to save
            return

        # Create table from current batch
        table = pa.table(self.data, schema=self.schema)

        # Save as partitioned dataset (append mode)
        pq.write_to_dataset(
            table,
            root_path=self.output_path,
            partition_cols=["year", "month"],
            basename_template=f"part-{self.batch_count}-{{i}}.parquet",
            existing_data_behavior="overwrite_or_ignore",
        )

        # Clear data for next batch
        for key in self.data:
            self.data[key].clear()

        self.batch_count += 1
        print(f"Saved batch {self.batch_count}, processed {self.count} changesets total")

    def changeset(self, c):
        self.count += 1

        pos_x_norm, pos_y_norm = get_normalized_coordinates(c)
        self.data["pos_x"].append(pos_x_norm)
        self.data["pos_y"].append(pos_y_norm)
        self.data["year"].append(c.created_at.year)
        self.data["month"].append(c.created_at.month)
        self.data["edit_count"].append(c.num_changes)

        user_name = c.user.replace("%20%", " ").replace("%2c%", ",")
        if user_name not in self.user_name_to_index:
            self.user_name_to_index[user_name] = len(self.user_name_to_index)

        self.data["user_index"].append(self.user_name_to_index.get(user_name, -1))
        self.data["corporation"].append(self.user_name_to_corporation.get(user_name, ""))

        tags = dict(c.tags)
        self.data["all_tags"].append(extract_all_tags(tags))
        created_by = get_created_by(tags, self.replace_rules_created_by)
        self.data["created_by"].append(created_by)
        self.data["imagery_used"].append(get_imagery_used(tags, self.replace_rules_imagery))
        self.data["hashtags"].append(get_hashtags(tags))
        self.data["source"].append(get_source(tags, self.replace_rules_source))
        self.data["mobile_os"].append(get_mobile_os(tags))
        self.data["is_bot"].append(tags.get("bot", "") == "yes")
        self.data["streetcomplete_quest"].append(get_streetcomplete_quest(created_by, tags))

        # Check if we need to save a batch
        if len(self.data["year"]) >= self.batch_size:
            self._save_batch()

    def finalize(self):
        """Save any remaining data in the final batch"""
        if self.data["year"]:  # If there's remaining data
            self._save_batch()
            print(f"Finished processing. Total: {self.count} changesets in {self.batch_count} batches")


def main():
    parser = argparse.ArgumentParser(description="Process OSM changesets and convert to partitioned Parquet dataset")
    parser.add_argument("changeset_path", help="Path to the OSM changeset file")
    parser.add_argument("output_path", help="Path to the output directory")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5_000_000,
        help="Number of changesets to process in each batch (default: 5_000_000)",
    )
    parser.add_argument("--overwrite", action="store_true", help="Delete output directory if it exists")

    args = parser.parse_args()

    # Handle existing output directory
    output_path = Path(args.output_path)
    if output_path.exists():
        if args.overwrite:
            print(f"Removing existing directory: {output_path}")
            shutil.rmtree(output_path)
        else:
            raise FileExistsError(
                f"Output directory '{output_path}' already exists. Use --overwrite to delete it or choose a different path."
            )

    # Define PyArrow schema for optimal performance
    schema_fields = [
        pa.field("year", pa.int16(), metadata={"description": "Year of the changeset."}),
        pa.field("month", pa.int8(), metadata={"description": "Month of the changeset."}),
        pa.field(
            "edit_count",
            pa.int32(),
            metadata={
                "description": "Number of edits in the changeset. An edit is creating, modifying or deleting nodes, ways or relations."
            },
        ),
        pa.field("user_index", pa.int32(), metadata={"description": "A unique number for each username."}),
        pa.field("pos_x", pa.int16(), metadata={"description": "Normalized longitude coordinate (0-360 range) of the changeset bounding box center."}),
        pa.field("pos_y", pa.int16(), metadata={"description": "Normalized latitude coordinate (0-180 range) of the changeset bounding box center."}),
        pa.field("is_bot", pa.bool_(), metadata={"description": "Whether the changeset was created by a bot (based on bot=yes tag)."}),
        pa.field("corporation", pa.string(), metadata={"description": "Corporation or organization associated with the username of the contributor."}),
        pa.field("created_by", pa.string(), metadata={"description": "Tool or application used to create the changeset."}),
        pa.field("imagery_used", pa.list_(pa.string()), metadata={"description": "List of imagery sources used for the changeset."}),
        pa.field("hashtags", pa.list_(pa.string()), metadata={"description": "List of hashtags associated with the changeset."}),
        pa.field("source", pa.list_(pa.string()), metadata={"description": "List of data sources used for the changeset."}),
        pa.field("streetcomplete_quest", pa.string(), metadata={"description": "StreetComplete quest type if the changeset was created by Stree.tComplete"}),
        pa.field("mobile_os", pa.string(), metadata={"description": "Mobile operating system (Android/iOS) detected from created_by tag."}),
        pa.field("all_tags", pa.list_(pa.string()), metadata={"description": "List of all tag prefixes (before colon) used in the changeset."}),
    ]

    # Create handler with batch processing support
    handler = ChangesetHandler(
        batch_size=args.batch_size, output_path=args.output_path, schema=pa.schema(schema_fields)
    )

    print(f"Processing {args.changeset_path} in batches of {args.batch_size} changesets...")

    handler.apply_file(args.changeset_path)
    handler.finalize()


if __name__ == "__main__":
    main()
