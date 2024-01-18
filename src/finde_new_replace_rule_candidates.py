import sys
from pathlib import Path

import numpy as np
import util

DATA_DIR = sys.argv[1]

debug_dir = Path(DATA_DIR) / "debug"
debug_dir.mkdir(exist_ok=True)

for filename, tag, replace_rule_fn in [
    ("general", "created_by", "replace_rules_created_by.json"),
    ("imagery", "imagery", "replace_rules_imagery_and_source.json"),
    ("source", "source", "replace_rules_imagery_and_source.json"),
]:
    print(tag)
    ddf = util.load_ddf(DATA_DIR, filename, (tag, "edits", "user_index"))

    if tag == "created_by":
        highest_number = np.iinfo(ddf[tag].dtype).max
        ddf = ddf[ddf[tag] < highest_number]

    index_to_tag = util.load_index_to_tag(DATA_DIR, tag)
    name_to_tags_and_link = util.load_json(f"/home/malboy/code/openstreetmap-statistics/src/{replace_rule_fn}")
    names_with_replace_rules = set(name_to_tags_and_link.keys())

    total_edits = ddf.groupby(tag)["edits"].sum().compute().sort_values(ascending=False)
    total_changesets = ddf.groupby(tag).size().compute().sort_values(ascending=False)
    total_users = util.get_total_contirbutor_count_tag_optimized(
        ddf, tag, total_changesets, min_amount_of_changeset=200
    )

    for total, save_fn in ((total_edits, "edits"), (total_changesets, "changesets"), (total_users, "contributors")):
        with (debug_dir / f"{tag}_{save_fn}.txt").open("w") as f:
            for index, value in total.items():
                name = index_to_tag[index]
                if name in names_with_replace_rules:
                    continue
                f.write(f"{value} {name}\n")
