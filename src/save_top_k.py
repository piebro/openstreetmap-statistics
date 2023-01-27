import os
import sys
import json
import numpy as np
import util


def load_index_to_tag(data_dir, data_name):
    with open(os.path.join(data_dir, f"index_to_tag_{data_name}.txt"), encoding="UTF-8") as f:
        return [l[:-1] for l in f.readlines()]


def main():
    tag_name_top_k_list = [("created_by", 100), ("streetcomplete_quest_type", 300), ("imagery", 100), ("hashtag", 100)]
    data_dir = sys.argv[1]

    tag_to_changesets, tag_to_edits, tag_to_contributors = [], [], []
    for tag_name, top_k in tag_name_top_k_list:
        count = len(load_index_to_tag(data_dir, tag_name))
        tag_to_changesets.append(np.zeros(count, dtype=np.int64))
        tag_to_edits.append(np.zeros(count, dtype=np.int64))
        tag_to_contributors.append([set() for _ in range(count)])

    for csv_line in sys.stdin:
        data = util.CSVData(csv_line)

        if data.created_by_index is not None:
            tag = int(data.created_by_index)
            tag_to_changesets[0][tag] += 1
            tag_to_edits[0][tag] += data.edits
            tag_to_contributors[0][tag].add(data.user_index)

        if data.streetcomplete_quest_type_index is not None:
            tag = int(data.streetcomplete_quest_type_index)
            tag_to_changesets[1][tag] += 1
            tag_to_edits[1][tag] += data.edits
            tag_to_contributors[1][tag].add(data.user_index)

        for imagery_index in data.imagery_index_list:
            tag_to_changesets[2][imagery_index] += 1
            tag_to_edits[2][imagery_index] += data.edits
            tag_to_contributors[2][imagery_index].add(data.user_index)

        for hashtag_index in data.hashtag_index_list:
            tag_to_changesets[3][hashtag_index] += 1
            tag_to_edits[3][hashtag_index] += data.edits
            tag_to_contributors[3][hashtag_index].add(data.user_index)

    top_k_dict = {}
    for i, (tag_name, top_k) in enumerate(tag_name_top_k_list):
        contributers_i = np.array([len(contributor_set) for contributor_set in tag_to_contributors[i]], dtype=np.int64)
        top_k_dict[tag_name] = {
            "changesets": np.argsort(-tag_to_changesets[i])[:top_k].tolist(),
            "edits": np.argsort(-tag_to_edits[i])[:top_k].tolist(),
            "contributors": np.argsort(-contributers_i)[:top_k].tolist(),
        }

    with open(os.path.join(data_dir, "top_k.json"), "w", encoding="UTF-8") as f:
        json.dump(top_k_dict, f)


if __name__ == "__main__":
    main()
