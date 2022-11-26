import os
import sys
import json

import numpy as np

def load_index_to_tag(data_dir, data_name):
    with open(os.path.join(data_dir, f"index_to_tag_{data_name}.txt")) as f:
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

    for line in sys.stdin:
        data = line[:-1].split(",")
        edits = int(data[0])
        user_id = int(data[2])
        
        created_by = data[7]
        if len(created_by)>0:
            tag = int(created_by)
            tag_to_changesets[0][tag] += 1
            tag_to_edits[0][tag] += edits
            tag_to_contributors[0][tag].add(user_id)

        streetcomplete_quest_type = data[8]
        if len(streetcomplete_quest_type)>0:
            tag = int(streetcomplete_quest_type)
            tag_to_changesets[1][tag] += 1
            tag_to_edits[1][tag] += edits
            tag_to_contributors[1][tag].add(user_id)

        imagery_list = data[9]
        if len(imagery_list)>0:
            for imagery in imagery_list.split(";"):
                tag = int(imagery)
                tag_to_changesets[2][tag] += 1
                tag_to_edits[2][tag] += edits
                tag_to_contributors[2][tag].add(user_id)

        hashtag_list = data[10]
        if len(hashtag_list)>0:
            for hashtag in hashtag_list.split(";"):
                tag = int(hashtag)
                tag_to_changesets[3][tag] += 1
                tag_to_edits[3][tag] += edits
                tag_to_contributors[3][tag].add(user_id)

    top_k_dict = {}
    for i, (tag_name, top_k) in enumerate(tag_name_top_k_list):
        contributers_i = np.array([len(contributor_set) for contributor_set in tag_to_contributors[i]], dtype=np.int64)
        top_k_dict[tag_name] = {
            "changesets": np.argsort(-tag_to_changesets[i])[:top_k].tolist(),
            "edits": np.argsort(-tag_to_edits[i])[:top_k].tolist(),
            "contributors": np.argsort(-contributers_i)[:top_k].tolist(),
        }

    with open(os.path.join(data_dir, "top_k.json"), "w") as f:
        json.dump(top_k_dict, f)

if __name__ == "__main__":
    main()