---
pretty_name: OpenStreetMap Dataset
size_categories:
- 100M<n<1B
task_categories:
- other
tags:
- openstreetmap
- osm
- geospatial
- mapping
- spatial-data
---

# OpenStreetMap Dataset

This is a parsed and enriched dataset of all OpenStreetMap changesets, changeset comments, notes, and note comments.

The dataset includes 4 different datasets:
- changeset_data - All OpenStreetMap changesets with enriched metadata (partitioned by year and month)
- changeset_comments_data - Comments on changesets from changeset discussions
- notes_data - Notes on the map with their locations and status
- notes_comments_data - Comments and actions on notes

This dataset can be used to analyze OpenStreetMap editing patterns, contributor activity, data quality discussions, and community interactions. For example, you can determine the number of contributors and edits per month, the number of contributors per editing software, changeset discussion patterns, note resolution rates, and many more statistics.

There is a website with these and many more statistics about OpenStreetMap at https://piebro.github.io/openstreetmap-statistics.

The dataset is monthly updated using this repo: https://github.com/piebro/openstreetmap-statistics and a GitHub Action job.

More information about the dataset can be found in the [AGENTS.md](https://github.com/piebro/openstreetmap-statistics/blob/master/AGENTS.md) file in the repository.

## License

The data is licensed under the Open Data Commons Open Database License (ODbL) by the OpenStreetMap Foundation (OSMF).
For more info have a look at: https://www.openstreetmap.org/copyright.
