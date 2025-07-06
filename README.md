# OpenStreetMap Statistics

Monthly updated statistics of OpenStreetMap. There is a [website](https://piebro.github.io/openstreetmap-statistics) to browse the generated plots and tables.

The plots and tables are organized in topics and questions I asked myself about OpenStreetMap. My motivation for this project was that I couldn't find some statistics I was interested in or that the data was outdated. That's why I created these statistics, which are easily updatable with a simple script run locally or with GitHub actions.

I'm experimenting with a [website](https://piebro.github.io/openstreetmap-statistics/src/questions) to show the statistics. Many plots are still missing, but I might migrate them in the future and change it as the default starting page.

## Methodology

All data is gathered from an OpenStreetMap [changeset file](https://planet.openstreetmap.org/planet/).
According to the OSM wiki, a [changeset](https://wiki.openstreetmap.org/wiki/Changeset) is a group of edits to the database by a single user over a short period.
Besides who made the changes and how many edits were made, each changeset can contain additional information, for example about which editor was used, source of edit, it may also list used imagery.

The Methodology used is the same as in https://wiki.openstreetmap.org/wiki/Editor_usage_stats and uses the same terms.
One important term which is used a lot is `edits`.
In these statistics, an edit is a change made to a node, way or relation.

That means changing one or multiple tags of one element always counts as one edit.
It also means that changing the geometry of a way or relation count as many edits since the position of many nodes changed.
This leads to an overrepresented of changes in the geometry of ways and relations compared to edits that add or change information to existing nodes.
It's important to keep this in mind looking and interpreting the data.

Another aspect is that the `created_by`, `imagery` and `source` tag use filters to determine the editing software and imagery.
Some categories are opinionated (e.g., should stats for Android and iOS editing apps be counted as one or two categories?), and other categories could be very reasonable, depending on the purpose.
The filtering process is done with simple rules to make it as transparent as possible and easily extendable by anyone.
The rules are defined at [src/replace_rules_created_by.json](src/replace_rules_created_by.json) and [src/replace_rules_imagery_and_source.json](src/replace_rules_imagery_and_source.json).

### Editing Software

Most changesets have a `created_by` tag which indicates which editing software was used to make the changes.
Many `created_by` tags also include the version number or additional irrelevant information for determining the editing software and are therefore filtered.

### Imagery Software

One optional tag for changesets is the `imagery` tag, which iD, Vespucci and Go Map!! use to add an image source if aerial or other imagery is used.
Many `imagery` tags also include irrelevant information for determining the used imagery and are therefore filtered.

### Cooperations

Most mapping is done by individual hobby mappers mapping independently, but there are also organized mapping activities where several people edit the map under specific instructions of others.
A list of all organized editing teams can be found [here](https://wiki.openstreetmap.org/wiki/Category:Organised_Editing_Teams).
The teams list all users (including inactive ones) who are mapping for them for transparency reasons.

I looked at each team in the list and added all the for-profit companies I could find.
The companies are added to [src/save_corporation_contributors.py](src/save_corporation_contributors.py), which extracts all user names and saves them in the assets folder.
The cooperation statistics are gathered with the list of users working at each company.
Incorrect and out-of-date user lists could be a source of error in the data.

## Data Schema

The processed data is stored in partitioned Parquet files with the following schema. Each changeset from OpenStreetMap is represented as a row with various attributes extracted and normalized for analysis.

```
year: BIGINT - Partition column
month: BIGINT - Partition column
edit_count: INTEGER - Number of edits in the changeset. An edit is creating, modifying or deleting nodes, ways or relations.
user_index: INTEGER - A unique number for each username.
pos_x: SMALLINT - Normalized longitude coordinate (0-360 range) of the changeset bounding box center.
pos_y: SMALLINT - Normalized latitude coordinate (0-180 range) of the changeset bounding box center.
created_by: VARCHAR - Tool or application used to create the changeset.
is_bot: BOOLEAN - Whether the changeset was created by a bot (based on bot=yes tag).
corporation: VARCHAR - Corporation or organization associated with the username of the contributor.
imagery_used: VARCHAR[] - List of imagery sources used for the changeset.
hashtags: VARCHAR[] - List of hashtags associated with the changeset.
source: VARCHAR[] - List of data sources used for the changeset.
streetcomplete_quest: VARCHAR - StreetComplete quest type if the changeset was created by Stree.tComplete
mobile_os: VARCHAR - Mobile operating system (Android/iOS) detected from created_by tag.
all_tags: VARCHAR[] - List of all tag prefixes (before colon) used in the changeset.
```

<details>
<summary>Click to view code to get the column descriptions from the parquet files</summary>

```bash
uv run python -c "
import duckdb
columns = duckdb.sql("SELECT * FROM 'data_250602_test_enriched/year=*/month=*/*.parquet' LIMIT 0").columns
types = duckdb.sql("SELECT * FROM 'data_250602_test_enriched/year=*/month=*/*.parquet' LIMIT 0").types
for col, dtype in zip(columns, types):
    print(f"{col}: {dtype}")
"
```

</details>


## Usage

### Update data

The code is tested on Ubuntu 24.04 but should work on every Linux distro.
If you dowload the latest changeset file manually, you can also use Windows or Mac.

```bash
git clone https://github.com/piebro/openstreetmap-statistics.git
cd openstreetmap-statistics

uv venv
uv sync 
```

Run the following commands to get the latest OSM changeset file.
```bash
rm $(ls *.osm.bz2)
wget -N https://planet.openstreetmap.org/planet/changesets-latest.osm.bz2.torrent

sudo apt install aria2
aria2c --seed-time 0 --check-integrity changesets-latest.osm.bz2.torrent -o changesets-latest.osm.bz2
```

Next, you can run the preprocessing pipeline and save the data in partitioned parquet files to easily query it.
The extraction can take some time (on my laptop this takes about 1:30h).
```bash
uv run openstreetmap_statistics/preprocessing.py changesets-latest.osm.bz2.torrent data
```

If you want to work on the preprocessing pipeline, you can iterate faster with by creating a subset of all data using the following command.
```bash
uv run openstreetmap_statistics/create_test_osm_bz2_file.py changesets-latest.osm.bz2.torrent changesets-latest-5000.osm.bz2 --skip-interval 5000
```

### Update notebooks

There are multiple question in `src/questions` and each one has a jupyter notebook to compute the relevant data for the question. To Execute all notebooks run:

```bash
for notebook in $(find src/questions -name calculations.ipynb); do
    jupyter nbconvert --to notebook --execute "$notebook" --output calculations.ipynb
done
```

### Convert between notebooks and Python scripts

To convert Jupyter notebooks to Python scripts with jupytext format (preserves markdown cells), use:

```bash
uv run jupytext --to py:percent notebook.ipynb
```

To convert Python scripts back to Jupyter notebooks, use:

```bash
uv run jupytext --to notebook script.py
```

For basic conversion (jupyter nbconvert):
```bash
uv run jupyter nbconvert --to python notebook.ipynb
```

For multiple notebooks:
```bash
for notebook in $(find . -name "*.ipynb"); do
    uv run jupytext --to py:percent "$notebook"
done
```

### Run and save notebooks

To run a notebook and save the results:

```bash
uv run jupyter nbconvert --to notebook --execute notebook.ipynb --output notebook.ipynb
```

To run a Python script as a notebook:

```bash
uv run jupytext --to notebook --execute script.py
```

### Update cooperation user names

You can update the list of cooperation with their osm user names in assets/corporation_contributors.json with the following command.
```bash
python3 src/save_corporation_contributors.py
```

### Update Jupyter Lite Notebook

```bash
pip install jupyterlite-core==0.1.0 jupyterlab~=3.5.1 jupyterlite-pyodide-kernel==0.0.6
jupyter lite build --contents src/custom_plots_browser.ipynb --output-dir jupyter_lite
```

### Update background map

You can update the background map in assets/background_map.png with the following command after installing two additional python dependencies like this `pip3 install geopandas pillow` and with a shapefile from https://www.naturalearthdata.com/downloads/110m-physical-vectors/.
```bash
python3 save_background_map.py <path-to-ne-110m-land-shape-file.shp>
```

### Update plotly-custom.min.js

Plotly custom is generated with these instructions https://github.com/plotly/plotly.js/blob/master/CUSTOM_BUNDLE.md using the following command.
```bash
npm run custom-bundle -- --traces scatter,bar,histogram2d --transforms none
```
This has the advantage of having a smaller plotly file while still being able to generate all needed plots.


## Contributing

If there are other topics and questions about OpenStreetMap you think are interesting and that can be abstracted from the changeset, feel free to open an issue or create a pull request.
Also, if you see any typos or other mistakes, feel free to correct them and create a pull request.

Another valuable way to contribute is to add editing software or imagery sources to [src/replace_rules_created_by.json](src/replace_rules_created_by.json) and [src/replace_rules_imagery_and_source.json](src/replace_rules_imagery_and_source.json).
The cmd `python3 src/finde_new_replace_rule_candidates.py temp` can be used to find new impactful candidates to add to the rules.
Adding rules can make the statistics more accurate and links help with the usability.
[JSON sorter](https://r37r0m0d3l.github.io/json_sort/) with `four spaces` can be used to sort and format the json correctly.

The Projected uses [Ruff](https://github.com/astral-sh/ruff) for linting and formatting. Run `ruff check` and `ruff format` in the project root directory tu use it.
[Prettier](https://prettier.io/playground/) is used for linting the javascript code with a `print-width` of 120, `tab-width` of 4 and [Stylelint](https://stylelint.io/demo/) is used for linting css code.
Furthermore, [Codespell](https://github.com/codespell-project/codespell) is used to find spelling mistakes and can be used with this command `codespell src README.md index.html assets/statistic_website.js`.

```bash
uv sync --extra dev
uv run pre-commit install
```

## Website Statistics

There is lightweight tracking with [Plausible](https://plausible.io/about) for the [website](https://piebro.github.io/openstreetmap-statistics/) to get infos about how many people are visiting. Everyone who is interested can look at these stats here: https://plausible.io/piebro.github.io%2Fopenstreetmap-statistics?period=30d. Only users without an AdBlocker are counted, so these statistics are under estimating the actual count of visitors. I would guess that quite a few people (including me) visiting the site have an AdBlocker.


## License

All code in this project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details. All data, maps and plots in this project are licensed under [Attribution 4.0 International (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/).
