# OpenStreetMap Statistics

Monthly updated statistics of OpenStreetMap. There is a [website](https://piebro.github.io/openstreetmap-statistics) to browse the generated plots and tables.

The plots and tables are organized in topics and questions I asked myself about OpenStreetMap. My motivation for this project was that I couldn't find some statistics I was interested in or that the data was outdated. That's why I created these statistics, which are easily updatable with a simple script run locally or with GitHub actions.

There is also a notebook to create [custom plots](https://piebro.github.io/openstreetmap-statistics/jupyter_lite/retro/notebooks/?path=custom_plots_browser.ipynb) with the data in a browser. You can use [this](https://github.com/piebro/openstreetmap-statistics/blob/master/src/custom_data_and_plots.ipynb) notebook if you want to create custom data with custom plots locally.

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
Some categories are opinionated (e.g., should stats for Android and iOS editing apps be counted separately?), and other categories could be very reasonable, depending on the purpose.
The filtering process is done with simple rules to make it as transparent as possible and easily extendable by anyone.
The rules are defined at [src/replace_rules_created_by.json](src/replace_rules_created_by.json) and [src/replace_rules_imagery_and_source.json](src/replace_rules_imagery_and_source.json).

### Editing Software

Most changesets have a `created_by` tag which indicates which editing software was used to make the changes.
Many `created_by` tags also include the version number or additional irrelevant information for determining the editing software and are therefore filtered.

### Imagery Software

One optional tag for changesets is the `imagery` tag, which iD, Vespucci and Go Map!! use to add an image source if aerial or other imagery is used.
Many `imagery` tags also include irrelevant information for determining the used imagery and are therefore filtered.

### Organised Teams

Most mapping is done by individual hobby mappers mapping independently, but there are also organized mapping activities where several people edit the map under specific instructions of others.
A list of all organized editing teams can be found [here](https://wiki.openstreetmap.org/wiki/Category:Organised_Editing_Teams).
The teams list all users (including inactive ones) who are mapping for them for transparency reasons.

The teams are added to [src/save_organised_teams_contributors.py](src/save_organised_teams_contributors.py), which extracts all user names and saves them in the assets folder.
The statistics are gathered with the list of users working at each team.
Incorrect and out-of-date user lists could be a source of error for this data.


## Usage

### Update data

The code is tested on Ubuntu 20.04 but should work on every Linux distro. I'm not sure about Windows or Mac.

```bash
# Install dependencies for downloading and handling the latest changeset and showing a progress bar
sudo apt install aria2 osmium-tool pv

# create a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# install python dependencies
pip3 install -r requirements.txt
```

Run the following commands to get the latest OSM changeset file.
```bash
rm $(ls *.osm.bz2)
wget -N https://planet.openstreetmap.org/planet/changesets-latest.osm.bz2.torrent
aria2c --seed-time 0 --check-integrity changesets-latest.osm.bz2.torrent
```

Next, you can extract the data and save it in a compressed CSV file like this. `pv` is used to generate a progress bar. The extraction can take some time (on my laptop this takes about 1:30h).
```bash
rm -r -d temp
osmium cat --output-format opl $(ls *.osm.bz2) | pv -s 140M -l | python3 src/changeset_to_parquet.py temp
```

If you want to add new topics, plots or tables and iterate faster with a subset of all data, you can use every 500th changeset like this.
```bash
osmium cat --output-format opl $(ls *.osm.bz2) | pv -s 140M -l | sed -n '0~500p' | python3 src/changeset_to_parquet.py temp_dev
```

In case you want to use the .osm filetype instead of .opl, extract the data like this
```bash
bzcat -d $(ls *.osm.bz2) > changesets.osm 
```

And to subset it
```bash
python3 -c "                                                  
import sys, xml.etree.ElementTree as ET
count = 0
print('<?xml version=\"1.0\" encoding=\"UTF-8\"?>')
print('<osm>')
for event, elem in ET.iterparse(sys.stdin, events=('end',)):
    if elem.tag == 'changeset':
        if count % 500 == 0:
            ET.ElementTree(elem).write(sys.stdout, encoding='unicode')
        count += 1
        elem.clear()
print('</osm>')
" < changesets.osm > changesets_subset.osm
```

Then
```bash
cat changesets.osm | python3 src/changeset_to_parquet.py temp osm
```


Next, you can generate the plots and tables like the following command or with `temp_dev` instead of `temp` for the folder name. On my laptop this takes also about 0:30h and it runs with less then 8GB of RAM.
```bash
python3 src/parquet_to_json_stats.py temp
```

### Update notebooks

There are multiple question in `src/questions` and each one has a jupyter notebook to compute the relevant data for the question. To Execute all notebooks run:

```bash
for notebook in $(find src/questions -name calculations.ipynb); do
    jupyter nbconvert --to notebook --execute "$notebook" --output calculations.ipynb
done
```

### Update Organised Teams user names

You can update the list of Organised Teams with their osm user names in assets/organised_teams_contributors.json with the following command.
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

## Website Statistics

There is lightweight tracking with [Plausible](https://plausible.io/about) for the [website](https://piebro.github.io/openstreetmap-statistics/) to get infos about how many people are visiting. Everyone who is interested can look at these stats here: https://plausible.io/piebro.github.io%2Fopenstreetmap-statistics?period=30d. Only users without an AdBlocker are counted, so these statistics are under estimating the actual count of visitors. I would guess that quite a few people (including me) visiting the site have an AdBlocker.


## License

All code in this project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details. All data, maps and plots in this project are licensed under [Attribution 4.0 International (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/).
