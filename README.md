# OpenStreetMap Statistics

Monthly updated statistics of OpenStreetMap. There is a [website](https://piebro.github.io/openstreetmap-statistics) to browse the generated plots and tables.

The plots and tables are organized in topics and questions I asked myself about OpenStreetMap. My Motivation for this project was that I couldn't find some statistics I was interested in or that the data was outdated. That's why I created these statistics, which are easily updatable with a simple script run locally or with GitHub actions. 


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

### Editing Software

Most changesets have a `created_by` tag which indicates which editing software was used to make the changes.
It's necessary to filter out version numbers or other information in the tag to get a better overview of what kind of editors are used.
The filtering is done with regex expression, which can be seen at [src/save_changesets_csv.py](src/save_changesets_csv.py).
The regex can eliminate the additional info but preserve the original editing software name.
In some cases, it might fail and be a source of error.
Feel free to open an issue if you see cases where the regex is not working correctly.

### Imagery Software

One optional tag for changesets is the imagery tag, which is used to add an image source if aerial or other imagery is used. 
It's necessary to filter the tag to get an overview of the most common image sources.
The filtering is done with `if` statements and can be reviewed at [src/save_changesets_csv.py](src/save_changesets_csv.py).
Feel free to open an issue if the filter is not working as intended.

### Cooperations

Most mapping is done by individual hobby mappers mapping independently, but there are also organized mapping activities where several people edit the map under specific instructions of others.
A list of all organized editing teams can be found [here](https://wiki.openstreetmap.org/wiki/Category:Organised_Editing_Teams).
The teams list all users (including inactive ones) who are mapping for them for transparency reasons.

I looked at each team in the list and added all the for-profit companies I could find.
The companies are added to [src/save_corporation_contibutors.py](src/save_corporation_contibutors.py), which extracts all user names and saves them in the assets folder.
The cooperation statistics are gathered with the list of users working at each company.
Incorrect and out-of-date user lists could be a source of error in the data.


## Usage

### Create plots and tables locally

The code is tested on Ubuntu but should work on every Linux distro. I'm not sure about Windows or Mac.

Install dependencies for downloading and handling the latest changeset and showing a progress bar `sudo apt install aria2 osmium-tool pv`.
Furthermore, install python 3.8 or higher `sudo apt install python3.8` with Numpy `pip3 install numpy`. 

Run the following commands to get the latest OSM changeset file.
```bash
wget -N https://planet.openstreetmap.org/planet/changesets-latest.osm.bz2.torrent
aria2c --seed-time 0 --check-integrity changesets-latest.osm.bz2.torrent
```

Next, you can extract the data and save it in a compressed CSV file like this. `pv` is used to generate a progress bar. The extraction can take some time.
```bash
mkdir temp
osmium cat --output-format opl $(ls *.osm.bz2) | pv -s 130M -l | python3 src/save_changesets_csv.py temp | gzip > temp/changesets.csv.gz
```

If you want to add new topics, plots or tables and iterate faster with a subset of all data, you can use every 500th changeset like this.
```bash
mkdir temp_dev
osmium cat --output-format opl $(ls *.osm.bz2) | pv -s 130M -l | sed -n '0~500p' | python3 src/save_changesets_csv.py temp_dev | gzip > temp_dev/changesets.csv.gz
```

Next, you can generate the plots and tables like the following command or with `temp_dev` instead of `temp` for the folder name. If you create a new topic, you can add it to the `generate_plots.sh` script.
```bash
bash generate_plots.sh temp
```

### Update cooperation user names

You can update the list of cooperation with their osm user names in assets/corporation_contributors.json with the following command.
```bash
python3 src/save_corporation_contibutors.py
```

### Update background map

You can update the background map in assets/background_map.png with the following command after installing two additional python dependencies like this `pip3 install geopandas pillow` and with a shapefile from https://www.naturalearthdata.com/downloads/110m-physical-vectors/.
```bash
python3 save_background_map.py <path-to-ne-110m-land-shape-file.shp>
```

### Update plotly-custom.min.js

Plotly custom is generated with these instructions https://github.com/plotly/plotly.js/blob/master/CUSTOM_BUNDLE.md using the following command.
```bash
npm run custom-bundle -- --traces scatter,histogram2d --transforms none
```
This has the advantage of having a smaller plotly file while still being able to generate all needed plots.

## Contributing

If there are other topics and questions about OpenStreetMap you think are interesting and that can be abstracted from the changeset, feel free to open an issue or create a pull request.
Also, if you see any typos or other mistakes, feel free to correct them and create a pull request.


## License

All code in this project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details. All data, maps and plots in this project are licensed under [Attribution 4.0 International (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/).
