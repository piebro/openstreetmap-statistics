import json
from pathlib import Path

import requests
from bs4 import BeautifulSoup


def get_usernames_from_links(url, soup=None):
    if soup is None:
        page = requests.get(url, timeout=10)
        soup = BeautifulSoup(page.content, "html.parser")
    link_prefix = "https://www.openstreetmap.org/user/"
    user_names = []
    for a in soup.find_all("a", href=True):
        href = a["href"]

        if href[:5] == "http:":
            href = f"https{href[4:]}"
        elif href[:2] == "//":
            href = f"https:{href}"
        elif href[:2] == '\\"':
            href = href[2:]

        if href[-2:] == '\\"':
            href = href[:-2]

        if href[: len(link_prefix)] == link_prefix:
            user_name = href[len(link_prefix) :]
            user_name = user_name.split("/")[0]
            user_names.append(user_name)

    return user_names


def get_usernames_from_tables(url, column_index):
    page = requests.get(url, timeout=10)
    soup = BeautifulSoup(page.content, "html.parser")
    users = []
    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) == 0:
            continue
        user = str(tds[column_index]).replace("<td>", "").replace("</td>", "").replace("\n", "")
        if user != "User Name":
            users.append(user)
    return users


def get_usernames_from_microsoft_link(url):
    page = requests.get(url, timeout=10)
    soup = BeautifulSoup(page.content, "html.parser")
    soup = soup.find_all("table")[0]
    users = []
    for td in soup.find_all("td"):
        user = str(td)[4:-6]
        if user != "":
            users.append(user)
    return users


# for these organizations look at teams here: https://wiki.openstreetmap.org/wiki/Category:Organised_Editing_Teams or
# https://wiki.openstreetmap.org/wiki/Organised_Editing/Activities
name_url_function = [
    ["Amazon", "https://wiki.openstreetmap.org/wiki/Organised_Editing/Activities/Amazon", get_usernames_from_links],
    ["Apple", "https://github.com/osmlab/appledata/wiki/Data-Team", get_usernames_from_links],
    ["AppLogica", "https://wiki.openstreetmap.org/wiki/AppLogica", get_usernames_from_links],
    ["Balad", "https://wiki.openstreetmap.org/wiki/Balad", get_usernames_from_links],
    ["Bolt", "https://wiki.openstreetmap.org/wiki/Organised_Editing/Activites/Bolt", get_usernames_from_links],
    ["DevSeed", "https://wiki.openstreetmap.org/wiki/DevSeed-Data", get_usernames_from_links],
    ["DigitalEgypt", "https://wiki.openstreetmap.org/wiki/DigitalEgypt", get_usernames_from_links],
    ["Expedia", "https://github.com/osmlab/expedia/wiki/Data-Team", get_usernames_from_links],
    ["Gojek", "https://wiki.openstreetmap.org/wiki/Gojek", get_usernames_from_links],
    [
        "Grab",
        "https://github.com/GRABOSM/Grab-Data/blob/master/Grab%20Data%20Team.md",
        lambda url: get_usernames_from_tables(url, 1),
    ],
    ["Graphmasters", "https://wiki.openstreetmap.org/wiki/Graphmasters", get_usernames_from_links],
    ["Kaart", "https://wiki.openstreetmap.org/wiki/Kaart", get_usernames_from_links],
    ["Komoot", "https://wiki.openstreetmap.org/wiki/Organised_Editing/Activities/komoot", get_usernames_from_links],
    ["Kontur", "https://wiki.openstreetmap.org/wiki/Kontur", get_usernames_from_links],
    ["Lightcyphers", "https://wiki.openstreetmap.org/wiki/Lightcyphers", get_usernames_from_links],
    [
        "Lyft",
        "https://github.com/OSM-DCT-Lyft/US/wiki/OSM-Team-Members#lyft-mapping-team-osm-ids",
        get_usernames_from_links,
    ],
    ["Mapbox", "https://wiki.openstreetmap.org/wiki/Mapbox#Mapbox_Data_Team", get_usernames_from_links],
    ["Mapbox", "https://wiki.openstreetmap.org/wiki/Mapbox/Previous_Team_Members", get_usernames_from_links],
    ["Meta", "https://wiki.openstreetmap.org/wiki/Organised_Editing/Activities/Facebook", get_usernames_from_links],
    [
        "Microsoft",
        "https://wiki.openstreetmap.org/wiki/Organised_Editing/Activities/Microsoft",
        get_usernames_from_microsoft_link,
    ],
    ["Neshan", "https://wiki.openstreetmap.org/wiki/Neshan", get_usernames_from_links],
    ["NextBillion.AI", "https://wiki.openstreetmap.org/wiki/NextBillion.AI-OSM", get_usernames_from_links],
    ["Ola", "https://wiki.openstreetmap.org/wiki/Ola", get_usernames_from_links],
    ["Rocketdata.io", "https://wiki.openstreetmap.org/wiki/Organised_Editing/Rocketdata.io", get_usernames_from_links],
    ["Snap", "https://wiki.openstreetmap.org/wiki/Organised_Editing/Activities/Snap", get_usernames_from_links],
    ["Snapp", "https://wiki.openstreetmap.org/wiki/Fa:Snapp", get_usernames_from_links],
    ["Stackbox", "https://wiki.openstreetmap.org/wiki/Organised_Editing/Activities/Stackbox", get_usernames_from_links],
    [
        "Swiggy",
        "https://wiki.openstreetmap.org/wiki/Organised_Editing/Activities/Swiggy",
        lambda url: get_usernames_from_tables(url, 1),
    ],
    [
        "TeleClinic",
        "https://wiki.openstreetmap.org/wiki/Organised_Editing/Activities/TeleClinic",
        get_usernames_from_links,
    ],
    ["Telenav", "https://wiki.openstreetmap.org/wiki/Telenav", get_usernames_from_links],
    ["TfNSW", "https://wiki.openstreetmap.org/wiki/TfNSW", get_usernames_from_links],
    ["TIDBO", "https://wiki.openstreetmap.org/wiki/Organised_Editing/Activities/TIDBO", get_usernames_from_links],
    ["TomTom", "https://wiki.openstreetmap.org/wiki/Organised_Editing/Activities/TomTom", get_usernames_from_links],
    ["Uber", "https://github.com/Uber-OSM/DataTeam/blob/master/README.md", get_usernames_from_links],
    [
        "VK_Maps",
        "https://wiki.openstreetmap.org/wiki/Organised_Editing/Activities/VK_Maps_Team",
        get_usernames_from_links,
    ],
    [
        "WIGeoGIS",
        "https://wiki.openstreetmap.org/wiki/Organised_Editing/Activities/Updating_assets_of_OMV_group",
        get_usernames_from_links,
    ],
    ["Wonder", "https://wiki.openstreetmap.org/wiki/Organised_Editing/Activities/Wonder", get_usernames_from_links],
    ["Zartico", "https://wiki.openstreetmap.org/wiki/Zartico", get_usernames_from_links],
]

name_to_url_username = {}
for i, (name, url, func) in enumerate(name_url_function):
    print(f"{i+1:02d} / {len(name_url_function)}: {name}")
    usernames = func(url)

    usernames = sorted(set(usernames))
    usernames = [
        username.replace("%20", "%20%")
        .replace(" ", "%20%")
        .replace("%21", "%21%")
        .replace("!", "%21%")
        .replace("%40", "%40%")
        .replace("@", "%40%")
        for username in usernames
    ]
    print("usernames:", usernames)
    if name not in name_to_url_username:
        name_to_url_username[name] = [url, usernames]
    else:
        name_to_url_username[name][1].extend(usernames)

with (Path("assets") / "corporation_contributors.json").open("w") as f:
    json.dump(name_to_url_username, f, sort_keys=True, indent=4)
