import os
import json
import requests
from bs4 import BeautifulSoup


def get_all_users_from_links(url, soup=None):
    if soup is None:
        page = requests.get(url)
        soup = BeautifulSoup(page.content, "html.parser")
    link_prefix = "https://www.openstreetmap.org/user/"
    user_names = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href[:5] == "http:":
            href = f"https{href[4:]}"
        elif href[:2] == "//":
            href = f"https:{href}"
        if href[: len(link_prefix)] == link_prefix:
            user_name = href[len(link_prefix) :]
            user_name = user_name.split("/")[0]
            user_names.append(user_name)

    return [url, user_names]


def get_all_users_from_mapbox_link(url):
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    return [url, get_all_users_from_links(None, soup.find_all("table")[0])[1]]


def get_all_users_from_grab_link(url):
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    users = []
    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) == 0:
            continue
        users.append(str(tds[1])[4:-5])
    return [url, users]


def get_all_users_from_microsoft_link(url):
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    soup = soup.find_all("table")[0]
    users = []
    for td in soup.find_all("td"):
        user = str(td)[4:-6]
        if user != "":
            users.append(user)
    return [url, users]


corporation_to_users = {
    "Amazon": get_all_users_from_links("https://wiki.openstreetmap.org/wiki/Organised_Editing/Activities/Amazon"),
    "Apple": get_all_users_from_links("https://github.com/osmlab/appledata/wiki/Data-Team"),
    "AppLogica": get_all_users_from_links("https://wiki.openstreetmap.org/wiki/AppLogica"),
    "Balad": get_all_users_from_links("https://wiki.openstreetmap.org/wiki/Balad"),
    "Bolt": get_all_users_from_links("https://wiki.openstreetmap.org/wiki/Organised_Editing/Activites/Bolt"),
    "DevSeed": get_all_users_from_links("https://wiki.openstreetmap.org/wiki/DevSeed-Data"),
    "DigitalEgypt": get_all_users_from_links("https://wiki.openstreetmap.org/wiki/DigitalEgypt"),
    "Expedia": get_all_users_from_links("https://github.com/osmlab/expedia/wiki/Data-Team"),
    "Gojek": get_all_users_from_links("https://wiki.openstreetmap.org/wiki/Gojek"),
    "Grab": get_all_users_from_grab_link("https://github.com/GRABOSM/Grab-Data/blob/master/Grab%20Data%20Team.md"),
    "Graphmasters": get_all_users_from_links("https://wiki.openstreetmap.org/wiki/Graphmasters"),
    "Kaart": get_all_users_from_links("https://wiki.openstreetmap.org/wiki/Kaart"),
    "Kontur": get_all_users_from_links("https://wiki.openstreetmap.org/wiki/Kontur"),
    "Lightcyphers": get_all_users_from_links("https://wiki.openstreetmap.org/wiki/Lightcyphers"),
    "Lyft": get_all_users_from_links(
        "https://github.com/OSM-DCT-Lyft/US/wiki/OSM-Team-Members#lyft-mapping-team-osm-ids"
    ),
    "Mapbox": get_all_users_from_mapbox_link("https://wiki.openstreetmap.org/wiki/Mapbox#Mapbox_Data_Team"),
    "Meta": get_all_users_from_links("https://wiki.openstreetmap.org/wiki/Organised_Editing/Activities/Facebook"),
    "Microsoft": get_all_users_from_microsoft_link(
        "https://wiki.openstreetmap.org/wiki/Organised_Editing/Activities/Microsoft"
    ),
    "Neshan": get_all_users_from_links("https://wiki.openstreetmap.org/wiki/Neshan"),
    "NextBillion.AI": get_all_users_from_links("https://wiki.openstreetmap.org/wiki/NextBillion.AI-OSM"),
    "Rocketdata.io": get_all_users_from_links("https://wiki.openstreetmap.org/wiki/Organised_Editing/Rocketdata.io"),
    "Snap": get_all_users_from_links("https://wiki.openstreetmap.org/wiki/Organised_Editing/Activities/Snap"),
    "Snapp": get_all_users_from_links("https://wiki.openstreetmap.org/wiki/Fa:Snapp"),
    "Stackbox": get_all_users_from_links("https://wiki.openstreetmap.org/wiki/Organised_Editing/Activities/Stackbox"),
    "Telenav": get_all_users_from_links("https://wiki.openstreetmap.org/wiki/Telenav"),
    "TfNSW": get_all_users_from_links("https://wiki.openstreetmap.org/wiki/TfNSW"),
    "TIDBO": get_all_users_from_links("https://wiki.openstreetmap.org/wiki/Organised_Editing/Activities/TIDBO"),
    "TomTom": get_all_users_from_links("https://wiki.openstreetmap.org/wiki/Organised_Editing/Activities/TomTom"),
    "Uber": get_all_users_from_links("https://github.com/Uber-OSM/DataTeam"),
    "WIGeoGIS": get_all_users_from_links(
        "https://wiki.openstreetmap.org/wiki/Organised_Editing/Activities/Updating_assets_of_OMV_group"
    ),
    "Wonder": get_all_users_from_links("https://wiki.openstreetmap.org/wiki/Organised_Editing/Activities/Wonder"),
    "Zartico": get_all_users_from_links("https://wiki.openstreetmap.org/wiki/Zartico"),
}

for name in corporation_to_users.keys():
    corporation_to_users[name][1] = sorted(list(set(corporation_to_users[name][1])))
    for i in range(len(corporation_to_users[name][1])):
        user_name = corporation_to_users[name][1][i]
        user_name = user_name.replace("%20", "%20%").replace(" ", "%20%")
        user_name = user_name.replace("%21", "%21%").replace("!", "%21%")
        user_name = user_name.replace("%40", "%40%").replace("@", "%40%")
        corporation_to_users[name][1][i] = user_name

with open(os.path.join("assets", "corporation_contributors.json"), "w") as f:
    json.dump(corporation_to_users, f, sort_keys=True, indent=4)
