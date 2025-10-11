import json
import re
import urllib.parse
from pathlib import Path

import requests
from bs4 import BeautifulSoup


def get_usernames_from_link(url, soup=None):
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


def get_usernames_from_grab_md(url):
    page = requests.get(url, timeout=10)
    content = page.text
    usernames = []
    lines = content.split("\n")
    for line_raw in lines:
        line = line_raw.strip()
        if line.startswith("| "):
            seconde_cell = line.split("|")[2]
            if seconde_cell.startswith(" OSM User Name"):
                continue
            if seconde_cell.startswith(" ----"):
                continue
            usernames.append(seconde_cell.strip())
    return usernames


def get_usernames_from_uber_md(url):
    page = requests.get(url, timeout=10)
    content = page.text
    pattern = r"https://www\.openstreetmap\.org/user/([^)]+)"
    usernames = re.findall(pattern, content)
    return usernames


def main():
    with Path("config/organised_teams.json").open(encoding="utf-8") as f:
        organised_teams = json.load(f)

    name_to_data = {}
    for i, org in enumerate(organised_teams):
        name, url, for_profit = org["name"], org["url"], org["for_profit"]
        print(f"{i + 1:02d} / {len(organised_teams)}: {name}")

        if name == "Grab":
            usernames = get_usernames_from_grab_md(url)
        elif name == "Microsoft":
            usernames = get_usernames_from_microsoft_link(url)
        elif name == "Uber":
            usernames = get_usernames_from_uber_md(url)
        else:
            usernames = get_usernames_from_link(url)

        usernames = sorted(set(usernames))
        usernames = [urllib.parse.unquote(username) for username in usernames]
        name_to_data[name] = {"url": url, "for_profit": for_profit, "usernames": usernames}

    with (Path("config") / "organised_teams_contributors.json").open("w") as f:
        json.dump(name_to_data, f, sort_keys=True, indent=4)


if __name__ == "__main__":
    main()
