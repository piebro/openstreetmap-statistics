import os
import sys
from contextlib import contextmanager
from unittest.mock import mock_open, patch

import duckdb
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import enrich_table


@contextmanager
def mock_json_files(mock_rules):
    """Context manager to mock JSON file reading with provided rules."""
    with (
        patch("builtins.open", mock_open()),
        patch("json.load", return_value=mock_rules),
        patch("pathlib.Path.exists", return_value=True),
    ):
        yield


@pytest.fixture(scope="session")
def db():
    """Create a fresh DuckDB connection for each test."""
    return duckdb.connect()


@pytest.fixture(scope="session")
def expressions():
    """Get column expressions only once per test session."""
    return enrich_table.get_column_expressions()


def run_query(db, select_expression):
    sql_query = f"SELECT {select_expression} FROM main"
    return [row[0] for row in db.execute(sql_query).fetchall()]


def test_expression_mid_pos_x_y(db, expressions):
    """Test mid_pos_x calculation with actual DuckDB queries."""
    db.execute("""
		CREATE OR REPLACE TABLE main AS SELECT * FROM VALUES 
			(-10.5, 45.2, 12.3, 52.1),
			(-10.5, 45.2, 12.3, 52.1),
			(0.0, 0.0, 1.0, 1.0),
			(-74.0, 40.7, -73.9, 40.8),
			(151.2, -33.8, 151.3, -33.7)
		AS t(bottom_left_lon, bottom_left_lat, top_right_lon, top_right_lat)
	""")
    expected_results = [181, 181, 181, 106, 331]
    assert expected_results == run_query(db, expressions["mid_pos_x"])
    expected_results = [139, 139, 91, 131, 56]
    assert expected_results == run_query(db, expressions["mid_pos_y"])


def test_expression_bot(db, expressions):
    """Test bot detection with actual DuckDB queries."""
    db.execute("""
		CREATE OR REPLACE TABLE main AS SELECT * FROM VALUES 
			(map(['bot'], ['yes'])),
			(map(['bot'], ['no'])),
			(map(['bot'], [''])),
			(map()::MAP(VARCHAR, VARCHAR)),
			(map(['other_tag'], ['value']))
		AS t(tags)
	""")
    expected_results = [True, False, False, False, False]
    assert expected_results == run_query(db, expressions["bot"])


def test_expression_created_by(db):
    """Test created_by normalization with mocked JSON rules."""
    mock_rules = {
        "iD": {"starts_with": ["iD ", "ID "], "type": "desktop_editor"},
        "JOSM": {"contains": ["JOSM"], "type": "desktop_editor"},
        "StreetComplete": {"starts_with": ["StreetComplete "], "type": "mobile_editor"},
        "Every Door": {"starts_with": ["Every Door "], "type": "mobile_editor"},
        "https://osm.wikidata.link/": {"ends_with": [".wikidata.link/"], "link": "https://osm.wikidata.link/"},
        "Mapzen": {
            "aliases": ["Mapzen Alpha 1"],
            "link": "https://wiki.openstreetmap.org/wiki/Mapzen",
            "type": "mobile_editor",
        },
    }

    with mock_json_files(mock_rules):
        created_by_expression = enrich_table.get_created_by_case_statement()
        print(created_by_expression)
        db.execute("""
            CREATE OR REPLACE TABLE main AS SELECT * FROM VALUES 
                (map(['created_by'], ['iD 2.18.5'])),
                (map(['created_by'], ['JOSM/18629'])),
                (map(['created_by'], ['StreetComplete 34.1'])),
                (map(['created_by'], ['Some Unknown Editor'])),
                (map(['created_by'], ['Every Door 4.0'])),
                (map(['created_by'], ['https://osm123.wikidata.link/'])),
                (map(['created_by'], ['Mapzen Alpha 1'])),
            AS t(tags)
        """)

        expected_results = [
            "iD",
            "JOSM",
            "StreetComplete",
            "Some Unknown Editor",
            "Every Door",
            "https://osm.wikidata.link/",
            "Mapzen",
        ]
        assert expected_results == run_query(db, created_by_expression)


def test_expression_device_type(db):
    """Test device type classification."""
    mock_rules = {
        "iD": {"starts_with": ["iD "], "type": "desktop_editor"},
        "JOSM": {"contains": ["JOSM"], "type": "desktop_editor"},
        "StreetComplete": {"starts_with": ["StreetComplete "], "type": "mobile_editor"},
        "Every Door": {"starts_with": ["Every Door "], "type": "mobile_editor"},
        "overpass-turbo": {"aliases": ["overpass-turbo"], "type": "tool"},
        "MapComplete": {"aliases": ["MapComplete"], "type": "web_editor"},  # not in allowed types
        "Unknown Editor": {"aliases": ["Unknown Editor"]},  # no type field
    }

    with mock_json_files(mock_rules):
        device_type_expression = enrich_table.get_device_type_case_statement()
        db.execute("""
            CREATE OR REPLACE TABLE main AS SELECT * FROM VALUES 
                ('iD'),
                ('JOSM'),
                ('StreetComplete'),
                ('Every Door'),
                ('overpass-turbo'),
                ('MapComplete'),  -- web_editor not in allowed types
                ('Unknown Editor'),  -- no type field
                ('Completely Unknown'),  -- not in rules
            AS t(created_by)
        """)

        expected_results = [
            "desktop_editor",  # iD
            "desktop_editor",  # JOSM
            "mobile_editor",  # StreetComplete
            "mobile_editor",  # Every Door
            "tool",  # overpass-turbo
            "other",  # MapComplete (web_editor not allowed)
            "other",  # Unknown Editor (no type field)
            "other",  # Completely Unknown (not in rules)
        ]

        results = run_query(db, device_type_expression)
        assert expected_results == results


def test_expression_imagery_used(db):
    """Test imagery_used normalization with key edge cases."""
    mock_rules = {
        ".gpx data file": {
            "aliases": ["Local GPX", "data file"],
            "ends_with": [".gpx"],
            "starts_with": ["GPX "],
        },
        "Bing Aerial Imagery": {
            "aliases": ["Bing", "bing", "aerial imagery"],
            "starts_with": ["Bing "],
        },
        "Survey": {
            "starts_with": ["survey", "Survey"],
        },
        "Custom": {
            "starts_with": ["Custom, ("],
        },
        "contains_test": {
            "contains": ["middle_text"],
        },
    }

    with mock_json_files(mock_rules):
        imagery_used_expression = enrich_table.get_imagery_used_case_statement()
        db.execute("""
            CREATE OR REPLACE TABLE main AS SELECT * FROM VALUES 
                (map(['imagery_used'], ['Local GPX;test.gpx'])),
                (map(['imagery_used'], ['test%20%file.gpx;  Bing Maps  '])),
                (map(['imagery_used'], [''])),
                (map()::MAP(VARCHAR, VARCHAR)),
                (map(['imagery_used'], ['Survey 2023;before_middle_text_after;unknown_source'])),
                (map(['imagery_used'], ['Custom%2c% (Survey test)'])),
            AS t(tags)
        """)

        expected_results = [
            [".gpx data file", ".gpx data file"],
            [".gpx data file", "Bing Aerial Imagery"],
            None,  # empty string
            None,  # null map
            ["Survey", "contains_test", "unknown_source"],
            ["Custom"],
        ]

        results = run_query(db, imagery_used_expression)
        assert expected_results == results


def test_expression_hashtags(db, expressions):
    """Test hashtags parsing with actual DuckDB queries."""
    db.execute("""
		CREATE OR REPLACE TABLE main AS SELECT * FROM VALUES 
			(map(['hashtags'], ['HOTOSM;MissingMaps'])),
			(map(['hashtags'], ['MyProject;Test;VALIDATION'])),
			(map(['hashtags'], [''])),
			(map()::MAP(VARCHAR, VARCHAR)),
			(map(['hashtags'], ['singlehashtag']))
		AS t(tags)
	""")
    expected_results = [["hotosm", "missingmaps"], ["myproject", "test", "validation"], None, None, ["singlehashtag"]]
    assert expected_results == run_query(db, expressions["hashtags"])


def test_expression_source(db):
    """Test source normalization with multiple separators."""
    mock_rules = {
        "Survey": {
            "starts_with": ["survey", "Survey"],
        },
        "GPS": {
            "aliases": ["gps", "GPS traces"],
            "starts_with": ["GPS "],
        },
        "Local Knowledge": {
            "aliases": ["local knowledge", "Local knowledge"],
        },
        "Bing Aerial Imagery": {
            "aliases": ["Bing", "bing"],
            "starts_with": ["Bing "],
        },
        "Knowledge": {
            "aliases": ["knowledge"],
        },
    }

    with mock_json_files(mock_rules):
        source_expression = enrich_table.get_source_case_statement()
        db.execute("""
            CREATE OR REPLACE TABLE main AS SELECT * FROM VALUES 
                (map(['source'], ['Survey;GPS traces / Local knowledge & bing | knowledge + unknown'])),
                (map(['source'], ['  survey  ;  GPS 123  '])),
                (map(['source'], [''])),
                (map()::MAP(VARCHAR, VARCHAR)),
                (map(['source'], ['knowledge'])),
                (map(['source'], ['unknown source'])),
            AS t(tags)
        """)

        expected_results = [
            ["Survey", "GPS", "Local Knowledge", "Bing Aerial Imagery", "Knowledge", "unknown"],
            ["Survey", "GPS"],
            None,
            None,
            ["Knowledge"],
            ["unknown source"],
        ]

        results = run_query(db, source_expression)
        assert expected_results == results


def test_expression_mobile_os(db, expressions):
    """Test mobile OS detection from created_by field."""
    db.execute("""
        CREATE OR REPLACE TABLE main AS SELECT * FROM VALUES 
            (map(['created_by'], ['StreetComplete 34.1 Android'])),
            (map(['created_by'], ['Every Door 4.0 iOS'])),
            (map(['created_by'], ['OsmAnd~ 4.2.7 android'])),
            (map(['created_by'], ['Go Map!! 2.15.2 ios'])),
            (map(['created_by'], ['JOSM/18629'])),  -- desktop editor
            (map(['created_by'], ['AndroidApp'])),  -- contains android
            (map(['created_by'], ['MyiOSApp'])),    -- contains ios
            (map()::MAP(VARCHAR, VARCHAR)),         -- null
        AS t(tags)
    """)

    expected_results = [
        "Android",  # StreetComplete Android
        "iOS",  # Every Door iOS
        "Android",  # OsmAnd android (lowercase)
        "iOS",  # Go Map!! ios (lowercase)
        None,  # JOSM (no mobile OS)
        "Android",  # AndroidApp (contains android)
        "iOS",  # MyiOSApp (contains ios)
        None,  # null tags
    ]

    results = run_query(db, expressions["mobile_os"])
    assert expected_results == results


def test_expression_streetcomplete_quest(db, expressions):
    """Test StreetComplete quest type normalization."""
    db.execute("""
        CREATE OR REPLACE TABLE  main AS SELECT * FROM VALUES 
            (map(['created_by', 'StreetComplete:quest_type'], ['StreetComplete', 'AddAccessibleForPedestrians'])),
            (map(['created_by', 'StreetComplete:quest_type'], ['StreetComplete', 'AddWheelChairAccessPublicTransport'])),
            (map(['created_by', 'StreetComplete:quest_type'], ['StreetComplete', 'AddWheelChairAccessToilets'])),
            (map(['created_by', 'StreetComplete:quest_type'], ['StreetComplete', 'AddSidewalks'])),
            (map(['created_by', 'StreetComplete:quest_type'], ['StreetComplete', 'AddHousenumber'])),  -- no mapping
            (map(['created_by'], ['StreetComplete'])),  -- no quest_type
            (map()::MAP(VARCHAR, VARCHAR)),  -- null
        AS t(tags)
    """)

    expected_results = [
        "AddProhibitedForPedestrians",  # mapped from AddAccessibleForPedestrians
        "AddWheelchairAccessPublicTransport",  # mapped from AddWheelChairAccessPublicTransport
        "AddWheelchairAccessPublicTransport",  # mapped from AddWheelChairAccessToilets
        "AddSidewalk",  # mapped from AddSidewalks
        "AddHousenumber",  # no mapping, return as-is
        None,  # StreetComplete but no quest_type
        None,  # null
    ]

    results = run_query(db, expressions["streetcomplete_quest"])
    assert expected_results == results


def test_expression_all_tags(db, expressions):
    """Test extraction of unique tag prefixes."""
    db.execute("""
        CREATE OR REPLACE TABLE main AS SELECT * FROM VALUES 
            (map(['highway', 'name', 'surface'], ['residential', 'Main St', 'asphalt'])),
            (map(['building', 'building:levels', 'building:material'], ['house', '2', 'brick'])),
            (map(['amenity', 'name:en', 'name:de', 'wheelchair'], ['restaurant', 'Restaurant', 'Restaurant', 'yes'])),
            (map(['addr:street', 'addr:housenumber', 'addr:city'], ['Main St', '123', 'City'])),
            (map()::MAP(VARCHAR, VARCHAR)),  -- empty map
        AS t(tags)
    """)

    expected_results = [
        ["highway", "name", "surface"],  # basic tags
        ["building"],  # building: prefix becomes just "building"
        ["amenity", "name", "wheelchair"],  # name:en and name:de both become "name"
        ["addr"],  # all addr: prefixes become "addr"
        [],  # empty map
    ]

    results = run_query(db, expressions["all_tags"])
    # Sort each result list for comparison since array_distinct may not preserve order
    results = [sorted(r) if r else r for r in results]
    expected_results = [sorted(r) if r else r for r in expected_results]
    assert expected_results == results


def test_expression_corporation(expressions):
    """Test corporation mapping from user names using lookup table."""
    mock_corp_data = {
        "Amazon": {
            "usernames": ["amazon1", "amazon2", "amazon3"],
            "for_profit": True,
        },
        "Apple": {
            "usernames": ["apple_user1", "apple_user2"],
            "for_profit": True,
        },
        "Microsoft": {
            "usernames": ["microsoft_user1", "microsoft_user2"],
            "for_profit": False,
        },
    }

    with mock_json_files(mock_corp_data):
        enrich_table.create_organised_team_lookup_table()

        duckdb.sql("""
            CREATE OR REPLACE TABLE  main AS SELECT * FROM VALUES
                ('amazon1'),
                ('amazon2'),
                ('amazon3'),
                ('apple_user1'),
                ('microsoft_user1'),
                ('microsoft_user2'),
            AS t(user_name)
        """)

        sql_query = f"""
            SELECT {expressions["organised_team"]}
            FROM  main
            LEFT JOIN organised_team_lookup team_lookup ON  main.user_name = team_lookup.user_name
        """
        results = [row[0] for row in duckdb.sql(sql_query).fetchall()]

        expected_results = [
            "Amazon",  # amazon1
            "Amazon",  # amazon2
            "Amazon",  # amazon3
            "Apple",  # apple_user1
            "Microsoft",  # microsoft_user1
            "Microsoft",  # microsoft_user2
        ]

        assert expected_results == results
