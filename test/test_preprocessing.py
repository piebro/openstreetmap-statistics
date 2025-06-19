import json
from contextlib import contextmanager
from unittest.mock import MagicMock, mock_open, patch

import pytest

import openstreetmap_statistics.preprocessing as preprocessing


@contextmanager
def mock_json_file_read(file_content):
    """Create a complete mock setup for reading JSON file content."""
    if not isinstance(file_content, str):
        file_content = json.dumps(file_content)

    mock_file = mock_open(read_data=file_content)

    with patch("builtins.open", mock_file):
        with patch("openstreetmap_statistics.preprocessing.Path") as mock_path_class:
            mock_path_instance = MagicMock()
            mock_path_class.return_value = mock_path_instance
            mock_path_instance.__truediv__.return_value = mock_path_instance
            mock_path_instance.open.return_value = mock_file.return_value

            yield


@pytest.mark.parametrize(
    "input_data,expected_output",
    [
        # Test case 1: Basic aliases
        (
            {
                "JOSM": {"aliases": ["JOSM/1.5", "JOSM/1.6", "JOSM/1.7"], "type": "desktop_editor"},
                "iD": {"aliases": ["iD/2.0", "iD/3.0"], "type": "web_editor"},
            },
            {
                "tag_to_name": {
                    "JOSM/1.5": "JOSM",
                    "JOSM/1.6": "JOSM",
                    "JOSM/1.7": "JOSM",
                    "iD/2.0": "iD",
                    "iD/3.0": "iD",
                },
                "starts_with_list": [],
                "ends_with_list": [],
                "contains_list": [],
            },
        ),
        # Test case 2: Basic starts_with rules
        (
            {
                "Potlatch": {"starts_with": ["Potlatch "], "type": "web_editor"},
                "Vespucci": {"starts_with": ["Vespucci/", "Vespucci "], "type": "mobile_editor"},
            },
            {
                "tag_to_name": {},
                "starts_with_list": [
                    (9, "Potlatch ", "Potlatch"),
                    (9, "Vespucci/", "Vespucci"),
                    (9, "Vespucci ", "Vespucci"),
                ],
                "ends_with_list": [],
                "contains_list": [],
            },
        ),
        # Test case 3: Basic ends_with rules
        (
            {
                "StreetComplete": {"ends_with": [" StreetComplete"], "type": "mobile_editor"},
                "Go Map!!": {"ends_with": [" Go Map!!"], "type": "mobile_editor"},
            },
            {
                "tag_to_name": {},
                "starts_with_list": [],
                "ends_with_list": [(15, " StreetComplete", "StreetComplete"), (9, " Go Map!!", "Go Map!!")],
                "contains_list": [],
            },
        ),
        # Test case 4: Basic contains rules
        (
            {"Bing": {"contains": ["Bing"], "type": "imagery"}, "Google": {"contains": ["Google"], "type": "imagery"}},
            {
                "tag_to_name": {},
                "starts_with_list": [],
                "ends_with_list": [],
                "contains_list": [("Bing", "Bing"), ("Google", "Google")],
            },
        ),
        # Test case 5: Mixed rules
        (
            {
                "JOSM": {
                    "aliases": ["JOSM/1.5"],
                    "starts_with": ["JOSM "],
                    "ends_with": [" JOSM"],
                    "contains": ["JOSM"],
                    "type": "desktop_editor",
                }
            },
            {
                "tag_to_name": {"JOSM/1.5": "JOSM"},
                "starts_with_list": [(5, "JOSM ", "JOSM")],
                "ends_with_list": [(5, " JOSM", "JOSM")],
                "contains_list": [("JOSM", "JOSM")],
            },
        ),
        # Test case 6: Empty input
        ({}, {"tag_to_name": {}, "starts_with_list": [], "ends_with_list": [], "contains_list": []}),
    ],
)
def test_create_replace_rules(input_data, expected_output):
    """Test create_replace_rules with various input scenarios."""
    with mock_json_file_read(input_data):
        result = preprocessing.create_replace_rules("config/replace_rules_created_by.json")
        assert result == expected_output


@pytest.mark.parametrize(
    "input_data,expected_output",
    [
        # Test case 1: Basic corporation mapping
        (
            {
                "Microsoft": ("https://microsoft.com", ["user1", "user2"]),
                "Google": ("https://google.com", ["user3"]),
            },
            {
                "user1": "Microsoft",
                "user2": "Microsoft",
                "user3": "Google",
            },
        ),
    ],
)
def test_load_user_name_to_corporation_dict(input_data, expected_output):
    """Test load_user_name_to_corporation_dict with various input scenarios."""
    with mock_json_file_read(input_data):
        result = preprocessing.load_user_name_to_corporation_dict("config/corporation_contributors.json")
        assert result == expected_output


@pytest.mark.parametrize(
    "input_string,expected_output",
    [
        # Test case 1: Simple separator splitting
        ("source1;source2", ["source1", "source2"]),
        ("source1|source2|source3", ["source1", "source2", "source3"]),
        ("source1+source2", ["source1", "source2"]),
        ("source1&source2", ["source1", "source2"]),
        ("source1,source2", ["source1", "source2"]),
        # Test case 2: Mixed separators
        ("source1;source2|source3+source4", ["source1", "source2", "source3", "source4"]),
        # Test case 3: Brackets protection - parentheses
        ("source1;func(param1;param2);source2", ["source1", "func(param1;param2)", "source2"]),
        # Test case 5: Nested brackets
        ("source1;func(inner(a;b);c);source2", ["source1", "func(inner(a;b);c)", "source2"]),
        ("source1;{outer:{inner;value}};source2", ["source1", "{outer:{inner;value}}", "source2"]),
        # Test case 6: URL protection (special case for '/')
        ("https://example.com/path;source2", ["https://example.com/path", "source2"]),
        ("http://example.com/path/subpath;source2", ["http://example.com/path/subpath", "source2"]),
        ("source1;https://api.example.com/v1/data", ["source1", "https://api.example.com/v1/data"]),
        # Test case 7: Normal slash splitting (no http)
        ("source1/source2", ["source1", "source2"]),
        # Test case 8: Empty parts and edge cases
        ("source1;;source2", ["source1", "", "source2"]),
        (";source1;", ["", "source1", ""]),
        ("", [""]),
        ("single_source", ["single_source"]),
        # Test case 9: Unmatched brackets (should still work)
        ("source1;(unmatched;source2", ["source1", "(unmatched;source2"]),
        ("source1;unmatched);source2", ["source1", "unmatched)", "source2"]),
    ],
)
def test_split_source_excluding_brackets(input_string, expected_output):
    """Test split_source_excluding_brackets with various input scenarios."""
    result = preprocessing.split_source_excluding_brackets(input_string)
    assert result == expected_output


@pytest.mark.parametrize(
    "input_tags,expected_output",
    [
        # Test case 1: Basic tags with prefixes and filtering
        (
            {"changesets_count": "primary", "comment": "some comment", "imagery_used": "bing", "created_by": "JOSM"},
            ["changesets_count", "comment", "created_by", "imagery_used"]
        ),
        # Test case 2: Tags with colons - should extract prefixes only
        (
            {"local:website": "en", "local:os": "en", "comment": "some comment"},
            ["comment", "local"]
        ),
    ],
)
def test_extract_all_tags(input_tags, expected_output):
    """Test extract_all_tags with various input scenarios."""
    result = preprocessing.extract_all_tags(input_tags)
    assert result == expected_output


@pytest.mark.parametrize(
    "bottom_left_lon,bottom_left_lat,top_right_lon,top_right_lat,bounds_valid,expected_output",
    [
        # Test case 1: Valid bounds - center coordinates
        (-1.0, 51.0, 1.0, 53.0, True, (180, 142)),  # center at (0, 52) -> (180, 142)
        # Test case 2: Valid bounds - different location
        (2.0, 48.0, 4.0, 50.0, True, (183, 139)),  # center at (3, 49) -> (183, 139) 
        # Test case 4: Invalid bounds
        (0, 0, 0, 0, False, (-1, -1)),
    ],
)
def test_get_normalized_coordinates(bottom_left_lon, bottom_left_lat, top_right_lon, top_right_lat, bounds_valid, expected_output):
    """Test get_normalized_coordinates with various input scenarios."""
    # Mock changeset object with bounds
    mock_changeset = MagicMock()
    mock_changeset.bounds.valid.return_value = bounds_valid
    
    if bounds_valid:
        mock_changeset.bounds.bottom_left.lon = bottom_left_lon
        mock_changeset.bounds.bottom_left.lat = bottom_left_lat
        mock_changeset.bounds.top_right.lon = top_right_lon
        mock_changeset.bounds.top_right.lat = top_right_lat
    
    result = preprocessing.get_normalized_coordinates(mock_changeset)
    assert result == expected_output


@pytest.mark.parametrize(
    "tag,replace_rules,expected_output",
    [
        # Test case 1: Exact match in tag_to_name
        (
            "JOSM/1.5",
            {
                "tag_to_name": {"JOSM/1.5": "JOSM", "iD/2.0": "iD"},
                "starts_with_list": [],
                "ends_with_list": [],
                "contains_list": [],
            },
            "JOSM"
        ),
        # Test case 2: Starts with match
        (
            "Potlatch 2.0",
            {
                "tag_to_name": {},
                "starts_with_list": [(9, "Potlatch ", "Potlatch")],
                "ends_with_list": [],
                "contains_list": [],
            },
            "Potlatch"
        ),
        # Test case 3: Ends with match
        (
            "Android StreetComplete",
            {
                "tag_to_name": {},
                "starts_with_list": [],
                "ends_with_list": [(15, " StreetComplete", "StreetComplete")],
                "contains_list": [],
            },
            "StreetComplete"
        ),
        # Test case 4: Contains match
        (
            "Bing aerial imagery",
            {
                "tag_to_name": {},
                "starts_with_list": [],
                "ends_with_list": [],
                "contains_list": [("Bing", "Bing"), ("Google", "Google")],
            },
            "Bing"
        ),
        # Test case 5: No match - return original tag
        (
            "Unknown Editor",
            {
                "tag_to_name": {"JOSM/1.5": "JOSM"},
                "starts_with_list": [(9, "Potlatch ", "Potlatch")],
                "ends_with_list": [(15, " StreetComplete", "StreetComplete")],
                "contains_list": [("Bing", "Bing")],
            },
            "Unknown Editor"
        ),
        # Test case 6: Priority order - tag_to_name wins over starts_with
        (
            "JOSM/1.5",
            {
                "tag_to_name": {"JOSM/1.5": "JOSM_Exact"},
                "starts_with_list": [(4, "JOSM", "JOSM_StartsWith")],
                "ends_with_list": [],
                "contains_list": [],
            },
            "JOSM_Exact"
        ),
    ],
)
def test_replace_with_rules(tag, replace_rules, expected_output):
    """Test replace_with_rules with various input scenarios."""
    result = preprocessing.replace_with_rules(tag, replace_rules)
    assert result == expected_output


@pytest.mark.parametrize(
    "tags,replace_rules,expected_output",
    [
        # Test case 1: Normal created_by tag with replacement rule
        (
            {"created_by": "JOSM/1.5"},
            {
                "tag_to_name": {"JOSM/1.5": "JOSM"},
                "starts_with_list": [],
                "ends_with_list": [],
                "contains_list": [],
            },
            "JOSM"
        ),
        # Test case 2: created_by tag with URL encoding that needs cleanup
        (
            {"created_by": "iD%20%2.0%2c%beta"},
            {
                "tag_to_name": {"iD 2.0,beta": "iD"},
                "starts_with_list": [],
                "ends_with_list": [],
                "contains_list": [],
            },
            "iD"
        ),
        # Test case 3: No created_by tag - should return None
        (
            {"highway": "primary", "name": "Main Street"},
            {
                "tag_to_name": {"JOSM/1.5": "JOSM"},
                "starts_with_list": [],
                "ends_with_list": [],
                "contains_list": [],
            },
            None
        ),
    ],
)
def test_get_created_by(tags, replace_rules, expected_output):
    """Test get_created_by with various input scenarios."""
    result = preprocessing.get_created_by(tags, replace_rules)
    assert result == expected_output


@pytest.mark.parametrize(
    "tags,replace_rules,expected_output",
    [
        # Test case 1: Normal imagery_used tag with multiple sources
        (
            {"imagery_used": "Bing;Google Maps;Mapbox"},
            {
                "tag_to_name": {"Bing": "Bing", "Google Maps": "Google", "Mapbox": "Mapbox"},
                "starts_with_list": [],
                "ends_with_list": [],
                "contains_list": [],
            },
            ["Bing", "Google", "Mapbox"]
        ),
        # Test case 2: imagery_used with URL encoding that needs cleanup
        (
            {"imagery_used": "Bing%20%aerial%2c%satellite"},
            {
                "tag_to_name": {"Bing aerial,satellite": "Bing"},
                "starts_with_list": [],
                "ends_with_list": [],
                "contains_list": [],
            },
            ["Bing"]
        ),
        # Test case 3: No imagery_used tag - should return None
        (
            {"highway": "primary", "name": "Main Street"},
            {
                "tag_to_name": {"Bing": "Bing"},
                "starts_with_list": [],
                "ends_with_list": [],
                "contains_list": [],
            },
            None
        ),
    ],
)
def test_get_imagery_used(tags, replace_rules, expected_output):
    """Test get_imagery_used with various input scenarios."""
    result = preprocessing.get_imagery_used(tags, replace_rules)
    assert result == expected_output


@pytest.mark.parametrize(
    "tags,expected_output",
    [
        # Test case 1: Normal hashtags with multiple values
        (
            {"hashtags": "hotosm;missingmaps;redcross"},
            ["hotosm", "missingmaps", "redcross"]
        ),
        # Test case 2: Hashtags with mixed case - should be lowercased
        (
            {"hashtags": "HotOSM;MissingMaps;RedCross"},
            ["hotosm", "missingmaps", "redcross"]
        ),
        # Test case 3: No hashtags tag - should return None
        (
            {"highway": "primary", "name": "Main Street"},
            None
        ),
    ],
)
def test_get_hashtags(tags, expected_output):
    """Test get_hashtags with various input scenarios."""
    result = preprocessing.get_hashtags(tags)
    assert result == expected_output


@pytest.mark.parametrize(
    "tags,replace_rules,expected_output",
    [
        # Test case 1: Normal source with multiple values and URL domain extraction
        (
            {"source": "survey;https://example.com/api/data;local knowledge"},
            {
                "tag_to_name": {"survey": "Survey", "local knowledge": "Local Knowledge"},
                "starts_with_list": [],
                "ends_with_list": [],
                "contains_list": [],
            },
            ["Survey", "https://example.com", "Local Knowledge"]
        ),
        # Test case 2: Source with prefix that needs to be cleaned
        (
            {"source": "source:Bing;GPS"},
            {
                "tag_to_name": {"Bing": "Bing", "GPS": "GPS"},
                "starts_with_list": [],
                "ends_with_list": [],
                "contains_list": [],
            },
            ["Bing", "GPS"]
        ),
        # Test case 3: No source tag - should return None
        (
            {"highway": "primary", "name": "Main Street"},
            {
                "tag_to_name": {"survey": "Survey"},
                "starts_with_list": [],
                "ends_with_list": [],
                "contains_list": [],
            },
            None
        ),
    ],
)
def test_get_source(tags, replace_rules, expected_output):
    """Test get_source with various input scenarios."""
    result = preprocessing.get_source(tags, replace_rules)
    assert result == expected_output


@pytest.mark.parametrize(
    "created_by,tags,expected_output",
    [
        # Test case 1: StreetComplete with quest that gets renamed
        (
            "StreetComplete",
            {"StreetComplete:quest_type": "AddAccessibleForPedestrians"},
            "AddProhibitedForPedestrians"
        ),
        # Test case 2: StreetComplete with quest that doesn't get renamed
        (
            "StreetComplete",
            {"StreetComplete:quest_type": "AddHousenumber"},
            "AddHousenumber"
        ),
        # Test case 3: Not StreetComplete or missing quest tag - should return None
        (
            "JOSM",
            {"StreetComplete:quest_type": "AddHousenumber"},
            None
        ),
    ],
)
def test_get_streetcomplete_quest(created_by, tags, expected_output):
    """Test get_streetcomplete_quest with various input scenarios."""
    result = preprocessing.get_streetcomplete_quest(created_by, tags)
    assert result == expected_output


@pytest.mark.parametrize(
    "tags,expected_output",
    [
        # Test case 1: Android created_by tag
        (
            {"created_by": "Vespucci 0.9.8 Android"},
            "Android"
        ),
        # Test case 2: iOS created_by tag  
        (
            {"created_by": "Go Map!! 2.5.1 iOS"},
            "iOS"
        ),
        # Test case 3: No mobile OS in created_by tag - should return None
        (
            {"created_by": "JOSM/1.5"},
            None
        ),
    ],
)
def test_get_mobile_os(tags, expected_output):
    """Test get_mobile_os with various input scenarios."""
    result = preprocessing.get_mobile_os(tags)
    assert result == expected_output
