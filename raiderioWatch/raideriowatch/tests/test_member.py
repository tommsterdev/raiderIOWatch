from member import Member, create_member_from_request
from pydantic import ValidationError
import logging

logger = logging.Logger(__name__)


def test_create_member_from_request_valid():
    member = {
        "rank": 4,
        "character": {
            "name": "Atmiana",
            "race": "Orc",
            "class": "Monk",
            "active_spec_name": "Windwalker",
            "active_spec_role": "DPS",
            "gender": "female",
            "faction": "horde",
            "achievement_points": 25800,
            "honorable_kills": 0,
            "region": "us",
            "realm": "Tichondrius",
            "last_crawled_at": "2024-01-28T18:56:20.000Z",
            "profile_url": "https://raider.io/characters/us/tichondrius/Atmiana",
            "profile_banner": "hordebanner18",
        },
    }

    result = create_member_from_request(member)
    assert result.rank == 4
    assert result.name == "Atmiana"
    assert result.race == "Orc"
    assert result.faction == "horde"


def test_create_member_from_request_missing_attributes():
    member = {
        "rank": 4,
        "character": {
            "name": "Atmiana",
            "race": "Orc",
            "class": "Monk",
            "gender": "female",
            "faction": "horde",
            "achievement_points": 25800,
            "honorable_kills": 0,
            "region": "us",
            "realm": "Tichondrius",
            "profile_url": "https://raider.io/characters/us/tichondrius/Atmiana",
            "profile_banner": "hordebanner18",
        },
    }
    result = create_member_from_request(member)
    assert result.rank == 4
    assert result.race == "Orc"
    assert result.score == 0
    assert result.ilvl == 0
