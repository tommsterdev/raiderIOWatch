from typing import Dict, Any, List, Optional
from pydantic import BaseModel, ValidationError


class Member(BaseModel):
    character_name: str
    region: str
    realm: str
    race: Optional[str]
    game_class: Optional[str]
    rank: Optional[int]
    faction: Optional[str]
    active_spec: Optional[str] = None
    active_role: Optional[str] = None
    last_crawled_at: Optional[str] = None
    score: float = 0.0
    ilvl: float = 0.0


def create_member_from_request(member_entry: Dict[str, Any]) -> Member:
    """
    Construct a Member object from raider io guild api request
    """
    member = {
        "character_name": member_entry["character"].get("name"),
        "region": member_entry["character"].get("region", "us"),
        "realm": member_entry["character"].get("realm"),
        "race": member_entry["character"].get("race"),
        "game_class": member_entry["character"].get("class"),
        "rank": member_entry["rank"],
        "active_spec": member_entry["character"].get("active_spec_name"),
        "active_role": member_entry["character"].get("active_spec_role"),
        "faction": member_entry["character"].get("faction"),
        "last_crawled_at": member_entry["character"].get("last_crawled_at"),
    }
    try:
        m = Member(**member)
    except ValidationError as e:
        print(e)
        raise

    return m
