from typing import Dict, Any, List

from models.member import Member
from models.db_item import DB_item


def create_DB_item_from_list(members: List[Member]) -> List[DB_item]:
    return [create_DB_item(member) for member in members]


def create_DB_item(member: Member) -> DB_item:
    """
    Constructs member ddb_item from dict
    """
    Item = {
        "character": member.character_name,
        "realm": member.realm,
        "region": member.region,
        "score": member.score,
        "ilvl": member.ilvl,
        "race": member.race,
        "game_class": member.game_class,
        "rank": member.rank,
        "faction": member.faction,
        "active_spec": member.active_spec,
        "active_role":member.active_role,
        "last_crawled_at": member.last_crawled_at,
    }
    return DB_item(**Item)


def extract_member_item(item: DB_item) -> Member:
    """
    deconstructs member ddb_item from dict
    """
    char_item = {
        "character_name": item["character"]["S"],
        "realm": item["realm"]["S"],
        "region": item["region"]["S"],
        "score": int(item["score"]["N"]),
        "rank": int(item["rank"]["N"]),
        "ilvl": int(item["ilvl"]["N"]),
        "race": (item["race"]["S"]),
        "game_class": (item["game_class"]["S"]),
        "faction": (item["game_class"]["S"]),
        "active_spec": (item["active_spec"]["S"]),
        "active_role": (item["active_role"]["S"]),
        "last_crawled_at": (item["last_crawled_at"]["S"]),
    }
    return Member(**char_item)