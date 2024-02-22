from typing import Optional
from pydantic import BaseModel


class Member(BaseModel):
    """
    A class to represent a member of a guild.
    """
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
