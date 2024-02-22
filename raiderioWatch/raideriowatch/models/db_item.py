from decimal import Decimal
from pydantic import BaseModel


class DB_item(BaseModel):
    """
    A class to represent database item of a member of a guild.
    """
    character: str
    realm : str
    region: str
    score: Decimal
    ilvl: Decimal
    race: str
    game_class: str
    rank: int
    faction: str
    active_spec: str
    active_role: str
    last_crawled_at: str
