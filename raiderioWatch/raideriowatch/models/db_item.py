from pydantic import BaseModel, Field
from decimal import Decimal


class DB_item(BaseModel):
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
