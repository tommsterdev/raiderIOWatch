from pydantic import BaseModel, Field


class DB_item(BaseModel):
    character: dict[str, str]
    realm : dict[str, str]
    region: dict[str, str]
    score: dict[str, float]
    ilvl: dict[str, str]
    race: dict[str, str]
    game_class: dict[str, str]
    rank: dict[str, str]
    faction: dict[str, str]
    active_spec: dict[str, str]
    active_role: dict[str, str]
    last_crawled_at: dict[str, str]
