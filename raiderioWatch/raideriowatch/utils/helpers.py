import logging
import json
from functools import partial
from typing import Any, Callable
from models.db_item import DB_item
from models.member import Member


filtered = Callable[[list[Member]], list[Member]]

def model_to_dict(items: list[DB_item]) -> list[dict]:
    return [item.dict() for item in items]


def chunks(items: list[DB_item], chunk_size: int = 25) -> list[DB_item]:
    """
    yield chunk_size chunks from list, maximum batch put item operation in dynamodb is 25 items.
    """
    for i in range(0, len(items), chunk_size):
        logging.debug(f"yielding chunk: {items[i: i+chunk_size]}\n")
        yield model_to_dict(items[i : i + chunk_size])


def filter_low_rank(members: list[Member]) -> list[Member]:
    return [member for member in members if member.rank < 4]

def filter_inactive(members: list[Member]) -> list[Member]:
    return [member for member in members if member.score and member.ilvl]








#UNUSED

def write_item_to_file(items: list[DB_item], output_file: str) -> None:
    logging.debug(items)
    with open(output_file, "w", encoding="utf-8") as f:
        for item in items:
            logging.debug(f"writing item: {item}")
            f.writelines(json.dumps(item, ensure_ascii=False, indent=1) + "\n")


def write_members(members: list[dict[str, Any]], output_file: str) -> None:

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(json.dumps(members, ensure_ascii=False, indent=4))



def load_data(input_file) -> list[Member]:
    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    return [Member(**member) for member in data]





    