import logging
import json
from typing import Any
from models.db_item import DB_item
from models.member import Member



def chunks(items: list[DB_item], chunk_size: int = 25) -> list[DB_item]:
    """
    yield chunk_size chunks from list
    """
    for i in range(0, len(items), chunk_size):
        logging.debug(f"yielding chunk: {items[i: i+chunk_size]}\n")
        yield items[i : i + chunk_size]


def write_item_to_file(items: list[DB_item], output_file: str) -> None:
    logging.debug(items)
    with open(output_file, "w", encoding="utf-8") as f:
        for item in items:
            logging.debug(f"writing item: {item}")
            item_dict = {"Item": item.model_dump()}
            logging.debug(f"item_dict == {item_dict.items()}")
            f.writelines(json.dumps(item_dict, ensure_ascii=False, indent=1) + "\n")


def write_members(members: list[dict[str, Any]], output_file: str) -> None:

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(json.dumps(members, ensure_ascii=False, indent=4))








#UNUSED

def load_data() -> list[Member]:
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    return [Member(**member) for member in data]





    