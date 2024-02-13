import boto3
import json
import logging
import os

from dotenv import load_dotenv
from typing import Dict, Any, List

from member import Member
from requests import JSONObject

# ddb = boto3.client('dynamodb')
# sns = boto3.client('sns')
load_dotenv()

logger = logging.getLogger(__name__)
# TODO: *** move to env variables ***
TABLE_NAME = os.getenv("TABLE_NAME")
SNS_ARN = os.getenv("SNS_ARN")
PARTITION_KEY = "PARTITION_KEY"
# ***********************************


# types
ATTRIBUTE = Dict[str, str | float | int | None]
ITEM = Dict[str, ATTRIBUTE]


def create_member_item_from_list(members: List[Member]) -> List[ITEM]:
    return [create_member_item(member) for member in members]


def create_member_item(member: Member) -> ITEM:
    """
    Constructs member ddb_item from dict
    """
    Item = {
        "character": {"S": member.character_name},
        "realm": {"S": member.realm},
        "region": {"S": member.region},
        "score": {"N": str(member.score)},
        "ilvl": {"N": str(member.ilvl)},
        "race": {"S": member.race},
        "game_class": {"S": member.game_class},
        "rank": {"N": str(member.rank)},
        "faction": {"S": member.faction},
        "active_spec": {"S": member.active_spec},
        "active_role": {"S": member.active_role},
    }
    return Item


def extract_member_item(item: Dict[str, Dict[str, Any]]) -> Member:
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


def chunks(items: List[ITEM], chunk_size: int = 25) -> List[ITEM]:
    """
    yield chunk_size chunks from list
    """
    for i in range(0, len(items), chunk_size):
        logging.debug(f"yielding chunk: {items[i: i+chunk_size]}\n")
        yield items[i : i + chunk_size]


def sns_publish(attributes: Dict[str, Any]) -> None:
    print(attributes)
    score = attributes["score"].get("N")
    name = attributes["name"].get("S")
    realm = attributes["realm"].get("S")
    last_crawled_at = attributes["last_crawled_at"].get("S")
    message = f"{name}, {realm} your score has increased! new m+ score is ***{score}***, recorded at {last_crawled_at}\n Sent from RaiderIOWatch"

    sns.publish(
        TopicArn=SNS_ARN,
        Message=message,
    )
    print(f"published {message} on topic {SNS_ARN}")
    return


def parse_data(data: Dict[str, Any]) -> (float, float):

    scores = data.get("mythic_plus_scores_by_season", None)
    """
    optionality get spec and role scores here
    """
    score = 0.0
    if scores:
        # get current season top score for all specs
        score = scores[0]["scores"]["all"]
        if score != 0:
            print(f"updating new score is {score}")

    # get gear information
    gear = data.get("gear", None)
    logging.info(data.get("gear"), None)
    ilvl = 0.0
    if gear:
        ilvl = float(gear["item_level_equipped"])

    return score, ilvl


def parse_unprocessed_items(
    unprocessed_items: Dict[str, List[Dict[str, ITEM]]]
) -> list[Member]:
    """
    unprocessed_items structure:
    'UnprocessedItems': {
        'string': [
            {
                'PutRequest': {
                    'Item': {
                        'string': {
                            'S': 'string',
                            'N': 'string',
                            'B': b'bytes',
                            'SS': [
                                'string',
                            ],
                            'NS': [
                                'string',
                            ],
                            'BS': [
                                b'bytes',
                            ],
                            'M': {
                                'string': {'... recursive ...'}
                            },
                            'L': [
                                {'... recursive ...'},
                            ],
                            'NULL': True|False,
                            'BOOL': True|False
                        }
                    }
                },
    """
    pass


def load_data() -> List[Member]:
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    return [Member(**member) for member in data]


def write_item_to_file(items: Dict[str, ITEM], output_file: str) -> None:
    logging.debug(items)
    with open(output_file, "w", encoding="utf-8") as f:
        for item in items:
            logging.debug(f"writing item: {item}")
            item_dict = {"Item": item}
            logging.debug(f"item_dict == {item_dict.items()}")
            f.writelines(json.dumps(item_dict, ensure_ascii=False, indent=1) + "\n")


def write_members(members: List[Dict[str, Any]], output_file: str) -> None:

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(json.dumps(members, ensure_ascii=False, indent=4))


def s3_upload_member(members: List[Dict[str, Any]], bucket: str, key: str) -> None:

    # create s3 object
    s3object = s3.Object(bucket, key)

    try:
        s3object.put(
            Body=(
                bytes(
                    json.dumps(
                        obj=members,
                        cls=EnhancedJSONEncoder,
                        ensure_ascii=False,
                        indent=4,
                    ).encode("utf-8")
                )
            ),
            ContentType="application/json",
        )
    except ClientError as e:
        logger.exception(
            "Error uploading object %s to bucket %s",
            bucket,
            key,
        )
        raise
    else:
        logger.info(f"Put object {key} to bucket {bucket}")
