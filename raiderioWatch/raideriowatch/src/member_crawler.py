import logging
import os
import httpx
from typing import Tuple
from dotenv import load_dotenv


from models.member import Member

from src.requests import httpx_get, JSONObject
from datetime import datetime

logging.basicConfig(
    filename="debug.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

load_dotenv()

FIELDS = os.getenv("FIELDS")
API_URL = os.getenv("API_URL")
INPUT_FILE = os.getenv("INPUT_FILE")
OUTFILE = os.getenv("OUTFILE")


print("Loading Function...")


async def parse_response(
    response: JSONObject,
) -> Tuple[float, float] | Tuple[None, None]:
    """
    Helper function to parse score and ilvl from http response json
    """
    score = None
    ilvl = None
    if response.get("mythic_plus_scores_by_season", None):
        # get score value
        score = response["mythic_plus_scores_by_season"][0]["scores"]["all"]
    if response.get("gear", None):
        #get ilvl value
        ilvl = response["gear"]["item_level_equipped"]
    return score, ilvl


async def request_member(client: httpx.Client, params: dict[str, str]) -> JSONObject:
    """
    Make http request

    param: params - query params dictionary
    param: client - httpx session manager
    return JSONObject
    """
    response: JSONObject = await httpx_get(url=API_URL, client=client, params=params)

    return response


async def crawl_member(member: Member, client: httpx.Client) -> Member:
    """
    helper function to construct api endpoint url and parse response
    """
    # construct query url
    params = {
        "region": member.region,
        "realm": member.realm,
        "name": member.character_name,
        "fields": FIELDS,
    }
    response: JSONObject = await request_member(client=client, params=params)

    member.score, member.ilvl = await parse_response(response)
    member.last_crawled_at = str(datetime.now())

    return member
