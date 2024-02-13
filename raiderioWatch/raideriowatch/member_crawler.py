import json
import logging
import os
import asyncio
import httpx
from typing import List, Tuple, Dict
from dotenv import load_dotenv


from member import Member
from guild_crawler import get_guild
from utils import (
    write_members,
    parse_data,
    create_member_item_from_list,
    ITEM,
    write_item_to_file,
    chunks,
)
from requests import httpx_get, JSONObject
from time import perf_counter


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
        score = response["mythic_plus_scores_by_season"][0]["scores"]["all"]
    if response.get("gear", None):
        ilvl = response["gear"]["item_level_equipped"]
    return score, ilvl


async def request_member(client: httpx.Client, params: Dict[str, str]) -> JSONObject:
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

    return member


async def main() -> None:

    start = perf_counter()
    async with httpx.AsyncClient() as client:
        guild_data: List[Member] = await get_guild(client)

    # filter inactive members:
    active_members = [member for member in guild_data if member.rank <= 4]

    elapsed = perf_counter() - start
    print(f"get guild execution time: {elapsed:.2f} seconds")

    print(f"getting member data...")
    async with httpx.AsyncClient() as client:
        crawled_members: List[Member] = await asyncio.gather(
            *[crawl_member(member, client) for member in active_members]
        )

    elapsed = perf_counter() - start
    print(f"crawl_members execution time: {elapsed:.2f} seconds")
    logging.info(f"logging time elapsed = {elapsed} seconds")
    output_file = "pydantic_data.json"
    # convert members to ddb compatible Items
    member_items: List[ITEM] = create_member_item_from_list(crawled_members)
    logging.info(f"member items : {member_items}")
    # chunk data
    chunk = chunks(items=member_items)
    while chunk:
        try:
            write_item_to_file(next(chunk), output_file="chunks.json")
        except StopIteration:
            break
    write_item_to_file(member_items, output_file="ddb_fomat_data.json")

    write_members(members=member_items, output_file=output_file)

    elapsed = perf_counter() - start
    print(f"dump objects and write to file execution time: {elapsed:.2f} seconds")


if __name__ == "__main__":
    asyncio.run(main())
