import json
import logging
import asyncio
import httpx

from typing import List, Tuple, Dict

from member import Member
from guild_crawler import get_guild
from utils import write_members, parse_data
from requests import httpx_get, JSONObject
from time import perf_counter


async def runner():

    async with httpx.AsyncClient() as client:
        guild_data: List[Member] = await get_guild(client)

    # filter inactive members:
    active_members = [member for member in guild_data if member.rank <= 4]

    elapsed = perf_counter() - start
    print(f"get guild execution time: {elapsed:.2f} seconds")

    print(f"getting member data...")
    async with httpx.AsyncClient() as client:
        crawled_members: List[Member] = await asyncio.gather(
            *[crawl_member(member, client) for member in guild_data]
        )

    elapsed = perf_counter() - start
    print(f"crawl_members execution time: {elapsed:.2f} seconds")

    members_json = [member.model_dump() for member in crawled_members]
    #TODO: update ddb


    elapsed = perf_counter() - start
    print(f"dump objects and write to file execution time: {elapsed:.2f} seconds")


def lambda_hander(event, context) -> Dict[str, Any]:

    asyncio.run(runner())
   
    return {
            "statusCode": 200,
            "body": json.dumps(event),
        }