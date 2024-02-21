import json
import logging
import asyncio
import httpx

from typing import List, Tuple, Dict

from member import Member
from guild_crawler import get_guild
from utils import write_members, parse_data, chunks
from requests import httpx_get, JSONObject
from time import perf_counter

ddb = boto3.resource('dynamodb')
table = ddb.table(os.environ(TABLE))

#types
Table = boto3.resource.dynamodb.Table


async def batch_writer(
    table: Table, items: list[DB_item], pk: Optional[str] = None, sk: Optional[str] = None
) -> None:
    with table.batch_writer() as batch:
        if pk or sk:
            batch.overwrite_by_pkey = [pk, sk]
        for item in items:
            batch.put_item(item)



async def runner():

    start = perf_counter()
    async with httpx.AsyncClient() as client:
        guild_data: list[Member] = await get_guild(client)

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
    filtered_members = [member for member in crawled_members if member.score and member.ilvl]
    member_items: list[DB_item] = create_DB_item_from_list(filtered_members)
    logging.info(f"member items : {member_items}")
    # chunk data
    chunk = chunks(items=member_items)
    while chunk:
        try:
            # write in chunks
            await batch_writer(table=table, items=next(chunk), pk='character')
        except StopIteration:
            break
    elapsed = perf_counter() - start
    print(f"dump objects and write to file execution time: {elapsed:.2f} seconds")


def lambda_hander(event, context) -> Dict[str, Any]:

    asyncio.run(runner())

    return {
        "statusCode": 200,
        "body": json.dumps(event),
    }
