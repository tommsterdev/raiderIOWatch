import json
import logging
import os
import asyncio
import boto3
import httpx
from typing import List, Tuple, Dict
from dotenv import load_dotenv
from botocore.exceptions import ClientError

from member_crawler import crawl_member
from models.member import Member
from db.ddb_write import batch_writer
from guild_crawler import get_guild
from utils.helpers import write_item_to_file, chunks
from utils.model_utils import create_DB_item_from_list

from time import perf_counter
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
TABLE = os.getenv('TABLE')

ddb = boto3.resource('dynamodb')

table = ddb.Table(TABLE)

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

    # convert members to ddb compatible Items
    filtered_members = [member for member in crawled_members if member.score and member.ilvl]
    member_items: List[DB_item] = create_DB_item_from_list(filtered_members)
    logging.info(f"member items : {member_items}")
    # chunk data
    chunk = chunks(items=member_items)
    while chunk:
        try:
            # write in chunks
            batch_writer(table=table, items=next(chunk))
        except StopIteration:
            break
        except ClientError as e:
            print(e)


    elapsed = perf_counter() - start
    logging.info(f"object write to dynamodb table execution time: {elapsed:.2f} seconds")


if __name__ == "__main__":
    asyncio.run(main())
