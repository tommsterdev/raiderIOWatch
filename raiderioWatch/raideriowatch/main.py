import logging
import os
import asyncio
import boto3
import httpx
from dotenv import load_dotenv
from botocore.exceptions import ClientError

from src.member_crawler import crawl_member
from models.member import Member
from models.db_item import DB_item
from db.ddb_write import batch_writer
from src.guild_crawler import get_guild
from utils.helpers import chunks
from utils.model_utils import create_DB_item
from utils.preprocess import preprocess, postprocess

from time import perf_counter


logging.basicConfig(
    filename="debug.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

load_dotenv()
# load environment variables
FIELDS = os.getenv("FIELDS")
API_URL = os.getenv("API_URL")
INPUT_FILE = os.getenv("INPUT_FILE")
OUTFILE = os.getenv("OUTFILE")
TABLE = os.getenv('TABLE')

# create ddb resource
ddb = boto3.resource('dynamodb')
# get table
table = ddb.Table(TABLE)


async def main() -> None:

    start = perf_counter()
    async with httpx.AsyncClient() as client:
        data: list[Member] = await get_guild(client)


    elapsed = perf_counter() - start
    logging.info(f"get guild execution time: {elapsed:.2f} seconds")

    # preprocess member data
    processed_members = preprocess(data)

    elapsed = perf_counter() - start
    logging.info(f"preprocess execution time: {elapsed:.2f} seconds")

    print("getting member data...")
    async with httpx.AsyncClient() as client:
        crawled_members: list[Member] = await asyncio.gather(
            *[crawl_member(member, client) for member in processed_members]
        )

    elapsed = perf_counter() - start
    logging.info(f"crawl_members execution time: {elapsed:.2f} seconds")

    # apply postprocess to members
    postprocess_members = postprocess(crawled_members)

    elapsed = perf_counter() - start
    logging.info(f"postprocess execution time: {elapsed:.2f} seconds")

    # convert members to ddb compatible Items
    member_items: list[DB_item] = create_DB_item(postprocess_members)
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
