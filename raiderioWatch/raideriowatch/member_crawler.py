import json
import logging
import os
import asyncio

from typing import List, Tuple
from dotenv import load_dotenv

from member import Member, create_member_from_request
from guild_crawler import get_guild
from utils import write_members, parse_data
from requests import http_get_async, JSONObject
from time import perf_counter


logger = logging.Logger(__name__)
load_dotenv()

FIELDS = os.getenv('FIELDS')
API_URL = os.getenv('API_URL')
INPUT_FILE = os.getenv('INPUT_FILE')
OUTFILE = os.getenv('OUTFILE')


print('Loading Function...')

async def parse_response(response: JSONObject) -> Tuple[float, float]:
    """
    Helper function to parse score and ilvl from http response json
    """
    score = response['mythic_plus_scores_by_season'][0]['scores']['all']
    ilvl = response['gear']['item_level_equipped']
    return score, ilvl

async def request_member(url: str) -> JSONObject:

    response: JSONObject = await http_get_async(url)

    if response.get('statusCode', 200) != 200:
        print(response['statusCode'])

    #parsed_response = await parse_response(response)


    return response


async def crawl_member(member: Member) -> Member:
    """
    helper function to construct api endpoint url and parse response
    """
    # construct query url
    
    endpoint = f"{API_URL}?region={member.region}&realm={member.realm}&name={member.name}&fields={FIELDS}"
    print(f'connecting to {endpoint}...')

    response = await request_member(endpoint)

    member.score, member.ilvl = await parse_response(response)

    
    return member



async def main() -> None:

    start = perf_counter()

    guild_data: List[Member] = await get_guild()

    elapsed = perf_counter() - start
    print(f'get guild execution time: {elapsed}')

    print(f'getting member data...')
    crawled_members: List[Member] = await asyncio.gather(*[crawl_member(member) for member in guild_data])

    elapsed = perf_counter() - start
    print(f'crawl_members execution time: {elapsed}')

    output_file = 'pydantic_data_with_scores.json'
    members_json = [member.model_dump() for member in crawled_members]
    write_members(members=members_json, output_file=output_file)

    elapsed = perf_counter() - start
    print(f'dump objects and write to file execution time: {elapsed}')


if __name__ == '__main__':
    asyncio.run(main())
