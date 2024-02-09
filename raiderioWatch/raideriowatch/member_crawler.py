import json
import logging
import os
import time
import concurrent.futures

from typing import Dict, Any, List
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from urllib3 import Timeout, PoolManager

from member import Member, create_member_from_request
from guild_crawler import get_guild
from utils import write_members, parse_data



timeout = Timeout(connect=1.0, read=1.0)
http = PoolManager(num_pools=10, timeout=timeout)
logger = logging.Logger(__name__)
load_dotenv()

FIELDS = os.getenv('FIELDS')
API_URL = os.getenv('API_URL')
INPUT_FILE = os.getenv('INPUT_FILE')
OUTFILE = os.getenv('OUTFILE')


print('Loading Function...')

def get_members(members: List[Member]) -> List[Dict[str, Any]]:

    # load members from file
    # members=load_data()

    # filter low rank members
    active_members = [member for member in members if member.rank <= 4]

    # count inactive members based on score
    num_inactive = 0
    
    for member in active_members:

        print(f'getting score and ilvl data for {member.name}...')
        try:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(crawl_member, character_name=member.name, realm=member.realm, region=member.region)
                score, ilvl = future.result()
            #score, ilvl = crawl_member(member)

        except urllib3.exceptions.TimeoutError as e:
            print(f'could not retrieve data for {member.name}, {member.realm}: {str(e)}')
            continue

        except urllib3.exceptions.MaxRetryError as e:
            print(f'could not retrieve data for {member.name}, {member.realm}: {str(e)}')
            continue

        if score == 0.0:
            num_inactive += 1

        member.score = score
        member.ilvl = ilvl

    print(f"number of inactive players / alt {num_inactive}")
    logger.info(f'number of members {len(active_members)} of them {num_inactive} are inactive.')

    return [member.model_dump() for member in active_members]



def crawl_member(character_name: str, realm: str, region: str = 'us') -> List[float] | None:

    logger.info(f"crawling member {character_name}, {realm}")
    full_url = f"{API_URL}?region={region}&realm={realm}&name={character_name}&fields={FIELDS}"
    try:
        # send get request to raider.io api
        response = http.request(method='GET', url=full_url)

        # check if received valid response
        if response.status != 200:
            data = json.loads(response.data.decode('utf-8'))
            logging.info(f'response status code={response.status}, error={data["error"]} : {data["message"]}')
            return None

        score, ilvl = parse_data(json.loads(response.data.decode('utf-8')))

    except (ClientError) as e:
        return {
            e.response['Error']["Code"],
            e.response['Error']["Message"],
        }
        raise
    else:
        return [score, ilvl]


def main() -> None:
    guild_data = get_guild()
    members = [create_member_from_request(member) for member in guild_data]
    crawled_members = get_members(members)
    output_file = 'pydantic_data_with_scores.json'
    write_members(members=crawled_members, output_file=output_file)


if __name__ == '__main__':
    main()
