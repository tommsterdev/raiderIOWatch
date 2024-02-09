import boto3
import json
import logging
import os
import time
from typing import Dict, Any, List
from dotenv import load_dotenv
from utils import write_members
from botocore.exceptions import ClientError
from member import Member
from urllib3 import Timeout, PoolManager
from concurrent.futures import ThreadPoolExecutor



timeout = Timeout(connect=1.0, read=1.0)
http = PoolManager(num_pools=10, timeout=timeout)
logger = logging.Logger(__name__)
load_dotenv()

FIELDS = os.getenv('FIELDS')
API_URL = os.getenv('API_URL')
INPUT_FILE = os.getenv('INPUT_FILE')
OUTFILE = os.getenv('OUTFILE')


print('Loading Function...')


def crawl_member(member: Member) -> List[float] | None:
    character_name = member.name
    realm = member.realm
    region = member.region
    if not region:
        region = 'us'
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

        # decode 
        score, ilvl = parse_data(json.loads(response.data.decode('utf-8')))

    except (ClientError) as e:
        return {
            e.response['Error']["Code"],
            e.response['Error']["Message"],
        }
        raise
    else:
        return [score, ilvl]


def parse_data(data: Dict[str,Any]) -> (float, float):

    scores = data.get('mythic_plus_scores_by_season', None)
    """
    optionality get spec and role scores here
    """
    score = 0.0
    if scores:
    # get current season top score for all specs
        score = scores[0]['scores']['all']
        if score != 0:
            print(f'updating new score is {score}')

    # get gear information
    gear = data.get('gear', None)
    logging.info(data.get('gear'), None)
    ilvl = 0.0
    if gear:
        ilvl = float(gear['item_level_equipped'])
    
    return score, ilvl


def load_data() -> List[Member]:
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return [Member(**member) for member in data]


def get_members() -> List[Dict[str, Any]]:
    ### time test
    st = time.time()
    ### \time test
    # load members from file
    members=load_data()

    # active member format {region, realm, name, score}
    active_members = [member for member in members if member.rank <= 4]

    # count inactive members based on score
    num_inactive = 0
    
    for member in active_members:
        # get member data
        print(f'getting score and ilvl data for {member.name}...')
        try:
            with ThreadPoolExecutor() as executor:
                future = executor.submit(crawl_member, member)
                score, ilvl = future.result()
            #score, ilvl = crawl_member(member)
        except urllib3.exceptions.TimeoutError as e:
            logging.info(str(e))
            print('could not retrieve data for {member.name}, {member.realm}')
            continue
        except urllib3.exceptions.MaxRetryError as e:
            print('could not retrieve data for {member.name}, {member.realm}')
            continue

        if score == 0.0:
            num_inactive += 1

        member.score = score
        member.ilvl = ilvl

    print(f"number of inactive players / alt {num_inactive}")
    logger.info(f'number of members {len(active_members)} of them {num_inactive} are inactive.')
    et = time.time()
    elapsed_time = et - st
    print(f'Execution time: {elapsed_time} seconds')
    return [member.model_dump() for member in active_members]


def lambda_hander(event, context) -> Dict[str, Any]:

    try:
        members = get_members(event['input_file'])
        output = event['output_file']
        """
        TODO:

        put / update members to dynamodb table or local file
        return
        """        
        return {
            'statusCode' : 200,
            'body' : json.dumps({'num_members' : len(members)})
        }
    except ClientError as e:
        return {
            'statusCode' : 500, 
            'body' : str(e)
        }

def main() -> None:
    members = get_members()
    output_file = 'pydantic_data_with_scores.json'
    write_members(members=members, output_file=output_file)


if __name__ == '__main__':
    main()
