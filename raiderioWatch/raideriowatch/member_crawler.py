import boto3
import json
import urllib3
import logging
from typing import Dict, Any
from dotenv import load_dotenv
from utils import write_members
from botocore.exceptions import ClientError

http = urllib3.PoolManager(num_pools=50)
logger = logging.Logger(__name__)
load_dotenv()

FIELDS = os.getenv('FIELDS')

API_URL = os.getenv('API_URL')
INPUT_FILE = os.getenv('INPUT_FILE')
OUTFILE = os.getenv('OUTFILE')


print('Loading Function...')


def crawl_member(member: Dict[str, str | float]) -> float | None:
    character_name = member.get("name", None)
    region = member.get("region", None)
    realm = member.get("realm", None)
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
        data = json.loads(response.data.decode('utf-8'))

        scores = data.get('mythic_plus_scores_by_season', None)
        # get current season top score for all specs
        score = scores[0]['scores']['all']

    except ClientError as e:
        return {
            e.response['Error']["Code"],
            e.response['Error']["Message"],
        }
    else:
        return score


def load_data(input_file: str) -> Dict[str, Any]:
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data


def get_members(input_file: str) -> List[Dict[str, Any]]:

    members=load_data(input_file)
    # active member format {region, realm, name, score}
    active_members = []

    # count inactive members based on score
    num_inactive = 0

    for _, value in members.items():
        # only get members with rank raider / m+ and higher
        if value['rank'] <= 4:
            active_members.append({'region': value['region'], 'realm': value['realm'], 'name' : value['name'], 'score': 0})


    for member in active_members:
        # get member data
        score = crawl_member(member)

        if not score:
            print(f'No score for player {member}')
            num_inactive += 1
            continue

        member['score'] = score

    print(f"number of inactive players / alt {num_inactive}")
    logger.info(f'number of members {len(active_members)} of them {num_inactive} are inactive.')
    return active_members


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
