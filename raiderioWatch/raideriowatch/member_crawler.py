import boto3
import json
import urllib3
import logging
from typing import Dict, Any
from guild_crawler import write_members
from botocore.exceptions import ClientError

http = urllib3.PoolManager(num_pools=50)
FIELDS = 'mythic_plus_scores_by_season:current'
GUILD_FIELD = ''
API_URL = "https://raider.io/api/v1/characters/profile"
GUILD_URL = 'https://raider.io/api/v1/guilds/profile'
GUILD_NAME = 'SWMG'
INPUT_FILE = "processed_members.json"
OUTFILE = 'members_to_ddb.json'

print('Loading Function...')



def crawl_member(member: Dict[str, str | float]) -> float | None:
    character_name = member.get("name", None)
    region = member.get("region", None)
    realm = member.get("realm", None)
    print(f"crawling member {character_name}, {realm}")
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

        # check if valid response
        # if data.get('statusCode', None) == 400:
        #     logging.info(data)
        #     return None

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




def get_members() -> int:
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        members = json.load(f)
    active_members = []
    num_inactive = 0
    for _, value in members.items():
        if value['rank'] <= 4:
            active_members.append({'region': value['region'], 'realm': value['realm'], 'name' : value['name'], 'score': 0})
    for member in active_members:
        score = crawl_member(member)
        if not score:
            print(f'No score for player {member}')
            num_inactive += 1
            continue
        member['score'] = score
    write_members(active_members, OUTFILE)
    print(f"number of inactive players / alt {num_inactive}")
    return len(active_members)

def lambda_hander(event, context) -> Dict[str, Any]:

    query_members = get_members()
    try:
        for member, field in query_members.items():

            #consturct full url = f"{API_URL}?region={params['region']}&realm={params['realm']}&name={params['name']}&fields={params['fields']}"
            full_url = f"{API_URL}?region={field['region']}&realm={field['realm']}&name={field['name']}&fields={field['fields']}"
            # get http response
            resp = http.request(method='GET', url=full_url)

            data = json.loads(resp.data.decode('utf-8'))

            # reconstruct object
            query_members[member]['score'] = data['mythic_plus_scores_by_season'][0]['scores']['all']
            """
            {
                "name" : character name,
                "region" : region,
                "realm" : realm,
                "score" : score,
            }
            
            
            
            """
        return {
            'statusCode' : 200,
            'body' : json.dumps(query_members.values())
        }
    except Exception as e:
        return {
            'statusCode' : 500, 
            'body' : str(e)
        }

if __name__ == '__main__':
    get_members()