import json
import os
import csv
import dataclasses
import urllib3
import logging
import boto3
from typing import Dict, Any, List
from dataclasses import dataclass
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from boto3.s3.transfer import S3UploadFailedError



logger = logging.getLogger(__name__)
http = urllib3.PoolManager()
s3 = boto3.resource('s3')
load_dotenv()


GUILD_URL = os.getenv('GUILD_URL')
GUILD_NAME = os.getenv('GUILD_NAME')
BUCKET = os.getenv('S3_BUCKET')
OUTFILE = os.getenv('S3_OBJECT')


class EnhancedJSONEncoder(json.JSONEncoder):
        def default(self, o):
            if dataclasses.is_dataclass(o):
                return dataclasses.asdict(o)
            return super().default(o)


@dataclass
class Member(json.JSONEncoder):
    name: str
    region: str
    realm: str
    race: str
    game_class: str
    active_spec: str
    active_role: str
    faction: str
    last_crawled_at: str
    rank: int
    score: int = 0

    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)

def create_member(member_entry: Dict[str, Any]) -> Member:
    rank = member_entry['rank']
    name = member_entry['character'].get('name')
    region = member_entry['character'].get('region', 'us')
    realm = member_entry['character'].get('realm', 'tichondrius')
    race = member_entry['character'].get('race')
    game_class = member_entry['character'].get('class')
    active_spec = member_entry['character'].get('active_spec_name')
    active_role = member_entry['character'].get('active_spec_role')
    faction = member_entry['character'].get('faction')
    last_crawled_at = member_entry['character'].get('last_crawled_at')

    return Member(name, region, realm, race, game_class, active_spec, active_role, faction, last_crawled_at, rank)



def write_members(members: List[Dict[str, Any]], output_file: str) -> None:

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(json.dumps(members, cls=EnhancedJSONEncoder, ensure_ascii=False, indent=4))


def s3_upload_member(members: List[Dict[str, Any]], bucket: str, key: str) -> None:

    # create s3 object
    s3object = s3.Object(bucket, key)

    try:
        s3object.put(
            Body = (bytes(json.dumps(obj=members, cls=EnhancedJSONEncoder, ensure_ascii=False, indent=4).encode('utf-8'))),
            ContentType = 'application/json'
        )
    except ClientError as e:
        logger.exception(
            "Error uploading object %s to bucket %s",
            bucket,
            key,
        )
        raise
    else:
        logger.info(f"Put object {key} to bucket {bucket}")



def get_guild() -> List[Dict[Any, Any]]:
    params = {
        "region" : "us",
        "realm" : "tichondrius",
        "name" : GUILD_NAME,
    }
    full_url = f"{GUILD_URL}?region={params['region']}&realm={params['realm']}&name={params['name']}&fields=members"
    print(f'requesting members from {full_url}')
    try:
        response = http.request(method='GET', url=full_url)
    except ClientError as e:
        logger.exception(
            f"Error connecting to {GUILD_URL}: {e}"
        )
    data = json.loads(response.data.decode('utf-8'))
    members = data['members']
    return members

def lambda_handler(event, context) -> None:
    guild_data = get_guild()
    members: Dict[Member] = {}
    for entry in data:
        character = create_member(entry)
        members[character.name] = character
    # write to s3 bucket
    s3_upload_member(members=members, bucket=BUCKET, key=OUTFILE)