import json
import os
import urllib3
import logging
import boto3
from typing import Dict, Any, List
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from boto3.s3.transfer import S3UploadFailedError
from member import Member, create_member_from_request
from utils import write_members, s3_upload_member



logger = logging.getLogger(__name__)
http = urllib3.PoolManager()
#s3 = boto3.resource('s3')
load_dotenv()


GUILD_URL = os.getenv('GUILD_URL')
GUILD_NAME = os.getenv('GUILD_NAME')
BUCKET = os.getenv('S3_BUCKET')
OUTFILE = os.getenv('S3_OBJECT')


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
    members = [create_member_from_request(member).model_dump() for member in guild_data]
    # write to s3 bucket
    s3_upload_member(members=members, bucket=BUCKET, key=OUTFILE)
    return

def main() -> None:
    guild_data = get_guild()
    members = [create_member_from_request(member).model_dump() for member in guild_data]
    write_members(members=members, output_file='pydantic_data.json')


if __name__ == '__main__':
    main()
    
