import json
import os
import logging
import httpx
from typing import Dict, Any, List
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from member import Member, create_member_from_request
from requests import httpx_get, JSONObject


logger = logging.getLogger(__name__)
load_dotenv()


GUILD_URL = os.getenv("GUILD_URL")
GUILD_NAME = os.getenv("GUILD_NAME")
BUCKET = os.getenv("S3_BUCKET")
OUTFILE = os.getenv("S3_OBJECT")


async def get_guild(client: httpx.Client) -> List[Member]:
    params = {
        "region": "us",
        "realm": "tichondrius",
        "name": GUILD_NAME,
        "fields": "members",
    }
    full_url = f"{GUILD_URL}?region={params['region']}&realm={params['realm']}&name={params['name']}&fields=members"
    print(f"requesting members from {full_url}")
    try:
        data: JSONObject = await httpx_get(url=GUILD_URL, client=client, params=params)
    except ClientError as e:
        logger.exception(f"Error connecting to {GUILD_URL}: {e}")
    members = [create_member_from_request(member) for member in data["members"]]
    return members
