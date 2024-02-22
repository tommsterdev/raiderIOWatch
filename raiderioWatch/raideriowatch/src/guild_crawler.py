import os
import logging
import httpx
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from models.member import Member
from utils.model_utils import create_member_from_list
from .requests import httpx_get, JSONObject


logger = logging.getLogger(__name__)
load_dotenv()


GUILD_URL = os.getenv("GUILD_URL")
GUILD_NAME = os.getenv("GUILD_NAME")
OUTFILE = os.getenv("S3_OBJECT")


async def get_guild(client: httpx.Client) -> list[Member]:
    """
    Retrieves the guild members from the specified region, realm, and guild name.

    Args:
        client (httpx.Client): The HTTP client used to make the request.

    Returns:
        List[Member]: A list of Member objects representing the guild members.
    """

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


    members = create_member_from_list(data["members"])
    return members
