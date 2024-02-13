import json
from pydantic import BaseModel
from typing import Optional

from guild_crawler import get_guild
from member import Member, create_member_from_request
from member_crawler import get_members


async def runner():

    guild_data: List[Member] = get_guild()

    crawled_members: List[Member] = await asyncio.gather(
        *[crawl_member(member) for member in guild_data]
    )

    output_file = "pydantic_data_with_scores.json"
    members_json = [member.model_dump() for member in crawled_members]
    write_members(members=members_json, output_file=output_file)


def lambda_hander(event, context) -> Dict[str, Any]:

    asyncio.run(runner())
    try:
        guild_data = get_guild()
        members = [
            create_member_from_request(member).model_dump() for member in guild_data
        ]
        crawled_members = get_members(members)
        # TODO : WRITE TO DDB
        output = event["output_file"]
        """
        TODO:

        put / update members to dynamodb table or local file
        return
        """
        return {
            "statusCode": 200,
            "body": json.dumps({"num_members": len(crawled_emmbers)}),
        }
    except ClientError as e:
        return {"statusCode": 500, "body": str(e)}
