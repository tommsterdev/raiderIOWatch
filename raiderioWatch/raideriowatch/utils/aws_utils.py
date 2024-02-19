import boto3
import json
import logging
import os

from dotenv import load_dotenv
from typing import Dict, Any, List

from .models.member import Member
from requests import JSONObject
from .models.db_item import DB_item

# ddb = boto3.client('dynamodb')
# sns = boto3.client('sns')
load_dotenv()

logger = logging.getLogger(__name__)
# TODO: *** move to env variables ***
TABLE_NAME = os.getenv("TABLE_NAME")
SNS_ARN = os.getenv("SNS_ARN")
PARTITION_KEY = "PARTITION_KEY"
# ***********************************



def sns_publish(member: Member) -> None:
    score = member.score
    name = member.character_name
    realm = member.realm
    last_crawled_at = member.last_crawled_at
    message = f"{name}, {realm} your score has increased! new m+ score is ***{score}***, recorded at {last_crawled_at}\n Sent from RaiderIOWatch"

    sns.publish(
        TopicArn=SNS_ARN,
        Message=message,
    )
    print(f"published {message} on topic {SNS_ARN}")
    return


def s3_upload_member(members: list[DB_item], bucket: str, key: str) -> None:

    # create s3 object
    s3object = s3.Object(bucket, key)

    try:
        s3object.put(
            Body=(
                bytes(
                    json.dumps(
                        obj=members,
                        indent=4,
                    ).encode("utf-8")
                )
            ),
            ContentType="application/json",
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
