import json
import boto3
import os
import logging
from botocore.exceptions import ClientError

from typing import Dict, Any

ddb = boto3.client("dynamodb")
Character = Dict[str, str | float | None]
table = os.environ['TABLE']

print("Loading function")


def get_score(response: Dict[str, Any]) -> float:
    item = response.get("Item")
    if item:
        # parse score from dynamodb response object
        score = item["score"]["N"]
    return float(score)


def get_item(character: Character):
    pk = character["character"]
    try:
        response = ddb.get_item(
            TableName=table,
            Key={
                'character': {'S' : str(pk)},
            },
        )
    except ClientError as e:
        logging.error(
            "Couldn't list tables. Reason: %s: %s",
            e.response["Error"]["Code"],
            e.response["Error"]["Message"],
        )
        raise
    return get_score(response)


def response(res) -> Dict[str, Any]:
    return {
        "statusCode": 200,
        "body": json.dumps(res),
        "headers": {
            "Content-Type": "application/json",
        },
    }


def lambda_handler(event, context):
    print(f'received event = {event}')
    # parse request
    character = event['queryStringParameters']['character']
    realm = event['queryStringParameters']['realm']
    
    # construct query object
    character = {
        "character": character,
        "realm": realm,
        "region": "us",
        "score": None,
    }
    print(character)
    character["score"] = get_item(character)

    print(character)

    return response(character)
