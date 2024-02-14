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


def get_item(character: str) -> Dict[str, Any]:
    pk = character
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
    return response['Item']


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

    item = get_item(character)
    
    #consturct response object
    character = {
        'character' : item['character']['S'],
        'realm' : item['realm']['S'],
        'score' : float(item['score']['N']),
        'ilvl' : float(item['ilvl']['N']),
    }


    return response(character)
    
