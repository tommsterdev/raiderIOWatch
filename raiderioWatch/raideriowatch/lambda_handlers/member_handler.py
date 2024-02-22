
import json
import logging
import os
from typing import Dict, Any

import boto3
from botocore.exceptions import ClientError

ddb = boto3.client("dynamodb")
table = os.environ['TABLE']

print("Loading function")


def get_item(character: str) -> Dict[str, Any]:
    
        """
        Retrieves an item from the DynamoDB table based on the given character.

        Args:
            character (str): The character name.

        Returns:
            Dict[str, Any]: The item retrieved from the DynamoDB table.
        """
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


def resp(res) -> Dict[str, Any]:
    """
    Generate a response dictionary with the given result.

    Args:
        res: The result to be included in the response body.

    Returns:
        A dictionary containing the response status code, body, and headers.
    """
    return {
        "statusCode": 200,
        "body": json.dumps(res),
        "headers": {
            "Content-Type": "application/json",
        },
    }


def lambda_handler(event, context):
    """
    Handles the Lambda function invocation.

    Args:
        event (dict): The event data passed to the Lambda function.
        context (object): The runtime information of the Lambda function.

    Returns:
        dict: The response object containing character information.
    """

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


    return resp(character)

