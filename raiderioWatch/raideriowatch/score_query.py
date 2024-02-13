import json
from typing import Dict, Any, List
import boto3

"""
ENV VAR
TODO: move to env in lambda
"""
TABLE_NAME = "guild_table"
PARTITION_KEY = "name"
SECONDARY_KEY = "score"


ddb_handler = boto3.client("dynamodb")


def ddb_get_item(name: str, realm: str) -> int:
    try:
        response = ddb_hander.get_item(
            TableName=TABLE_NAME,
            Key={
                "character": {"S": name},
                "realm": {"S": realm},
            },
        )
        print(
            f"got response from db {response['character']}, {response['realm']} score {response['score']}."
        )
    except Exception as e:
        return {"statusCode": 500, "body": str(e)}
    else:
        return response.get("score")


def get_top_scores() -> List[Dict[str, Any]]:
    pass


def lambda_handler(event: Dict[str, Any], context: Any):
    # parse query string params
    char_name = event["queryStringParameter"]["name"]
    realm = event["queryStringParameter"]["realm"]
    score: int | None = None

    print(f"fetching score for {char_name}, {realm}")

    score = ddb_get_item(char_name, realm)

    # construct response object
    response_item = {}
    response_item["name"] = char_name
    response_item["realm"] = realm
    response_item["score"] = score
    response_item["message"] = f"highest score for {char_name}, {realm} is {score}"

    # construct http response object
    response = {}
    response["statusCode"] = 200
    response["headers"] = {}
    response["headers"]["Content-Type"] = "application/json"
    response["body"] = json.dumps(response_item)

    # return response
    return response
