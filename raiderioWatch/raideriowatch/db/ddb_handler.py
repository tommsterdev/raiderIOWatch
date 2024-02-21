import boto3

from boto3.dynamodb.conditions import Key, Attr

from dotenv import load_dotenv
from typing import Dict, Optional
from utils import ITEM
from ..requests import JSONObject
from ddb import DDBTable

load_dotenv()
ddb = boto3.client("dynamodb")


def get_db():
    try:
        table = ddb.Table(TABLE)
        yield table
    finally ddb.close()

    
def lambda_handler(event, context) -> JSONObject:
    table_name = event["table"]
    key_schema = event["schema"]
    ddb_table = DDBTable(ddb, table_name=table_name, key_schema=key_schema)
    ddb_table.table.wait_until_exists()

    return {
        "statusCode": 200,
        "body": f"Created table {table_name} with schema {key_schema}",
    }
