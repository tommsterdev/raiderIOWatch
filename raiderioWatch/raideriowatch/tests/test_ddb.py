import boto3
import json
import os
from ddb import put_item_ddb

ddb = boto3.client('dynamodb')

def test_put_item():
    item = {
        'character': os.getenv('NAME'),
        'realm': os.getenv('REALM'),
        'region' : os.getenv('REGION'),
        'score' : os.getenv('SCORE'),
        }
    table = ddb.Table(os.getenv("TABLE_NAME"))

