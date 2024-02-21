import boto3
from utils.model_utils import DB_item
from typing import Optional

#types
Table = boto3.resource('dynamodb').Table

def batch_writer(
    table: Table, items: list[DB_item]
) -> None:
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)