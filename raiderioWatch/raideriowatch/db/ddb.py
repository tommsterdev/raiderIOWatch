
import boto3
import logging

from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError



logging.basicConfig(
    filename="debug.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

Schema = dict[str, str]


class DDBTable:
    """Represents Amazon DynamoDB table of guild data."""

    def __init__(
        self,
        dynamic_resources: boto3.resource,
        table_name: str,
        key_schema: list[Schema],
    ):
        """
        dynamic resource : A Boto3 DynamoDB resource.
        """
        self.dynamic_resources = dynamic_resources
        self.table = None
        self.key_schema = key_schema

    def create_table(self, table_name: str) -> boto3.resource:
        """
        Create an Amazon DynamoDB table that stores character data.
        The table uses the character name as the partition key and
        Score as the sort key
        """
        try:
            self.table = self.dynamic_resources.create_table(
                TableName=table_name,
                KeySchema=self.key_schema,
                AttributeDefinitions=[
                    {"AttributeName": "character", "AttributeType": "S"},
                    {"AttributeName": "score", "AttributeType": "N"},
                    {"AttributeName": "realm", "AttributeType": "S"},
                    {"AttributeName": "region", "AttributeType": "S"},
                    {"AttributeName": "ilvl", "AttributeType": "N"},
                    {"AttributeName": "race", "AttributeType": "S"},
                    {"AttributeName": "game_class", "AttributeType": "S"},
                    {"AttributeName": "faction", "AttributeType": "S"},
                    {"AttributeName": "rank", "AttributeType": "N"},
                    {"AttributeName": "active_spec", "AttributeType": "S"},
                    {"AttributeName": "active_role", "AttributeType": "S"},
                    {"AttributeName": "last_crawled_at", "AttributeType": "S"},
                ],
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                logging.error(
                    "Couldn't find table %s. Reason: %s: %s",
                    table_name,
                    e.response["Error"]["Code"],
                    e.response["Error"]["Message"],
                )
                raise
            else:
                logging.error(
                    "Couldn't create table %s. Reason: %s: %s",
                    table_name,
                    e.response["Error"]["Code"],
                    e.response["Error"]["Message"],
                )
                raise
        else:
            return self.table

    def list_tables(self) -> list[boto3.resource]:
        """
        Lists the Amazon DynamoDB tables for the current account.
        """
        try:
            tables = []
            for table in self.dynamic_resources.tables.all():
                tables.append(table)
        except ClientError as e:
            logging.error(
                "Couldn't list tables. Reason: %s: %s",
                e.response["Error"]["Code"],
                e.response["Error"]["Message"],
            )
            raise
        else:
            return tables

    async def get_item(
        self,
        Key: Key,
        AttributesToGet: list[str],
        ReturnConsumedCapacity: str,
        ProjectionExpression: str,
        ExpressionAttributeNames: dict[str, str],
        ConsistentRead: bool = False,
    ) -> dict[str, str]:
        response = self.table.get_item(
            Key=self.Key,
            AttributesToGet=self.AttributesToGet,
            ConsistentRead=self.ConsistentRead,
            ReturnConsumedCapacity=self.ReturnConsumedCapacity,
            ProjectionExpression=self.ProjectionExpression,
            ExpressionAttributeNames=self.ExpressionAttributeNames,
        )

        return response

   
    async def query(
        self,
        pk: str,
        target: str,
    ) -> dict[str, str]:

        response = self.table.get_item(KeyConditionExpressio=Key(pk).eq(target))
        return response

    async def delete_table(self) -> None:
        self.table.delete()

 