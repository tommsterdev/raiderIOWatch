import boto3
import logging

from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from typing import Any, Dict, List, Optional
from member import Member
from utils import parse_unprocessed_items, create_member_item, ITEM, ATTRIBUTE

logging.basicConfig(
    filename="debug.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
Key = Dict[str, str | int | bool | None]
Schema = Dict[str, str]


class DDBTable:
    """Represents Amazon DynamoDB table of guild data."""

    def __init__(
        self,
        dynamic_resources: boto3.resource,
        table_name: str,
        key_schema: List[Schema],
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
                exists = False
            else:
                logging.error(
                    "Couldn't create table %s. Reason: %s: %s",
                    table_name,
                    e.response["Error"]["Code"],
                    e.response["Error"]["Message"],
                )
                raise
        else:
            # TODO: table.wait_until_exists()
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
        Key: Key,
        AttributesToGet: List[str],
        ReturnConsumedCapacity: str,
        ProjectionExpression: str,
        ExpressionAttributeNames: Dict[str, str],
        ConsistentRead: bool = False,
    ):
        response = self.table.get_item(
            Key=Key,
            AttributesToGet=AttributesToGet,
            ConsistentRead=ConsistentRead,
            ReturnConsumedCapacity=ReturnConsumedCapacity,
            ProjectionExpression=ProjectionExpression,
            ExpressionAttributeNames=ExpressionAttributeNames,
        )

        return response

    async def batch_writer(
        self, items: List[ITEM], pk: Optional[str], sk: Optional[str]
    ) -> None:
        with self.table.batch_writer() as batch:
            if pk or sk:
                batch.overwrite_by_pkey = [pk, sk]
            for item in items:
                batch.put_item(item)

    async def query(
        self,
        pk: str,
        target: str,
    ) -> Dict[str, str]:

        response = self.table.get_item(KeyConditionExpressio=Key(pk).eq(target))
        items = response["Items"]
        return response

    async def delete_table(self):
        self.table.delete()

    # def add_member(member: Member) -> None:
    #     """
    #     Adds a member to the guild table.

    #     param name: character name
    #     param score: m+ score (current season)
    #     param last_crawled_at: last time character info was retreived
    #     param region: region default us
    #     param realm: realm default tichondrius
    #     """
    #     try:
    #         self.table.put_item(
    #             Item=create_member_item(member)
    #         )
    #     except ClientError as e:
    #         logging.error(
    #             "Couldn't add character %s to table %s. Reason: %s: %s",
    #             name,
    #             self.table.name,
    #             e.response["Error"]["Code"],
    #             e.resopnse["Error"]["Message"],
    #         )
    #         raise

    # def batch_add_member(members: List[Member]) -> List[Member]:
    #     """
    #     batch adds a member to the guild table.

    #     returns any unprocessed items

    #     """
    #     {'PutRequest': {'Item': {'PK': {'S': 'hi'}, 'SK': {'S': 'user-1'}}}}
    #     items: Dict[str, ITEM] = {'Item':create_member_item(member) for member in members}
    #     requests = {'PutRequest':item for item in items}
    #     table_name: str = self.table_name

    #     try:
    #         response = self.batch_write_item(
    #             RequestItems= {
    #                 self.table_name : [requests]
    #             },
    #             ReturnConsumedCapacity='INDEXES'|'TOTAL'|'NONE',
    #             ReturnItemCollectionMetrics='SIZE'|'NONE'
    #         )
    #         if response.get('UnprocessedItems', None):
    #             unprocessed_members = parse_unprocessed_items(response['UnprocessedItems'])

    #     except ClientError as e:
    #         logging.error(
    #             "Couldn't add character %s to table %s. Reason: %s: %s",
    #             name,
    #             self.table.name,
    #             e.response["Error"]["Code"],
    #             e.resopnse["Error"]["Message"],
    #         )
    #         raise
    #     else:
    #         return unprocessed_members

    # def get_member(self, name: str, score: int) -> Dict[str, int]:
    #     """
    #     Get member data from the table for a specific member.

    #     param: name: the name of the member character.
    #     param: score: highest m+ score for member character.
    #     :return: data about the requested member
    #     """
    #     try:
    #         response = self.table.get_item(Key={"character": name})
    #     except ClientError as e:
    #         logging.error(
    #             "Couldn't retrieve character %s from table %s. Reason %s: %s",
    #             name,
    #             self.table.name,
    #             e.response["Error"]["Code"],
    #             e.response["Error"]["Message"],
    #         )
    #         raise
    #     else:
    #         return response("Item")

    # def update_member_score_if_different(
    #     self, name: str, score: int
    # ) -> Dict[str, int] | None:
    #     """
    #     Update a member character's score

    #     param: name : the name of the member character.
    #     param score : highest m+ score for member character.

    #     :return: The fields that were updated, with their new value

    #     """

    #     try:
    #         response = self.table.get_item(Key={"character": name})
    #         item = response.get("Item")
    #         if item:
    #             current_val: float = float(item["score"]['N'])
    #             if score != current_val:
    #                 # update score
    #                 response = self.table.update_item(
    #                     Key={
    #                         "character": name,
    #                     },
    #                     UpdateExpression="SET score = :val",
    #                     ExpressionAttributeValues={":val": score},
    #                     ReturnValues="UPDATED_NEW",
    #                 )
    #                 updated_item = response.get("Attributes")
    #                 print("Item updated Successfully")
    #                 return updated_item
    #             else:
    #                 print(
    #                     "New value is the same as the existing value. No update performed"
    #                 )
    #         else:
    #             print("Item not found")
    #             return None

    #     except ClientError as e:
    #         logging.error(
    #             "Couldn't update character %s in table %s. Reason: %s: %s",
    #             name,
    #             self.table.name,
    #             e.response["Error"]["Code"],
    #             e.response["Error"]["Message"],
    #         )
    #         raise
    #     else:
    #         return response["Attributes"]
