import boto3
import logging

from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from typing import Any, Dict, List, Optional
from member import Member
from utils import create_member_item, ITEM, ATTRIBUTE


logger = logging.getLogger(__name__)


class DDBTable:
    """Represents Amazon DynamoDB table of guild data."""

    def __init__(self, dynamic_resources: boto3.resource):
        """
        dynamic resource : A Boto3 DynamoDB resource.
        """
        self.dynamic_resources = dynamic_resources
        self.table = None

    # character: str = ''
    # rank: int = 0
    # race: str = ''
    # spec: str = ''
    # role: str = ''
    # score: int = 0

    def create_table(self, table_name: str) -> boto3.resource:
        """
        Create an Amazon DynamoDB table that stores character data.
        The table uses the character name as the partition key and
        Score as the sort key
        """
        try:
            self.table = self.dynamic_resources.create_table(
                TableName=table_name,
                KeySchema=[
                    {"AttributeName": "character", "KeyType": "HASH"},  # partition key
                    {"AttributeName": "score", "KeyType": "RANGE"},  # Sort key
                ],
                AttributeDefinitions=[
                    {"AttributeName": "character", "AttributeType": "S"},
                    {"AttributeName": "score", "AttributeType": "N"},
                    {"AttributeName": "realm", "AttributeType": "S"},
                    {"AttributeName": "region", "AttributeType": "S"},
                ],
                ProvisionedThroughput={
                    "ReadCapacityUnits": 10,
                    "WriteCapacityUnits": 10,
                },
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                exists = False
            else:
                logger.error(
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
            logger.error(
                "Couldn't list tables. Reason: %s: %s",
                e.response["Error"]["Code"],
                e.response["Error"]["Message"],
            )
            raise
        else:
            return tables

    def add_member(member: Member) -> None:
        """
        Adds a member to the guild table.

        param name: character name
        param score: m+ score (current season)
        param last_crawled_at: last time character info was retreived
        param region: region default us
        param realm: realm default tichondrius
        """
        try:
            self.table.put_item(
                Item=create_member_item(member)
            )
        except ClientError as e:
            logger.error(
                "Couldn't add character %s to table %s. Reason: %s: %s",
                name,
                self.table.name,
                e.response["Error"]["Code"],
                e.resopnse["Error"]["Message"],
            )
            raise


    def batch_add_member(members: List[Member]) -> List[Member]:
        """
        batch adds a member to the guild table.

        returns any unprocessed items
        

        """
        {'PutRequest': {'Item': {'PK': {'S': 'hi'}, 'SK': {'S': 'user-1'}}}}
        items: Dict[str, ITEM] = {'Item':create_member_item(member) for member in members}
        requests = {'PutRequest':item for item in items}
        table_name: str = self.table_name
        requested_items=
        'PutRequest'
        
        try:
            response = self.batch_write_item(
                RequestItems= {
                    self.table_name : [create_member_item(member) for member in members]
                }
            )
        except ClientError as e:
            logger.error(
                "Couldn't add character %s to table %s. Reason: %s: %s",
                name,
                self.table.name,
                e.response["Error"]["Code"],
                e.resopnse["Error"]["Message"],
            )
            raise

    def get_member(self, name: str, score: int) -> Dict[str, int]:
        """
        Get member data from the table for a specific member.

        param: name: the name of the member character.
        param: score: highest m+ score for member character.
        :return: data about the requested member
        """
        try:
            response = self.table.get_item(Key={"character": name, "score": score})
        except ClientError as e:
            logger.error(
                "Couldn't retrieve character %s from table %s. Reason %s: %s",
                name,
                self.table.name,
                e.response["Error"]["Code"],
                e.response["Error"]["Message"],
            )
            raise
        else:
            return response("Item")

    def update_member_score_if_different(
        self, name: str, score: int
    ) -> Dict[str, int] | None:
        """
        Update a member character's score

        param: name : the name of the member character.
        param score : highest m+ score for member character.

        :return: The fields that were updated, with their new value

        """

        try:
            response = self.table.get_item(Key={"character": name})
            item = response.get("Item")
            if item:
                current_val: int = item["score"]
                if score != current_val:
                    # update score
                    response = self.table.update_item(
                        Key={
                            "character": name,
                        },
                        UpdateExpression="SET score = :val",
                        ExpressionAttributeValues={":val": score},
                        ReturnValues="UPDATED_NEW",
                    )
                    updated_item = response.get("Attributes")
                    print("Item updated Successfully")
                    return updated_item
                else:
                    print(
                        "New value is the same as the existing value. No update performed"
                    )
            else:
                print("Item not found")
                return None

        except ClientError as e:
            logger.error(
                "Couldn't update character %s in table %s. Reason: %s: %s",
                name,
                self.table.name,
                e.response["Error"]["Code"],
                e.response["Error"]["Message"],
            )
            raise
        else:
            return response["Attributes"]


def main() -> None:
    pass


if __name__ == "__main__":
    main()
