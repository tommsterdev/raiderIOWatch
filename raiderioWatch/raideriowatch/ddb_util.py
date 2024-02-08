import boto3
from typing import Dict, Any
import json
import logging
import os
from dotenv import load_dotenv

ddb = boto3.client('dynamodb')
sns = boto3.client('sns')
load_dotenv()

logger = logging.getLogger(__name__)
# TODO: *** move to env variables ***
TABLE_NAME = os.getenv('TABLE_NAME')
SNS_ARN = os.getenv('SNS_ARN')
PARTITION_KEY = ('PARTITION_KEY')
# ***********************************


def create_member_item(self, item: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Constructs member ddb_item from dict
    """
    ddb_item = {
        'character': {'S': item.get('character', '')},
        'realm' : {'S': item.get('realm', '')},
        'region': {'S': item.get('region', 'us')},
        'score' : {'N': item.get('score', 0)},
    }
    return ddb_item

def extract_member_item(self, item: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """
    deconstructs member ddb_item from dict
    """
    char_item = {
        'character': item['character']['S'],
        'realm' : item['realm']['S'],
        'region': item['region']['S'],
        'score' : int(item['score']['N']),
    }
    return char_item


def ddb_put_item(table_name: str, item: Dict[str, Any]) -> None:
        """
        adds a generic item  to the dynamodb table.

        param name: character name
        """
        # construct ddb item
        ddb_item = creater_member_item(item)
        try:
            response = ddb.put_item(
                TableName=table_name,
                Item=ddb_item
            )
            logging.info(response)
        except ClientError as e:
            logger.error(
                "Error adding item %s to table %s. Reason: %s: %s",
                ddb_item,
                self.table.name,
                e.response["Error"]["Code"],
                e.resopnse["Error"]["Message"],
            )
            raise

# TODO: make generic 
def ddb_update_item(table_name:str, pk: str, sk: str | None, item: Dict[str, Any]) -> None:

    try:

        key = {
            pk : {'S' : item['character']},
        }
        response = ddb.update_item(
            TableName = table_name,
            Key = key,
            UpdateExpression='SET score = :score',
            ExpressionAttributeValues = {
                ':score' : {'N': str(item['score'])},
            },
            ReturnValues = 'ALL_NEW'
        )
        
        #extract the updated attributes from the response
        updated_attributes = response.get('Attributes', {})
        return {
            'statusCode' : 200,
            'body': updated_attributes
        }
    except Exception as e:
        return {
            'statusCode' : 500,
            'body' : str(e),
        }
        
def query_ddb_entry(partition_value: Any) -> Dict[str, Any]:
    try:
        # query dynamodb table
        resp = ddb.query(
            TableName = TABLE_NAME,
            KeyConditionExpression=f"#{PARTITION_KEY} = :{PARTITION_VALUE}",
            ExpressionAttributeNames={f'#{PARTITION_KEY}': f'{PARTITION_VALUE}'},
            ExpressionAttributeValues={
                f':{PARTITION_VALUE}': {'S': partition_value}
                }
        )
    except Exception as e:
        return {
            'statusCode': 500,
            'body' : str(e)
        }
    else:
        return resp
    
def sns_publish(attributes: Dict[str, Any]) -> None:
    print(attributes)
    score = attributes['score'].get('N')
    name = attributes['name'].get('S')
    realm = attributes['realm'].get('S')
    last_crawled_at = attributes['last_crawled_at'].get('S')
    message = f'{name}, {realm} your score has increased! new m+ score is ***{score}***, recorded at {last_crawled_at}\n Sent from RaiderIOWatch'
   
    sns.publish(
        TopicArn=SNS_ARN,
        Message=message,
    )
    print(f'published {message} on topic {SNS_ARN}')
    return
    





def lambda_handler(event, context) -> Dict[str, Any]:
    try:
        # get relevant fields
        name: str = event.get('name', None)
        score: int = event.get('score', None)
        last_crawled_at: str = event.get('last_crawled_at', None)
        realm: str = event.get('realm', 'tichondrius')
        if not name:
            return {
                'statusCode': 400,
                'body': 'Error: Name attribute is missing in event'
            }

        resp = query_ddb_entry(partition_value=name)
        
        items = resp.get('Items', [])
        parse_items = []

        if not items:
            return {
                'statusCode' : 404,
                'body': f'Error: no match for $KEY={PARTITION_VALUE}'
            }

        for item in items:
            parse_item = {
                'name' : item.get('name', {}).get('S', None),
                'score' : int(item.get('score', {}).get('N', 0)),
                'last_crawled_at' : item.get('last_crawled_at', {}).get('S', None),
                'realm' : item.get('realm', {}).get('S', None),
            }
            parse_items.append(parse_item)

        if items[0]:
            query_resp = items[0]
            query_score = int(query_resp['score']['N'])
            # compare event score with query response score
            # if event score > query score, update value
            if score > query_score:
                print('updating score...')
                updated_attributes = update_db_entry(name=name, score=score, last_crawled_at=last_crawled_at, realm=realm)
                print('publishing to raiderIOWatch...')
                sns_publish(updated_attributes['body'])

            # else do nothing
            else:
                print('Score unchanged.')
                return {
                    'statusCode': 200,
                    'body' : json.dumps(items[0])
                }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body' : str(e)
        }
    else:
        return updated_attributes

