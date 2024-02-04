import boto3
from typing import Dict, Any
import json
import logging

ddb = boto3.client('dynamodb')
sns = boto3.client('sns')

logger = logging.getLogger(__name__)

TABLE_NAME = 'raider-io-ranks'
SNS_ARN = 'arn:aws:sns:us-east-1:339713051594:raider-io-watch'
PARTITION_KEY = 'name'
PARTITION_VALUE = 'name'

    
def update_db_entry(name: str, score: int, last_crawled_at: str, realm: str) -> str:

    try:
        response = ddb.update_item(
            TableName = TABLE_NAME,
            Key = {f'name': {'S': name}},
            UpdateExpression='SET score = :score, last_crawled_at = :last_crawled_at, realm = :realm',
            ExpressionAttributeValues = {
                ':score' : {'N': str(score)},
                ':last_crawled_at': {'S': last_crawled_at},
                ':realm' : {'S': realm},
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

