import boto3
import json
import logging
import os

from dotenv import load_dotenv
from typing import Dict, Any, List

from member import Member

#ddb = boto3.client('dynamodb')
#sns = boto3.client('sns')
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
    
def parse_data(data: Dict[str,Any]) -> (float, float):

    scores = data.get('mythic_plus_scores_by_season', None)
    """
    optionality get spec and role scores here
    """
    score = 0.0
    if scores:
    # get current season top score for all specs
        score = scores[0]['scores']['all']
        if score != 0:
            print(f'updating new score is {score}')

    # get gear information
    gear = data.get('gear', None)
    logging.info(data.get('gear'), None)
    ilvl = 0.0
    if gear:
        ilvl = float(gear['item_level_equipped'])
    
    return score, ilvl


def load_data() -> List[Member]:
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return [Member(**member) for member in data]


def write_members(members: List[Dict[str, Any]], output_file: str) -> None:

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(json.dumps(members, ensure_ascii=False, indent=4))


def s3_upload_member(members: List[Dict[str, Any]], bucket: str, key: str) -> None:

    # create s3 object
    s3object = s3.Object(bucket, key)

    try:
        s3object.put(
            Body = (bytes(json.dumps(obj=members, cls=EnhancedJSONEncoder, ensure_ascii=False, indent=4).encode('utf-8'))),
            ContentType = 'application/json'
        )
    except ClientError as e:
        logger.exception(
            "Error uploading object %s to bucket %s",
            bucket,
            key,
        )
        raise
    else:
        logger.info(f"Put object {key} to bucket {bucket}")



