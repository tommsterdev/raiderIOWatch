import boto3
from typing import Any, Dict, List
from create_ddb import create_table, add_member, get_member, update_member_score_if_different

def main() -> None:
    # init dynamodb client
    dynamodb = boto3.resource('dynamodb')
    # define table parameters
    table_name = "Raider_IO_State"

    createed_table = create_table(table_name)

    item_to_add: Dict[str, Any] = {
        'name' : 'testMonkey',
        'score': int,
        'last_crawled_at': '',
    }
    added_item = add_member(name='testMonkey',score=4000,last_crawled_at=None)
    test_member = get_member('testMonkey', 4000)
    updated = update_member_score_if_different('testMonkey', 4100)
    print(updated)
    updated = update_member_score_if_different('testMonkey', 4100)
    print(updated) 
















if __name__ == "__main__":
    main()