import json
import os
import csv
from typing import Dict, Any, List
import dataclasses
from dataclasses import dataclass
import urllib3





http = urllib3.PoolManager()
FIELDS = 'mythic_plus_score_by_season:current'
GUILD_FIELD = ''
API_URL = "https://raider.io/api/v1/characters/profile"
GUILD_URL = 'https://raider.io/api/v1/guilds/profile'
GUILD_NAME = 'SWMG'


class EnhancedJSONEncoder(json.JSONEncoder):
        def default(self, o):
            if dataclasses.is_dataclass(o):
                return dataclasses.asdict(o)
            return super().default(o)


@dataclass
class Member(json.JSONEncoder):
    name: str
    region: str
    realm: str
    race: str
    game_class: str
    active_spec: str
    active_role: str
    faction: str
    last_crawled_at: str
    rank: int
    score: int = 0

    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)

def create_member(member_entry: Dict[str, Any]) -> Member:
    rank = member_entry['rank']
    name = member_entry['character'].get('name')
    region = member_entry['character'].get('region', 'us')
    realm = member_entry['character'].get('realm', 'tichondrius')
    race = member_entry['character'].get('race')
    game_class = member_entry['character'].get('class')
    active_spec = member_entry['character'].get('active_spec_name')
    active_role = member_entry['character'].get('active_spec_role')
    faction = member_entry['character'].get('faction')
    last_crawled_at = member_entry['character'].get('last_crawled_at')

    return Member(name, region, realm, race, game_class, active_spec, active_role, faction, last_crawled_at, rank)

def write_members(members: List[Dict[str, Any]], output_file: str) -> None:
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(json.dumps(members, cls=EnhancedJSONEncoder, ensure_ascii=False, indent=4))


def get_guild() -> Dict[Any, Any]:
    params = {
        "region" : "us",
        "realm" : "tichondrius",
        "name" : GUILD_NAME,
    }
    full_url = f"{GUILD_URL}?region={params['region']}&realm={params['realm']}&name={params['name']}&fields=members"
    print(f'requesting members from {full_url}')
    response = http.request(method='GET', url=full_url)

    data = json.loads(response.data.decode('utf-8'))
    print(f'got response from raider-io server test example={data["members"][-1]}')
    members = data['members']
    return members


def main() -> None:
    data = get_guild()
    members: Dict[Member] = {}
    for entry in data:
        character = create_member(entry)
        members[character.name] = character
    write_members(members, 'processed_members.json')

if __name__ == '__main__':
    main()