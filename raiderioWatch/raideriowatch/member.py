import json
import os
import csv
import dataclasses
import urllib3
import logging
import boto3
from typing import Dict, Any, List
from dataclasses import dataclass


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

    return Member(
        name=name,
        region=region,
        realm=realm,
        race=race,
        game_class=game_class,
        active_spec=active_spec,
        active_role=active_role,
        faction=faction,
        last_crawled_at=last_crawled_at,
        rank=rank
        )