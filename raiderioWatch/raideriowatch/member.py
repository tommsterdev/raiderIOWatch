import json
import os
import csv
import dataclasses
import urllib3
import logging
import boto3
from typing import Dict, Any, List
from dataclasses import dataclass
from pydantic import BaseModel


class Member(BaseModel):
    name: str
    region: str
    realm: str
    race: str
    game_class: str
    active_spec: str | None
    active_role: str | None
    faction: str 
    last_crawled_at: str | None
    rank: int
    score: float = 0.0
    ilvl: float = 0.0



def create_member_from_request(member_entry: Dict[str, Any]) -> Dict[str, Any]:
    """
    Construct a Member object from raider io guild api request
    """
    member = {
        'rank' : member_entry['rank'],
        'name' : member_entry['character'].get('name'),
        'region' :  member_entry['character'].get('region', 'us'),
        'realm' : member_entry['character'].get('realm'),
        'race' : member_entry['character'].get('race'),
        'game_class' : member_entry['character'].get('class'),
        'active_spec' : member_entry['character'].get('active_spec_name'),
        'active_role' : member_entry['character'].get('active_spec_role'),
        'faction' : member_entry['character'].get('faction'),
        'last_crawled_at' : member_entry['character'].get('last_crawled_at'),
    }  

    return Member(**member)