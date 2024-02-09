from member_crawler import load_data
import os
from member import Member
import logging


INPUT_FILE = os.getenv('INPUT_FILE')

def test_load_data_returns_valid_member():
    guild_data = load_data()
    for member in guild_data:
        logging.info(member)
        assert(isinstance(member, Member))
