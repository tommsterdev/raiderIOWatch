import os
import json
import logging
from guild_crawler import get_guild


def test_get_guild_returns_list():
    response = get_guild()
    # logging.info(response)
    logging.info(f"example response: {response[0]}")
    logging.info(f"response length = {len(response)}")
    assert isinstance(response, list)
