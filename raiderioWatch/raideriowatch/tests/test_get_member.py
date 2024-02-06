from member_crawler import crawl_member
import os
import json
import logging


def test_member_crawler_returns_integer():
    response = crawl_member({'region' : os.getenv('REGION'), 'realm' : os.getenv('REALM'), 'name' : os.getenv('NAME')})
    logging.info(response)
    assert(isinstance(response, float))

def test_member_crawler_bad_region():
    response = crawl_member({'region' : os.getenv("BAD_REGION"), 'realm' : os.getenv("REALM"), 'name' : os.getenv("NAME")})
    assert(not response)


def test_member_crawler_bad_realm():
    response = crawl_member({'region' : os.getenv("REGION"), 'realm' : os.getenv("BAD_REALM"), 'name' : os.getenv("NAME")})
    assert(not response)


def test_member_crawler_bad_name():
    response = crawl_member({'region' : os.getenv("REGION"), 'realm' : os.getenv("REALM"), 'name' : os.getenv("BAD_NAME")})
    assert(not response)


def test_member_crawler_inactive_member():
    response = crawl_member({'region' : os.getenv("REGION"), 'realm' : os.getenv("REALM"), 'name' : os.getenv("NO_SCORE")})
    assert(not response)


