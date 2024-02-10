from ..raideriowatch.member_crawler import member_crawler
import logging
import urllib3



def test_num_pulls_thirty():
    http = urllib3.PoolManager(num_pull=30)
    logging.info(http)
    


def test_num_pools_ten():
    http = urllib3.PoolManager(num_pull=10)
    pass


def test_num_pools_hundred(num_pull=100):
    pass