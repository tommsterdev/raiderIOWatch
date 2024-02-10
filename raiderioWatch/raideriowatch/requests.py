import asyncio
from typing import Dict, List
from urllib3 import PoolManager, Timeout


# types
JSON = int | str | float | bool | None | Dict[str, "JSON"] | List["JSON"]
JSONObject = Dict[str, "JSON"]
JSONList = List["JSON"]
timeout = Timeout(connect=1.0, read=1.0)
http = PoolManager(num_pools=10, timeout=timeout)

def http_get(url: str) -> JSONObject:
    try:
        response = http.request("GET", url=url)

    except urllib3.exceptions.TimeoutError as e:
        print(f'could not retrieve data from{e.url}: {e.message}')

    except urllib3.exceptions.MaxRetryError as e:
        print(f'could not retrieve data from{e.url}: {e.message}')
        
    return response.json()

async def http_get_async(url: str) -> JSONObject:
    return await asyncio.to_thread(http_get, url)

