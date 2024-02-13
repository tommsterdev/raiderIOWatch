import asyncio
import httpx
from typing import Dict, List

# from urllib3 import PoolManager, Timeout
import os
from dotenv import load_dotenv

load_dotenv()

API_URL = os.getenv("API_URL")


# types
JSON = int | str | float | bool | None | Dict[str, "JSON"] | List["JSON"]
JSONObject = dict[str, "JSON"]
JSONList = List["JSON"]
# timeout = Timeout(connect=1.0, read=1.0)
# http = PoolManager(num_pools=100, timeout=timeout)


async def httpx_get(
    url: str, client: httpx.Client, params: Dict[str, str]
) -> JSONObject:
    try:
        response = await client.get(url=url, params=params)
        response.raise_for_status()

    except httpx.RequestError as e:
        print(f"could not retrieve data from {e.request.url!r}")
        return {"Error": e.request.url}
    except httpx.HTTPStatusError as e:
        print(f"could not retrieve data from{e.request}: {e.request.url!r}")
        return {"statusCode": e.request, "Error": e.request.url}

    return response.json()


"""
urllib3 blocking requests 

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

"""
