import logging
import pytest
from src.guild_crawler import get_guild

import httpx

from models.member import Member

@pytest.mark.asyncio
async def test_get_guild_returns_list():
    async with httpx.AsyncClient() as client:
        response: list[Member] = await get_guild(client)
    logging.debug(f"response: {response}")
    assert isinstance(response, list)

@pytest.mark.asyncio
async def test_get_guild_returns_valid_data():
    async with httpx.AsyncClient() as client:
        response: list[Member] = await get_guild(client)
    assert all(isinstance(item, Member) for item in response)
    assert all(hasattr(item, "character_name") and hasattr(item, "realm") for item in response)

@pytest.mark.asyncio
async def test_get_guild_returns_non_empty_list():
    async with httpx.AsyncClient() as client:
        response: list[Member] = await get_guild(client)
    assert len(response) > 0


