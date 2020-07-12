import asyncio
from fastredis.connections import (
    AsyncConnection,
    AsyncConnectionStr
)
import pytest


REDIS_IP = '127.0.0.1'


@pytest.fixture(scope='function', autouse=False)
def loop():
    loop = asyncio.get_event_loop()
    if loop.is_closed():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    yield loop
    loop.close()


def test_connect(loop):
    async def test():
        async with AsyncConnection(REDIS_IP) as redis:
            assert isinstance(redis, AsyncConnectionStr)
    loop.run_until_complete(test())


def test_command(loop):
    KEY = 'testkey'
    VALUE = 'testvalue'
    async def test():
        async with AsyncConnection(REDIS_IP) as redis:
            assert await redis.command(f'SET {KEY} {VALUE}') == 'OK'
            assert await redis.command(f'GET {KEY}') == VALUE
            assert await redis.command(f'DEL {KEY}') == 1
    loop.run_until_complete(test())


