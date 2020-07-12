import asyncio
import pytest

from fastredis.exceptions import *
import fastredis.hiredis as hiredis
from fastredis.wrappers_async import (
    redis_command,
    redis_connect,
    redis_set_connect_cb,
    redis_set_disconnect_cb,
    redis_disconnect,
    redis_free
)

REDIS_IP = '127.0.0.1'


@pytest.fixture(scope='function', autouse=False)
def loop():
    loop = asyncio.get_event_loop()
    if loop.is_closed():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    yield loop
    loop.close()


@pytest.fixture(scope='function', autouse=False)
def connected(loop):
    """Fixture to connect to redis asynchronously."""

    context, not_garbage = redis_connect(REDIS_IP)
    connected = loop.create_future()
    disconnected = loop.create_future()

    def connected_cb(_context, status):
        if status != hiredis.REDIS_OK:
            try:
                raise_context_error(context)
            except Exception as e:
                connected.set_exception(e)
                return
        connected.set_result(True)

    def disconnected_cb(_context, status):
        if status != hiredis.REDIS_OK:
            try:
                raise_context_error(context)
            except Exception as e:
                disconnected.set_exception(e)
                return
        disconnected.set_result(True)

    not_garbage += redis_set_connect_cb(context, connected_cb)
    not_garbage += redis_set_disconnect_cb(context, disconnected_cb)

    loop.run_until_complete(connected)

    yield loop, context

    redis_free(context)
    loop.run_until_complete(disconnected)


def test_redis_connect_free(loop):
    async def test():
        context, not_garbage = redis_connect(REDIS_IP)
        connected = loop.create_future()
        disconnected = loop.create_future()

        def connected_cb(_context, status):
            if status != hiredis.REDIS_OK:
                try:
                    raise_context_error(context)
                except Exception as e:
                    connected.set_exception(e)
                    raise
            connected.set_result(True)

        def disconnected_cb(_context, status):
            if status != hiredis.REDIS_OK:
                try:
                    raise_context_error(context)
                except Exception as e:
                    disconnected.set_exception(e)
                    raise
            disconnected.set_result(True)

        not_garbage += redis_set_connect_cb(context, connected_cb)
        not_garbage += redis_set_disconnect_cb(context, disconnected_cb)
        await connected
        redis_free(context)
        await disconnected

    loop.run_until_complete(test())


def test_redis_connect_disconnect(loop):
    """Tests connecting to redis and using redis_disconnect() to clean up.

    redis_disconnect() will block new commands from being added to the write
    buffer, flush the write buffer, process all replies, and then free the
    context. This is a less abrupt cleanup than redis_free().
    """

    async def test():
        context, not_garbage = redis_connect(REDIS_IP)
        connected = loop.create_future()
        disconnected = loop.create_future()

        def connected_cb(_context, status):
            if status != hiredis.REDIS_OK:
                try:
                    raise_context_error(context)
                except Exception as e:
                    connected.set_exception(e)
                    raise
            connected.set_result(True)

        def disconnected_cb(_context, status):
            if status != hiredis.REDIS_OK:
                try:
                    raise_context_error(context)
                except Exception as e:
                    disconnected.set_exception(e)
                    raise
            disconnected.set_result(True)

        not_garbage += redis_set_connect_cb(context, connected_cb)
        not_garbage += redis_set_disconnect_cb(context, disconnected_cb)
        await connected
        redis_disconnect(context)
        await disconnected

    loop.run_until_complete(test())


def test_redis_command(connected):
    loop, context = connected
    KEY = 'testkey'
    VALUE = 'testvalue'

    async def test():
        assert await redis_command(context, f'SET {KEY} {VALUE}') == 'OK'
        assert await redis_command(context, f'GET {KEY}') == VALUE
        with pytest.raises(ReplyError):
            await redis_command(context, f'ZADD {KEY} 0 {VALUE}')
        assert await redis_command(context, f'DEL {KEY}') == 1
        assert await redis_command(context, f'GET {KEY}') == None

    loop.run_until_complete(test())


