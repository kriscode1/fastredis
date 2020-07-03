import pytest

from fastredis.exceptions import *
from fastredis.wrappersb import (
    redis_command,
    redis_connect,
    redis_free,
    redis_write,
    redis_read,
)


@pytest.fixture(scope='function', autouse=False)
def context():
    c = redis_connect('127.0.0.1'.encode())
    yield c
    redis_free(c)


def test_redis_connect():
    context = redis_connect('127.0.0.1'.encode())
    assert context is not None
    redis_free(context)


def test_redis_command_b(context):
    key = 'testkey'
    value = 'testvalue'
    assert redis_command(context, f'SET {key} {value}'.encode()) == b'OK'
    assert redis_command(context, f'GET {key}'.encode()) == value.encode()
    with pytest.raises(ReplyError):
        redis_command(context, f'ZADD {key} 0 {value}'.encode())
    assert redis_command(context, f'DEL {key}'.encode()) == 1
    assert redis_command(context, f'GET {key}'.encode()) == None


def test_redis_connect_with_timeout_success():
    c = redis_connect(b'127.0.0.1', timeout=1)
    assert c is not None
    redis_free(c)


def test_redis_connect_with_timeout_failure():
    with pytest.raises(IOError):
        redis_connect(b'128.0.0.1', timeout=1)


def test_redis_write_and_read(context):
    keys = [f'testkey{i}' for i in range(10)]
    for key in keys:
        redis_write(context, f'SET {key} {key}'.encode())
        redis_write(context, f'GET {key}'.encode())
    for key in keys:
        assert redis_read(context) == b'OK'
        assert redis_read(context) == key.encode()
