import pytest

from fastredis.exceptions import *
from fastredis.wrappers import (
    redis_command,
    redis_connect,
    redis_free,
    redis_write,
    redis_read
)


@pytest.fixture(scope='function', autouse=False)
def context():
    c = redis_connect('127.0.0.1')
    yield c
    redis_free(c)


def test_redis_connect():
    context = redis_connect('127.0.0.1')
    assert context is not None
    redis_free(context)


def test_redis_command(context):
    key = 'testkey'
    value = 'testvalue'
    assert redis_command(context, f'SET {key} {value}') == 'OK'
    assert redis_command(context, f'GET {key}') == value
    with pytest.raises(ReplyError):
        redis_command(context, f'ZADD {key} 0 {value}')
    assert redis_command(context, f'DEL {key}') == 1
    assert redis_command(context, f'GET {key}') == None


def test_redis_connect_with_timeout_success():
    c = redis_connect('127.0.0.1', timeout=1)
    assert c is not None
    redis_free(c)


def test_redis_connect_with_timeout_failure():
    with pytest.raises(IOError):
        redis_connect('128.0.0.1', timeout=1)


def test_redis_write_and_read(context):
    keys = [f'testkey{i}' for i in range(10)]
    for key in keys:
        redis_write(context, f'SET {key} {key}')
        redis_write(context, f'GET {key}')
    for key in keys:
        assert redis_read(context) == 'OK'
        assert redis_read(context) == key
