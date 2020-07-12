import pytest

from fastredis import hiredis
from fastredis.wrapper_tools import convert_reply_array


REDIS_IP = '127.0.0.1'
REDIS_PORT = 6379


@pytest.fixture(scope='function', autouse=False)
def context():
    c = hiredis.redisConnect(REDIS_IP, REDIS_PORT)
    yield c
    hiredis.redisFree(c)


def cleanup_keys(context, keys):
    """Test helper to cleanup keys from redis."""

    for key in keys:
        r = hiredis.redisCommand(context, f'DEL {key}')
        if r is not None:
            hiredis.freeReplyObject(r)


def test_reply_constants_exist():
    constants = (
        'REDIS_REPLY_STRING',
        'REDIS_REPLY_ARRAY',
        'REDIS_REPLY_INTEGER',
        'REDIS_REPLY_NIL',
        'REDIS_REPLY_STATUS',
        'REDIS_REPLY_ERROR'
    )
    for constant in constants:
        assert hasattr(hiredis, constant)


def test_error_constants_exist():
    constants = (
        'REDIS_ERR',
        'REDIS_OK',
        'REDIS_ERR_IO',
        'REDIS_ERR_EOF',
        'REDIS_ERR_PROTOCOL',
        'REDIS_ERR_OOM',
        'REDIS_ERR_OTHER'
    )
    for constant in constants:
        assert hasattr(hiredis, constant)


@pytest.mark.timeout(3)
def test_redisConnect():
    c = hiredis.redisConnect(REDIS_IP, REDIS_PORT)
    assert c is not None
    assert c.err == hiredis.REDIS_OK
    hiredis.redisFree(c)


def test_redisCommand_noargs(context):
    key = 'testkey'
    value = 'testvalue'
    r = hiredis.redisCommand(context, f'SET {key} {value}')
    assert r is not None
    assert r.type == hiredis.REDIS_REPLY_STATUS
    assert r.str == 'OK'
    hiredis.freeReplyObject(r)
    r = hiredis.redisCommand(context, f'GET {key}')
    assert r is not None
    assert r.type == hiredis.REDIS_REPLY_STRING
    assert r.str == value
    hiredis.freeReplyObject(r)
    cleanup_keys(context, (key,))


def test_redisCommand_args(context):
    """Tests that using varargs is properly unsupported.

    The alternative is a seg fault.
    """
    key = 'testkey'
    value = 'testvalue'
    with pytest.raises(TypeError):
        hiredis.redisCommand(context, 'SET %s %s', key, value)


def test_redisCommand_reply_integer(context):
    key = 'testkey'
    value = 'foobar'
    value_bits = 26
    r = hiredis.redisCommand(context, f'SET {key} {value}')
    assert r is not None
    assert r.type == hiredis.REDIS_REPLY_STATUS
    assert r.str == 'OK'
    hiredis.freeReplyObject(r)
    r = hiredis.redisCommand(context, f'BITCOUNT {key}')
    assert r is not None
    assert r.type == hiredis.REDIS_REPLY_INTEGER
    assert r.integer == value_bits
    hiredis.freeReplyObject(r)
    cleanup_keys(context, (key,))


def test_redisCommand_reply_nil(context):
    key = 'testkey'
    r = hiredis.redisCommand(context, f'DEL {key}')
    assert r is not None
    assert r.type == hiredis.REDIS_REPLY_INTEGER
    assert r.integer in (0, 1)
    hiredis.freeReplyObject(r)
    r = hiredis.redisCommand(context, f'GET {key}')
    assert r is not None
    assert r.type == hiredis.REDIS_REPLY_NIL
    hiredis.freeReplyObject(r)
    cleanup_keys(context, (key,))


def test_redisCommand_reply_array(context):
    keys = [f'testkey{i}' for i in range(5)]
    for key in keys:
        r = hiredis.redisCommand(context, f'SET {key} {key}')
        assert r is not None
        assert r.type == hiredis.REDIS_REPLY_STATUS
        assert r.str == 'OK'
    r = hiredis.redisCommand(context, f'KEYS testkey*')
    assert r is not None
    assert r.type == hiredis.REDIS_REPLY_ARRAY
    convert_reply_array(r)
    assert r.elements >= len(keys)
    returned_keys = set()
    for subr in r.pyelements:
        assert subr.type == hiredis.REDIS_REPLY_STRING
        assert subr.len == len(subr.str)
        returned_keys.add(subr.str)
    assert set(keys) <= returned_keys
    cleanup_keys(context, keys)


def test_redisConnectWithTimeout():
    timeout = hiredis.timeval()
    timeout.tv_sec = 3
    timeout.tv_usec = 0
    c = hiredis.redisConnectWithTimeout(REDIS_IP, REDIS_PORT, timeout)
    assert c is not None
    hiredis.redisFree(c)


def test_redisAppendCommand(context):
    commands = (
        'SET testkey testvalue',
        'GET testkey',
        'DEL testkey'
    )
    for command in commands:
        assert (hiredis.redisAppendCommand(context, command)
            == hiredis.REDIS_OK
        )


def test_redisGetReply(context):
    KEY = 'testkey'
    VALUE = 'testvalue'
    commands = (
        f'SET {KEY} {VALUE}',
        f'GET {KEY}',
        f'DEL {KEY}'
    )
    for command in commands:
        assert (hiredis.redisAppendCommand(context, command)
            == hiredis.REDIS_OK
        )

    r = hiredis.redisReplyOut()
    hiredis.redisGetReplyOL(context, r)
    assert r.ret == hiredis.REDIS_OK
    assert r.reply.str == 'OK'
    hiredis.freeReplyObject(r.reply)

    hiredis.redisGetReplyOL(context, r)
    assert r.ret == hiredis.REDIS_OK
    assert r.reply.str == VALUE
    hiredis.freeReplyObject(r.reply)

    hiredis.redisGetReplyOL(context, r)
    assert r.ret == hiredis.REDIS_OK
    assert r.reply.integer == 1
    hiredis.freeReplyObject(r.reply)
