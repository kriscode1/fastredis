import pytest

from fastredis import hiredis
from fastredis import hiredisb
from fastredis.wrapper_tools import convert_reply_array_b


REDIS_IP = '127.0.0.1'.encode()
REDIS_PORT = 6379


@pytest.fixture(scope='function', autouse=False)
def context():
    c = hiredisb.redisConnect_b(REDIS_IP, REDIS_PORT)
    yield c
    hiredisb.redisFree_b(c)


def cleanup_keys(context, keys):
    """Test helper to cleanup keys from redis."""

    for key in keys:
        r = hiredisb.redisCommand_b(context, f'DEL {key}'.encode())
        if r is not None:
            hiredisb.freeReplyObject_b(r)


@pytest.mark.timeout(3)
def test_redisConnect_b():
    c = hiredisb.redisConnect_b(REDIS_IP, REDIS_PORT)
    assert c is not None
    assert c.err == hiredis.REDIS_OK
    hiredisb.redisFree_b(c)


def test_redisCommand_b_noargs(context):
    key = 'testkey'
    value = 'testvalue'
    r = hiredisb.redisCommand_b(context, f'SET {key} {value}'.encode())
    assert r is not None
    assert r.type == hiredis.REDIS_REPLY_STATUS
    assert r.str == b'OK'
    hiredisb.freeReplyObject_b(r)
    r = hiredisb.redisCommand_b(context, f'GET {key}'.encode())
    assert r is not None
    assert r.type == hiredis.REDIS_REPLY_STRING
    assert r.str == value.encode()
    hiredisb.freeReplyObject_b(r)
    cleanup_keys(context, (key,))


def test_redisCommand_b_args(context):
    """Tests that using varargs is properly unsupported.

    The alternative is a seg fault.
    """
    key = 'testkey'
    value = 'testvalue'
    with pytest.raises(TypeError):
        hiredisb.redisCommand_b(context, 'SET %s %s'.encode(), key, value)


def test_redisCommand_b_reply_integer(context):
    key = 'testkey'
    value = 'foobar'
    value_bits = 26
    r = hiredisb.redisCommand_b(context, f'SET {key} {value}'.encode())
    assert r is not None
    assert r.type == hiredis.REDIS_REPLY_STATUS
    assert r.str == b'OK'
    hiredisb.freeReplyObject_b(r)
    r = hiredisb.redisCommand_b(context, f'BITCOUNT {key}'.encode())
    assert r is not None
    assert r.type == hiredis.REDIS_REPLY_INTEGER
    assert r.integer == value_bits
    hiredisb.freeReplyObject_b(r)
    cleanup_keys(context, (key,))


def test_redisCommand_b_reply_nil(context):
    key = 'testkey'
    r = hiredisb.redisCommand_b(context, f'DEL {key}'.encode())
    assert r is not None
    assert r.type == hiredis.REDIS_REPLY_INTEGER
    assert r.integer in (0, 1)
    hiredisb.freeReplyObject_b(r)
    r = hiredisb.redisCommand_b(context, f'GET {key}'.encode())
    assert r is not None
    assert r.type == hiredis.REDIS_REPLY_NIL
    hiredisb.freeReplyObject_b(r)
    cleanup_keys(context, (key,))


def test_redisCommand_b_reply_array(context):
    keys = [f'testkey{i}' for i in range(5)]
    for key in keys:
        r = hiredisb.redisCommand_b(context, f'SET {key} {key}'.encode())
        assert r is not None
        assert r.type == hiredis.REDIS_REPLY_STATUS
        assert r.str == b'OK'
    r = hiredisb.redisCommand_b(context, f'KEYS testkey*'.encode())
    assert r is not None
    assert r.type == hiredis.REDIS_REPLY_ARRAY
    convert_reply_array_b(r)
    assert r.elements >= len(keys)
    returned_keys = set()
    for subr in r.pyelements:
        assert subr.type == hiredis.REDIS_REPLY_STRING
        assert subr.len == len(subr.str)
        returned_keys.add(subr.str)
    assert set(key.encode() for key in keys) <= returned_keys
    cleanup_keys(context, keys)


def test_redisConnectWithTimeout_b():
    timeout = hiredis.timeval()
    timeout.tv_sec = 3
    timeout.tv_usec = 0
    c = hiredisb.redisConnectWithTimeout_b(REDIS_IP, REDIS_PORT, timeout)
    assert c is not None
    hiredisb.redisFree_b(c)


def test_redisAppendCommand_b(context):
    commands = (
        'SET testkey testvalue',
        'GET testkey',
        'DEL testkey'
    )
    for command in commands:
        assert (hiredisb.redisAppendCommand_b(context, command.encode())
            == hiredis.REDIS_OK
        )


def test_redisGetReply_b(context):
    KEY = 'testkey'
    VALUE = 'testvalue'
    commands = (
        f'SET {KEY} {VALUE}',
        f'GET {KEY}',
        f'DEL {KEY}'
    )
    for command in commands:
        assert (hiredisb.redisAppendCommand_b(context, command.encode())
            == hiredis.REDIS_OK
        )

    r = hiredisb.redisReplyOut_b()
    hiredisb.redisGetReplyOL_b(context, r)
    assert r.ret == hiredis.REDIS_OK
    assert r.reply.str == b'OK'
    hiredisb.freeReplyObject_b(r.reply)

    hiredisb.redisGetReplyOL_b(context, r)
    assert r.ret == hiredis.REDIS_OK
    assert r.reply.str == VALUE.encode()
    hiredisb.freeReplyObject_b(r.reply)

    hiredisb.redisGetReplyOL_b(context, r)
    assert r.ret == hiredis.REDIS_OK
    assert r.reply.integer == 1
    hiredisb.freeReplyObject_b(r.reply)
