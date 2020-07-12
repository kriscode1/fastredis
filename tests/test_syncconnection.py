from fastredis.connections import (
    SyncConnection,
    SyncConnectionBytes,
    SyncConnectionStr
)


REDIS_IP = '127.0.0.1'


def test_connect():
    with SyncConnection(REDIS_IP) as redis:
        assert isinstance(redis, SyncConnectionStr)


def test_command():
    KEY = 'testkey'
    VALUE = 'testvalue'
    with SyncConnection(REDIS_IP) as redis:
        assert redis.command(f'SET {KEY} {VALUE}') == 'OK'
        assert redis.command(f'GET {KEY}') == VALUE
        assert redis.command(f'DEL {KEY}') == 1


def test_write_read():
    KEY = 'testkey'
    VALUE = 'testvalue'
    with SyncConnection(REDIS_IP) as redis:
        redis.write(f'SET {KEY} {VALUE}')
        redis.write(f'GET {KEY}')
        redis.write(f'DEL {KEY}')
        assert redis.read() == 'OK'
        assert redis.read() == VALUE
        assert redis.read() == 1


def test_bytes():
    KEY = 'testkey'
    VALUE = 'testvalue'
    with SyncConnection(REDIS_IP.encode(), encoding=None) as r:
        assert isinstance(r, SyncConnectionBytes)
        assert r.command(f'SET {KEY} {VALUE}'.encode('utf-8')) == b'OK'
        assert r.command(f'GET {KEY}'.encode('utf-8')) == VALUE.encode('utf-8')
        assert r.command(f'DEL {KEY}'.encode('utf-8')) == 1
