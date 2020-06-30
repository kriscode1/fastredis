from fastredis.connections import SyncConnection


REDIS_IP = '127.0.0.1'


def test_syncconnection_connect():
    with SyncConnection(REDIS_IP) as redis:
        pass


def test_syncconnection_command():
    with SyncConnection(REDIS_IP) as redis:
        KEY = 'testkey'
        VALUE = 'testvalue'
        assert redis.command(f'SET {KEY} {VALUE}') == 'OK'
        assert redis.command(f'GET {KEY}') == VALUE
        assert redis.command(f'DEL {KEY}') == 1


def test_syncconnection_read_write():
    KEY = 'testkey'
    VALUE = 'testvalue'
    with SyncConnection(REDIS_IP) as redis:
        redis.write(f'SET {KEY} {VALUE}')
        redis.write(f'GET {KEY}')
        redis.write(f'DEL {KEY}')
        assert redis.read() == 'OK'
        assert redis.read() == VALUE
        assert redis.read() == 1

