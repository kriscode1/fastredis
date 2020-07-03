import pytest


REDIS_IP = '127.0.0.1'
REDIS_PORT = 6379
KEY = 'testkey'
VAL = 'testvalue'

REDIS_IP_B = REDIS_IP.encode()
KEY_B = KEY.encode()
VAL_B = VAL.encode()


@pytest.fixture(scope='function', autouse=False)
def keys():
    KEY_COUNT = 1000
    yield [f'testkey{i}' for i in range(KEY_COUNT)]


############################################################
############################################################
# Str Benchmarks
############################################################
############################################################


@pytest.mark.benchmark(group='single_set_get_del')
def test_single_set_get_del_fastredis(benchmark):
    import fastredis as fr
    def work():
        with fr.SyncConnection(REDIS_IP, REDIS_PORT) as r:
            assert r.command('SET testkey testvalue') == 'OK'
            assert r.command('GET testkey') == 'testvalue'
            assert r.command('DEL testkey') == 1
    benchmark(work)


@pytest.mark.benchmark(group='single_set_get_del')
def test_single_set_get_del_redis(benchmark):
    import redis
    def work():
        with redis.Redis(REDIS_IP, REDIS_PORT, decode_responses=True) as r:
            assert r.set(KEY, VAL)
            assert r.get(KEY) == VAL
            assert r.delete(KEY) == 1
    benchmark(work)


@pytest.mark.benchmark(group='single_set_get_del')
def test_single_set_get_del_pyredis(benchmark):
    import pyredis
    def work():
        r = None
        try:
            r = pyredis.Client(host=REDIS_IP, port=REDIS_PORT)
            r.set(KEY, VAL).decode() == 'OK'
            r.get(KEY).decode() == VAL
            r.delete(KEY) == 1
        finally:
            if r is not None:
                r.close()
    benchmark(work)


# https://github.com/cf020031308/redisio
# @pytest.mark.benchmark(group='single_set_get_del')
# def test_single_set_get_del_fastredis(benchmark):
#     import redisio
#     def work():
#         with redisio.Client(REDIS_IP, REDIS_PORT) as r:
#             assert r.command('SET testkey testvalue') == 'OK'
#             assert r.command('GET testkey') == 'testvalue'
#             assert r.command('DEL testkey') == 1
#     benchmark(work)


############################################################


@pytest.mark.benchmark(group='set_del')
def test_set_del_fastredis(benchmark, keys):
    import fastredis as fr
    def work():
        with fr.SyncConnection(REDIS_IP, REDIS_PORT) as r:
            for key in keys:
                assert r.command(f'SET {key} {key}') == 'OK'
            for key in keys:
                assert r.command(f'DEL {key}') == 1
    benchmark(work)


@pytest.mark.benchmark(group='set_del')
def test_set_del_redis(benchmark, keys):
    import redis
    def work():
        with redis.Redis(REDIS_IP, REDIS_PORT) as r:
            for key in keys:
                assert r.set(key, key)
            for key in keys:
                assert r.delete(key) == 1
    benchmark(work)


@pytest.mark.benchmark(group='set_del')
def test_set_del_pyredis(benchmark, keys):
    import pyredis
    def work():
        r = None
        try:
            r = pyredis.Client(host=REDIS_IP, port=REDIS_PORT)
            for key in keys:
                assert r.set(key, key).decode() == 'OK'
            for key in keys:
                assert r.delete(key) == 1
        finally:
            if r is not None:
                r.close()
    benchmark(work)

############################################################

@pytest.mark.benchmark(group='set_get_del')
def test_set_get_del_fastredis(benchmark, keys):
    import fastredis as fr
    def work():
        with fr.SyncConnection(REDIS_IP, REDIS_PORT) as r:
            for key in keys:
                assert r.command(f'SET {key} {key}') == 'OK'
            for key in keys:
                assert r.command(f'GET {key}') == key
            for key in keys:
                assert r.command(f'DEL {key}') == 1
    benchmark(work)


@pytest.mark.benchmark(group='set_get_del')
def test_set_get_del_redis(benchmark, keys):
    import redis
    def work():
        with redis.Redis(REDIS_IP, REDIS_PORT, decode_responses=True) as r:
            for key in keys:
                assert r.set(key, key)
            for key in keys:
                assert r.get(key) == key
            for key in keys:
                assert r.delete(key) == 1
    benchmark(work)


@pytest.mark.benchmark(group='set_get_del')
def test_set_get_del_pyredis(benchmark, keys):
    import pyredis
    def work():
        r = None
        try:
            r = pyredis.Client(host=REDIS_IP, port=REDIS_PORT)
            for key in keys:
                assert r.set(key, key).decode() == 'OK'
            for key in keys:
                assert r.get(key).decode() == key
            for key in keys:
                assert r.delete(key) == 1
        finally:
            if r is not None:
                r.close()
    benchmark(work)


############################################################
############################################################
# Byte Benchmarks
############################################################
############################################################


@pytest.mark.benchmark(group='b_single_set_get_del')
def test_b_single_set_get_del_fastredis(benchmark):
    import fastredis as fr
    def work():
        with fr.SyncConnection(REDIS_IP_B, REDIS_PORT, encoding=None) as r:
            assert r.command(b'SET testkey testvalue') == b'OK'
            assert r.command(b'GET testkey') == b'testvalue'
            assert r.command(b'DEL testkey') == 1
    benchmark(work)


@pytest.mark.benchmark(group='b_single_set_get_del')
def test_b_single_set_get_del_redis(benchmark):
    import redis
    def work():
        with redis.Redis(REDIS_IP_B, REDIS_PORT) as r:
            assert r.set(KEY_B, VAL_B)
            assert r.get(KEY_B) == VAL_B
            assert r.delete(KEY_B) == 1
    benchmark(work)


@pytest.mark.benchmark(group='b_single_set_get_del')
def test_b_single_set_get_del_pyredis(benchmark):
    import pyredis
    def work():
        r = None
        try:
            r = pyredis.Client(host=REDIS_IP_B, port=REDIS_PORT)
            r.set(KEY_B, VAL_B) == b'OK'
            r.get(KEY_B) == VAL_B
            r.delete(KEY_B) == 1
        finally:
            if r is not None:
                r.close()
    benchmark(work)

############################################################

@pytest.mark.benchmark(group='b_set_del')
def test_b_set_del_fastredis(benchmark, keys):
    import fastredis as fr
    def work():
        with fr.SyncConnection(REDIS_IP_B, REDIS_PORT, encoding=None) as r:
            for key in keys:
                assert r.command(f'SET {key} {key}'.encode()) == b'OK'
            for key in keys:
                assert r.command(f'DEL {key}'.encode()) == 1
    benchmark(work)


@pytest.mark.benchmark(group='b_set_del')
def test_b_set_del_redis(benchmark, keys):
    import redis
    def work():
        with redis.Redis(REDIS_IP_B, REDIS_PORT) as r:
            for key in keys:
                key = key.encode()
                assert r.set(key, key)
            for key in keys:
                assert r.delete(key.encode()) == 1
    benchmark(work)


@pytest.mark.benchmark(group='b_set_del')
def test_b_set_del_pyredis(benchmark, keys):
    import pyredis
    def work():
        r = None
        try:
            r = pyredis.Client(host=REDIS_IP_B, port=REDIS_PORT)
            for key in keys:
                assert r.set(key, key) == b'OK'
            for key in keys:
                assert r.delete(key) == 1
        finally:
            if r is not None:
                r.close()
    benchmark(work)

############################################################

@pytest.mark.benchmark(group='b_set_get_del')
def test_b_set_get_del_fastredis(benchmark, keys):
    import fastredis as fr
    def work():
        with fr.SyncConnection(REDIS_IP_B, REDIS_PORT, encoding=None) as r:
            for key in keys:
                assert r.command(f'SET {key} {key}'.encode()) == b'OK'
            for key in keys:
                assert r.command(f'GET {key}'.encode()) == key.encode()
            for key in keys:
                assert r.command(f'DEL {key}'.encode()) == 1
    benchmark(work)


@pytest.mark.benchmark(group='b_set_get_del')
def test_b_set_get_del_redis(benchmark, keys):
    import redis
    def work():
        with redis.Redis(REDIS_IP_B, REDIS_PORT) as r:
            for key in keys:
                key = key.encode()
                assert r.set(key, key)
            for key in keys:
                key = key.encode()
                assert r.get(key) == key
            for key in keys:
                assert r.delete(key.encode()) == 1
    benchmark(work)


@pytest.mark.benchmark(group='b_set_get_del')
def test_b_set_get_del_pyredis(benchmark, keys):
    import pyredis
    def work():
        r = None
        try:
            r = pyredis.Client(host=REDIS_IP_B, port=REDIS_PORT)
            for key in keys:
                key = key.encode()
                assert r.set(key, key) == b'OK'
            for key in keys:
                key = key.encode()
                assert r.get(key) == key
            for key in keys:
                assert r.delete(key.encode()) == 1
        finally:
            if r is not None:
                r.close()
    benchmark(work)
