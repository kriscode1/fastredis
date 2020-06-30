import pytest


REDIS_IP = '127.0.0.1'
REDIS_PORT = 6379


@pytest.fixture(scope='function', autouse=False)
def keys():
    KEY_COUNT = 10000
    yield [f'testkey{i}' for i in range(KEY_COUNT)]

@pytest.mark.benchmark(group='single_set_get_del')
def test_fastredis_single_set_get_del(benchmark, keys):
    import fastredis as fr
    def work():
        c = fr.redis_connect(REDIS_IP, REDIS_PORT)
        fr.redis_command(c, 'SET testkey testvalue')
        fr.redis_command(c, 'GET testkey')
        fr.redis_command(c, 'DEL testkey')
        fr.redis_free(c)
    benchmark(work)


@pytest.mark.benchmark(group='single_set_get_del')
def test_redis_single_set_get_del(benchmark, keys):
    import redis
    def work():
        r = redis.Redis(host=REDIS_IP, port=REDIS_PORT)
        r.set('testkey', 'testvalue')
        r.get('testkey')
        r.delete('testkey')
    benchmark(work)


@pytest.mark.benchmark(group='set_del')
def test_fastredis_set_del(benchmark, keys):
    import fastredis as fr
    def work():
        c = fr.redis_connect(REDIS_IP, REDIS_PORT)
        for key in keys:
            fr.redis_command(c, f'SET {key} {key}')
        for key in keys:
            fr.redis_command(c, f'DEL {key}')
        fr.redis_free(c)
    benchmark(work)


@pytest.mark.benchmark(group='set_del')
def test_redis_set_del(benchmark, keys):
    import redis
    def work():
        r = redis.Redis(host=REDIS_IP, port=REDIS_PORT)
        for key in keys:
            r.set(key, key)
        for key in keys:
            r.delete(key)
    benchmark(work)


@pytest.mark.benchmark(group='set_get_del')
def test_fastredis_set_get_del(benchmark, keys):
    import fastredis as fr
    def work():
        c = fr.redis_connect(REDIS_IP, REDIS_PORT)
        for key in keys:
            fr.redis_command(c, f'SET {key} {key}')
        for key in keys:
            fr.redis_command(c, f'GET {key}')
        for key in keys:
            fr.redis_command(c, f'DEL {key}')
        fr.redis_free(c)
    benchmark(work)


@pytest.mark.benchmark(group='set_get_del')
def test_redis_set_get_del(benchmark, keys):
    import redis
    def work():
        r = redis.Redis(host=REDIS_IP, port=REDIS_PORT)
        for key in keys:
            r.set(key, key)
        for key in keys:
            r.get(key)
        for key in keys:
            r.delete(key)
    benchmark(work)

@pytest.mark.benchmark(group='fast')
def test_fast(benchmark):
    def work():
        x = 10
    benchmark(work)
