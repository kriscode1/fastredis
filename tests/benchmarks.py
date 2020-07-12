import asyncio
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
    KEY_COUNT = 10_000
    yield [f'testkey{i}' for i in range(KEY_COUNT)]


############################################################
############################################################
# Str Benchmarks
############################################################
############################################################


@pytest.mark.benchmark(group='connect_disconnect')
def test_connect_disconnect_fastredis(benchmark):
    import fastredis as fr
    def work():
        with fr.SyncConnection(REDIS_IP, REDIS_PORT) as r:
            pass
    benchmark(work)


@pytest.mark.benchmark(group='connect_disconnect')
def test_connect_disconnect_redis(benchmark):
    import redis
    def work():
        with redis.Redis(REDIS_IP, REDIS_PORT, decode_responses=True) as r:
            pass
    benchmark(work)


@pytest.mark.benchmark(group='connect_disconnect')
def test_connect_disconnect_pyredis(benchmark):
    import pyredis
    def work():
        r = None
        try:
            r = pyredis.Client(host=REDIS_IP, port=REDIS_PORT)
        finally:
            if r is not None:
                r.close()
    benchmark(work)


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


@pytest.mark.benchmark(group='b_connect_disconnect')
def test_b_connect_disconnect_fastredis(benchmark):
    import fastredis as fr
    def work():
        with fr.SyncConnection(REDIS_IP, REDIS_PORT) as r:
            pass
    benchmark(work)


@pytest.mark.benchmark(group='b_connect_disconnect')
def test_b_connect_disconnect_redis(benchmark):
    import redis
    def work():
        with redis.Redis(REDIS_IP, REDIS_PORT, decode_responses=True) as r:
            pass
    benchmark(work)


@pytest.mark.benchmark(group='b_connect_disconnect')
def test_b_connect_disconnect_pyredis(benchmark):
    import pyredis
    def work():
        r = None
        try:
            r = pyredis.Client(host=REDIS_IP, port=REDIS_PORT)
        finally:
            if r is not None:
                r.close()
    benchmark(work)


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


############################################################
############################################################
# Async Str Benchmarks
############################################################
############################################################


@pytest.fixture(scope='function', autouse=False)
def loop():
    loop = asyncio.get_event_loop()
    if loop.is_closed():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    yield loop
    loop.close()


############################################################


@pytest.mark.benchmark(group='async_connect_disconnect')
def test_a_connect_disconnect_fastredis(benchmark, loop):
    import fastredis as fr
    def work():
        async def async_work():
            async with fr.AsyncConnection(REDIS_IP, REDIS_PORT) as r:
                pass

        loop.run_until_complete(async_work())
    benchmark(work)


@pytest.mark.benchmark(group='async_connect_disconnect')
def test_a_connect_disconnect_aioredis(benchmark, loop):
    import aioredis
    def work():
        async def async_work():
            try:
                r = await aioredis.create_redis((REDIS_IP, REDIS_PORT))
            finally:
                r.close()
                await r.wait_closed()

        loop.run_until_complete(async_work())
    benchmark(work)


############################################################


@pytest.mark.benchmark(group='async_single_set_get_del')
def test_a_single_set_get_del_fastredis(benchmark, loop):
    import fastredis as fr
    def work():
        async def async_work():
            async with fr.AsyncConnection(REDIS_IP, REDIS_PORT) as r:
                assert await r.command('SET testkey testvalue') == 'OK'
                assert await r.command('GET testkey') == 'testvalue'
                assert await r.command('DEL testkey') == 1

        loop.run_until_complete(async_work())
    benchmark(work)


@pytest.mark.benchmark(group='async_single_set_get_del')
def test_a_single_set_get_del_aioredis(benchmark, loop):
    import aioredis
    def work():
        async def async_work():
            try:
                r = await aioredis.create_redis(
                    (REDIS_IP, REDIS_PORT),
                    encoding='utf-8'
                )
                assert await r.set('testkey', 'testvalue')
                assert await r.get('testkey') == 'testvalue'
                assert await r.delete('testkey') == 1
            finally:
                r.close()
                await r.wait_closed()

        loop.run_until_complete(async_work())
    benchmark(work)


# ############################################################


@pytest.mark.benchmark(group='async_set_del')
def test_a_set_del_fastredis(benchmark, loop, keys):
    import fastredis as fr
    def work():
        async def async_work():
            async with fr.AsyncConnection(REDIS_IP, REDIS_PORT) as r:
                for key in keys:
                    assert await r.command(f'SET {key} {key}') == 'OK'
                for key in keys:
                    assert await r.command(f'DEL {key}') == 1

        loop.run_until_complete(async_work())
    benchmark(work)


@pytest.mark.benchmark(group='async_set_del')
def test_a_set_del_aioredis(benchmark, loop, keys):
    import aioredis
    def work():
        async def async_work():
            try:
                r = await aioredis.create_redis(
                    (REDIS_IP, REDIS_PORT),
                    encoding='utf-8'
                )
                for key in keys:
                    assert await r.set(key, key)
                for key in keys:
                    assert await r.delete(key) == 1
            finally:
                r.close()
                await r.wait_closed()

        loop.run_until_complete(async_work())
    benchmark(work)


# ############################################################


@pytest.mark.benchmark(group='async_set_get_del')
def test_a_set_get_del_fastredis(benchmark, loop, keys):
    import fastredis as fr
    def work():
        async def async_work():
            async with fr.AsyncConnection(REDIS_IP, REDIS_PORT) as r:
                for key in keys:
                    assert await r.command(f'SET {key} {key}') == 'OK'
                for key in keys:
                    assert await r.command(f'GET {key}') == key
                for key in keys:
                    assert await r.command(f'DEL {key}') == 1

        loop.run_until_complete(async_work())
    benchmark(work)


@pytest.mark.benchmark(group='async_set_get_del')
def test_a_set_get_del_aioredis(benchmark, loop, keys):
    import aioredis
    def work():
        async def async_work():
            try:
                r = await aioredis.create_redis(
                    (REDIS_IP, REDIS_PORT),
                    encoding='utf-8'
                )
                for key in keys:
                    assert await r.set(key, key)
                for key in keys:
                    assert await r.get(key) == key
                for key in keys:
                    assert await r.delete(key) == 1
            finally:
                r.close()
                await r.wait_closed()

        loop.run_until_complete(async_work())
    benchmark(work)
